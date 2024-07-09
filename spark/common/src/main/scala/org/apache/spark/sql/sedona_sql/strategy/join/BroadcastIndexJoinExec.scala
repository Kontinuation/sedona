/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.core.spatialOperator.{SpatialPredicate, SpatialPredicateEvaluators}
import org.apache.sedona.sql.utils.{GeometrySerializer, RasterSerializer}

import scala.collection.JavaConverters._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, GenericInternalRow, JoinedRow, Predicate, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import org.locationtech.jts.index.SpatialIndex

import java.util

case class BroadcastIndexJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    streamShape: Expression,
    indexBuildSide: JoinSide,
    windowJoinSide: JoinSide,
    joinType: JoinType,
    spatialPredicate: SpatialPredicate,
    extraCondition: Option[Expression] = None,
    distance: Option[Expression] = None)
    extends SedonaBinaryExecNode
    with TraitJoinQueryBase
    with Logging {

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x: Any =>
        throw new IllegalArgumentException(
          s"BroadcastIndexJoinExec should not take $x as the JoinType")
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private val (streamed, broadcast) = indexBuildSide match {
    case LeftSide => (right, left.asInstanceOf[SpatialIndexExec])
    case RightSide => (left, right.asInstanceOf[SpatialIndexExec])
  }

  // Using lazy val to avoid serialization
  @transient private lazy val boundCondition: (InternalRow => Boolean) = extraCondition match {
    case Some(condition) =>
      Predicate.create(condition, streamed.output ++ broadcast.output).eval _ // SPARK3 anchor
    //      newPredicate(condition, broadcast.output ++ streamed.output).eval _ // SPARK2 anchor
    case None =>
      (r: InternalRow) => true
  }

  protected def createResultProjection(): InternalRow => InternalRow = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output,
        (streamed.output ++ broadcast.output).map(_.withNullability(true)))
  }

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  private val (windowExpression, objectExpression) = if (indexBuildSide == windowJoinSide) {
    (broadcast.shape, streamShape)
  } else {
    (streamShape, broadcast.shape)
  }

  private val isRasterPredicate = windowExpression.dataType
    .isInstanceOf[RasterUDT] || objectExpression.dataType.isInstanceOf[RasterUDT]

  private val spatialExpression = (distance, spatialPredicate, isRasterPredicate) match {
    case (Some(r), SpatialPredicate.INTERSECTS, false) =>
      s"ST_Distance($windowExpression, $objectExpression) <= $r"
    case (Some(r), _, false) => s"ST_Distance($windowExpression, $objectExpression) < $r"
    case (None, _, false) => s"ST_$spatialPredicate($windowExpression, $objectExpression)"
    case (None, _, true) => s"RS_$spatialPredicate($windowExpression, $objectExpression)"
  }

  override def simpleString(maxFields: Int): String =
    super.simpleString(maxFields) + s" $spatialExpression" // SPARK3 anchor
//  override def simpleString: String = super.simpleString + s" $spatialExpression" // SPARK2 anchor

  // Make sure that geometries from broadcast (indexed) side are always on the left of the predicate
  private val actualPredicate =
    if (indexBuildSide == windowJoinSide) spatialPredicate
    else SpatialPredicate.inverse(spatialPredicate)

  private def innerJoin(
      streamIter: Iterator[(Geometry, UnsafeRow)],
      broadcastIndex: Broadcast[SpatialIndex]): Iterator[InternalRow] = {
    val joinedRow = new JoinedRow
    val index = broadcastIndex.value
    val refiner = BroadcastIndexJoinExec.createJoinCandidateRefiner(actualPredicate)
    streamIter.flatMap { case (geom, row) =>
      joinedRow.withLeft(row)
      val candidates =
        index.query(geom.getEnvelopeInternal).asInstanceOf[java.util.List[Geometry]]
      refiner
        .refine(candidates, geom)
        .map(result => joinedRow.withRight(result.getUserData.asInstanceOf[UnsafeRow]))
        .filter(boundCondition)
    }
  }

  private def semiJoin(
      streamIter: Iterator[(Geometry, UnsafeRow)],
      broadcastIndex: Broadcast[SpatialIndex]): Iterator[InternalRow] = {
    val joinedRow = new JoinedRow
    val index = broadcastIndex.value
    val refiner = BroadcastIndexJoinExec.createJoinCandidateRefiner(actualPredicate)
    streamIter.flatMap { case (geom, row) =>
      val left = row
      joinedRow.withLeft(left)
      val candidates =
        index.query(geom.getEnvelopeInternal).asInstanceOf[java.util.List[Geometry]]
      val anyMatches = refiner
        .refine(candidates, geom)
        .map(candidate => joinedRow.withRight(candidate.getUserData.asInstanceOf[UnsafeRow]))
        .exists(boundCondition)

      if (anyMatches) {
        Iterator.single(left)
      } else {
        Iterator.empty
      }
    }
  }

  private def antiJoin(
      streamIter: Iterator[(Geometry, UnsafeRow)],
      broadcastIndex: Broadcast[SpatialIndex]): Iterator[InternalRow] = {
    val joinedRow = new JoinedRow
    val index = broadcastIndex.value
    val refiner = BroadcastIndexJoinExec.createJoinCandidateRefiner(actualPredicate)
    streamIter.flatMap { case (geom, row) =>
      val left = row
      joinedRow.withLeft(row)
      val anyMatches =
        if (geom == null) false
        else {
          val candidates =
            index.query(geom.getEnvelopeInternal).asInstanceOf[java.util.List[Geometry]]
          refiner
            .refine(candidates, geom)
            .map(candidate => joinedRow.withRight(candidate.getUserData.asInstanceOf[UnsafeRow]))
            .exists(boundCondition)
        }

      if (anyMatches) {
        Iterator.empty
      } else {
        Iterator.single(left)
      }
    }
  }

  private def outerJoin(
      streamIter: Iterator[(Geometry, UnsafeRow)],
      broadcastIndex: Broadcast[SpatialIndex]): Iterator[InternalRow] = {
    val joinedRow = new JoinedRow
    val nullRow = new GenericInternalRow(broadcast.output.length)
    val index = broadcastIndex.value
    val refiner = BroadcastIndexJoinExec.createJoinCandidateRefiner(actualPredicate)

    streamIter.flatMap { case (geom, row) =>
      joinedRow.withLeft(row)
      val candidates =
        if (geom == null) Iterator.empty
        else {
          val candidates =
            index.query(geom.getEnvelopeInternal).asInstanceOf[java.util.List[Geometry]]
          refiner.refine(candidates, geom)
        }

      new RowIterator {
        private var found = false
        override def advanceNext(): Boolean = {
          while (candidates.hasNext) {
            val candidateRow = candidates.next().getUserData.asInstanceOf[UnsafeRow]
            if (boundCondition(joinedRow.withRight(candidateRow))) {
              found = true
              return true
            }
          }
          if (!found) {
            joinedRow.withRight(nullRow)
            found = true
            return true
          }
          false
        }
        override def getRow: InternalRow = joinedRow
      }.toScala
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val boundStreamShape = BindReferences.bindReference(streamShape, streamed.output)
    val streamResultsRaw = streamed.execute().asInstanceOf[RDD[UnsafeRow]]

    val broadcastIndex = broadcast.executeBroadcast[SpatialIndex]()

    val streamShapes = createStreamShapes(streamResultsRaw, boundStreamShape)

    streamShapes.mapPartitions { streamedIter =>
      val joinedIter = joinType match {
        case _: InnerLike =>
          innerJoin(streamedIter, broadcastIndex)
        case LeftSemi =>
          semiJoin(streamedIter, broadcastIndex)
        case LeftAnti =>
          antiJoin(streamedIter, broadcastIndex)
        case LeftOuter | RightOuter =>
          outerJoin(streamedIter, broadcastIndex)
        case x: Any =>
          throw new IllegalArgumentException(
            s"BroadcastIndexJoinExec should not take $x as the JoinType")
      }

      val resultProj = createResultProjection()
      joinedIter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  private def createStreamShapes(
      streamResultsRaw: RDD[UnsafeRow],
      boundStreamShape: Expression) = {
    distance match {
      case Some(distanceExpression) =>
        val boundDistanceRef = BindReferences.bindReference(distanceExpression, streamed.output)
        streamResultsRaw.map(row => {
          val geom = boundStreamShape.eval(row).asInstanceOf[Array[Byte]]
          if (geom == null) {
            (null, row)
          } else {
            val geometry = GeometrySerializer.deserialize(geom)
            val radius = boundDistanceRef.eval(row).asInstanceOf[Double]
            val envelope = geometry.getEnvelopeInternal
            envelope.expandBy(radius)
            (geometry.getFactory.toGeometry(envelope), row)
          }
        })
      case _ =>
        streamResultsRaw.map(row => {
          val serializedObject = boundStreamShape.eval(row).asInstanceOf[Array[Byte]]
          if (serializedObject == null) {
            (null, row)
          } else {
            val shape = if (isRasterPredicate) {
              if (boundStreamShape.dataType.isInstanceOf[RasterUDT]) {
                val raster = RasterSerializer.deserialize(serializedObject)
                try {
                  JoinedGeometryRaster.rasterToWGS84Envelope(raster)
                } finally {
                  raster.dispose(true)
                }
              } else {
                val geom = GeometrySerializer.deserialize(serializedObject)
                JoinedGeometryRaster.geometryToWGS84Envelope(geom)
              }
            } else {
              GeometrySerializer.deserialize(serializedObject)
            }
            (shape, row)
          }
        })
    }
  }

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}

object BroadcastIndexJoinExec {
  private def createJoinCandidateRefiner(predicate: SpatialPredicate): Refiner = {
    predicate match {
      case SpatialPredicate.INTERSECTS | SpatialPredicate.COVERED_BY | SpatialPredicate.WITHIN =>
        new PrepareStreamSideRefiner(predicate)

      case SpatialPredicate.CONTAINS | SpatialPredicate.COVERS =>
        new PrepareBuildSideRefiner(predicate)

      case _ => new PlainRefiner(predicate)
    }
  }

  trait Refiner {
    def refine(buildSide: java.util.List[Geometry], streamSide: Geometry): Iterator[Geometry]
  }

  private class PlainRefiner(predicate: SpatialPredicate) extends Refiner {
    private val evaluator = SpatialPredicateEvaluators.create(predicate)
    override def refine(
        buildSide: java.util.List[Geometry],
        streamSide: Geometry): Iterator[Geometry] = {
      buildSide.iterator.asScala.filter(evaluator.eval(_, streamSide))
    }
  }

  private class PrepareBuildSideRefiner(predicate: SpatialPredicate) extends Refiner {
    private val evaluator = SpatialPredicateEvaluators.create(predicate)
    private val factory = new PreparedGeometryFactory()
    private val preparedGeometries = new util.IdentityHashMap[Geometry, PreparedGeometry]()
    override def refine(
        buildSide: java.util.List[Geometry],
        streamSide: Geometry): Iterator[Geometry] =
      buildSide.iterator.asScala.filter { candidate =>
        val preparedGeom = preparedGeometries.get(candidate) match {
          case null =>
            val preparedGeom = factory.create(candidate)
            preparedGeometries.put(candidate, preparedGeom)
            preparedGeom
          case g: PreparedGeometry => g
        }
        evaluator.eval(preparedGeom, streamSide)
      }
  }

  private class PrepareStreamSideRefiner(predicate: SpatialPredicate) extends Refiner {
    private val evaluator = SpatialPredicateEvaluators.create(predicate)
    private val factory = new PreparedGeometryFactory()
    override def refine(
        buildSide: java.util.List[Geometry],
        streamSide: Geometry): Iterator[Geometry] = {
      if (buildSide.isEmpty) Iterator.empty
      else {
        val preparedGeom = factory.create(streamSide)
        buildSide.iterator.asScala.filter(evaluator.eval(_, preparedGeom))
      }
    }
  }
}
