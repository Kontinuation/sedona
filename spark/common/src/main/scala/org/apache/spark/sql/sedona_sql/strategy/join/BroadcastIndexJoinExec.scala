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

import org.apache.sedona.core.enums.ExecutionMode
import org.apache.sedona.core.spatialOperator.{SpatialPredicate, SpatialPredicateEvaluators}
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.{GeometrySerializer, RasterSerializer}

import scala.collection.JavaConverters._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, GenericInternalRow, JoinedRow, Predicate, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import org.locationtech.jts.geom.MultiPoint
import org.locationtech.jts.geom.Point
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
    isGeography: Boolean,
    distance: Option[Expression] = None,
    unneededStreamAttributes: Seq[Attribute] = Seq.empty,
    numOutputRowsMetrics: Option[SQLMetric] = None,
    executionMode: Option[ExecutionMode] = None)
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
    "numOutputRows" -> numOutputRowsMetrics.getOrElse(
      SQLMetrics.createMetric(sparkContext, "number of output rows")))

  private val (streamed, broadcast) = indexBuildSide match {
    case LeftSide => (right, left.asInstanceOf[SpatialIndexExec])
    case RightSide => (left, right.asInstanceOf[SpatialIndexExec])
  }

  // Using lazy val to avoid serialization
  @transient private lazy val boundCondition: (InternalRow => Boolean) = extraCondition match {
    case Some(condition) =>
      Predicate.create(condition, streamed.output ++ broadcast.output).eval _
    case None =>
      (r: InternalRow) => true
  }

  protected def createResultProjection(): InternalRow => InternalRow = joinType match {
    case LeftExistence(_) =>
      // Some attributes may be projected away, which breaks the original nullability assumption
      UnsafeProjection.create(output, output.map(_.withNullability(true)))
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
    super.simpleString(maxFields) + s" $spatialExpression"

  // Make sure that geometries from broadcast (indexed) side are always on the left of the predicate
  private val actualPredicate =
    if (indexBuildSide == windowJoinSide) spatialPredicate
    else SpatialPredicate.inverse(spatialPredicate)

  private def innerJoin(
      streamIter: Iterator[(Geometry, UnsafeRow)],
      broadcastIndex: Broadcast[SpatialIndex],
      sedonaConf: SedonaConf): Iterator[InternalRow] = {
    val joinedRow = new JoinedRow
    val index = broadcastIndex.value
    val refiner = BroadcastIndexJoinExec.createJoinCandidateRefiner(
      executionMode,
      actualPredicate,
      sedonaConf)
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
      broadcastIndex: Broadcast[SpatialIndex],
      sedonaConf: SedonaConf): Iterator[InternalRow] = {
    val joinedRow = new JoinedRow
    val index = broadcastIndex.value
    val refiner = BroadcastIndexJoinExec.createJoinCandidateRefiner(
      executionMode,
      actualPredicate,
      sedonaConf)
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
      broadcastIndex: Broadcast[SpatialIndex],
      sedonaConf: SedonaConf): Iterator[InternalRow] = {
    val joinedRow = new JoinedRow
    val index = broadcastIndex.value
    val refiner = BroadcastIndexJoinExec.createJoinCandidateRefiner(
      executionMode,
      actualPredicate,
      sedonaConf)
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
      broadcastIndex: Broadcast[SpatialIndex],
      sedonaConf: SedonaConf): Iterator[InternalRow] = {
    val joinedRow = new JoinedRow
    val nullRow = new GenericInternalRow(broadcast.output.length)
    val index = broadcastIndex.value
    val refiner = BroadcastIndexJoinExec.createJoinCandidateRefiner(
      executionMode,
      actualPredicate,
      sedonaConf)

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
    val sedonaConf = SedonaConf.fromActiveSession()
    val numOutputRows = longMetric("numOutputRows")
    val boundStreamShape = BindReferences.bindReference(streamShape, streamed.output)
    val streamResultsRaw = streamed.execute().asInstanceOf[RDD[UnsafeRow]]
    val broadcastIndex = broadcast.executeBroadcast[SpatialIndex]()
    val streamShapes = createStreamShapes(streamResultsRaw, boundStreamShape)

    streamShapes.mapPartitions { streamedIter =>
      val joinedIter = joinType match {
        case _: InnerLike =>
          innerJoin(streamedIter, broadcastIndex, sedonaConf)
        case LeftSemi =>
          semiJoin(streamedIter, broadcastIndex, sedonaConf)
        case LeftAnti =>
          antiJoin(streamedIter, broadcastIndex, sedonaConf)
        case LeftOuter | RightOuter =>
          outerJoin(streamedIter, broadcastIndex, sedonaConf)
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
        streamResultsRaw.mapPartitions { iter =>
          val projector = createUnsafeRowProjector(streamed, unneededStreamAttributes)
          iter.map { row =>
            val geom = boundStreamShape.eval(row).asInstanceOf[Array[Byte]]
            if (geom == null) {
              (null, projector(row))
            } else {
              val geometry = GeometrySerializer.deserialize(geom)
              val radius = boundDistanceRef.eval(row).asInstanceOf[Double]
              val envelope =
                JoinedGeometry.geometryToExpandedEnvelope(geometry, radius, isGeography)
              (envelope, projector(row))
            }
          }
        }
      case _ =>
        streamResultsRaw.mapPartitions { iter =>
          val projector = createUnsafeRowProjector(streamed, unneededStreamAttributes)
          iter.map { row =>
            val serializedObject = boundStreamShape.eval(row).asInstanceOf[Array[Byte]]
            if (serializedObject == null) {
              (null, projector(row))
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
              (shape, projector(row))
            }
          }
        }
    }
  }

  private def createUnsafeRowProjector(
      plan: SparkPlan,
      unneeded: Seq[Attribute]): UnsafeRow => UnsafeRow = {
    projection(plan, unneeded) match {
      case Some(attrs) =>
        val projection = GenerateUnsafeProjection.generate(attrs)
        (row: UnsafeRow) => projection(row)
      case None => (row: UnsafeRow) => row
    }
  }

  override def isGeographyDistanceJoin: Boolean = distance.isDefined && isGeography

  override def distanceExpression: Option[Expression] = distance

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}

object BroadcastIndexJoinExec {
  private def createJoinCandidateRefiner(
      executionMode: Option[ExecutionMode],
      predicate: SpatialPredicate,
      sedonaConf: SedonaConf): Refiner = {
    executionMode match {
      case Some(ExecutionMode.PREPARE_BUILD) => new PrepareBuildSideRefiner(predicate)
      case Some(ExecutionMode.PREPARE_STREAM) => new PrepareBuildSideRefiner(predicate)
      case Some(ExecutionMode.PREPARE_NONE) => new PlainRefiner(predicate)
      case None =>
        predicate match {
          case SpatialPredicate.INTERSECTS =>
            val maxSamples = sedonaConf.getMaxSamplesForAdaptiveBroadcastJoinExecutionMode
            new AdaptiveIntersectRefiner(maxSamples)

          case SpatialPredicate.COVERED_BY | SpatialPredicate.WITHIN =>
            new PrepareStreamSideRefiner(predicate)

          case SpatialPredicate.CONTAINS | SpatialPredicate.COVERS =>
            new PrepareBuildSideRefiner(predicate)

          case _ => new PlainRefiner(predicate)
        }
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

  private class AdaptiveIntersectRefiner(maxSamples: Long) extends Refiner {
    private var currentRefiner: Refiner = new PrepareStreamSideRefiner(
      SpatialPredicate.INTERSECTS)
    private val factory = new PreparedGeometryFactory()
    private var seenStreamSide = 0L
    private var seenBuildSide = 0L
    private var buildSideTotalPoints = 0L
    private var streamSideTotalPoints = 0L
    private var buildSideOnlyHasPoints = true
    private var streamSideOnlyHasPoints = true
    override def refine(
        buildSide: java.util.List[Geometry],
        streamGeom: Geometry): Iterator[Geometry] = {
      seenStreamSide += 1
      if (seenStreamSide > maxSamples) {
        currentRefiner.refine(buildSide, streamGeom)
      } else {
        // Collect statistics to decide which refiner to use
        streamSideTotalPoints += streamGeom.getNumPoints
        if (!streamGeom.isInstanceOf[Point] && !streamGeom.isInstanceOf[MultiPoint]) {
          streamSideOnlyHasPoints = false
        }
        val result =
          if (buildSide.isEmpty) Seq.empty
          else {
            seenBuildSide += buildSide.size()
            val preparedGeom = factory.create(streamGeom)
            buildSide.asScala.filter { buildGeom =>
              buildSideTotalPoints += buildGeom.getNumPoints
              if (!buildGeom.isInstanceOf[Point] && !buildGeom.isInstanceOf[MultiPoint]) {
                buildSideOnlyHasPoints = false
              }
              preparedGeom.intersects(buildGeom)
            }
          }
        if (seenStreamSide == maxSamples) {
          // Collected enough samples, determine which refiner to use
          val buildSideMeanPoints =
            if (seenBuildSide > 0) buildSideTotalPoints.toDouble / seenBuildSide else 1
          val streamSideMeanPoints =
            if (seenStreamSide > 0) streamSideTotalPoints.toDouble / seenStreamSide else 1
          if (buildSideOnlyHasPoints) {
            currentRefiner = new PrepareStreamSideRefiner(SpatialPredicate.INTERSECTS)
          } else if (streamSideOnlyHasPoints) {
            currentRefiner = new PrepareBuildSideRefiner(SpatialPredicate.INTERSECTS)
          } else if (buildSideMeanPoints > streamSideMeanPoints) {
            currentRefiner = new PrepareBuildSideRefiner(SpatialPredicate.INTERSECTS)
          } else {
            currentRefiner = new PrepareStreamSideRefiner(SpatialPredicate.INTERSECTS)
          }
        }
        result.iterator
      }
    }
  }
}
