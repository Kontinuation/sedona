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

import org.apache.sedona.common.subDivide.SubdivideOptions
import org.apache.sedona.core.enums.{IndexType, JoinSubdivideMode}
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialOperator.Subdivide.{isSubdivideAccurate, SubdividedPart, SubdivideRDDOptions}
import org.apache.sedona.core.spatialOperator.{JoinQuery, SpatialPredicate, Subdivide}
import org.apache.sedona.core.spatialPartitioning.BroadcastedSpatialPartitioner
import org.apache.sedona.core.spatialPartitioning.OuterJoinSpatialPartitioner
import org.apache.sedona.core.spatialPartitioning.OuterJoinSpatialPartitioner.OuterJoinUserData
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioningMetrics
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, Literal, Predicate, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.sedona_sql.strategy.join.TraitJoinQueryBase.projectUnsafeRow
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.sedona_sql.utils.UnsafeRowRDDSorter.sortUnsafeRowRDD
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.sedona_sql.utils.JoinedUnsafeRowRDDSorter
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import org.apache.spark.api.java.function.{Function0 => JavaFunction0, Function2 => JavaFunction2}
import org.apache.spark.sql.sedona_sql.strategy.join.TraitAdvancedJoinQueryExec.getNullUnsafeRow
import org.apache.spark.sql.sedona_sql.strategy.join.TraitAdvancedJoinQueryExec.getUnsafeRowFromUserData
import org.apache.spark.sql.sedona_sql.strategy.join.TraitAdvancedJoinQueryExec.joinTypeOf

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.Try

/**
 * TraitJoinQueryExec using advanced self-driving spatial join. This implementation of spatial
 * join does not honor most of the settings in SedonaConf, because it is designed to be
 * self-driving and tune these configurations automatically.
 */
trait TraitAdvancedJoinQueryExec extends TraitJoinQueryExec {
  self: SparkPlan =>

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case x: Any =>
        throw new IllegalArgumentException(
          s"BroadcastIndexJoinExec should not take $x as the JoinType")
    }
  }

  /**
   * These are the attributes that will be discarded by the outer ProjectExec, so that it is
   * possible to discard their values when performing the spatial join. These attributes are the
   * joined geometries for most of the time, and we can discard the original geometries when
   * subdividing is enabled to reduce the amount of shuffle-write/read and memory usage when
   * original geometries are actually not needed.
   *
   * Notice that when we enable geometry subdividing, we may need to keep the original geometries
   * even though they are not needed in the final output or extra condition.
   */
  val unneededLeftAttributes: Seq[Attribute] = Nil
  val unneededRightAttributes: Seq[Attribute] = Nil

  private lazy val sedonaConf = SedonaConf.fromActiveSession

  override lazy val metrics: Map[String, SQLMetric] = if (sedonaConf.useAdvancedSpatialJoin) {
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of output rows (without dedup)"),
      "buildCount" -> SQLMetrics.createMetric(sparkContext, "number of build side"),
      "streamCount" -> SQLMetrics.createMetric(sparkContext, "number of stream side"),
      "candidateCount" -> SQLMetrics.createMetric(sparkContext, "number of candidates"),
      "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build spatial index"),
      "buildLeftTasks" -> SQLMetrics.createMetric(
        sparkContext,
        "number of tasks building the left side"),
      "buildRightTasks" -> SQLMetrics.createMetric(
        sparkContext,
        "number of tasks building the right side"),
      "prepareBuildTasks" -> SQLMetrics.createMetric(
        sparkContext,
        "number of tasks preparing the build side"),
      "prepareStreamTasks" -> SQLMetrics.createMetric(
        sparkContext,
        "number of tasks preparing the stream side"),
      "subdivideLeft" -> SQLMetrics.createMetric(sparkContext, "subdivide left side"),
      "subdivideRight" -> SQLMetrics.createMetric(sparkContext, "subdivide right side"),
      "localSubdivideLeft" -> SQLMetrics.createMetric(
        sparkContext,
        "subdivide left side in local join"),
      "localSubdivideRight" -> SQLMetrics.createMetric(
        sparkContext,
        "subdivide right side in local join"))
  } else {
    Map.empty
  }

  def isDistanceJoin: Boolean = false

  override protected def doExecute(): RDD[InternalRow] = {
    if (sedonaConf.useAdvancedSpatialJoin) {
      doAdvancedExecute(sedonaConf)
    } else {
      super.doExecute()
    }
  }

  private def doAdvancedExecute(sedonaConf: SedonaConf): RDD[InternalRow] = {
    val boundLeftShape = BindReferences.bindReference(leftShape, left.output)
    val boundRightShape = BindReferences.bindReference(rightShape, right.output)
    val leftResultsRaw = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResultsRaw = right.execute().asInstanceOf[RDD[UnsafeRow]]
    val sortedLeftResultsRaw = sortUnsafeRowRDD(leftResultsRaw, left.schema)
    val sortedRightResultsRaw = sortUnsafeRowRDD(rightResultsRaw, right.schema)

    // Convert the input data to SpatialRDD
    var subdivideLeftRDDOptions: Option[SubdivideRDDOptions] = None
    var subdivideRightRDDOptions: Option[SubdivideRDDOptions] = None
    var localSubdivideLeftOptions: Option[SubdivideOptions] = None
    var localSubdivideRightOptions: Option[SubdivideOptions] = None
    var isLeftGeometryAccurate = true
    var isRightGeometryAccurate = true
    var (leftShapes, rightShapes) =
      toSpatialRddPair(leftResultsRaw, boundLeftShape, rightResultsRaw, boundRightShape)
    val (sortedLeftShapes, sortedRightShapes) = toSpatialRddPair(
      sortedLeftResultsRaw,
      boundLeftShape,
      sortedRightResultsRaw,
      boundRightShape)

    // Subdivide the RDD when subdivide mode is ALWAYS.
    if (sedonaConf.getSpatialJoinSubdivideLeft == JoinSubdivideMode.ALWAYS) {
      log.info("Subdividing left RDD, as the mode is set to ALWAYS")
      subdivideLeftRDDOptions = Some(sedonaConf.getLeftSubdivideRDDOptions)
      leftShapes = Subdivide.subdivideSpatialRDD(sortedLeftShapes, subdivideLeftRDDOptions.get)
    }
    if (sedonaConf.getSpatialJoinSubdivideRight == JoinSubdivideMode.ALWAYS) {
      log.info("Subdividing right RDD, as the mode is set to ALWAYS")
      subdivideRightRDDOptions = Some(sedonaConf.getRightSubdivideRDDOptions)
      rightShapes = Subdivide.subdivideSpatialRDD(sortedRightShapes, subdivideRightRDDOptions.get)
    }
    if (sedonaConf.getLocalJoinSubdivideLeft == JoinSubdivideMode.ALWAYS) {
      log.info("Subdividing left geometries in local join, as the mode is set to ALWAYS")
      localSubdivideLeftOptions = Some(sedonaConf.getLeftLocalJoinSubdivideOptions)
    }
    if (sedonaConf.getLocalJoinSubdivideRight == JoinSubdivideMode.ALWAYS) {
      log.info("Subdividing right geometries in local join, as the mode is set to ALWAYS")
      localSubdivideRightOptions = Some(sedonaConf.getRightLocalJoinSubdivideOptions)
    }

    // Analyze both sides for doing spatial partitioning, and probably subdivide the datasets
    analyzeLeftAndRight(leftShapes, rightShapes)
    sortedLeftShapes.setStatistics(leftShapes.getStatistics)
    sortedRightShapes.setStatistics(rightShapes.getStatistics)
    val spatialPartitionResult = leftShapes.createSpatialPartitioner(
      sedonaConf.getJoinGridType,
      rightShapes,
      sedonaConf.getFallbackPartitionNum,
      sedonaConf)
    if (spatialPartitionResult == null) {
      return computeJoinResultForDisjointInputs(
        leftResultsRaw,
        rightResultsRaw,
        left.output,
        right.output,
        joinType)
    }
    var spatialPartitioner = spatialPartitionResult.getLeft
    var spatialPartitioningMetrics = spatialPartitionResult.getRight

    // Try subdivide the spatial RDD if auto subdivide is enabled
    if (sedonaConf.getSpatialJoinSubdivideLeft == JoinSubdivideMode.AUTO ||
      sedonaConf.getSpatialJoinSubdivideRight == JoinSubdivideMode.AUTO) {
      val canDiscardLeftGeometry =
        unneededLeftAttributes.nonEmpty && spatialPredicate == SpatialPredicate.INTERSECTS
      val canDiscardRightGeometry =
        unneededRightAttributes.nonEmpty && spatialPredicate == SpatialPredicate.INTERSECTS
      val options = Subdivide.determineSubdivideOptions(
        leftShapes,
        rightShapes,
        spatialPartitioner,
        canDiscardLeftGeometry,
        canDiscardRightGeometry,
        sedonaConf)
      if (options.getLeft.globalOptions != null && sedonaConf.getSpatialJoinSubdivideLeft == JoinSubdivideMode.AUTO) {
        log.info("Subdividing left RDD as automatically determined")
        leftShapes =
          Subdivide.subdivideSpatialRDD(sortedLeftShapes, options.getLeft.globalOptions)
        subdivideLeftRDDOptions = Some(options.getLeft.globalOptions)
      }
      if (options.getRight.globalOptions != null && sedonaConf.getSpatialJoinSubdivideRight == JoinSubdivideMode.AUTO) {
        log.info("Subdividing right RDD as automatically determined")
        rightShapes =
          Subdivide.subdivideSpatialRDD(sortedRightShapes, options.getRight.globalOptions)
        subdivideRightRDDOptions = Some(options.getRight.globalOptions)
      }
      if (options.getLeft.localOptions != null && sedonaConf.getLocalJoinSubdivideLeft == JoinSubdivideMode.AUTO) {
        log.info("Subdividing left geometries in local join as automatically determined")
        localSubdivideLeftOptions = Some(options.getLeft.localOptions)
      }
      if (options.getRight.localOptions != null && sedonaConf.getLocalJoinSubdivideRight == JoinSubdivideMode.AUTO) {
        log.info("Subdividing right geometries in local join as automatically determined")
        localSubdivideRightOptions = Some(options.getRight.localOptions)
      }
    }

    // Update subdivide metrics
    subdivideLeftRDDOptions.foreach { _ => metrics("subdivideLeft").set(1) }
    subdivideRightRDDOptions.foreach { _ => metrics("subdivideRight").set(1) }
    localSubdivideLeftOptions.foreach { _ => metrics("localSubdivideLeft").set(1) }
    localSubdivideRightOptions.foreach { _ => metrics("localSubdivideRight").set(1) }
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

    // Re-analyze the subdivided datasets if necessary
    val needReAnalyze = leftShapes.getStatistics == null || rightShapes.getStatistics == null
    (leftShapes.getStatistics, rightShapes.getStatistics) match {
      case (null, null) => analyzeLeftAndRight(leftShapes, rightShapes, reAnalyze = true)
      case (null, _) =>
        withJobDescription("Re-analyzing left shapes") { leftShapes.advancedAnalyze() }
      case (_, null) =>
        withJobDescription("Re-analyzing right shapes") { rightShapes.advancedAnalyze() }
      case _ => // Do nothing
    }

    // Recompute the spatial partitioner using re-analyzed data, which is more balanced for subdivided
    // geometries
    if (needReAnalyze) {
      val spatialPartitionResult = leftShapes.createSpatialPartitioner(
        sedonaConf.getJoinGridType,
        rightShapes,
        sedonaConf.getFallbackPartitionNum,
        sedonaConf)
      spatialPartitioner = spatialPartitionResult.getLeft
      spatialPartitioningMetrics = spatialPartitionResult.getRight
      sortedLeftShapes.setStatistics(leftShapes.getStatistics)
      sortedRightShapes.setStatistics(rightShapes.getStatistics)
    }

    // Decide whether we can discard the geometry attributes from the user data to reduce the amount of
    // shuffle-write/read as well as memory usage.
    subdivideLeftRDDOptions.foreach { options =>
      isLeftGeometryAccurate = isSubdivideAccurate(leftShapes, options.options)
    }
    subdivideRightRDDOptions.foreach { options =>
      isRightGeometryAccurate = isSubdivideAccurate(rightShapes, options.options)
    }
    if (isLeftGeometryAccurate && isRightGeometryAccurate) {
      leftShapes = discardUnneededAttributes(
        leftShapes,
        subdivideLeftRDDOptions,
        left,
        unneededLeftAttributes,
        "left")
      rightShapes = discardUnneededAttributes(
        rightShapes,
        subdivideRightRDDOptions,
        right,
        unneededRightAttributes,
        "right")
    } else {
      if (Seq(subdivideLeftRDDOptions, subdivideRightRDDOptions).flatten.exists(_.keepUserData)) {
        log.warn(
          "No attributes removed from row data when keepRowData is set to true. " +
            "This may cause significant amount of shuffle-write/read and memory usage.")
      }
    }

    // If either side is subdivided, we need to transform another side to assign unique IDs to each geometry and
    // use SubdividedParts as user data. This is for de-duplicating the join result.
    (subdivideLeftRDDOptions, subdivideRightRDDOptions) match {
      case (None, None) => // Do nothing
      case (Some(_), Some(_)) => // Do nothing
      case (Some(_), None) =>
        rightShapes = Subdivide.noOpSubdivideSpatialRDD(sortedRightShapes)
      case (None, Some(_)) =>
        leftShapes = Subdivide.noOpSubdivideSpatialRDD(sortedLeftShapes)
    }

    // Spatial partitioning
    if (spatialPartitioner.numPartitions > 100) {
      // Reduce the size of serialized task closure by broadcasting the spatial partitioner
      spatialPartitioner = new BroadcastedSpatialPartitioner(
        sparkContext.broadcast(spatialPartitioner))
    }
    // Wrap the spatial partitioner to support out-of-bounds partitions when doing outer join. Please
    // note that we'll still perform an inner join when global subdivide is enabled, since subdivided
    // outer joins are handled specially by joining back with original datasets.
    if (joinType != Inner && subdivideLeftRDDOptions.isEmpty && subdivideRightRDDOptions.isEmpty) {
      val numOufOfBoundsPartitions = determineNumOutOfBoundsPartitions(
        joinType,
        spatialPartitioner,
        spatialPartitioningMetrics)
      val outerSpatialPartitioner =
        new OuterJoinSpatialPartitioner(spatialPartitioner, numOufOfBoundsPartitions, true)
      val otherSpatialPartitioner =
        new OuterJoinSpatialPartitioner(spatialPartitioner, numOufOfBoundsPartitions, false)
      joinType match {
        case LeftOuter =>
          leftShapes.spatialPartitioning(outerSpatialPartitioner, sedonaConf)
          rightShapes.spatialPartitioning(otherSpatialPartitioner, sedonaConf)
        case RightOuter =>
          leftShapes.spatialPartitioning(otherSpatialPartitioner, sedonaConf)
          rightShapes.spatialPartitioning(outerSpatialPartitioner, sedonaConf)
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported join type: $joinType")
      }
    } else {
      leftShapes.spatialPartitioning(spatialPartitioner, sedonaConf)
      rightShapes.spatialPartitioning(spatialPartitioner, sedonaConf)
    }

    // Perform spatial join
    if (sedonaConf.getSpatialPartitionerSavePath.nonEmpty) {
      saveSpatialPartitionerToFile(
        leftShapes.getPartitioner,
        sedonaConf.getSpatialPartitionerSavePath)
    }
    val resultRdd = (subdivideLeftRDDOptions, subdivideRightRDDOptions) match {
      case (None, None) =>
        // No subdivision applied, so we can run the join directly
        val joinedRdd = runSpatialJoin(
          sedonaConf,
          leftShapes,
          rightShapes,
          joinType,
          spatialPredicate,
          extraCondition,
          localSubdivideLeftOptions,
          localSubdivideRightOptions)
        joinType match {
          case Inner => innerJoinedRddToRowRdd(joinedRdd)
          case LeftOuter | RightOuter => outerJoinedRddToRowRdd(joinedRdd)
          case _ => throw new UnsupportedOperationException(s"Unsupported join type: $joinType")
        }
      case _ =>
        // When subdivide kicks in, the spatial predicate for running join must be INTERSECTS. If
        // the original predicate is anything other than INTERSECTS, we need to change it to INTERSECTS, and
        // add the original predicate to extraCondition to filter out the false positives.
        val newExtraCondition = spatialPredicate match {
          case SpatialPredicate.INTERSECTS => extraCondition
          case _ => Some(condition)
        }
        // If the subdivided geometries are not accurate, we need to refine the INTERSECTS join result using
        // the original geometries. This refinement could use prepared geometries so it will be faster than
        // simply leaving everything to evaluating the extra condition.
        val refineIntersectsUsingOriginalGeometries =
          !(isLeftGeometryAccurate && isRightGeometryAccurate) && !isDistanceJoin && !isRasterJoin(
            boundLeftShape,
            boundRightShape)
        // Join type is INNER when running the spatial run algorithm regardless of the actual join
        // type. We'll join back with the original datasets using outer join later in
        // joinedSubdividedRddToRowRdd if the actual join type is outer join.
        val joinedRdd = runSpatialJoin(
          sedonaConf,
          leftShapes,
          rightShapes,
          Inner,
          SpatialPredicate.INTERSECTS,
          None,
          localSubdivideLeftOptions,
          localSubdivideRightOptions)
        joinedSubdividedRddToRowRdd(
          joinType,
          joinedRdd,
          sortedLeftResultsRaw,
          sortedRightResultsRaw,
          subdivideLeftRDDOptions,
          subdivideRightRDDOptions,
          refineIntersectsUsingOriginalGeometries,
          boundLeftShape,
          boundRightShape,
          newExtraCondition)
    }

    // We've already built the joined RDD. Now it is safe to free up memory used by the statistics data,
    // especially sampled envelopes on both sides
    leftShapes.forgetStatistics()
    rightShapes.forgetStatistics()
    sortedLeftShapes.forgetStatistics()
    sortedRightShapes.forgetStatistics()
    resultRdd
  }

  private def computeJoinResultForDisjointInputs(
      leftResultsRaw: RDD[UnsafeRow],
      rightResultsRaw: RDD[UnsafeRow],
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute],
      joinType: JoinType): RDD[InternalRow] = {
    joinType match {
      case Inner => sparkContext.emptyRDD
      case LeftOuter =>
        leftResultsRaw.mapPartitions { iter =>
          val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
          val rightNullRow = getNullUnsafeRow(right.output)
          iter.map { row =>
            joiner.join(row, rightNullRow)
          }
        }
      case RightOuter =>
        rightResultsRaw.mapPartitions { iter =>
          val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
          val leftNullRow = getNullUnsafeRow(left.output)
          iter.map { row =>
            joiner.join(leftNullRow, row)
          }
        }
      case _ => throw new UnsupportedOperationException(s"Unsupported join type: $joinType")
    }
  }

  private def joinedSubdividedRddToRowRdd(
      joinType: JoinType,
      joinedRdd: RDD[(Geometry, Geometry)],
      sortedLeftResultsRaw: RDD[UnsafeRow],
      sortedRightResultsRaw: RDD[UnsafeRow],
      subdivideLeftRDDOptions: Option[SubdivideRDDOptions],
      subdivideRightRDDOptions: Option[SubdivideRDDOptions],
      refineIntersectsUsingOriginalGeometries: Boolean,
      boundLeftShape: Expression,
      boundRightShape: Expression,
      extraCondition: Option[Expression]): RDD[InternalRow] = {
    // Remove duplicated result caused by subdividing the geometries
    def mapSubdividedGeometry(geom: Geometry): (Long, UnsafeRow) = {
      val part = geom.getUserData.asInstanceOf[SubdividedPart]
      val row = part.userData.asInstanceOf[UnsafeRow]
      (part.id, row)
    }
    var joinedRowsWithIds = joinedRdd
      .map { case (left, right) =>
        val (leftId, leftRow) = mapSubdividedGeometry(left)
        val (rightId, rightRow) = mapSubdividedGeometry(right)
        ((leftId, rightId), (leftRow, rightRow))
      }
      .reduceByKey((rowsTuple, _) => rowsTuple)

    // TODO: If both the left side and the right side are subdivided without user data kept. we need to count how many
    //  geometries from the left and the right side are present in the join result (joinedRowsWithIds), because the
    //  order of the following equi-join matters. If we join with the smaller side first, we can reduce the amount of
    //  shuffle-write/read.

    // If either side does not take user data, we need to recover the original user data by joining with
    // the original RDD
    var lastJoinedSide: Option[JoinSide] = None
    subdivideLeftRDDOptions.foreach { options =>
      if (!options.keepUserData) {
        joinedRowsWithIds =
          recoverOriginalRowData(joinedRowsWithIds, sortedLeftResultsRaw, joinWithLeftSide = true)
        lastJoinedSide = Some(LeftSide)
      }
    }
    subdivideRightRDDOptions.foreach { options =>
      if (!options.keepUserData) {
        joinedRowsWithIds = recoverOriginalRowData(
          joinedRowsWithIds,
          sortedRightResultsRaw,
          joinWithLeftSide = false)
        lastJoinedSide = Some(RightSide)
      }
    }

    val joinedRowsWithKeys: RDD[(Long, UnsafeRow)] = joinedRowsWithIds.mapPartitions { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)

      // The UnsafeRow in joinedRowsWithKeys is not always the joined row. The actual row depends
      // on the join type. For inner join, the row is the joined row. For left/right outer join,
      // the row is the row on the non-outer side. We'll perform a outer-join with the outer side
      // later to create joined rows.
      val createJoinResult = joinType match {
        case LeftOuter =>
          (leftId: Long, _: Long, _: UnsafeRow, rightRow: UnsafeRow) => (leftId, rightRow)
        case RightOuter =>
          (_: Long, rightId: Long, leftRow: UnsafeRow, _: UnsafeRow) => (rightId, leftRow)
        case Inner =>
          (_: Long, _: Long, leftRow: UnsafeRow, rightRow: UnsafeRow) =>
            (-1L, joiner.join(leftRow, rightRow))
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported join type: $joinType")
      }

      // Similar to createJoinResult, but taking boundCondition into consideration.
      val createOrIgnoreJoinResult = extraCondition match {
        case Some(condition) =>
          val boundCondition = Predicate.create(condition, output)
          joinType match {
            case LeftOuter | RightOuter =>
              (leftId: Long, rightId: Long, leftRow: UnsafeRow, rightRow: UnsafeRow) => {
                val joinedRow = joiner.join(leftRow, rightRow)
                if (boundCondition.eval(joinedRow)) {
                  Some(createJoinResult(leftId, rightId, leftRow, rightRow))
                } else {
                  None
                }
              }
            case Inner =>
              (_: Long, _: Long, leftRow: UnsafeRow, rightRow: UnsafeRow) => {
                val joinedRow = joiner.join(leftRow, rightRow)
                if (boundCondition.eval(joinedRow)) Some((-1L, joinedRow)) else None
              }
            case _ =>
              throw new UnsupportedOperationException(s"Unsupported join type: $joinType")
          }
        case None =>
          (leftId: Long, rightId: Long, leftRow: UnsafeRow, rightRow: UnsafeRow) =>
            Some(createJoinResult(leftId, rightId, leftRow, rightRow))
      }

      val joinRows = {
        if (!refineIntersectsUsingOriginalGeometries) {
          (leftId: Long, rightId: Long, l: UnsafeRow, r: UnsafeRow) =>
            createOrIgnoreJoinResult(leftId, rightId, l, r)
        } else {
          // If either side is in-accurate, we have to re-evaluate INTERSECTS predicate using the original geometries.
          // This is good to have even we have extraCondition to evaluate, because this will be using prepared
          // geometries, so it will reduce the amount of false-positives for extraCondition very fast and improve the
          // overall speed than simply relying on evaluating extraCondition.
          lastJoinedSide match {
            case Some(LeftSide) =>
              // Prepare the left side
              var lastLeftId: Long = -1
              var lastPreparedLeft: PreparedGeometry = null
              val factory = new PreparedGeometryFactory()
              (leftId: Long, rightId: Long, l: UnsafeRow, r: UnsafeRow) => {
                if (leftId == -1 || rightId == -1) {
                  Some(createJoinResult(leftId, rightId, l, r))
                } else {
                  if (leftId != lastLeftId) {
                    lastPreparedLeft = factory.create(boundLeftShape.toGeometry(l))
                    lastLeftId = leftId
                  }
                  val rightGeom = boundRightShape.toGeometry(r)
                  if (lastPreparedLeft.intersects(rightGeom)) {
                    createOrIgnoreJoinResult(leftId, rightId, l, r)
                  } else {
                    None
                  }
                }
              }
            case Some(RightSide) =>
              // Prepare the right side
              var lastRightId: Long = -1
              var lastPreparedRight: PreparedGeometry = null
              val factory = new PreparedGeometryFactory()
              (leftId: Long, rightId: Long, l: UnsafeRow, r: UnsafeRow) => {
                if (leftId == -1 || rightId == -1) {
                  Some(createJoinResult(leftId, rightId, l, r))
                } else {
                  if (rightId != lastRightId) {
                    lastPreparedRight = factory.create(boundRightShape.toGeometry(r))
                    lastRightId = rightId
                  }
                  val leftGeom = boundLeftShape.toGeometry(l)
                  if (lastPreparedRight.intersects(leftGeom)) {
                    createOrIgnoreJoinResult(leftId, rightId, l, r)
                  } else {
                    None
                  }
                }
              }
            case None =>
              // This won't be common: if we need the original geometry here, we should not keep it with the user data,
              // otherwise the shuffle write/read will be huge. The subdivided spatial join planner should have avoided
              // this situation.
              (leftId: Long, rightId: Long, l: UnsafeRow, r: UnsafeRow) => {
                if (leftId == -1 || rightId == -1) {
                  Some(createJoinResult(leftId, rightId, l, r))
                } else {
                  val leftGeom = boundLeftShape.toGeometry(l)
                  val rightGeom = boundRightShape.toGeometry(r)
                  if (leftGeom.intersects(rightGeom)) {
                    createOrIgnoreJoinResult(leftId, rightId, l, r)
                  } else {
                    None
                  }
                }
              }
          }
        }
      }

      iter.flatMap { case ((leftId, rightId), (leftRow, rightRow)) =>
        joinRows(leftId, rightId, leftRow, rightRow)
      }
    }

    joinType match {
      case Inner => joinedRowsWithKeys.map(_._2)
      case LeftOuter =>
        val rightNullRow = getNullUnsafeRow(right.output)
        val originalRowWithId =
          Subdivide.attachId(sortedLeftResultsRaw).rdd.map { case (id, row) => (id.toLong, row) }
        originalRowWithId.leftOuterJoin(joinedRowsWithKeys).mapPartitions { iter =>
          val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
          iter.map { case (_, (leftRow, rightSide)) =>
            rightSide match {
              case Some(rightRow) => joiner.join(leftRow, rightRow)
              case None => joiner.join(leftRow, rightNullRow)
            }
          }
        }
      case RightOuter =>
        val leftNullRow = getNullUnsafeRow(left.output)
        val originalRowWithId =
          Subdivide.attachId(sortedRightResultsRaw).rdd.map { case (id, row) => (id.toLong, row) }
        joinedRowsWithKeys.rightOuterJoin(originalRowWithId).mapPartitions { iter =>
          val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
          iter.map { case (_, (leftSide, rightRow)) =>
            leftSide match {
              case Some(leftRow) => joiner.join(leftRow, rightRow)
              case None => joiner.join(leftNullRow, rightRow)
            }
          }
        }
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported join type: $joinType")
    }
  }

  private def recoverOriginalRowData(
      joinedRowsWithIds: RDD[((Long, Long), (UnsafeRow, UnsafeRow))],
      originalRdd: RDD[UnsafeRow],
      joinWithLeftSide: Boolean): RDD[((Long, Long), (UnsafeRow, UnsafeRow))] = {
    val originalRowWithId =
      Subdivide.attachId(originalRdd).rdd.map { case (id, row) => (id.toLong, row) }
    val keyedByJoinedId = joinedRowsWithIds.map { case ((leftId, rightId), (leftRow, rightRow)) =>
      if (joinWithLeftSide) (leftId, (rightId, rightRow)) else (rightId, (leftId, leftRow))
    }
    if (joinWithLeftSide) {
      originalRowWithId.join(keyedByJoinedId).map {
        case (leftId, (leftRow, (rightId, rightRow))) => ((leftId, rightId), (leftRow, rightRow))
      }
    } else {
      keyedByJoinedId.join(originalRowWithId).map {
        case (rightId, ((leftId, leftRow), rightRow)) => ((leftId, rightId), (leftRow, rightRow))
      }
    }
  }

  private def runSpatialJoin(
      sedonaConf: SedonaConf,
      leftShapes: SpatialRDD[Geometry],
      rightShapes: SpatialRDD[Geometry],
      joinType: JoinType,
      spatialPredicate: SpatialPredicate,
      extraCondition: Option[Expression],
      localSubdivideLeftOptions: Option[SubdivideOptions],
      localSubdivideRightOptions: Option[SubdivideOptions]) = {
    // Create an extraFilterCreator to filter geometry pairs using extra condition within the
    // local spatial join evaluator.
    val outputAttributes = this.output
    val leftSchema = left.schema
    val rightSchema = right.schema
    val extraFilterCreator: JavaFunction0[JavaFunction2[Geometry, Geometry, java.lang.Boolean]] =
      extraCondition match {
        case Some(condition) =>
          () => {
            val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
            val boundCondition = Predicate.create(condition, outputAttributes)
            (l: Geometry, r: Geometry) => {
              val leftRow = getUnsafeRowFromUserData(l)
              val rightRow = getUnsafeRowFromUserData(r)
              val joinedRow = joiner.join(leftRow, rightRow)
              boundCondition.eval(joinedRow)
            }
          }
        case None => null
      }

    // Run spatial join
    val metricBuildCount = longMetric("buildCount")
    val metricCandidateCount = longMetric("candidateCount")
    val metricStreamCount = longMetric("streamCount")
    val metricResultCount = longMetric("numOutputRows")
    val metricBuildTime = longMetric("buildTime")
    val metricBuildLeftTasks = longMetric("buildLeftTasks")
    val metricBuildRightTasks = longMetric("buildRightTasks")
    val metricPrepareBuildTasks = longMetric("prepareBuildTasks")
    val metricPrepareStreamTasks = longMetric("prepareStreamTasks")
    val joinParams = new JoinParams(
      true,
      spatialPredicate,
      extraFilterCreator,
      joinTypeOf(joinType),
      IndexType.RTREE,
      sedonaConf.getJoinBuildSide,
      -1,
      null,
      null,
      metricBuildCount,
      metricStreamCount,
      metricResultCount,
      metricCandidateCount,
      metricBuildTime,
      metricBuildLeftTasks,
      metricBuildRightTasks,
      metricPrepareBuildTasks,
      metricPrepareStreamTasks,
      localSubdivideLeftOptions.orNull,
      localSubdivideRightOptions.orNull)
    val joinedRdd = JoinQuery.spatialJoin(leftShapes, rightShapes, joinParams).rdd
    joinedRdd
  }

  private def analyzeLeftAndRight(
      leftShapes: SpatialRDD[Geometry],
      rightShapes: SpatialRDD[Geometry],
      reAnalyze: Boolean = false): Unit = {
    val counter = TraitAdvancedJoinQueryExec.counter.getAndIncrement()
    val jobGroupName = s"AnalyzeSpatialData - $counter"
    val descPrefix = if (reAnalyze) "Re-analyzing" else "Analyzing"
    val session = SparkSession.getActiveSession.orNull
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)

    // Run left and right side analysis in parallel
    val executionContext = ExecutionContext.global
    val analyzeLeftFuture = Future {
      SQLExecution.withExecutionId(session, executionId) {
        sparkContext.setJobGroup(jobGroupName, s"$descPrefix left shapes")
        leftShapes.advancedAnalyze()
      }
    }(executionContext)
    val analyzeRightFuture = Future {
      SQLExecution.withExecutionId(session, executionId) {
        sparkContext.setJobGroup(jobGroupName, s"$descPrefix right shapes")
        rightShapes.advancedAnalyze()
      }
    }(executionContext)

    // Wait for both sides to finish. If any side fails, cancel the other side and throw an exception
    var leftResult: Option[Try[Boolean]] = None
    var rightResult: Option[Try[Boolean]] = None
    val waitTimeout = Duration(200, MILLISECONDS)
    while (leftResult.isEmpty || rightResult.isEmpty) {
      if (leftResult.isEmpty) {
        leftResult = waitForAnalyzeJobToFinish(analyzeLeftFuture, jobGroupName, waitTimeout)
      }
      if (rightResult.isEmpty) {
        rightResult = waitForAnalyzeJobToFinish(analyzeRightFuture, jobGroupName, waitTimeout)
      }
    }
  }

  private def waitForAnalyzeJobToFinish[T](
      future: Future[T],
      jobGroupName: String,
      duration: Duration): Option[Try[T]] = {
    try {
      Await.ready(future, duration)
      val result = future.value
      result.get.failed.foreach { e =>
        sparkContext.cancelJobGroup(jobGroupName)
        throw new RuntimeException("Failed to analyze dataset", e)
      }
      result
    } catch {
      case _: TimeoutException => None
      case e: Throwable =>
        sparkContext.cancelJobGroup(jobGroupName)
        throw e
    }
  }

  /**
   * Run the body with the given job description.
   *
   * @param description
   *   Job description
   * @param body
   *   Body to run
   */
  private def withJobDescription[T](description: String)(body: => T): T = {
    val oldDescription = sparkContext.getLocalProperty("spark.job.description")
    sparkContext.setJobDescription(description)
    try {
      body
    } finally {
      sparkContext.setJobDescription(oldDescription)
    }
  }

  /**
   * Assemble the projection for the left and right side to eliminate unneeded geometry attributes
   * @param plan
   *   Spark plan
   * @param unneeded
   *   Unneeded attributes
   * @return
   *   Projection expressions
   */
  private def projection(plan: SparkPlan, unneeded: Seq[Attribute]): Option[Seq[Expression]] = {
    if (unneeded.isEmpty) None
    else {
      Some(plan.output.map { ref =>
        if (unneeded.exists(_.semanticEquals(ref))) {
          Literal(null, ref.dataType)
        } else {
          BindReferences.bindReference(ref.asInstanceOf[Expression], plan.output)
        }
      })
    }
  }

  private def discardUnneededAttributes(
      spatialRDD: SpatialRDD[Geometry],
      subdivideRDDOptions: Option[SubdivideRDDOptions],
      plan: SparkPlan,
      unneededAttributes: Seq[Attribute],
      side: String): SpatialRDD[Geometry] = {
    val projectionExpr = projection(plan, unneededAttributes)
    subdivideRDDOptions match {
      case Some(options) =>
        if (options.keepUserData && unneededAttributes.nonEmpty && spatialPredicate == SpatialPredicate.INTERSECTS) {
          log.info(
            s"Discard unneeded attributes on subdivided $side: $unneededAttributes, projection: $projectionExpr")
          projectSubdividedSpatialRDD(spatialRDD, projectionExpr)
        } else {
          if (options.keepUserData) {
            log.warn(
              s"No attributes discarded for $side side when subdividing was enabled and keepRowData was set to true. " +
                "This may result in lots of shuffle writes and reads.")
          }
          spatialRDD
        }
      case None =>
        // We need the original geometries to remove extra rows with null on the non-outer side
        // when running outer joins (see outerJoinedRddToRowRdd), so this optimization is only
        // correct when running inner joins.
        if (joinType != Inner || unneededAttributes.isEmpty) spatialRDD
        else {
          log.info(
            s"Discard unneeded attributes on $side: $unneededAttributes, projection: $projectionExpr")
          projectSpatialRDD(spatialRDD, projectionExpr)
        }
    }
  }

  private def projectSubdividedSpatialRDD(
      spatialRDD: SpatialRDD[Geometry],
      projection: Option[Seq[Expression]]): SpatialRDD[Geometry] = {
    projection match {
      case Some(_) =>
        val rawSpatialRDD = spatialRDD.rawSpatialRDD.rdd.mapPartitions { shapes =>
          val toUserData = projectUnsafeRow(projection)
          shapes.map { shape =>
            val part = shape.getUserData.asInstanceOf[SubdividedPart]
            val rowData = part.userData.asInstanceOf[UnsafeRow]
            val newUserData = toUserData(rowData)
            part.userData = newUserData
            shape
          }
        }
        val newSpatialRDD = new SpatialRDD[Geometry]
        newSpatialRDD.setRawSpatialRDD(rawSpatialRDD)
        newSpatialRDD.setStatistics(spatialRDD.getStatistics)
        newSpatialRDD
      case None => spatialRDD
    }
  }

  private def innerJoinedRddToRowRdd(joinedRdd: RDD[(Geometry, Geometry)]): RDD[InternalRow] = {
    joinedRdd.mapPartitions { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      iter.map { case (l, r) =>
        val leftRow = l.getUserData.asInstanceOf[UnsafeRow]
        val rightRow = r.getUserData.asInstanceOf[UnsafeRow]
        joiner.join(leftRow, rightRow)
      }
    }
  }

  private def outerJoinedRddToRowRdd(joinedRdd: RDD[(Geometry, Geometry)]): RDD[InternalRow] = {
    val leftNullUnsafeRow = getNullUnsafeRow(left.output)
    val rightNullUnsafeRow = getNullUnsafeRow(right.output)
    val joinedRowsWithKeysRdd = joinedRdd.mapPartitions { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      iter.flatMap { case (l, r) =>
        val leftRow = if (l != null) getUnsafeRowFromUserData(l) else leftNullUnsafeRow
        val rightRow = if (r != null) getUnsafeRowFromUserData(r) else rightNullUnsafeRow
        val joinedRow = joiner.join(leftRow, rightRow)
        val key = joinType match {
          case LeftOuter => leftRow.hashCode()
          case RightOuter => rightRow.hashCode()
        }
        Some((key, joinedRow))
      }
    }

    // Remove joined rows that are all null on the other side when there are non-null rows present.
    // We have to colocate rows with the same outer-side together by doing a partitionBy.
    val repartitionedJoinedRowsRdd =
      joinedRowsWithKeysRdd.partitionBy(new HashPartitioner(joinedRdd.getNumPartitions)).map(_._2)
    val perPartitionSortedJoinedRowsRdd = JoinedUnsafeRowRDDSorter.sortJoinedUnsafeRowRDD(
      repartitionedJoinedRowsRdd,
      joinType,
      schema,
      output,
      left.output,
      right.output)
    perPartitionSortedJoinedRowsRdd.mapPartitions { iter =>
      val (outerProjection, otherProjection) =
        JoinedUnsafeRowRDDSorter.createProjections(joinType, output, left.output, right.output)
      var currentKeyRow: UnsafeRow = null
      var seenNonNullRows = false
      iter.flatMap { joinedRow =>
        val keyRow = outerProjection(joinedRow)
        val otherRow = otherProjection(joinedRow)
        if (keyRow != currentKeyRow) {
          currentKeyRow = keyRow.copy()
          seenNonNullRows = false
        }
        if (JoinedUnsafeRowRDDSorter.isAllNull(otherRow)) {
          if (seenNonNullRows) None else Some(joinedRow)
        } else {
          seenNonNullRows = true
          Some(joinedRow)
        }
      }
    }
  }

  private def determineNumOutOfBoundsPartitions(
      joinType: JoinType,
      spatialPartitioner: SpatialPartitioner,
      spatialPartitioningMetrics: SpatialPartitioningMetrics): Int = {
    val numPartitions = joinType match {
      case LeftOuter =>
        spatialPartitioner.numPartitions * (1 - spatialPartitioningMetrics.getLeftPartitionedRatio)
      case RightOuter =>
        spatialPartitioner.numPartitions * (1 - spatialPartitioningMetrics.getRightPartitionedRatio)
      case _ => throw new UnsupportedOperationException(s"Unsupported join type: $joinType")
    }
    Math.min(Math.max(1, numPartitions.toInt), sedonaConf.getMaxGuessedPartitionNumber)
  }
}

object TraitAdvancedJoinQueryExec {
  val counter = new java.util.concurrent.atomic.AtomicLong(0)

  /**
   * Get UnsafeRow object from the user data of a geometry object.
   * @param geom
   *   Geometry object
   * @return
   *   UnsafeRow object
   */
  def getUnsafeRowFromUserData(geom: Geometry): UnsafeRow = {
    geom.getUserData match {
      case unsafeRow: UnsafeRow => unsafeRow
      case d: OuterJoinUserData => d.userData.asInstanceOf[UnsafeRow]
      case _ => throw new RuntimeException("Unexpected user data")
    }
  }

  def joinTypeOf(joinType: JoinType): org.apache.sedona.core.enums.JoinType =
    joinType match {
      case Inner => org.apache.sedona.core.enums.JoinType.INNER
      case LeftOuter => org.apache.sedona.core.enums.JoinType.LEFT_OUTER
      case RightOuter => org.apache.sedona.core.enums.JoinType.RIGHT_OUTER
      case FullOuter => org.apache.sedona.core.enums.JoinType.FULL_OUTER
      case _ => throw new UnsupportedOperationException(s"Unsupported join type: $joinType")
    }

  def getNullUnsafeRow(attributes: Seq[Attribute]): UnsafeRow = {
    val nullableAttributes = attributes.map(_.withNullability(true))
    val nullRow = new GenericInternalRow(attributes.length)
    val projection = UnsafeProjection.create(nullableAttributes, nullableAttributes)
    projection(nullRow)
  }
}
