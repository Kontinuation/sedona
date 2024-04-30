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

import org.apache.sedona.core.enums.{IndexType, JoinSubdivideMode}
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialOperator.Subdivide.{SubdivideRDDOptions, SubdividedPart, isSubdivideAccurate}
import org.apache.sedona.core.spatialOperator.{JoinQuery, SpatialPredicate, Subdivide}
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, Literal, Predicate, UnsafeRow}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}
import org.apache.spark.sql.sedona_sql.strategy.join.TraitJoinQueryBase.projectUnsafeRow
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.Try

/**
 * TraitJoinQueryExec using advanced self-driving spatial join. This implementation of spatial join
 * does not honor most of the settings in SedonaConf, because it is designed to be self-driving and
 * tune these configurations automatically.
 */
trait TraitAdvancedJoinQueryExec extends TraitJoinQueryExec {
  self: SparkPlan =>

  /**
   * These are the attributes that will be discarded by the outer ProjectExec, so that it is possible
   * to discard their values when performing the spatial join. These attributes are the joined geometries
   * for most of the time, and we can discard the original geometries when subdividing is enabled to reduce
   * the amount of shuffle-write/read and memory usage when original geometries are actually not needed.
   *
   * Notice that when we enable geometry subdividing, we may need to keep the original geometries even though
   * they are not needed in the final output or extra condition.
   */
  val unneededLeftAttributes: Seq[Attribute] = Nil
  val unneededRightAttributes: Seq[Attribute] = Nil

  private lazy val sedonaConf = SedonaConf.fromActiveSession

  override lazy val metrics: Map[String, SQLMetric] = if (sedonaConf.useAdvancedSpatialJoin) {
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "buildCount" -> SQLMetrics.createMetric(sparkContext, "number of build side"),
      "streamCount" -> SQLMetrics.createMetric(sparkContext, "number of stream side"),
      "candidateCount" -> SQLMetrics.createMetric(sparkContext, "number of candidates"),
      "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build spatial index"),
      "buildLeftTasks" -> SQLMetrics.createMetric(sparkContext, "number of tasks building the left side"),
      "buildRightTasks" -> SQLMetrics.createMetric(sparkContext, "number of tasks building the right side"),
      "prepareBuildTasks" -> SQLMetrics.createMetric(sparkContext, "number of tasks preparing the build side"),
      "prepareStreamTasks" -> SQLMetrics.createMetric(sparkContext, "number of tasks preparing the stream side"))
  } else {
    Map.empty
  }

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

    // Convert the input data to SpatialRDD
    var subdivideLeftRDDOptions: Option[SubdivideRDDOptions] = None
    var subdivideRightRDDOptions: Option[SubdivideRDDOptions] = None
    var isLeftGeometryAccurate = true
    var isRightGeometryAccurate = true
    var (leftShapes, rightShapes) = toSpatialRddPair(leftResultsRaw, boundLeftShape, rightResultsRaw, boundRightShape)

    // Subdivide the RDD when subdivide mode is ALWAYS.
    if (sedonaConf.getSpatialJoinSubdivideLeft == JoinSubdivideMode.ALWAYS) {
      log.info("Subdividing left RDD, as the mode is set to ALWAYS")
      subdivideLeftRDDOptions = Some(sedonaConf.getLeftSubdivideRDDOptions)
      leftShapes = Subdivide.subdivideSpatialRDD(leftShapes, subdivideLeftRDDOptions.get)
    }
    if (sedonaConf.getSpatialJoinSubdivideRight == JoinSubdivideMode.ALWAYS) {
      log.info("Subdividing right RDD, as the mode is set to ALWAYS")
      subdivideRightRDDOptions = Some(sedonaConf.getRightSubdivideRDDOptions)
      rightShapes = Subdivide.subdivideSpatialRDD(rightShapes, subdivideRightRDDOptions.get)
    }

    // Analyze both sides for doing spatial partitioning, and probably subdivide the datasets
    analyzeLeftAndRight(leftShapes, rightShapes)

    // Subdivide the spatial RDD if necessary
    var (needReAnalyzeLeft, needReAnalyzeRight) = (false, false)
    if (sedonaConf.getSpatialJoinSubdivideLeft == JoinSubdivideMode.AUTO) {
      val options = Subdivide.determineSubdivideOptions(leftShapes, sedonaConf)
      if (options.isPresent) {
        log.info("Subdividing left RDD as automatically determined")
        leftShapes = Subdivide.subdivideSpatialRDD(leftShapes, options.get)
        subdivideLeftRDDOptions = Some(options.get)
        needReAnalyzeLeft = leftShapes.getStatistics == null
      }
    }
    if (sedonaConf.getSpatialJoinSubdivideRight == JoinSubdivideMode.AUTO) {
      val options = Subdivide.determineSubdivideOptions(rightShapes, sedonaConf)
      if (options.isPresent) {
        log.info("Subdividing right RDD as automatically determined")
        rightShapes = Subdivide.subdivideSpatialRDD(rightShapes, options.get)
        subdivideRightRDDOptions = Some(options.get)
        needReAnalyzeRight = rightShapes.getStatistics == null
      }
    }

    // Re-analyze the subdivided datasets if necessary
    (leftShapes.getStatistics, rightShapes.getStatistics) match {
      case (null, null) => analyzeLeftAndRight(leftShapes, rightShapes, reAnalyze = true)
      case (null, _) => withJobDescription("Re-analyzing left shapes") { leftShapes.advancedAnalyze() }
      case (_, null) => withJobDescription("Re-analyzing right shapes") { rightShapes.advancedAnalyze() }
      case _ => // Do nothing
    }

    // Decide whether we can discard the geometry attributes from the user data to reduce the amount of
    // shuffle-write/read as well as memory usage.
    subdivideLeftRDDOptions.foreach { options => isLeftGeometryAccurate = isSubdivideAccurate(leftShapes, options) }
    subdivideRightRDDOptions.foreach { options => isRightGeometryAccurate = isSubdivideAccurate(rightShapes, options) }
    if (isLeftGeometryAccurate && isRightGeometryAccurate) {
      leftShapes = discardUnneededAttributes(
        leftShapes, subdivideLeftRDDOptions, left, unneededLeftAttributes, "left")
      rightShapes = discardUnneededAttributes(
        rightShapes, subdivideRightRDDOptions, right, unneededRightAttributes, "right")
    } else {
      if (Seq(subdivideLeftRDDOptions, subdivideRightRDDOptions).flatten.exists(_.keepUserData)) {
        log.warn("No attributes removed from row data when keepRowData is set to true. " +
          "This may cause significant amount of shuffle-write/read and memory usage.")
      }
    }

    // If either side is subdivided, we need to transform another side to assign unique IDs to each geometry and
    // use SubdividedParts as user data. This is for de-duplicating the join result.
    (subdivideLeftRDDOptions, subdivideRightRDDOptions) match {
      case (None, None) => // Do nothing
      case (Some(_), Some(_)) => // Do nothing
      case (Some(_), None) =>
        rightShapes = Subdivide.noOpSubdivideSpatialRDD(rightShapes)
      case (None, Some(_)) =>
        leftShapes = Subdivide.noOpSubdivideSpatialRDD(leftShapes)
    }

    // Spatial partitioning
    if (sedonaConf.getFallbackPartitionNum == -1) {
      leftShapes.spatialPartitioning(sedonaConf.getJoinGridType, rightShapes)
    } else {
      leftShapes.spatialPartitioning(sedonaConf.getJoinGridType, rightShapes, sedonaConf.getFallbackPartitionNum)
    }

    // Perform spatial join
    if (leftShapes.spatialPartitionedRDD == null) {
      // Skipped spatial partitioning because the join result is empty
      sparkContext.emptyRDD
    } else {
      if (sedonaConf.getSpatialPartitionerSavePath.nonEmpty) {
        saveSpatialPartitionerToFile(leftShapes.getPartitioner, sedonaConf.getSpatialPartitionerSavePath)
      }
      (subdivideLeftRDDOptions, subdivideRightRDDOptions) match {
        case (None, None) =>
          // No subdivision applied, so we can run the join directly
          val joinedRdd = runSpatialJoin(sedonaConf, leftShapes, rightShapes, spatialPredicate)
          joinedRddToRowRdd(joinedRdd)
        case _ =>
          // When subdivide kicks in, the spatial predicate for running join must be INTERSECTS. If
          // the original predicate is anything other than INTERSECTS, we need to change it to INTERSECTS, and
          // add the original predicate to extraCondition to filter out the false positives.
          val newExtraCondition = spatialPredicate match {
            case SpatialPredicate.INTERSECTS => extraCondition
            case _ => Some(condition)
          }
          // If we're running simple ST_Intersects/ST_Contains/ST_Within join but something like a distance join or
          // raster join, and the subdivided geometries are not accurate, we need to refine the join result using
          // the original geometries. This refinement could use prepared geometries so it will be faster than
          // simply leaving everything to evaluating the extra condition.
          val refineIntersectsUsingOriginalGeometries =
            !(isLeftGeometryAccurate && isRightGeometryAccurate) && extraCondition.isEmpty
          val joinedRdd = runSpatialJoin(sedonaConf, leftShapes, rightShapes, SpatialPredicate.INTERSECTS)
          joinedSubdividedRddToRowRdd(joinedRdd, leftResultsRaw, rightResultsRaw,
            subdivideLeftRDDOptions, subdivideRightRDDOptions, refineIntersectsUsingOriginalGeometries,
            boundLeftShape, boundRightShape,
            newExtraCondition)
      }
    }
  }

  private def joinedSubdividedRddToRowRdd(joinedRdd: RDD[(Geometry, Geometry)],
    leftResultsRaw: RDD[UnsafeRow], rightResultsRaw: RDD[UnsafeRow],
    subdivideLeftRDDOptions: Option[SubdivideRDDOptions],
    subdivideRightRDDOptions: Option[SubdivideRDDOptions],
    refineIntersectsUsingOriginalGeometries: Boolean,
    boundLeftShape: Expression, boundRightShape: Expression,
    extraCondition: Option[Expression]): RDD[InternalRow] = {
    // Remove duplicated result caused by subdividing the geometries
    def mapSubdividedGeometry(geom: Geometry): (Long, UnsafeRow) = {
      val part = geom.getUserData.asInstanceOf[SubdividedPart]
      val row = part.userData.asInstanceOf[UnsafeRow]
      (part.id, row)
    }
    var joinedRowsWithIds = joinedRdd.map { case (left, right) =>
      val (leftId, leftRow) = mapSubdividedGeometry(left)
      val (rightId, rightRow) = mapSubdividedGeometry(right)
      ((leftId, rightId), (leftRow, rightRow))
    }.reduceByKey((rowsTuple, _) => rowsTuple)

    // TODO: If both the left side and the right side are subdivided without user data kept. we need to count how many
    //  geometries from the left and the right side are present in the join result (joinedRowsWithIds), because the
    //  order of the following equi-join matters. If we join with the smaller side first, we can reduce the amount of
    //  shuffle-write/read.

    // If either side does not take user data, we need to recover the original user data by joining with
    // the original RDD
    var lastJoinedSide: Option[JoinSide] = None
    subdivideLeftRDDOptions.foreach { options =>
      if (!options.keepUserData) {
        joinedRowsWithIds = recoverOriginalRowData(joinedRowsWithIds, leftResultsRaw, joinWithLeftSide = true)
        lastJoinedSide = Some(LeftSide)
      }
    }
    subdivideRightRDDOptions.foreach { options =>
      if (!options.keepUserData) {
        joinedRowsWithIds = recoverOriginalRowData(joinedRowsWithIds, rightResultsRaw, joinWithLeftSide = false)
        lastJoinedSide = Some(RightSide)
      }
    }

    // Convert pair of joined rows to a single joined row
    joinedRowsWithIds.mapPartitions { iter =>
      val joinRow = {
        val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
        if (!refineIntersectsUsingOriginalGeometries) {
          (_: Long, _: Long, l: UnsafeRow, r: UnsafeRow) => Some(joiner.join(l, r))
        } else {
          // If either side is in-accurate, we have to re-evaluate INTERSECTS predicate using the original geometries
          lastJoinedSide match {
            case Some(LeftSide) =>
              // Prepare the left side
              var lastLeftId: Long = -1
              var lastPreparedLeft: PreparedGeometry = null
              val factory = new PreparedGeometryFactory()
              (leftId: Long, _: Long, l: UnsafeRow, r: UnsafeRow) => {
                if (leftId != lastLeftId) {
                  lastPreparedLeft = factory.create(boundLeftShape.toGeometry(l))
                  lastLeftId = leftId
                }
                val rightGeom = boundRightShape.toGeometry(r)
                if (lastPreparedLeft.intersects(rightGeom)) {
                  Some(joiner.join(l, r))
                } else {
                  None
                }
              }
            case Some(RightSide) =>
              // Prepare the right side
              var lastRightId: Long = -1
              var lastPreparedRight: PreparedGeometry = null
              val factory = new PreparedGeometryFactory()
              (_: Long, rightId: Long, l: UnsafeRow, r: UnsafeRow) => {
                if (rightId != lastRightId) {
                  lastPreparedRight = factory.create(boundRightShape.toGeometry(r))
                  lastRightId = rightId
                }
                val leftGeom = boundLeftShape.toGeometry(l)
                if (lastPreparedRight.intersects(leftGeom)) {
                  Some(joiner.join(l, r))
                } else {
                  None
                }
              }
            case None =>
              // This won't be common: if we need the original geometry here, we should not keep it with the user data,
              // otherwise the shuffle write/read will be huge. The subdivided spatial join planner should have avoided
              // this situation.
              (_: Long, _: Long, l: UnsafeRow, r: UnsafeRow) => {
                val leftGeom = boundLeftShape.toGeometry(l)
                val rightGeom = boundRightShape.toGeometry(r)
                if (leftGeom.intersects(rightGeom)) {
                  Some(joiner.join(l, r))
                } else {
                  None
                }
              }
          }
        }
      }

      val joined = iter.flatMap { case ((leftId, rightId), (leftRow, rightRow)) =>
        joinRow(leftId, rightId, leftRow, rightRow)
      }
      extraCondition match {
        case Some(condition) =>
          val boundCondition = Predicate.create(condition, output)
          joined.filter(row => boundCondition.eval(row))
        case None => joined
      }
    }
  }

  private def recoverOriginalRowData[T](joinedRowsWithIds: RDD[((Long, Long), (UnsafeRow, UnsafeRow))],
    originalRdd: RDD[UnsafeRow], joinWithLeftSide: Boolean): RDD[((Long, Long), (UnsafeRow, UnsafeRow))] = {
    val originalRowWithId = Subdivide.attachId(originalRdd).rdd.map { case (id, row) => (id.toLong, row) }
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

  private def runSpatialJoin(sedonaConf: SedonaConf,
    leftShapes: SpatialRDD[Geometry], rightShapes: SpatialRDD[Geometry],
    spatialPredicate: SpatialPredicate) = {
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
    val joinParams = new JoinParams(true, spatialPredicate, IndexType.RTREE, sedonaConf.getJoinBuildSide,
      metricBuildCount, metricStreamCount, metricResultCount, metricCandidateCount, metricBuildTime,
      metricBuildLeftTasks, metricBuildRightTasks,
      metricPrepareBuildTasks, metricPrepareStreamTasks)
    val joinedRdd = JoinQuery.spatialJoin(leftShapes, rightShapes, joinParams).rdd
    // We've already built the joined RDD. Now it is safe to free up memory used by the statistics data,
    // especially sampled envelopes on both sides
    leftShapes.forgetStatistics()
    rightShapes.forgetStatistics()
    joinedRdd
  }

  private def analyzeLeftAndRight(leftShapes: SpatialRDD[Geometry], rightShapes: SpatialRDD[Geometry],
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

  private def waitForAnalyzeJobToFinish[T](future: Future[T], jobGroupName: String, duration: Duration): Option[Try[T]] = {
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
   * @param spark       Spark session
   * @param description Job description
   * @param body        Body to run
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
   * @param plan    Spark plan
   * @param unneeded Unneeded attributes
   * @return Projection expressions
   */
  private def projection(plan: SparkPlan, unneeded: Seq[Attribute]): Option[Seq[Expression]] = {
    if (unneeded.isEmpty) None else {
      Some(plan.output.map { ref =>
        if (unneeded.exists(_.semanticEquals(ref))) {
          Literal(null, ref.dataType)
        } else {
          BindReferences.bindReference(ref.asInstanceOf[Expression], plan.output)
        }
      })
    }
  }

  private def discardUnneededAttributes(spatialRDD: SpatialRDD[Geometry],
    subdivideRDDOptions: Option[SubdivideRDDOptions],
    plan: SparkPlan, unneededAttributes: Seq[Attribute], side: String): SpatialRDD[Geometry] = {
    val projectionExpr = projection(plan, unneededAttributes)
    subdivideRDDOptions match {
      case Some(options) =>
        if (options.keepUserData && unneededAttributes.nonEmpty && spatialPredicate == SpatialPredicate.INTERSECTS) {
          log.info(s"Discard unneeded attributes on subdivided $side: $unneededAttributes, projection: $projectionExpr")
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
        if (unneededAttributes.isEmpty) spatialRDD else {
          log.info(s"Discard unneeded attributes on $side: $unneededAttributes, projection: $projectionExpr")
          projectSpatialRDD(spatialRDD, projectionExpr)
        }
    }
  }

  private def projectSubdividedSpatialRDD(spatialRDD: SpatialRDD[Geometry],
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
}

object TraitAdvancedJoinQueryExec {
  val counter = new java.util.concurrent.atomic.AtomicLong(0)
}
