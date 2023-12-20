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

import org.apache.sedona.core.enums.IndexType
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, UnsafeRow}
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.locationtech.jts.geom.Geometry

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
    val (leftShapes, rightShapes) = toSpatialRddPair(leftResultsRaw, boundLeftShape, rightResultsRaw, boundRightShape)

    // Analyze both sides and do spatial partitioning
    analyzeLeftAndRight(leftShapes, rightShapes)
    if (sedonaConf.getFallbackPartitionNum == -1) {
      leftShapes.spatialPartitioning(sedonaConf.getJoinGridType, rightShapes)
    } else {
      leftShapes.spatialPartitioning(sedonaConf.getJoinGridType, rightShapes, sedonaConf.getFallbackPartitionNum)
    }

    if (leftShapes.spatialPartitionedRDD == null) {
      // Skipped spatial partitioning because the join result is empty
      sparkContext.emptyRDD
    } else {
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
      joinedRddToRowRdd(joinedRdd)
    }
  }

  private def analyzeLeftAndRight(leftShapes: SpatialRDD[Geometry], rightShapes: SpatialRDD[Geometry]): Unit = {
    val counter = TraitAdvancedJoinQueryExec.counter.getAndIncrement()
    val jobGroupName = s"AnalyzeSpatialData - $counter"
    val session = SparkSession.getActiveSession.orNull
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)

    // Run left and right side analysis in parallel
    val executionContext = ExecutionContext.global
    val analyzeLeftFuture = Future {
      SQLExecution.withExecutionId(session, executionId) {
        sparkContext.setJobGroup(jobGroupName, "Analyzing left shapes")
        leftShapes.advancedAnalyze()
      }
    }(executionContext)
    val analyzeRightFuture = Future {
      SQLExecution.withExecutionId(session, executionId) {
        sparkContext.setJobGroup(jobGroupName, "Analyzing right shapes")
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
}

object TraitAdvancedJoinQueryExec {
  val counter = new java.util.concurrent.atomic.AtomicLong(0)
}
