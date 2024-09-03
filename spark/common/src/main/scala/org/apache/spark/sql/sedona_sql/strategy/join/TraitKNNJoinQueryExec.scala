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

import org.apache.commons.lang3.Range
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialPartitioning.{QuadTreeRTPartitioner, SpatialPartitioner, ZOrderPartitioner}
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.{ExecutorResourceUtils, SedonaConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, Predicate, UnsafeRow}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}
import org.apache.spark.sql.sedona_sql.strategy.join.TraitKNNJoinQueryExec.knnJoinPartitionNumOptimizer
import org.locationtech.jts.geom.{Envelope, Geometry}

import java.io.PrintWriter
import java.nio.file.Paths
import java.util
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.Try

/**
 * TraitKNNJoinQueryExec is a trait that extends the TraitJoinQueryExec trait and provides the
 * necessary functionality to execute a KNN join operation.
 *
 * It is used by the KNNJoinExec class to execute a KNN join operation. The KNN join operation is
 * a k-nearest neighbors join that finds the k-nearest neighbors of each object in the right
 * dataset for each query in the left dataset.
 */
trait TraitKNNJoinQueryExec extends TraitJoinQueryExec {
  self: SparkPlan =>

  protected var broadcastJoin: Boolean = false
  protected var querySide: JoinSide = null

  private lazy val sedonaConf = SedonaConf.fromActiveSession
  override lazy val metrics: Map[String, SQLMetric] = Map.empty

  override protected def doExecute(): RDD[InternalRow] = {
    // Execute the join
    executeKNNJoin(sedonaConf)
  }

  /**
   * Executes a KNN (k-nearest neighbors) join operation using the Sedona spatial library.
   *
   * This method binds the left and right shape references to their respective outputs, executes
   * the left and right datasets as RDDs, converts them to spatial RDDs, and performs spatial
   * partitioning based on Sedona configuration.
   *
   * The number of partitions is determined either by a predefined fallback value or optimized
   * based on the sizes of the object and query shapes. If the partitioning fails, it uses the
   * fallback value.
   *
   * It saves the spatial partitioner to a file if specified, gets the KNN join parameters,
   * performs the KNN join, and finally converts the matched RDD to RowRDD.
   *
   * @param sedonaConf
   *   The Sedona configuration settings.
   * @return
   *   RDD[InternalRow] The result of the KNN join as an RDD of InternalRows.
   */
  private def executeKNNJoin(sedonaConf: SedonaConf): RDD[InternalRow] = {
    val (querySparkPlan: SparkPlan, objectSparkPlan: SparkPlan, swapped: Boolean) =
      getQueryAndObjectPlans(leftShape)

    val boundQueryShape = BindReferences.bindReference(leftShape, querySparkPlan.output)
    val boundObjectShape = BindReferences.bindReference(rightShape, objectSparkPlan.output)

    val queryResultsRaw = querySparkPlan.execute().asInstanceOf[RDD[UnsafeRow]]
    val objectResultsRaw = objectSparkPlan.execute().asInstanceOf[RDD[UnsafeRow]]

    val sedonaConf = SedonaConf.fromActiveSession

    val (queryShapes, objectShapes) =
      toSpatialRddPair(queryResultsRaw, boundQueryShape, objectResultsRaw, boundObjectShape)

    // Analyze both sides for doing spatial partitioning, and probably subdivide the datasets
    analyzeLeftAndRight(objectShapes, queryShapes)

    val joinParams: JoinParams = getKNNJoinParams

    // calculate the optimized or predefined number of partitions
    // and do spatial partitioning
    var numPartitions = -1
    try {
      if (sedonaConf.getFallbackPartitionNum != -1) {
        numPartitions = sedonaConf.getFallbackPartitionNum
      } else {
        // Infer the amount of available executor memory for running local spatial join.
        val context = objectShapes.rawSpatialRDD.context
        val availableMemory = ExecutorResourceUtils.inferExecutionMemory(context)
        val stat = objectShapes.getStatistics
        val estimatedRowSizeInBytes =
          if (stat != null && stat.getEstimatedSizeInBytes > 0) stat.getEstimatedSizeInBytes
          else -1
        // object shapes are the dominant side
        numPartitions = knnJoinPartitionNumOptimizer(
          availableMemory,
          estimatedRowSizeInBytes,
          objectShapes.rawSpatialRDD.partitions.size(),
          queryShapes.rawSpatialRDD.partitions.size(),
          objectShapes.approximateTotalCount,
          queryShapes.approximateTotalCount,
          sedonaConf.getMaxRowsPerPartitionInKNNJoins,
          joinParams.k)
      }
      // object shapes are the dominant side
      doSpatialPartitioning(objectShapes, queryShapes, numPartitions, sedonaConf)
    } catch {
      case e: IllegalArgumentException => {
        log.error(e.getMessage)
        // Partition number are not qualified
        // Use fallback num partitions specified in SedonaConf
        numPartitions = sedonaConf.getFallbackPartitionNum
        doSpatialPartitioning(queryShapes, objectShapes, numPartitions, sedonaConf)
      }
    }

    // Save the spatial partitioner to file if the path is set
    if (sedonaConf.getSpatialPartitionerSavePath.nonEmpty) {
      saveKNNPartitionerToFile(
        objectShapes.getPartitioner,
        sedonaConf.getSpatialPartitionerSavePath)
    }

    val matchesRDD: RDD[(Geometry, Geometry)] =
      (queryShapes.spatialPartitionedRDD, objectShapes.spatialPartitionedRDD) match {
        case (null, null) =>
          if (broadcastJoin) {
            JoinQuery
              .knnJoin(
                queryShapes,
                objectShapes,
                joinParams,
                sedonaConf.isIncludeTieBreakersInKNNJoins,
                broadcastJoin)
              .rdd
          } else {
            sparkContext.parallelize(Seq[(Geometry, Geometry)]())
          }
        case _ =>
          JoinQuery
            .knnJoin(
              queryShapes,
              objectShapes,
              joinParams,
              sedonaConf.isIncludeTieBreakersInKNNJoins,
              broadcastJoin)
            .rdd
      }

    // Convert the matchesRDD to RowRDD
    joinedRddToRowRdd(matchesRDD, swapped)
  }

  /**
   * Gets the query and object plans based on the left shape.
   *
   * This method checks if the left shape is part of the left or right plan and returns the query
   * and object plans accordingly.
   *
   * @param leftShape
   *   The left shape expression.
   * @return
   *   (SparkPlan, SparkPlan) The query and object plans.
   */
  private def getQueryAndObjectPlans(leftShape: Expression) = {
    val isLeftQuerySide =
      left.toString().toLowerCase().contains(leftShape.toString().toLowerCase())
    if (isLeftQuerySide) {
      querySide = LeftSide
      (left, right, false)
    } else {
      querySide = RightSide
      (right, left, true)
    }
  }

  /**
   * Converts the joined RDD of geometries to an RDD of InternalRows.
   *
   * This method maps over the partitions of the joined RDD, creating an UnsafeRow joiner that
   * combines the left and right rows based on the given schemas.
   *
   * Each geometry's user data is expected to be an UnsafeRow, and the joiner is used to produce
   * joined rows from the left and right geometry pairs.
   *
   * @param joinedRdd
   *   The RDD containing pairs of joined geometries.
   * @return
   *   RDD[InternalRow] The resulting RDD of joined InternalRows.
   */
  protected def joinedRddToRowRdd(
      joinedRdd: RDD[(Geometry, Geometry)],
      swapped: Boolean): RDD[InternalRow] = {
    joinedRdd.mapPartitions { iter =>
      val joinRow = {
        val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
        (l: UnsafeRow, r: UnsafeRow) => joiner.join(l, r)
      }

      val joined = iter.map { case (l, r) =>
        val leftRow = l.getUserData.asInstanceOf[UnsafeRow]
        val rightRow = r.getUserData.asInstanceOf[UnsafeRow]
        if (swapped)
          joinRow(rightRow, leftRow)
        else
          joinRow(leftRow, rightRow)
      }

      // Apply the extra join conditions if it exists (e.g., S.ID < Q.ID)
      extraCondition match {
        case Some(condition) =>
          val boundCondition = Predicate.create(condition, output)
          joined.filter(row => boundCondition.eval(row))
        case None => joined
      }
    }
  }

  private def saveKNNPartitionerToFile(
      partitioner: SpatialPartitioner,
      savePath: String): Unit = {
    partitioner match {
      case null =>
        log.warn("[SedonaSQL] Spatial partitioner is null. Skip saving to file.")

      case qt: QuadTreeRTPartitioner =>
        val filePath = createFilePath(savePath, "quadtree-rt")
        log.info(s"[SedonaSQL] Saving QuadTreeRT partitioner to file: $filePath")
        writeGridsToFile(filePath, qt.getOverlappedGrids)

      case zo: ZOrderPartitioner =>
        val filePath = createFilePath(savePath, "zorder")
        log.info(s"[SedonaSQL] Saving ZOrder partitioner to file: $filePath")
        writeGridsToFile(filePath, zo.getOverlappedRanges)

      case _ =>
        log.info("[SedonaSQL] Spatial partitioner type is not supported for saving to file.")
    }
  }

  private def createFilePath(savePath: String, partitionerType: String): String = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Paths.get(savePath).toFile.mkdirs()
    Paths
      .get(savePath, s"partitioner-$partitionerType-$executionId-${System.currentTimeMillis()}")
      .toString
  }

  private def writeGridsToFile(
      filePath: String,
      grids: java.util.Map[Integer, java.util.List[Envelope]]): Unit = {
    val writer = new PrintWriter(filePath)
    try {
      grids.forEach { case (key, envelopes) =>
        envelopes.forEach { envelope =>
          writer.write(
            s"$key,${envelope.getMinX},${envelope.getMinY},${envelope.getMaxX},${envelope.getMaxY}\n")
        }
      }
    } finally {
      writer.close()
    }
  }

  private def writeGridsToFile(
      filePath: String,
      ranges: util.List[Range[java.lang.Long]]): Unit = {
    val writer = new PrintWriter(filePath)
    try {
      var rangeId = 1
      ranges.forEach { range =>
        writer.write(s"$rangeId,${range.getMinimum},${range.getMaximum}\n")
        rangeId += 1
      }
    } finally {
      writer.close()
    }
  }

  // The following methods are abstract and must be implemented by the concrete class
  // that extends this trait.
  // Override these methods to provide the necessary functionality for the KNN join.
  def getKNNJoinParams: JoinParams
}

object TraitKNNJoinQueryExec {
  val counter = new java.util.concurrent.atomic.AtomicLong(0)

  /**
   * This method optimizes the number of partitions for a k-Nearest Neighbors (kNN) join in Spark.
   * It determines an appropriate number of partitions to ensure sufficient parallelism without
   * introducing too much overhead due to a large number of partitions. The method considers
   * object-side partitions, query-side partitions, the count of objects, the number of neighbors,
   * a defined maximum row count per partition, and ensures the final partition number is not less
   * than `querySidePartNum / numNeighbor`.
   *
   * @param objectSidePartNum
   *   The number of partitions on the object side.
   * @param querySidePartNum
   *   The number of partitions on the query side.
   * @param objectSideCount
   *   The total count of objects in the object side dataset.
   * @param querySideCount
   *   The total count of objects in the query side dataset.
   * @param numNeighbor
   *   The number of neighbors to consider in the kNN join.
   * @param maxRowsPerPartitionByConfig
   *   The maximum number of rows allowed per partition, calculated based on the available memory
   *   and the average row size.
   * @return
   *   The optimized number of partitions to use for the kNN join.
   *
   * Step-by-Step Estimation to Determine maxRowsPerPartition:
   *
   *   1. Determine Target Partition Size:
   *      - With 36GB of executor memory and 6 cores per executor, allocate a portion of the
   *        memory to each partition.
   *      - Target partition size is set to 512MB per partition for a balanced approach.
   *
   * 2. Estimate Partition Size:
   *   - Target Partition Size: 512MB per partition.
   *   - Memory Available per Core:
   *     - 36GB / 6 cores = 6GB per core.
   *     - Allocating approximately 1/12th of this memory per partition gives around 512MB per
   *       partition.
   *
   * 3. Calculate Max Rows Per Partition:
   *   - Average Row Size: Assume an average row size of 1KB (adjust based on actual data).
   *   - Convert target partition size into KB:
   *     - 512MB = 512 * 1024 KB = 524,288KB.
   *   - Max rows per partition = 524,288KB / 1KB per row = 524,288 rows.
   *
   * 4. Adjust Based on Workload:
   *   - Increase maxRowsPerPartition if the job has too many small tasks, which can cause
   *     overhead in task scheduling.
   *   - Decrease maxRowsPerPartition if high memory usage or frequent garbage collection is
   *     observed, indicating that partitions are too large.
   *
   * Additional Considerations:
   *   - Ensure the final partition number is not less than `querySidePartNum / numNeighbor` to
   *     account for the query side's parallelism needs.
   */
  def knnJoinPartitionNumOptimizer(
      availableMemory: Long,
      estimatedSizeInBytes: Long,
      objectSidePartNum: Int,
      querySidePartNum: Int,
      objectSideCount: Long,
      querySideCount: Long,
      maxRowsPerPartitionByConfig: Long,
      numNeighbor: Int): Int = {

    // Determine the maximum number of rows per partition
    val maxRowsPerPartition = if (estimatedSizeInBytes > 0) {
      val maxRowsPerPartitionByMemoryCal =
        math.ceil(availableMemory / (estimatedSizeInBytes * 2)).toLong
      math.min(maxRowsPerPartitionByMemoryCal, maxRowsPerPartitionByConfig)
    } else {
      maxRowsPerPartitionByConfig
    }

    // Determine candidatePartitionNum based on the maxRowsPerPartition
    val candidatePartitionNum = (objectSideCount / maxRowsPerPartition).toInt

    // Ensure the final partition number is not less than querySidePartNum / numNeighbor
    val minQuerySidePartitionNum = querySidePartNum / numNeighbor

    // Ensure the final partition number does not cause the query side to exceed maxRowsPerPartition
    val minAllowedQueryPartitions =
      math.max((querySideCount / maxRowsPerPartition).toInt, minQuerySidePartitionNum)

    // Determine the final number of partitions
    val finalPartitionNum = if (objectSidePartNum > candidatePartitionNum) {
      objectSidePartNum
    } else if (candidatePartitionNum > 0) {
      candidatePartitionNum
    } else {
      200 // Default to 200 partitions if no other condition is met
    }

    // Cap the partition number to be no more than half of the object side count
    val maxAllowedPartitions = math.min((objectSideCount / 2), Int.MaxValue.toLong).toInt

    // Ensure finalPartitionNum is not less than minQuerySidePartitionNum and not more than minAllowedQueryPartitions
    math.max(
      math.min(math.max(finalPartitionNum, minAllowedQueryPartitions), maxAllowedPartitions),
      1)
  }
}
