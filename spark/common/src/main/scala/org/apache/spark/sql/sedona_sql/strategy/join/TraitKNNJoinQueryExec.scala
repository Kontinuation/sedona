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

import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{BindReferences, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.locationtech.jts.geom.Geometry

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

  private lazy val sedonaConf = SedonaConf.fromActiveSession
  override lazy val metrics: Map[String, SQLMetric] = Map.empty

  override protected def doExecute(): RDD[InternalRow] = {
    // Check if the join is supported
    isSupported

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
    val boundLeftShape = BindReferences.bindReference(leftShape, left.output)
    val boundRightShape = BindReferences.bindReference(rightShape, right.output)

    val leftResultsRaw = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResultsRaw = right.execute().asInstanceOf[RDD[UnsafeRow]]

    val sedonaConf = SedonaConf.fromActiveSession

    val (queryShapes, objectShapes) =
      toSpatialRddPair(leftResultsRaw, boundLeftShape, rightResultsRaw, boundRightShape)

    objectShapes.analyze()
    log.info(
      "[SedonaSQL] Number of partitions on the objectShapes (right): " + rightResultsRaw.partitions.size)

    // calculate the optimized or predefined number of partitions
    // and do spatial partitioning
    var numPartitions = -1
    try {
      if (sedonaConf.getFallbackPartitionNum != -1) {
        numPartitions = sedonaConf.getFallbackPartitionNum
      } else {
        // object shapes are the dominant side
        numPartitions = joinPartitionNumOptimizer(
          objectShapes.rawSpatialRDD.partitions.size(),
          queryShapes.rawSpatialRDD.partitions.size(),
          objectShapes.approximateTotalCount)
      }
      // object shapes are the dominant side
      doSpatialPartitioning(objectShapes, queryShapes, numPartitions, sedonaConf)
    } catch {
      case e: IllegalArgumentException => {
        print(e.getMessage)
        // Partition number are not qualified
        // Use fallback num partitions specified in SedonaConf
        numPartitions = sedonaConf.getFallbackPartitionNum
        doSpatialPartitioning(queryShapes, objectShapes, numPartitions, sedonaConf)
      }
    }

    // Save the spatial partitioner to file if the path is set
    if (sedonaConf.getSpatialPartitionerSavePath.nonEmpty) {
      saveSpatialPartitionerToFile(
        queryShapes.getPartitioner,
        sedonaConf.getSpatialPartitionerSavePath)
    }

    val joinParams: JoinParams = getKNNJoinParams

    val matchesRDD: RDD[(Geometry, Geometry)] =
      (queryShapes.spatialPartitionedRDD, objectShapes.spatialPartitionedRDD) match {
        case (null, null) =>
          sparkContext.parallelize(Seq[(Geometry, Geometry)]())
        case _ => JoinQuery.knnJoin(queryShapes, objectShapes, joinParams).rdd
      }

    // Convert the matchesRDD to RowRDD
    joinedRddToRowRdd(matchesRDD)
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
  override protected def joinedRddToRowRdd(
      joinedRdd: RDD[(Geometry, Geometry)]): RDD[InternalRow] = {
    joinedRdd.mapPartitions { iter =>
      val joinRow = {
        val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
        (l: UnsafeRow, r: UnsafeRow) => joiner.join(l, r)
      }

      val joined = iter.map { case (l, r) =>
        val leftRow = l.getUserData.asInstanceOf[UnsafeRow]
        val rightRow = r.getUserData.asInstanceOf[UnsafeRow]
        joinRow(leftRow, rightRow)
      }
      joined
    }
  }

  // The following methods are abstract and must be implemented by the concrete class
  // that extends this trait.
  // Override these methods to provide the necessary functionality for the KNN join.
  def getKNNJoinParams: JoinParams

  // Check if the join is supported
  // This method should throw an exception if the join is not supported.
  // Override this method to provide the necessary functionality for the KNN join.
  def isSupported: Boolean
}

object TraitKNNJoinQueryExec {
  val counter = new java.util.concurrent.atomic.AtomicLong(0)
}
