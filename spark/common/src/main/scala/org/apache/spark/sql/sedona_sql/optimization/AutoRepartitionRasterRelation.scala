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
package org.apache.spark.sql.sedona_sql.optimization

import org.apache.sedona.core.utils.ExecutorResourceUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sedona_sql.io.raster.RasterFileFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.binaryfile.BinaryFileFormat
import org.apache.spark.sql.functions.sum
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
 * Automatically repartition relations loaded using the raster data source, since we'd like to
 * distribute the dataframe containing out-db raster column data across the cluster. This usually
 * won't introduce too much performance overhead, since the size of the out-db raster values are
 * usually way smaller than the raster data referenced by the out-db raster values, so even a
 * large raster dataset won't be too large when being loaded as a dataframe of out-db rasters.
 */

class AutoRepartitionRasterRelation(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (isAutoRepartitionEnabled) {
      val visited = mutable.HashSet.empty[LogicalRelation]
      plan transformDown {
        case p @ Repartition(_, _, r: LogicalRelation) =>
          visited.add(r)
          p
        case r: LogicalRelation if isRasterRelation(r) =>
          if (visited.contains(r)) {
            r
          } else {
            visited.add(r)
            val numPartitions = getNumPartitions(r)
            Repartition(numPartitions, shuffle = true, r)
          }
      }
    } else {
      plan
    }

  private def isAutoRepartitionEnabled: Boolean = {
    sparkSession.conf.get("spark.wherobots.raster.load.autoRepartition", "true").toBoolean
  }

  private def isRasterRelation(lr: LogicalRelation): Boolean = {
    lr.relation match {
      case r: HadoopFsRelation => r.fileFormat.isInstanceOf[RasterFileFormat]
      case _ => false
    }
  }

  private def getNumPartitions(lr: LogicalRelation): Int = {
    sparkSession.conf.get("spark.wherobots.raster.load.numPartitions", "0").toInt match {
      case 0 =>
        val conf = sparkSession.sparkContext.getConf
        val isDynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(conf) ||
          sparkSession.conf.get("spark.wherobots.testing.dynamicAllocation", "false").toBoolean
        if (isDynamicAllocationEnabled) {
          // Determine number of partitions based on the total file size. The executors
          // could scale up to process more partitions concurrently.
          val relation = lr.relation.asInstanceOf[HadoopFsRelation]
          val options = relation.options
          val location = relation.location
          val paths = location.rootPaths
          val dfFiles = sparkSession.baseRelationToDataFrame(
            DataSource
              .apply(
                sparkSession,
                paths = paths.map(_.toString),
                className = classOf[BinaryFileFormat].getName,
                options = options ++ Map(DataSource.GLOB_PATHS_KEY -> "false"))
              .resolveRelation(checkFilesExist = false))
          val totalSize = dfFiles.select("length").agg(sum("length")).collect()(0).getLong(0)
          val sizePerPartition = Utils.byteStringAsBytes(
            sparkSession.conf.get("spark.wherobots.raster.load.perPartitionSize", "500mb"))
          val parallelism = ExecutorResourceUtils.inferParallelism(sparkSession.sparkContext)
          Math.max(parallelism, totalSize / sizePerPartition).toInt
        } else {
          // Try distributing the workload to all executor cores in the cluster
          sparkSession.sparkContext.defaultParallelism * 4
        }
      case n => n
    }
  }
}
