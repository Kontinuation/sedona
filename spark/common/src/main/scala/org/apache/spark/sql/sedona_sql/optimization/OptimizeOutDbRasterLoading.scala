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
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sedona_sql.io.raster.RasterTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.spark.sql.catalyst.plans.logical.LocalLimit
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.binaryfile.BinaryFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.sedona_sql.expressions.raster.RS_BandPath
import org.apache.spark.sql.sedona_sql.io.raster.RasterScan
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.util.Utils

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Rule to optimize out-db raster loading.
 *
 *   1. Determine should we load raster metadata eagerly. If any raster function that requires
 *      metadata is used in the query, we should load the metadata early in the raster data
 *      source. The raster data source is capable of loading raster metadata in parallel, so this
 *      will improve the performance of the query.
 *   1. Automatically repartition relations loaded using the raster data source, since we'd like
 *      to distribute the dataframe containing out-db raster column data across the cluster. This
 *      usually won't introduce too much performance overhead, since the size of the out-db raster
 *      values are usually way smaller than the raster data referenced by the out-db raster
 *      values, so even a large raster dataset won't be too large when being loaded as a dataframe
 *      of out-db rasters.
 */
class OptimizeOutDbRasterLoading(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val sedonaConf = new SedonaConf(sparkSession.conf)
    if (sedonaConf.isEnableRasterLoadAutoRepartition) {
      val visited = mutable.HashSet.empty[DataSourceV2ScanRelation]
      val limited = mutable.HashSet.empty[DataSourceV2ScanRelation]
      var loadRasterMetadata = false
      plan transformDown {
        case p @ Repartition(_, _, r: DataSourceV2ScanRelation) =>
          visited.add(r)
          p
        case r: DataSourceV2ScanRelation if isRasterRelation(r) =>
          val rasterScan = r.scan.asInstanceOf[RasterScan]
          if (visited.contains(r) || limited.contains(r)) {
            if (loadRasterMetadata && rasterScan.loadRasterMetadata.isEmpty) {
              val newRasterScan = r.copy(scan = rasterScan.copy(loadRasterMetadata = Some(true)))
              visited.add(newRasterScan)
              newRasterScan
            } else {
              r
            }
          } else {
            visited.add(r)
            val numPartitions = getNumPartitions(r, sedonaConf)

            // Only override the loadRasterMetadata option if it is not set. If user explicitly
            // set the option, we should respect it.
            if (loadRasterMetadata && rasterScan.loadRasterMetadata.isEmpty) {
              val newRasterScan = r.copy(scan = rasterScan.copy(loadRasterMetadata = Some(true)))
              visited.add(newRasterScan)
              Repartition(numPartitions, shuffle = true, newRasterScan)
            } else {
              Repartition(numPartitions, shuffle = true, r)
            }
          }
        case p: V2WriteCommand =>
          if (p.query.schema.existsRecursively(_.isInstanceOf[RasterUDT])) {
            // The query writes a raster dataframe into a data source. We should load the
            // raster metadata eagerly in the data source in case of the data source needs
            // to persist it.
            loadRasterMetadata = true
          }
          p
        case p: GlobalLimit =>
          // We should not repartition the data source if the query has a global limit,
          // otherwise we have to load all data to shuffle them, which defeats the purpose
          // of the global limit.
          findRasterScanDirectlyUnderLimit(p.child).foreach { r =>
            limited.add(r)
          }
          p
        case p =>
          // This is an imprecise way to determine whether we need to load raster metadata.
          // If anything in the plan calls a raster function that requires metadata, we should
          // load the metadata early in the raster data source.
          if (!loadRasterMetadata) {
            val needsMetadata = p.expressions.exists { expr =>
              expr.exists { e =>
                e.getClass.getSimpleName.startsWith("RS_") &&
                e.getClass.getName.startsWith("org.apache.spark.sql.sedona_sql") &&
                !e.isInstanceOf[RS_BandPath]
              }
            }
            if (needsMetadata) {
              loadRasterMetadata = true
            }
          }
          p
      }
    } else {
      plan
    }
  }

  private def isRasterRelation(relation: DataSourceV2ScanRelation): Boolean = {
    relation.scan.isInstanceOf[RasterScan]
  }

  private def getNumPartitions(scanRel: DataSourceV2ScanRelation, sedonaConf: SedonaConf): Int = {
    sedonaConf.getRasterLoadNumPartitions match {
      case 0 =>
        val conf = sparkSession.sparkContext.getConf
        val isDynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(conf) ||
          sparkSession.conf.get("spark.wherobots.testing.dynamicAllocation", "false").toBoolean
        if (isDynamicAllocationEnabled) {
          // Determine number of partitions based on the total file size. The executors
          // could scale up to process more partitions concurrently.
          val paths = scanRel.relation.table.asInstanceOf[RasterTable].paths
          val options = scanRel.relation.options.asScala.toMap
          val dfFiles = sparkSession.baseRelationToDataFrame(
            DataSource
              .apply(
                sparkSession,
                paths = paths,
                className = classOf[BinaryFileFormat].getName,
                options = options ++ Map(DataSource.GLOB_PATHS_KEY -> "false"))
              .resolveRelation(checkFilesExist = false))
          val totalSize = dfFiles.select("length").agg(sum("length")).collect()(0).getLong(0)
          val sizePerPartition = sedonaConf.getRasterLoadPerPartitionSize
          val parallelism = ExecutorResourceUtils.inferParallelism(sparkSession.sparkContext)
          Math.max(parallelism, totalSize / sizePerPartition).toInt
        } else {
          // Try distributing the workload to all executor cores in the cluster
          sparkSession.sparkContext.defaultParallelism * 4
        }
      case n => n
    }
  }

  @tailrec
  private def findRasterScanDirectlyUnderLimit(
      plan: LogicalPlan): Option[DataSourceV2ScanRelation] = {
    plan match {
      // Direct child is a DataSourceV2ScanRelation
      case r: DataSourceV2ScanRelation if isRasterRelation(r) =>
        Some(r)
      // Look through local limit
      case LocalLimit(_, child) =>
        findRasterScanDirectlyUnderLimit(child)
      // Look through Project nodes
      case Project(_, child) =>
        findRasterScanDirectlyUnderLimit(child)
      // Look through Filter nodes
      case Filter(_, child) =>
        findRasterScanDirectlyUnderLimit(child)
      // No match found
      case _ =>
        None
    }
  }
}
