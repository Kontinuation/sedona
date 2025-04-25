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
package org.apache.spark.sql.sedona_sql.io.raster

import org.apache.sedona.common.raster.outdb.ThreadLocalOutDbResourcePool
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.SupportsPushDownLimit
import org.apache.spark.sql.connector.read.SupportsPushDownTableSample
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.sedona_sql.utils.SparkHadoopUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

case class RasterScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap,
    rasterOptions: RasterOptions)
    extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownTableSample
    with SupportsPushDownLimit {

  private var pushedTableSample: Option[TableSampleInfo] = None
  private var pushedLimit: Option[Int] = None

  override def build(): Scan = {
    val loadRasterMetadata = if (rasterOptions.retile) Some(true) else rasterOptions.loadMetadata
    RasterScan(
      sparkSession,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      options,
      rasterOptions,
      pushedDataFilters,
      partitionFilters,
      dataFilters,
      loadRasterMetadata,
      pushedTableSample,
      pushedLimit)
  }

  override def pushTableSample(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long): Boolean = {
    if (withReplacement || rasterOptions.retile) {
      false
    } else {
      pushedTableSample = Some(TableSampleInfo(lowerBound, upperBound, withReplacement, seed))
      true
    }
  }

  override def pushLimit(limit: Int): Boolean = {
    pushedLimit = Some(limit)
    true
  }

  override def isPartiallyPushed: Boolean = rasterOptions.retile
}

case class RasterScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    rasterOptions: RasterOptions,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty,
    loadRasterMetadata: Option[Boolean] = None,
    pushedTableSample: Option[TableSampleInfo] = None,
    pushedLimit: Option[Int] = None)
    extends FileScan
    with Batch {

  private lazy val inputPartitions = {
    var partitions = super.planInputPartitions()

    // Sample the files based on the table sample
    pushedTableSample.foreach { tableSample =>
      val r = new Random(tableSample.seed)
      var partitionIndex = 0
      partitions = partitions.flatMap {
        case filePartition: FilePartition =>
          val files = filePartition.files
          val sampledFiles = files.filter(_ => r.nextDouble() < tableSample.upperBound)
          if (sampledFiles.nonEmpty) {
            val index = partitionIndex
            partitionIndex += 1
            Some(FilePartition(index, sampledFiles))
          } else {
            None
          }
        case partition =>
          throw new IllegalArgumentException(
            s"Unexpected partition type: ${partition.getClass.getCanonicalName}")
      }
    }

    // Limit the number of files to read
    pushedLimit.foreach { limit =>
      val partiallySelectedPartitions = mutable.ArrayBuffer.empty[FilePartition]
      var remaining = limit
      val limitedPartitions = partitions.iterator.takeWhile(_ => remaining > 0).map { partition =>
        val filePartition = partition.asInstanceOf[FilePartition]
        val files = filePartition.files
        if (files.length <= remaining) {
          remaining -= files.length
          filePartition
        } else {
          val selectedFiles = files.take(remaining)
          remaining = 0
          FilePartition(filePartition.index, selectedFiles)
        }
      }
      partitions = limitedPartitions.toArray
    }

    partitions
  }

  private val rasterLoadingParallelism = {
    val sedonaConf = new SedonaConf(sparkSession.conf)
    sedonaConf.getRasterLoadingParallelism
  }

  override def planInputPartitions(): Array[InputPartition] = {
    if (loadRasterMetadata.getOrElse(false) && rasterLoadingParallelism > 0) {
      planInputPartitionsForParallelLoading()
    } else {
      convertInputPartitions(inputPartitions)
    }
  }

  private def convertInputPartitions(
      inputPartitions: Array[InputPartition]): Array[InputPartition] = {
    // Simply use the default implementation to compute input partitions for all files
    inputPartitions.map {
      case filePartition: FilePartition =>
        RasterInputPartition(filePartition.index, filePartition.files)
      case partition =>
        throw new IllegalArgumentException(
          s"Unexpected partition type: ${partition.getClass.getCanonicalName}")
    }
  }

  private def planInputPartitionsForParallelLoading(): Array[InputPartition] = {
    // The number of rasters in each partition should be no less than rasterLoadingParallelism,
    // otherwise we won't be able to fully utilize the parallelism.
    val minRasterPerPartition = rasterLoadingParallelism * 2

    // Regroup the files into partitions with at least minRasterPerPartition rasters
    if (inputPartitions.isEmpty) {
      return Array.empty
    }
    val files = inputPartitions.flatMap {
      case filePartition: FilePartition => filePartition.files
      case partition =>
        throw new IllegalArgumentException(
          s"Unexpected partition type: ${partition.getClass.getCanonicalName}")
    }

    if (files.length / inputPartitions.length >= minRasterPerPartition) {
      // If the number of rasters in each partition is greater than minRasterPerPartition, we
      // don't need to rearrange the partitioning.
      convertInputPartitions(inputPartitions)
    } else {
      // Rearrange the partitioning to have minRasterPerPartition rasters in each partition
      files
        .groupBy(_.partitionValues)
        .flatMap { case (_, files) =>
          files.grouped(minRasterPerPartition)
        }
        .zipWithIndex
        .map { case (groupedFiles, index) =>
          RasterInputPartition(index, groupedFiles)
        }
        .toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap)

    // Attach wherobots-raster specific configurations to the Hadoop configuration
    SparkHadoopUtil.attachWherobotsHadoopConfigurations(
      sparkSession.sparkContext.conf,
      hadoopConf)

    val loadRasterMetadata = this.loadRasterMetadata.getOrElse(false)
    if (loadRasterMetadata && rasterLoadingParallelism > 0) {
      // Disable out-db raster pooling when loading raster metadata in parallel, since raster object
      // pool is thread local, we are loading raster metadata from raster loading worker threads so
      // these cached raster objects cannot be used by the executor threads, caching them are
      // simply useless.
      // Also, there will be lots of worker threads for loading raster metadata, each thread
      // has its own pool, there will be lots of useless raster objects pooled by the worker
      // threads, which will even exhaust maximum opened file descriptors or network connections.
      hadoopConf.set(ThreadLocalOutDbResourcePool.FREE_RESOURCES_POOL_SIZE_CONF_KEY, "0")
    }

    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    RasterPartitionReaderFactory(
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      rasterOptions,
      pushedFilters,
      loadRasterMetadata,
      rasterLoadingParallelism)
  }

  override def equals(obj: Any): Boolean =
    super.equals(obj) && obj.isInstanceOf[RasterScan] &&
      this.loadRasterMetadata == obj.asInstanceOf[RasterScan].loadRasterMetadata
}
