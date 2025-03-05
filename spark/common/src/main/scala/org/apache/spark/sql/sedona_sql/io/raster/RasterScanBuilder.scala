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

import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

case class RasterScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap,
    rasterOptions: RasterOptions)
    extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

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
      loadRasterMetadata)
  }
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
    loadRasterMetadata: Option[Boolean] = None)
    extends FileScan
    with Batch {

  private val rasterLoadingParallelism = {
    val sedonaConf = new SedonaConf(sparkSession.conf)
    sedonaConf.getRasterLoadingParallelism
  }

  override def planInputPartitions(): Array[InputPartition] = {
    if (loadRasterMetadata.getOrElse(false) && rasterLoadingParallelism > 0) {
      planInputPartitionsForParallelLoading()
    } else {
      convertInputPartitions(super.planInputPartitions())
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
    val inputPartitions = super.planInputPartitions()
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
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    RasterPartitionReaderFactory(
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      rasterOptions,
      pushedFilters,
      loadRasterMetadata.getOrElse(false),
      rasterLoadingParallelism)
  }

  override def equals(obj: Any): Boolean =
    super.equals(obj) && obj.isInstanceOf[RasterScan] &&
      this.loadRasterMetadata == obj.asInstanceOf[RasterScan].loadRasterMetadata
}
