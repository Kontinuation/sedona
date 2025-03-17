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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.sedona.common.raster.RasterConstructors
import org.apache.sedona.common.raster.outdb.LazyLoadOutDbGridCoverage2D
import org.apache.sedona.common.raster.outdb.OutDbGridCoverage2D
import org.apache.sedona.common.raster.outdb.OutDbResourcePool.ResourceKey
import org.apache.sedona.common.raster.outdb.ThreadLocalOutDbResourcePool
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.io.raster.RasterPartitionReader.rasterToInternalRows
import org.apache.spark.sql.sedona_sql.io.raster.RasterTable.MAX_AUTO_TILE_SIZE
import org.apache.spark.sql.sedona_sql.io.raster.RasterTable.RASTER
import org.apache.spark.sql.sedona_sql.io.raster.RasterTable.TILE_X
import org.apache.spark.sql.sedona_sql.io.raster.RasterTable.TILE_Y
import org.apache.spark.sql.types.StructType
import org.geotools.coverage.grid.GridCoverage2D
import org.slf4j.LoggerFactory

import java.net.URI
import scala.collection.JavaConverters._

class RasterPartitionReader(
    configuration: Configuration,
    partitionedFiles: Array[PartitionedFile],
    dataSchema: StructType,
    rasterOptions: RasterOptions,
    loadRasterMetadata: Boolean)
    extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[RasterPartitionReader])

  // Track the current file index we're processing
  private var currentFileIndex = 0

  // Current raster being processed
  private var currentRaster: OutDbGridCoverage2D = _

  // Current row
  private var currentRow: InternalRow = _

  // Iterator for the current file's tiles
  private var currentIterator: Iterator[InternalRow] = Iterator.empty

  override def next(): Boolean = {
    // If current iterator has more elements, return true
    if (currentIterator.hasNext) {
      currentRow = currentIterator.next()
      return true
    }

    // If current iterator is exhausted, but we have more files, load the next file
    if (currentFileIndex < partitionedFiles.length) {
      loadNextFile()
      if (currentIterator.hasNext) {
        currentRow = currentIterator.next()
        return true
      }
    }

    // No more data
    false
  }

  override def get(): InternalRow = {
    currentRow
  }

  override def close(): Unit = {
    if (currentRaster != null) {
      currentRaster.dispose(true)
      currentRaster = null
    }
  }

  private def loadNextFile(): Unit = {
    // Clean up previous raster if exists
    if (currentRaster != null) {
      currentRaster.dispose(true)
      currentRaster = null
    }

    if (currentFileIndex >= partitionedFiles.length) {
      currentIterator = Iterator.empty
      return
    }

    val partition = partitionedFiles(currentFileIndex)
    val path = new Path(new URI(partition.filePath.toString()))

    try {
      val params = Map(
        ThreadLocalOutDbResourcePool.READER_AUTO_RESCALE_CONF_KEY -> rasterOptions.autoRescale.toString).asJava
      val resourceKey = new ResourceKey(path, configuration, params)
      currentRaster = if (loadRasterMetadata) {
        val start = System.currentTimeMillis()
        logger.info(s"Loading raster metadata from $path")
        val raster = OutDbGridCoverage2D.create(path.toString, resourceKey)
        val cost = System.currentTimeMillis() - start
        logger.info(s"Raster metadata loaded successfully from $path in $cost ms")
        raster
      } else {
        new LazyLoadOutDbGridCoverage2D(path.toString, resourceKey)
      }
      currentIterator = rasterToInternalRows(currentRaster, dataSchema, rasterOptions)
      currentFileIndex += 1
    } catch {
      case e: Exception =>
        if (currentRaster != null) {
          currentRaster.dispose(true)
          currentRaster = null
        }
        throw e
    }
  }
}

object RasterPartitionReader {
  def rasterToInternalRows(
      currentRaster: OutDbGridCoverage2D,
      dataSchema: StructType,
      rasterOptions: RasterOptions): Iterator[InternalRow] = {
    val retile = rasterOptions.retile
    val tileWidth = rasterOptions.tileWidth
    val tileHeight = rasterOptions.tileHeight
    val padWithNoData = rasterOptions.padWithNoData

    val writer = new UnsafeRowWriter(dataSchema.length)
    writer.resetRowWriter()

    if (retile) {
      val (tw, th) = (tileWidth, tileHeight) match {
        case (Some(tw), Some(th)) => (tw, th)
        case (None, None) =>
          // Use the internal tile size of the input raster
          val path = currentRaster.getOutDbPath
          val tw = currentRaster.getRenderedImage.getTileWidth
          val th = currentRaster.getRenderedImage.getTileHeight
          val tileSizeError =
            "Please set tileWidth and tileHeight explicitly, or convert the raster to Cloud Optimized GeoTIFF (COG) using tools like gdal_translate. " +
              "Reference: https://docs.wherobots.com/latest/references/havasu/raster/performance-tips/#using-cloud-optimized-geotiff-cog-for-out-db-rasters"
          if (tw >= MAX_AUTO_TILE_SIZE || th >= MAX_AUTO_TILE_SIZE) {
            throw new IllegalArgumentException(
              s"Internal tile size of $path is too large ($tw x $th). " + tileSizeError)
          }
          if (tw == 0 || th == 0) {
            throw new IllegalArgumentException(
              s"Internal tile size of $path contains zero ($tw x $th). " + tileSizeError)
          }
          if (tw / th > 10 || th / tw > 10) {
            throw new IllegalArgumentException(
              s"Internal tile shape of $path is too thin ($tw x $th). " + tileSizeError)
          }
          (tw, th)
        case _ =>
          throw new IllegalArgumentException("Both tileWidth and tileHeight must be set")
      }

      val iter =
        RasterConstructors.generateTiles(currentRaster, null, tw, th, padWithNoData, Double.NaN)
      iter.asScala.map { tile =>
        val tileRaster = tile.getCoverage
        writer.reset()
        writeRaster(writer, dataSchema, tileRaster, tile.getTileX, tile.getTileY)
        tileRaster.dispose(true)
        writer.getRow
      }
    } else {
      writeRaster(writer, dataSchema, currentRaster, 0, 0)
      Iterator.single(writer.getRow)
    }
  }

  private def writeRaster(
      writer: UnsafeRowWriter,
      dataSchema: StructType,
      raster: GridCoverage2D,
      x: Int,
      y: Int): Unit = {
    dataSchema.fieldNames.zipWithIndex.foreach {
      case (RASTER, i) => writer.write(i, RasterUDT.serialize(raster))
      case (TILE_X, i) => writer.write(i, x)
      case (TILE_Y, i) => writer.write(i, y)
      case (other, _) =>
        throw QueryExecutionErrors.unsupportedFieldNameError(other)
    }
  }
}
