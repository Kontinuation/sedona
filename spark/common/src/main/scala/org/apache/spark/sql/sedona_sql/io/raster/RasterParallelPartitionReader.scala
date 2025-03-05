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
import org.apache.sedona.common.raster.outdb.OutDbGridCoverage2D
import org.apache.sedona.common.raster.outdb.OutDbResourcePool.ResourceKey
import org.apache.sedona.common.raster.outdb.ThreadLocalOutDbResourcePool
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sedona_sql.io.raster.RasterPartitionReader.rasterToInternalRows
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.net.URI
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class RasterParallelPartitionReader(
    configuration: Configuration,
    partitionedFiles: Array[PartitionedFile],
    dataSchema: StructType,
    rasterOptions: RasterOptions,
    rasterLoadingParallelism: Int)
    extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[RasterParallelPartitionReader])

  // Track the current file index we're processing
  private var numRastersLoaded = 0

  // Current raster being processed
  private var currentRaster: OutDbGridCoverage2D = _

  // Current row
  private var currentRow: InternalRow = _

  // Iterator for the current file's tiles
  private var currentIterator: Iterator[InternalRow] = Iterator.empty

  private val queue = new LinkedBlockingQueue[Try[Array[Byte]]](rasterLoadingParallelism * 2)

  private val isRunning = new AtomicBoolean(true)

  private val submitThread: Thread = {
    val thread = new Thread(() => {
      val executor = RasterParallelPartitionReader.getExecutor(rasterLoadingParallelism)
      partitionedFiles.foreach(file => {
        if (isRunning.get()) {
          executor.execute(() => {
            var raster: OutDbGridCoverage2D = null
            try {
              val start = System.currentTimeMillis()
              logger.info(s"Loading raster metadata from ${file.filePath}")
              val path = new Path(new URI(file.filePath.toString()))
              val params = Map(
                ThreadLocalOutDbResourcePool.READER_AUTO_RESCALE_CONF_KEY -> rasterOptions.autoRescale.toString).asJava
              val resourceKey = new ResourceKey(path, configuration, params)
              raster = OutDbGridCoverage2D.create(path.toString, resourceKey)
              val cost = System.currentTimeMillis() - start
              val serializedRaster = RasterUDT.serialize(raster)
              raster.dispose(true)
              logger.info(
                s"Raster metadata loaded successfully from ${file.filePath} in $cost ms")
              queue.put(Success(serializedRaster))
            } catch {
              case e: Exception =>
                if (raster != null) raster.dispose(true)
                queue.put(Failure(e))
                throw e
            }
          })
        }
      })
    })
    thread.setName("raster-metadata-loader-submit-thread-" + thread.getId)
    thread.setDaemon(true)
    thread
  }

  submitThread.start()

  override def next(): Boolean = {
    // If current iterator has more elements, return true
    if (currentIterator.hasNext) {
      currentRow = currentIterator.next()
      return true
    }

    // If current iterator is exhausted, but we have more files, load the next file
    if (numRastersLoaded < partitionedFiles.length) {
      queue.take() match {
        case Success(serializedRaster) =>
          numRastersLoaded += 1
          currentRaster =
            RasterUDT.deserialize(serializedRaster).asInstanceOf[OutDbGridCoverage2D]
          currentIterator = rasterToInternalRows(currentRaster, dataSchema, rasterOptions)
          if (currentIterator.hasNext) {
            currentRow = currentIterator.next()
            return true
          }
        case Failure(e) =>
          isRunning.set(false)
          throw e
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
    isRunning.set(false)
    submitThread.join()
  }
}

object RasterParallelPartitionReader {
  private var executorService: ExecutorService = _

  private def getExecutor(parallelism: Int): ExecutorService = synchronized {
    if (executorService == null) {
      val workQueue = new java.util.concurrent.LinkedBlockingQueue[Runnable](parallelism * 2)
      val threadPoolExecutor = new ThreadPoolExecutor(
        parallelism,
        parallelism,
        10L,
        java.util.concurrent.TimeUnit.SECONDS,
        workQueue,
        new ThreadFactory {
          override def newThread(r: Runnable): Thread = {
            val t = Executors.defaultThreadFactory().newThread(r)
            t.setName("raster-metadata-loader-" + t.getId)
            t.setDaemon(true)
            t
          }
        })
      threadPoolExecutor.allowCoreThreadTimeOut(true)
      threadPoolExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy)
      executorService = threadPoolExecutor
    }
    executorService
  }
}
