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
package org.apache.sedona.stats.sampling

import org.apache.sedona.stats.MetricsRegistrator
import org.apache.sedona.util.DfUtils.getGeometryColumnName
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.sedona_sql.expressions.st_aggregates.ST_Envelope_Aggr
import org.apache.spark.sql.sedona_sql.expressions.st_functions.ST_Centroid
import org.apache.spark.sql.{Dataset, Row}
import org.locationtech.jts.geom.Point
import org.slf4j.{Logger, LoggerFactory}

object SpatiallyStratifiedSampling {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Spatially stratified sampling of a DataFrame containing spatial data.
   *
   * @param dataframe
   *   DataFrame containing spatial data to be sampled. Must contain a geometryColumn column.
   * @param fraction
   *   SpatiallyStratifiedSampling rate between 0 and 1
   * @param partitionCount
   *   Number of partitions to divide the data into. If not a perfect square, the number of
   *   partitions in each dimension will be rounded to the nearest integer.
   * @param geometry
   *   Column containing the geometry data. Default is "geometry"
   * @param seed
   *   Seed for sampling the data
   * @return
   *   the input DataFrame sampled down to the specified rate
   */
  def spatiallyStratifiedSample(
      dataframe: Dataset[Row],
      fraction: Double,
      partitionCount: Int,
      geometry: String = null,
      seed: Long = 42): Dataset[Row] = {

    MetricsRegistrator.getOrCreate.SpatiallyStratifiedSamplePerform.inc()

    // Validate input parameters
    val geometryColumn =
      if (geometry != null) geometry else getGeometryColumnName(dataframe.schema)

    if (fraction <= 0 || fraction > 1) {
      throw new IllegalArgumentException(
        "SpatiallyStratifiedSampling rate must be between 0 and 1")
    }
    if (partitionCount <= 0) {
      throw new IllegalArgumentException("Partition count must be greater than 0")
    }

    if (!dataframe.columns.contains(geometryColumn)) {
      throw new IllegalArgumentException(
        s"$geometryColumn Column must be present in the DataFrame")
    }

    val floatPartitions = math.sqrt(partitionCount)
    val partitions = floatPartitions.toInt

    if (floatPartitions != partitions.toDouble) {
      logger.warn(
        "partitionCount is not a perfect square. the number of partitions in each dimension will be rounded to the nearest integer.")
    }

    // Use bounds of data to assign cellIds
    val bounds = dataframe
      .select(ST_Envelope_Aggr(dataframe(geometryColumn)).alias("bounds"))
      .collect()(0)(0)
      .asInstanceOf[org.locationtech.jts.geom.Polygon]
      .getEnvelopeInternal

    def getCellId(point: Point): Int = {
      val cellX =
        ((point.getCoordinate.x - bounds.getMinX) / (bounds.getMaxX - bounds.getMinX) * partitions).toInt
      val cellY =
        ((point.getCoordinate.y - bounds.getMinY) / (bounds.getMaxY - bounds.getMinY) * partitions).toInt
      cellY * partitions + cellX
    }

    val getCellIdUdf = udf(getCellId _)
    dataframe
      .withColumn("cellId", getCellIdUdf(ST_Centroid(col(geometryColumn))))
      .stat
      .sampleBy(
        col("cellId"),
        (0 until partitions).flatMap { y =>
          (0 until partitions).map { x =>
            y * partitions + x -> fraction
          }
        }.toMap,
        seed = seed)
      .drop(col("cellId"))
  }
}
