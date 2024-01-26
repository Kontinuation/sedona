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

package org.apache.sedona.sql.utils

import org.apache.sedona.common.raster.outdb.HadoopConfigSerializer
import org.apache.sedona.common.raster.serde.Serde
import org.apache.spark.SparkEnv
import org.apache.spark.sql.sedona_sql.utils.SparkHadoopUtil
import org.geotools.coverage.grid.GridCoverage2D

/**
 * This raster serializer object uses {@link Serde} to serialize and deserialize raster. It serializes out-db rasters
 * without configurations since serialized {@link org.apache.hadoop.conf.Configuration} takes too much space. We'll
 * use the configuration retrieved from {@link org.apache.spark.SparkEnv} to deserialize out-db rasters.
 */
object RasterSerializer {
  private lazy val hadoopConf = {
    val sparkEnv = SparkEnv.get
    if (sparkEnv == null) {
      throw new IllegalStateException("SparkEnv is null. Cannot use RasterSerializer without an active spark context")
    }
    val sparkConf = sparkEnv.conf
    SparkHadoopUtil.newConfiguration(sparkConf)
  }

  lazy val serializedConf: Array[Byte] = HadoopConfigSerializer.serialize(hadoopConf)

  /**
   * Given a raster returns array of bytes
   *
   * @param raster raster to serialize
   * @return Array of bites represents this geometry
   */
  def serialize(raster: GridCoverage2D): Array[Byte] = {
    Serde.serialize(raster, false)
  }

  /**
   * Given ArrayData returns Geometry
   *
   * @param value serialized raster
   * @return GridCoverage2D
   */
  def deserialize(value: Array[Byte]): GridCoverage2D = {
    Serde.deserialize(value, serializedConf)
  }
}
