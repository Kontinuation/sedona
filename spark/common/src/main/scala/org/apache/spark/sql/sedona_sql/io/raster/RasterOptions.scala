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

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class RasterOptions(@transient private val parameters: CaseInsensitiveMap[String])
    extends Serializable {
  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  // The following options are used to read raster data

  /**
   * Whether to retile the raster data. If true, the raster data will be retiled into smaller
   * tiles. If false, the raster data will be read as a single tile.
   */
  val retile: Boolean = parameters.getOrElse("retile", "true").toBoolean

  /**
   * The width of the tile. This is only effective when retile is true. If retile is true and
   * tileWidth is not set, the default value is the width of the internal tiles in the raster
   * files. Each raster file may have different internal tile sizes.
   */
  val tileWidth: Option[Int] = parameters.get("tileWidth").map(_.toInt)

  /**
   * The height of the tile. This is only effective when retile is true. If retile is true and
   * tileHeight is not set, the default value is the same as tileWidth. IF tileHeight is set,
   * tileWidth must be set as well.
   */
  val tileHeight: Option[Int] = parameters
    .get("tileHeight")
    .map { value =>
      require(tileWidth.isDefined, "tileWidth must be set when tileHeight is set")
      value.toInt
    }
    .orElse(tileWidth)

  /**
   * Rescale pixel values according to the metadata of the raster data. If true, the pixel values
   * will be computed using the offset and scale in the metadata. The pixel types will be float64
   * when auto rescale happens. If false, the pixel values will be read as they are stored in the
   * raster files.
   */
  val autoRescale: Boolean = parameters.getOrElse("autoRescale", "false").toBoolean

  /**
   * Load metadata of the rasters eagerly. If true, the out-db rasters read from raster source
   * will be OutDbGridCoverage, otherwise they will be LazyLoadOutDbGridCoverage. Eager loading
   * will always happen when retile is true, so this option is only effective when retile is
   * false.
   *
   * Eagerly loading metadata enables parallel loading of raster metadata within the reader of
   * this data source, which can speed up further operations that require raster metadata.
   */
  val loadMetadata: Option[Boolean] = parameters.get("loadMetadata").map(_.toBoolean)

  // The following options are used to write raster data

  // The file format of the raster image
  val fileExtension: String = parameters.getOrElse("fileExtension", ".tiff")
  // Column of the raster image name
  val rasterPathField: Option[String] = parameters.get("pathField")
  // Column of the raster image itself
  val rasterField: Option[String] = parameters.get("rasterField")
  // Use direct committer to directly write to the final destination
  val useDirectCommitter: Boolean = parameters.getOrElse("useDirectCommitter", "true").toBoolean
}
