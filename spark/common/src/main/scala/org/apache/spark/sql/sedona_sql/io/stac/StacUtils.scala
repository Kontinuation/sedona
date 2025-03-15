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
package org.apache.spark.sql.sedona_sql.io.stac

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.sedona.common.raster.outdb.LazyLoadOutDbGridCoverage2D
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetSpatialFilter
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.io.stac.StacAssetType._
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.locationtech.jts.geom.Envelope

import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Try

object StacUtils {

  // Function to load JSON from URL or service
  def loadStacCollectionToJson(opts: Map[String, String]): String = {
    val urlFull: String = getFullCollectionUrl(opts)

    loadStacCollectionToJson(urlFull)
  }

  def getFullCollectionUrl(opts: Map[String, String]) = {
    val url = opts.getOrElse(
      "path",
      opts.getOrElse(
        "service",
        throw new IllegalArgumentException("Either 'path' or 'service' must be provided")))
    val urlFinal = if (url.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://.*")) url else s"file://$url"
    urlFinal
  }

  // Function to load JSON from URL or service
  def loadStacCollectionToJson(url: String, maxRetries: Int = 3): String = {
    var retries = 0
    var success = false
    var result: String = ""

    while (retries < maxRetries && !success) {
      try {
        result = if (url.startsWith("s3://") || url.startsWith("s3a://")) {
          SparkSession.active.read.textFile(url).collect().mkString("\n")
        } else {
          Source.fromURL(url).mkString
        }
        success = true
      } catch {
        case e: Exception =>
          retries += 1
          if (retries >= maxRetries) {
            throw new RuntimeException(
              s"Failed to load STAC collection from $url after $maxRetries attempts",
              e)
          }
      }
    }

    result
  }

  // Function to get the base URL from the collection URL or service
  def getStacCollectionBasePath(opts: Map[String, String]): String = {
    val ref = opts.getOrElse(
      "path",
      opts.getOrElse(
        "service",
        throw new IllegalArgumentException("Either 'path' or 'service' must be provided")))
    getStacCollectionBasePath(ref)
  }

  // Function to get the base URL from the collection URL or service
  def getStacCollectionBasePath(collectionUrl: String): String = {
    val urlPattern = "(https?://[^/]+/|http://[^/]+/).*".r
    val filePattern = "(file:///.*/|/.*/).*".r

    collectionUrl match {
      case urlPattern(baseUrl) => baseUrl
      case filePattern(basePath) =>
        if (basePath.startsWith("file://")) basePath else s"file://$basePath"
      case _ => throw new IllegalArgumentException(s"Invalid URL or file path: $collectionUrl")
    }
  }

  /**
   * Infer the schema of the STAC data source table.
   *
   * This method checks if a cached schema exists for the given data source options. If not, it
   * processes the STAC collection and saves it as a GeoJson file. The schema is then inferred
   * from this GeoJson file.
   *
   * @param opts
   *   Mapping of data source options, which should include either 'url' or 'service'.
   * @return
   *   The inferred schema of the STAC data source table.
   * @throws IllegalArgumentException
   *   If neither 'url' nor 'service' are provided.
   */
  def inferStacSchema(opts: Map[String, String]): StructType = {
    val stacCollectionJsonString = loadStacCollectionToJson(opts)

    // Create the ObjectMapper
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // Parse the STAC collection JSON
    val collection = mapper.readTree(stacCollectionJsonString)

    // Extract the stac_version
    val stacVersion = collection.get("stac_version").asText()

    // Return the corresponding schema based on the stac_version
    stacVersion match {
      case "1.0.0" => StacTable.SCHEMA_V1_0_0
      case version if version.matches("1\\.[1-9]\\d*\\.\\d*") => StacTable.SCHEMA_V1_1_0
      // Add more cases here for other versions if needed
      case _ => throw new IllegalArgumentException(s"Unsupported STAC version: $stacVersion")
    }
  }

  /**
   * Promote the properties field to the top level of the row.
   */
  def promotePropertiesToTop(row: InternalRow, schema: StructType): InternalRow = {
    val propertiesIndex = schema.fieldIndex("properties")
    val propertiesStruct = schema("properties").dataType.asInstanceOf[StructType]
    val propertiesRow = row.getStruct(propertiesIndex, propertiesStruct.fields.length)

    val newValues = schema.fields.zipWithIndex.flatMap {
      case (field, index) if field.name == "properties" =>
        propertiesStruct.fields.zipWithIndex.map { case (propField, propIndex) =>
          propertiesRow.get(propIndex, propField.dataType)
        }
      case (_, index) => Seq(row.get(index, schema(index).dataType))
    }

    InternalRow.fromSeq(newValues)
  }

  /**
   * Update the schema to include the promoted properties.
   *
   * @param schema
   *   The schema to update.
   * @return
   *   The updated schema.
   */
  def updatePropertiesPromotedSchema(schema: StructType): StructType = {
    val propertiesIndex = schema.fieldIndex("properties")
    val propertiesStruct = schema("properties").dataType.asInstanceOf[StructType]

    val newFields = schema.fields.flatMap {
      case StructField("properties", _, _, _) => propertiesStruct.fields
      case other => Seq(other)
    }

    StructType(newFields)
  }

  /**
   * Convert a URL-encoded string to outdb raster binary.
   *
   * @param url
   *   The URL-encoded string.
   * @return
   *   The raster binary.
   */
  def linkToRaster(link: String, conf: Configuration): Array[Byte] = {
    val path = new Path(new URI(link))
    val raster = new LazyLoadOutDbGridCoverage2D(link, path, conf)
    RasterUDT.serialize(raster)
  }

  /**
   * Builds the output row with the raster field in the assets map.
   *
   * @param row
   *   The input row.
   * @param schema
   *   The schema of the input row.
   * @return
   *   The output row with the raster field in the assets map.
   */
  def buildOutDbRasterFields(
      row: InternalRow,
      schema: StructType,
      configuration: Configuration): InternalRow = {
    val newValues = new Array[Any](schema.fields.length)

    schema.fields.zipWithIndex.foreach {
      case (StructField("assets", MapType(StringType, valueType: StructType, _), _, _), index) =>
        val assetsMap = row.getMap(index)
        if (assetsMap != null) {
          val updatedAssets = assetsMap
            .keyArray()
            .array
            .zip(assetsMap.valueArray().array)
            .map { case (key, value) =>
              val assetRow = value.asInstanceOf[InternalRow]
              if (assetRow != null) {
                val hrefIndex = valueType.fieldIndex("href")
                val typeIndex = valueType.fieldIndex("type")
                val href = Try(Option(assetRow.getString(hrefIndex))).getOrElse(None)
                val assetType = Try(Option(assetRow.getString(typeIndex))).getOrElse(None)
                val rast =
                  if (href.isDefined && assetType.isDefined && isImageAssetType(assetType.get)) {
                    linkToRaster(href.get, configuration)
                  } else {
                    null
                  }
                val newAssetValues = new Array[Any](valueType.fields.length + 1)
                valueType.fields.zipWithIndex.foreach { case (field, i) =>
                  newAssetValues(i) = assetRow.get(i, field.dataType)
                }
                newAssetValues(valueType.fields.length) = rast
                key -> InternalRow.fromSeq(newAssetValues)
              } else {
                key -> null
              }
            }
            .toMap
          newValues(index) = ArrayBasedMapData(updatedAssets)
        } else {
          newValues(index) = null
        }
      case (_, index) =>
        newValues(index) = row.get(index, schema.fields(index).dataType)
    }

    InternalRow.fromSeq(newValues)
  }

  /**
   * Update the schema to include the raster field in the assets map.
   *
   * @param schema
   *   The schema to update.
   * @return
   *   The updated schema.
   */
  def updateRasterAddedSchema(schema: StructType): StructType = {
    val newFields = schema.fields.map {
      case StructField(
            "assets",
            MapType(StringType, valueType: StructType, nullable1),
            nullable2,
            metadata) =>
        val newValueType =
          StructType(valueType.fields :+ StructField("rast", new RasterUDT(), nullable = true))
        StructField("assets", MapType(StringType, newValueType, nullable1), nullable2, metadata)
      case other => other
    }

    StructType(newFields)
  }

  /**
   * Returns the number of partitions to use for reading the data.
   *
   * The number of partitions is determined based on the number of items, the number of partitions
   * requested, the maximum number of item files per partition, and the default parallelism.
   *
   * @param itemCount
   *   The number of items in the collection.
   * @param numPartitions
   *   The number of partitions requested.
   * @param maxPartitionItemFiles
   *   The maximum number of item files per partition.
   * @param defaultParallelism
   *   The default parallelism.
   * @return
   *   The number of partitions to use for reading the data.
   */
  def getNumPartitions(
      itemCount: Int,
      numPartitions: Int,
      maxPartitionItemFiles: Int,
      defaultParallelism: Int): Int = {
    if (numPartitions > 0) {
      numPartitions
    } else {
      val maxSplitFiles = if (maxPartitionItemFiles > 0) {
        Math.min(maxPartitionItemFiles, Math.ceil(itemCount.toDouble / defaultParallelism).toInt)
      } else {
        Math.ceil(itemCount.toDouble / defaultParallelism).toInt
      }
      Math.max(1, Math.ceil(itemCount.toDouble / maxSplitFiles).toInt)
    }
  }

  /**
   * Returns the asset type based on the content type.
   *
   * @param assetType
   *   The content type of the asset.
   * @return
   *   The asset type.
   */
  def getAssetType(assetType: String): AssetType = {
    assetType.toLowerCase match {
      case t if t.startsWith("image/tiff; application=geotiff") => GeoTIFF
      case t if t.startsWith("image/jp2") => JPEG2000
      case t if t.startsWith("image/png") => PNG
      case t if t.startsWith("image/jpeg") => JPEG
      case t if t.startsWith("text/xml") || t.startsWith("application/xml") => XML
      case t if t.startsWith("application/json") => JSON
      case t if t.startsWith("text/plain") => PlainText
      case t if t.startsWith("application/geo+json") => GeoJSON
      case t if t.startsWith("application/geopackage+sqlite3") => GeoPackage
      case t if t.startsWith("application/x-hdf5") => HDF5
      case t if t.startsWith("application/x-hdf") => HDF
      case t if t.startsWith("application/vnd.laszip+copc") => COPC
      case t if t.startsWith("application/vnd.apache.parquet") => Parquet
      case t if t.startsWith("application/3dtiles+json") => Tiles3D
      case t if t.startsWith("application/vnd.pmtiles") => PMTiles
      case _ => Other
    }
  }

  /**
   * Returns whether the asset type is an image asset type.
   *
   * @param assetType
   *   The asset type.
   * @return
   *   Whether the asset type is an image asset type.
   */
  def isImageAssetType(assetType: String): Boolean = {
    getAssetType(assetType) match {
      case GeoTIFF | JPEG2000 | PNG | JPEG => true
      case _ => false
    }
  }

  /** Returns the temporal filter string based on the temporal filter. */
  def getFilterBBox(filter: GeoParquetSpatialFilter): String = {
    def calculateUnionBBox(filter: GeoParquetSpatialFilter): Envelope = {
      filter match {
        case GeoParquetSpatialFilter.AndFilter(left, right) =>
          val leftEnvelope = calculateUnionBBox(left)
          val rightEnvelope = calculateUnionBBox(right)
          leftEnvelope.expandToInclude(rightEnvelope)
          leftEnvelope
        case GeoParquetSpatialFilter.OrFilter(left, right) =>
          val leftEnvelope = calculateUnionBBox(left)
          val rightEnvelope = calculateUnionBBox(right)
          leftEnvelope.expandToInclude(rightEnvelope)
          leftEnvelope
        case leaf: GeoParquetSpatialFilter.LeafFilter =>
          leaf.queryWindow.getEnvelopeInternal
      }
    }

    val unionEnvelope = calculateUnionBBox(filter)
    s"bbox=${unionEnvelope.getMinX}%2C${unionEnvelope.getMinY}%2C${unionEnvelope.getMaxX}%2C${unionEnvelope.getMaxY}"
  }

  /** Returns the temporal filter string based on the temporal filter. */
  def getFilterTemporal(filter: TemporalFilter): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

    def formatDateTime(dateTime: LocalDateTime): String = {
      if (dateTime == null) ".." else dateTime.format(formatter)
    }

    def calculateUnionTemporal(filter: TemporalFilter): (LocalDateTime, LocalDateTime) = {
      filter match {
        case TemporalFilter.AndFilter(left, right) =>
          val (leftStart, leftEnd) = calculateUnionTemporal(left)
          val (rightStart, rightEnd) = calculateUnionTemporal(right)
          val start =
            if (leftStart == null || (rightStart != null && rightStart.isBefore(leftStart)))
              rightStart
            else leftStart
          val end =
            if (leftEnd == null || (rightEnd != null && rightEnd.isAfter(leftEnd))) rightEnd
            else leftEnd
          (start, end)
        case TemporalFilter.OrFilter(left, right) =>
          val (leftStart, leftEnd) = calculateUnionTemporal(left)
          val (rightStart, rightEnd) = calculateUnionTemporal(right)
          val start =
            if (leftStart == null || (rightStart != null && rightStart.isBefore(leftStart)))
              rightStart
            else leftStart
          val end =
            if (leftEnd == null || (rightEnd != null && rightEnd.isAfter(leftEnd))) rightEnd
            else leftEnd
          (start, end)
        case TemporalFilter.LessThanFilter(_, value) =>
          (null, value)
        case TemporalFilter.GreaterThanFilter(_, value) =>
          (value, null)
        case TemporalFilter.EqualFilter(_, value) =>
          (value, value)
      }
    }

    val (start, end) = calculateUnionTemporal(filter)
    if (end == null) s"datetime=${formatDateTime(start)}/.."
    else s"datetime=${formatDateTime(start)}/${formatDateTime(end)}"
  }

  /** Adds the spatial and temporal filters to the base URL. */
  def addFiltersToUrl(
      baseUrl: String,
      spatialFilter: Option[GeoParquetSpatialFilter],
      temporalFilter: Option[TemporalFilter]): String = {
    val spatialFilterStr = spatialFilter.map(StacUtils.getFilterBBox).getOrElse("")
    val temporalFilterStr = temporalFilter.map(StacUtils.getFilterTemporal).getOrElse("")

    val filters = Seq(spatialFilterStr, temporalFilterStr).filter(_.nonEmpty).mkString("&")
    val urlWithFilters = if (filters.nonEmpty) s"&$filters" else ""
    s"$baseUrl$urlWithFilters"
  }
}
