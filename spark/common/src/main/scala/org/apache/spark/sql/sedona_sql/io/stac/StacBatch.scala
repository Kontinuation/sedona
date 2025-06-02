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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.execution.datasources.parquet.{GeoParquetSpatialFilter, GeometryFieldMetaData}
import org.apache.spark.sql.sedona_sql.io.stac.StacBatch.{parseTemporalIntervalsByDay, parseTemporalIntervalsByMonth}
import org.apache.spark.sql.sedona_sql.io.stac.StacUtils.getNumPartitions
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.net.URLEncoder
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import scala.util.Random
import scala.util.control.Breaks.breakable

// For Scala 2.12 and 2.13 compatibility
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._
//import scala.jdk.CollectionConverters._ // For Scala 2.13, will be ignored in 2.12

/**
 * The `StacBatch` class represents a batch of partitions for reading data in the SpatioTemporal
 * Asset Catalog (STAC) data source. It implements the `Batch` interface from Apache Spark's data
 * source API.
 *
 * This class provides methods to plan input partitions and create a partition reader factory,
 * which are necessary for batch data processing.
 */
case class StacBatch(
    broadcastConf: Broadcast[SerializableConfiguration],
    stacCollectionUrl: String,
    stacCollectionJson: String,
    schema: StructType,
    opts: Map[String, String],
    spatialFilter: Option[GeoParquetSpatialFilter],
    temporalFilter: Option[TemporalFilter],
    limitFilter: Option[Int])
    extends Batch {

  private val defaultItemsLimitPerRequest: Int = {
    val itemsLimitMax = opts.getOrElse("itemsLimitMax", "-1").toInt
    val limitPerRequest = opts.getOrElse("itemsLimitPerRequest", "10").toInt
    if (itemsLimitMax > 0 && limitPerRequest > itemsLimitMax) itemsLimitMax else limitPerRequest
  }
  private val itemsLoadProcessReportThreshold =
    opts.getOrElse("itemsLoadProcessReportThreshold", "1000000").toInt
  private var itemMaxLeft: Int = -1
  private var lastReportCount: Int = 0

  val mapper = new ObjectMapper()

  /**
   * Sets the maximum number of items left to process.
   *
   * @param value
   *   The maximum number of items left.
   */
  def setItemMaxLeft(value: Int): Unit = {
    itemMaxLeft = value
  }

  /**
   * Plans the input partitions for reading data from the STAC data source.
   *
   * @return
   *   An array of input partitions for reading STAC data.
   */
  override def planInputPartitions(): Array[InputPartition] = {
    val stacCollectionBasePath = StacUtils.getStacCollectionBasePath(stacCollectionUrl)

    // Initialize the itemLinks array
    val itemLinks = scala.collection.mutable.ArrayBuffer[String]()

    // Get the maximum number of items to process
    val itemsLimitMax = limitFilter match {
      case Some(limit) if limit >= 0 => limit
      case _ => opts.getOrElse("itemsLimitMax", "-1").toInt
    }
    val checkItemsLimitMax = itemsLimitMax > 0

    // Start the recursive collection of item links
    setItemMaxLeft(itemsLimitMax)

    collectItemLinks(
      stacCollectionBasePath,
      stacCollectionUrl,
      stacCollectionJson,
      itemLinks,
      checkItemsLimitMax)

    // Handle when the number of items is less than 1
    if (itemLinks.isEmpty) {
      return Array.empty[InputPartition]
    }

    val numPartitions = getNumPartitions(
      itemLinks.length,
      opts.getOrElse("numPartitions", "-1").toInt,
      opts.getOrElse("maxPartitionItemFiles", "-1").toInt,
      opts.getOrElse("defaultParallelism", "1").toInt)

    // Handle when the number of items is less than the number of partitions
    if (itemLinks.length < numPartitions) {
      return itemLinks.zipWithIndex.map { case (item, index) =>
        StacPartition(index, Array(item), new java.util.HashMap[String, String]())
      }.toArray
    }

    // Determine how many items to put in each partition
    val partitionSize = Math.ceil(itemLinks.length.toDouble / numPartitions).toInt

    // Group the item links into partitions, but randomize first for better load balancing
    Random
      .shuffle(itemLinks)
      .grouped(partitionSize)
      .zipWithIndex
      .map { case (items, index) =>
        // Create a StacPartition for each group of items
        StacPartition(index, items.toArray, new java.util.HashMap[String, String]())
      }
      .toArray
  }

  /**
   * Recursively processes collections and collects item links.
   *
   * @param collectionBasePath
   *   The base path of the STAC collection.
   * @param collectionJson
   *   The JSON string representation of the STAC collection.
   * @param itemLinks
   *   The list of item links to populate.
   */
  def collectItemLinks(
      collectionBasePath: String,
      collectionUrl: String,
      collectionJson: String,
      itemLinks: scala.collection.mutable.ArrayBuffer[String],
      needCountNextItems: Boolean): Unit = {

    // end early if there are no more items to process
    if (needCountNextItems && itemMaxLeft <= 0) return

    if (itemLinks.size - lastReportCount >= itemsLoadProcessReportThreshold) {
      Console.out.println(s"Searched or partitioned ${itemLinks.size} items so far.")
      lastReportCount = itemLinks.size
    }

    def iterateItemsWithLimit(itemUrl: String, needCountNextItems: Boolean): Boolean = {
      // Load the item URL and process the response
      var nextUrl: Option[String] = Some(itemUrl)
      breakable {
        while (nextUrl.isDefined) {
          val itemJson = StacUtils.loadStacCollectionToJson(nextUrl.get)
          val itemRootNode = mapper.readTree(itemJson)
          // Check if there exists a "next" link
          val itemLinksNode = itemRootNode.get("links")
          if (itemLinksNode == null) {
            return true
          }
          val itemIterator = itemLinksNode.elements()
          nextUrl = None
          while (itemIterator.hasNext) {
            val itemLinkNode = itemIterator.next()
            val itemRel = itemLinkNode.get("rel").asText()
            val itemHref = itemLinkNode.get("href").asText()
            if (itemRel == "next") {
              // Only check the number of items returned if there are more items to process
              val numberReturnedNode = itemRootNode.get("numberReturned")
              val numberReturned = if (numberReturnedNode == null) {
                // From STAC API Spec:
                // The optional limit parameter limits the number of
                // items that are presented in the response document.
                // The default value is 10.
                defaultItemsLimitPerRequest
              } else {
                numberReturnedNode.asInt()
              }
              // count the number of items returned and left to be processed
              itemMaxLeft = itemMaxLeft - numberReturned
              // early exit if there are no more items to process
              if (needCountNextItems && itemMaxLeft <= 0) {
                return true
              }
              nextUrl = Some(if (itemHref.startsWith("http") || itemHref.startsWith("file")) {
                itemHref
              } else {
                collectionBasePath + itemHref
              })
            }
          }
          if (nextUrl.isDefined) {
            itemLinks += nextUrl.get
          }
        }
      }
      false
    }

    // Parse the JSON string into a JsonNode (tree representation of JSON)
    val rootNode: JsonNode = mapper.readTree(collectionJson)

    // Extract item links from the "links" array
    val collectType = rootNode.get("type").asText()
    val linksNode = rootNode.get("links")
    val iterator = linksNode.elements()

    // Check if the collection is a "FeatureCollection" or "Collection"
    if (collectType == "FeatureCollection") {
      // If the collection is a "FeatureCollection", we need to check if there are any items
      itemLinks += getItemLink(
        collectionUrl,
        defaultItemsLimitPerRequest,
        spatialFilter,
        temporalFilter)
      iterateItemsWithLimit(
        getItemLink(collectionUrl, defaultItemsLimitPerRequest, spatialFilter, temporalFilter),
        needCountNextItems)
      return
    } else if (collectType != "Collection") {
      // throw an error if the collection type is not "Collection" or "FeatureCollection"
      throw new IllegalArgumentException(
        s"Invalid STAC collection type: '$collectType'. Expected 'Collection' or 'FeatureCollection'.")
    }

    // Iterate through the links in the collection
    while (iterator.hasNext) {
      val linkNode = iterator.next()
      val rel = linkNode.get("rel").asText()
      val href = linkNode.get("href").asText()

      // item links are identified by the "rel" value of "item" or "items"
      if (rel == "item" || rel == "items") {
        // need to handle relative paths and local file paths
        val itemUrl = if (href.startsWith("http") || href.startsWith("file")) {
          href
        } else {
          collectionBasePath + href
        }
        if (rel == "items" && href.startsWith("http")) {
          if (spatialFilter.isEmpty && temporalFilter.isEmpty && limitFilter.isEmpty && !needCountNextItems) {
            // if no filters are provided, add the item link with the default limit
            // we split the temporal intervals into monthly or daily intervals
            // and construct the item links for each interval
            // such way, we distribute the items into partitions based on the temporal intervals on to spark executors
            val temporalPartitionInterval =
              opts.getOrElse("temporalPartitionInterval", "month").toLowerCase
            val temporalIntervals = temporalPartitionInterval match {
              case "month" => parseTemporalIntervalsByMonth(collectionJson)
              case "day" => parseTemporalIntervalsByDay(collectionJson)
              case invalid =>
                throw new IllegalArgumentException(
                  s"Invalid temporalPartitionInterval: '$invalid'. Valid values are 'day' or 'month'.")
            }
            temporalIntervals.foreach { interval =>
              val itemsLink = itemUrl + "?datetime=" + URLEncoder.encode(
                interval,
                "UTF-8") + "&limit=" + defaultItemsLimitPerRequest + "&checknext=true"
              itemLinks += itemsLink
            }
          } else {
            // if spatial or temporal filters are provided, add the item link with the filters
            itemLinks += getItemLink(
              itemUrl,
              defaultItemsLimitPerRequest,
              spatialFilter,
              temporalFilter)
          }
        } else {
          itemLinks += itemUrl
        }
        if (needCountNextItems && itemMaxLeft <= 0) {
          return
        } else {
          if (rel == "item" && needCountNextItems) {
            // count the number of items returned and left to be processed
            itemMaxLeft = itemMaxLeft - 1
          } else if (rel == "items" && href.startsWith("http")) {
            if (spatialFilter.isEmpty && temporalFilter.isEmpty && limitFilter.isEmpty && !needCountNextItems) {
              return // no need to iterate through the items if no filters are provided
            }
            // iterate through the items and check if the limit is reached (if needed)
            if (iterateItemsWithLimit(
                getItemLink(itemUrl, defaultItemsLimitPerRequest, spatialFilter, temporalFilter),
                needCountNextItems)) return
          }
        }
      } else if (rel == "child") {
        val childUrl = if (href.startsWith("http") || href.startsWith("file")) {
          href
        } else {
          collectionBasePath + href
        }
        // Recursively process the linked collection
        val linkedCollectionJson = StacUtils.loadStacCollectionToJson(childUrl)
        val nestedCollectionBasePath = StacUtils.getStacCollectionBasePath(childUrl)
        val collectionFiltered =
          filterCollection(linkedCollectionJson, spatialFilter, temporalFilter)

        if (!collectionFiltered) {
          collectItemLinks(
            nestedCollectionBasePath,
            childUrl,
            linkedCollectionJson,
            itemLinks,
            needCountNextItems)
        }
      }
    }
  }

  /** Adds an item link to the list of item links. */
  def getItemLink(
      itemUrl: String,
      defaultItemsLimitPerRequest: Int,
      spatialFilter: Option[GeoParquetSpatialFilter],
      temporalFilter: Option[TemporalFilter]): String = {
    val separator = if (itemUrl.contains("?")) "&" else "?"
    val baseUrl = itemUrl + separator + "limit=" + defaultItemsLimitPerRequest
    val urlWithFilters = StacUtils.addFiltersToUrl(baseUrl, spatialFilter, temporalFilter)
    urlWithFilters
  }

  /**
   * Filters a collection based on the provided spatial and temporal filters.
   *
   * @param collectionJson
   *   The JSON string representation of the STAC collection.
   * @param spatialFilter
   *   The spatial filter to apply to the collection.
   * @param temporalFilter
   *   The temporal filter to apply to the collection.
   * @return
   *   `true` if the collection is filtered out, `false` otherwise.
   */
  def filterCollection(
      collectionJson: String,
      spatialFilter: Option[GeoParquetSpatialFilter],
      temporalFilter: Option[TemporalFilter]): Boolean = {

    val mapper = new ObjectMapper()
    val rootNode: JsonNode = mapper.readTree(collectionJson)

    // Filter based on spatial extent
    val spatialFiltered = spatialFilter match {
      case Some(filter) =>
        val extentNode = rootNode.path("extent").path("spatial").path("bbox")
        if (extentNode.isMissingNode) {
          false
        } else {
          val bbox = extentNode
            .elements()
            .map { bboxNode =>
              val minX = bboxNode.get(0).asDouble()
              val minY = bboxNode.get(1).asDouble()
              val maxX = bboxNode.get(2).asDouble()
              val maxY = bboxNode.get(3).asDouble()
              (minX, minY, maxX, maxY)
            }
            .toList

          !bbox.exists { case (minX, minY, maxX, maxY) =>
            val geometryTypes = Seq("Polygon")
            val bbox = Seq(minX, minY, maxX, maxY)

            val geometryFieldMetaData = GeometryFieldMetaData(
              encoding = "WKB",
              geometryTypes = geometryTypes,
              bbox = bbox,
              crs = None,
              covering = None)

            filter.evaluate(Map("geometry" -> geometryFieldMetaData))
          }
        }
      case None => false
    }

    // Filter based on temporal extent
    val temporalFiltered = temporalFilter match {
      case Some(filter) =>
        val extentNode = rootNode.path("extent").path("temporal").path("interval")
        if (extentNode.isMissingNode) {
          // if extent is missing, we assume the collection is not filtered
          true
        } else {
          // parse the temporal intervals
          val formatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)
            .optionalEnd()
            .appendPattern("'Z'")
            .toFormatter()

          val intervals = extentNode
            .elements()
            .map { intervalNode =>
              val start = LocalDateTime.parse(intervalNode.get(0).asText(), formatter)
              val end = LocalDateTime.parse(intervalNode.get(1).asText(), formatter)
              (start, end)
            }
            .toList

          // check if the filter evaluates to true for any of the interval start or end times
          !intervals.exists { case (start, end) =>
            filter.evaluate(Map("datetime" -> start)) ||
            filter.evaluate(Map("datetime" -> end))
          }
        }
      // if the collection is not filtered, return false
      case None => false
    }

    spatialFiltered || temporalFiltered
  }

  /**
   * Creates a partition reader factory for reading data from the STAC data source.
   *
   * @return
   *   A partition reader factory for reading STAC data.
   */
  override def createReaderFactory(): PartitionReaderFactory = { (partition: InputPartition) =>
    {
      new StacPartitionReader(
        broadcastConf,
        partition.asInstanceOf[StacPartition],
        schema,
        opts,
        spatialFilter,
        temporalFilter)
    }
  }
}

object StacBatch {

  /**
   * Parses the temporal interval information from a STAC collection JSON and breaks it down into
   * monthly chunks for optimized processing.
   *
   * This method:
   *   1. Extracts temporal intervals from the collection's metadata 2. For each interval, breaks
   *      it into month-by-month periods 3. Formats each period as "start/end" timestamp strings
   *      compatible with STAC API queries
   *
   * This monthly chunking approach optimizes driver performance by:
   *   - Enabling more efficient parallel processing across Spark executors
   *   - Preventing the driver from having to process large result sets from a single query
   *   - Allowing temporal-based partitioning for better load balancing
   *
   * Used primarily when querying STAC APIs without filters to ensure manageable data volumes by
   * distributing workload across time periods rather than loading all data at once.
   *
   * Ref: https://api.stacspec.org/v1.0.0/ogcapi-features/#tag/Features/operation/getFeatures
   *
   * @param collectionJson
   *   The JSON string representation of a STAC collection
   * @return
   *   Array of formatted temporal interval strings (format: "start/end")
   */
  def parseTemporalIntervalsByMonth(collectionJson: String): Array[String] = {
    val mapper = new ObjectMapper()
    val rootNode: JsonNode = mapper.readTree(collectionJson)
    val temporalNode = rootNode.path("extent").path("temporal").path("interval")
    val intervals = new scala.collection.mutable.ArrayBuffer[String]()
    val (formatter, outputFormatter) = createTemporalFormatters()
    val today = LocalDate.now(ZoneOffset.UTC).atStartOfDay()

    temporalNode.elements().asScala.foreach { intervalNode =>
      val start = LocalDateTime
        .parse(intervalNode.get(0).asText(), formatter)
        .withDayOfMonth(1) // First day of month
        .withHour(0) // 00 hours
        .withMinute(0) // 00 minutes
        .withSecond(0) // 00 seconds
        .withNano(0) // 000000 nanoseconds

      val rawEnd =
        if (intervalNode.get(1).isNull) today
        else LocalDateTime.parse(intervalNode.get(1).asText(), formatter)
      val end = rawEnd
        .withDayOfMonth(rawEnd.toLocalDate.lengthOfMonth()) // Last day of month
        .withHour(23) // 23 hours
        .withMinute(59) // 59 minutes
        .withSecond(59) // 59 seconds
        .withNano(999999999) // 999999999 nanoseconds

      var current = start.withDayOfMonth(1)
      while (!current.isAfter(end)) {
        val next = current
          .plusMonths(1)
          .withDayOfMonth(1)
          .minusDays(1)
          .withHour(23)
          .withMinute(59)
          .withSecond(59)
          .withNano(999999999)
        val intervalEnd =
          if (next.isAfter(end))
            end.withHour(23).withMinute(59).withSecond(59).withNano(999999999)
          else next
        intervals += s"${current.format(outputFormatter)}/${intervalEnd.format(outputFormatter)}"
        current = current.plusMonths(1).withDayOfMonth(1)
      }
    }

    intervals.toArray
  }

  /**
   * Parses the temporal interval information from a STAC collection JSON and breaks it down into
   * daily chunks for finer-grained optimized processing.
   *
   * This method:
   *   1. Extracts temporal intervals from the collection's metadata 2. For each interval, breaks
   *      it into day-by-day periods 3. Formats each period as "start/end" timestamp strings
   *      compatible with STAC API queries
   *
   * This daily chunking approach provides more granular parallelism than monthly chunking:
   *   - Creates smaller, more numerous partitions for better work distribution
   *   - Allows for more precise temporal filtering at the day level
   *   - Further reduces the risk of timeouts on large STAC collections
   *   - May improve load balancing across executors with daily-level granularity
   *
   * Useful for very large collections or when more fine-grained parallelism is needed.
   *
   * @param collectionJson
   *   The JSON string representation of a STAC collection
   * @return
   *   Array of formatted daily temporal interval strings (format: "start/end")
   */
  def parseTemporalIntervalsByDay(collectionJson: String): Array[String] = {
    val mapper = new ObjectMapper()
    val rootNode: JsonNode = mapper.readTree(collectionJson)
    val temporalNode = rootNode.path("extent").path("temporal").path("interval")
    val intervals = new scala.collection.mutable.ArrayBuffer[String]()
    val (formatter, outputFormatter) = createTemporalFormatters()
    val today = LocalDate.now(ZoneOffset.UTC).atStartOfDay()

    temporalNode.elements().asScala.foreach { intervalNode =>
      val start = LocalDateTime
        .parse(intervalNode.get(0).asText(), formatter)
        .withHour(0) // 00 hours
        .withMinute(0) // 00 minutes
        .withSecond(0) // 00 seconds
        .withNano(0) // 000000 nanoseconds

      val rawEnd =
        if (intervalNode.get(1).isNull) today
        else LocalDateTime.parse(intervalNode.get(1).asText(), formatter)
      val end = rawEnd
        .withHour(23) // 23 hours
        .withMinute(59) // 59 minutes
        .withSecond(59) // 59 seconds
        .withNano(999999999) // 999999999 nanoseconds

      var current = start
      while (!current.isAfter(end)) {
        // Calculate the end of current day (23:59:59.999999999)
        val intervalEnd = current
          .withHour(23)
          .withMinute(59)
          .withSecond(59)
          .withNano(999999999)

        // Use the calculated day end or actual end date, whichever comes first
        val dayEnd = if (intervalEnd.isAfter(end)) end else intervalEnd

        // Format and add the interval to our collection
        intervals += s"${current.format(outputFormatter)}/${dayEnd.format(outputFormatter)}"

        // Move to the next day
        current = current.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0)
      }
    }

    intervals.toArray
  }

  /**
   * Creates DateTimeFormatter instances for parsing and formatting STAC temporal intervals.
   *
   * The input formatter is flexible and handles various date formats:
   *   - 2017-01-01
   *   - 2017-01-01T00:00:00Z
   *   - 2017-01-01T00:00:00.000Z
   *   - 2017-01-01T00:00:00.000000Z
   *
   * The output formatter produces consistent date format for STAC API queries.
   *
   * @return
   *   A tuple of (inputFormatter, outputFormatter)
   */
  private def createTemporalFormatters(): (DateTimeFormatter, DateTimeFormatter) = {
    val inputFormatter = new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd")
      // Make time part optional
      .optionalStart()
      .appendLiteral('T')
      .appendPattern("HH:mm:ss")
      // Make fractional seconds optional with variable precision
      .optionalStart()
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .optionalEnd()
      .appendLiteral('Z')
      .optionalEnd()
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
      .toFormatter()

    val outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")

    (inputFormatter, outputFormatter)
  }
}
