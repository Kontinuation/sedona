/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wherobots.sedona.sql.monitoring

import com.google.gson.{Gson, JsonObject}
import com.wherobots.sedona.common.monitoring.{CloudWatchUtils, S3Utils}
import org.apache.log4j.Logger
import org.apache.sedona.sql.UDF.Catalog
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.s3.S3Client

import java.io.{PrintWriter, StringWriter}
import java.sql.Timestamp
import java.util
import java.util.UUID
import java.util.regex.Pattern

class SqlListener(userid:String, s3bucket:String, bucketPrefix:String, s3client:S3Client, cwClient:CloudWatchClient
                  , product:String, dimensionDataPoints:util.HashMap[String, String])
  extends QueryExecutionListener {
  private val gson = new Gson()
  private val logger = Logger.getLogger("Wherobots SQL Metrics Monitor")
  private val dimensionDataPointsUpdated = new util.HashMap[String, String](dimensionDataPoints)
  dimensionDataPointsUpdated.put("metric-type", "sql-funcs")

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    produceLog(qe, null)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    produceLog(qe, exception)
  }

  def produceLog(qe: QueryExecution, exception: Exception): Unit = {
    // Upload the log after each sql query
    var planAnalyzed = qe.analyzed.treeString(verbose = false)
    val planPhysical = qe.sparkPlan.treeString(verbose = false)
    val planAll = planAnalyzed + "\n" + planPhysical
//    println("analyze: " + planAnalyzed)
//    println("physical: " + planPhysical)
//    println("all: " + planAll)

    val functionCallMap = initFunctionCallMap()
    // Find ST aggregate func such as ST_Union_Aggr. This plan might show this func multiple times but we just keep one
    findMatch(planAnalyzed, Seq("(?<=\\.)(ST_[^@]+(?=@))"), functionCallMap)
    // Remove all these special ST aggregate funcs so they won't be caught by the next regex by mistake
    planAnalyzed = planAnalyzed.replaceAll("(?i)(ST_Envelope_Aggr|ST_Intersection_Aggr|ST_Union_Aggr)", "")
    // Find all regular ST funcs
    findMatch(planAnalyzed, Seq("(?i)(\\bST_[A-Za-z0-9]+|\\bRS_[A-Za-z0-9]+)"), functionCallMap)
    // Find all internal join algorithms
    findMatch(planPhysical, Seq("(?i)(\\bDistanceJoin|\\bRangeJoin|\\bBroadcastIndexJoin|\\bgeoparquet|\\bgeotiff)"), functionCallMap)
    val responseCW = CloudWatchUtils.putMetric(cwClient, s3bucket + "/" + bucketPrefix, functionCallMap, dimensionDataPointsUpdated)
    val timestamp = new Timestamp(System.currentTimeMillis()).getTime.toString
    val objectKey = bucketPrefix + "/" + timestamp + "-" + UUID.randomUUID()
    // Prepare the log record
    val log = new JsonObject
    log.addProperty("userid", userid)
    log.addProperty("type", "sql")
    log.addProperty("timestamp", timestamp)
    log.addProperty("product", product)
    log.addProperty("executedPlan", planAll)
    if (exception != null) {
      log.addProperty("exception", exception.getMessage)
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      exception.printStackTrace(pw)
      log.addProperty("exceptionStackTrace", sw.toString)
    }
    val responseS3 = S3Utils.putObject(s3client, s3bucket, objectKey, log.toString)
    logger.info("Query aggregator response: " + responseCW.sdkHttpResponse().statusCode() + " " + responseCW.sdkHttpResponse().isSuccessful)
    logger.info("Log response: " + responseS3.sdkHttpResponse().statusCode() + " " + responseS3.sdkHttpResponse().isSuccessful)
  }

  def findMatch(input:String, patterns:Seq[String], fMap:util.HashMap[String, java.lang.Double]):Unit = {
    patterns.foreach(pattern => {
      val matcher = Pattern.compile(pattern).matcher(input)
      while (matcher.find()) {
        fMap.computeIfPresent(matcher.group().toLowerCase, (k, v) => v + 1.0)
      }
    })
  }

  def initFunctionCallMap(): util.HashMap[String, java.lang.Double] = {
    val functionCallMap = new util.HashMap[String, java.lang.Double]
    Catalog.expressions.foreach(f => functionCallMap.put(f._1.funcName.toLowerCase, 0))
    Catalog.aggregateExpressions.foreach(f => functionCallMap.put(f.getClass.getSimpleName.toLowerCase, 0))
    val list = Seq(
      "DistanceJoin",
      "RangeJoin",
      "BroadcastIndexJoin",
      "geoparquet",
      "geotiff"
    )
    list.foreach(f => functionCallMap.put(f.toLowerCase, 0))
    functionCallMap
  }
}
