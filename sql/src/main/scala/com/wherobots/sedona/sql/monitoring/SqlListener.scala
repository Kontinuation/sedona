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

import com.google.gson.{Gson, JsonArray, JsonObject}
import com.wherobots.sedona.common.monitoring.S3Utils
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.io.{PrintWriter, StringWriter}
import java.sql.Timestamp
import java.util.UUID

class SqlListener(userid:String, s3bucket:String, bucketPrefix:String, s3client:S3AsyncClient, product:String) extends QueryExecutionListener {
  private val gson = new Gson()

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    produceLog(qe, null)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    produceLog(qe, exception)
  }

  def produceLog(qe: QueryExecution, exception: Exception): Unit = {
    // Upload the log after each sql query
    val json = qe.executedPlan.toJSON
    val timestamp = new Timestamp(System.currentTimeMillis()).getTime.toString
    val objectKey = bucketPrefix + "/" + timestamp + "-" + UUID.randomUUID()
    val planJson = gson.fromJson(json, classOf[JsonArray])
    val userid = qe.sparkSession.conf.get("wherobots.userid", "dummy@test.com")
    // Prepare the log record
    val log = new JsonObject
    log.addProperty("userid", userid)
    log.addProperty("type", "sql")
    log.addProperty("timestamp", timestamp)
    log.addProperty("product", product)
    log.add("executedPlan", planJson)
    if (exception != null) {
      log.addProperty("exception", exception.getMessage)
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      exception.printStackTrace(pw)
      log.addProperty("exceptionStackTrace", sw.toString)
    }
    S3Utils.putObject(s3client, s3bucket, objectKey, log.toString);
//    println(log)
  }
}
