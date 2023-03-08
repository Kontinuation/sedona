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

import com.google.gson.JsonObject
import com.wherobots.sedona.common.monitoring.S3Utils
import org.apache.spark.scheduler._
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.{Properties, UUID}

class IoListener(userid:String, s3bucket:String, bucketPrefix:String, s3client:S3AsyncClient, product:String) extends SparkListener {
  private val jobsCompleted = new AtomicInteger(0)
  private val stagesCompleted = new AtomicInteger(0)
  private val tasksCompleted = new AtomicInteger(0)
  private val executorRuntime = new AtomicLong(0L)
  private val recordsRead = new AtomicLong(0L)
  private val recordsWritten = new AtomicLong(0L)
  private val bytesRead = new AtomicLong(0L)
  private val bytesWritten = new AtomicLong(0L)


//  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
//    log.warn("***************** Aggregate metrics *****************************")
//    log.warn(s"* Jobs = ${jobsCompleted.get()}, Stages = ${stagesCompleted.get()}, Tasks = ${tasksCompleted}")
//    log.warn(s"* Executor runtime = ${executorRuntime.get()}ms, Records Read = ${recordsRead.get()}, Records written = ${recordsWritten.get()}")
//    log.warn("*****************************************************************")
//  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    // Since Spark allows concurrent jobs and stages, it is hard to correctly calculate statistics per job or per stage
    // The log here is just a snapshot of current counters at the end of each job. It does not guarantee that it
    // correctly reflects the statistics per job
    // For example, the jobCompleted counter may not even be 1 in the log
    // What we really care is that given a certain time period, these statistics are eventually correct
    jobsCompleted.incrementAndGet()
    produceLog(jobEnd.jobId, jobEnd.time)
    // Clean up the variables per job
    jobsCompleted.set(0)
    stagesCompleted.set(0)
    tasksCompleted.set(0)
    executorRuntime.set(0L)
    recordsRead.set(0L)
    recordsWritten.set(0L)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stagesCompleted.incrementAndGet()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    tasksCompleted.incrementAndGet()
    executorRuntime.addAndGet(taskEnd.taskMetrics.executorRunTime)
    recordsRead.addAndGet(taskEnd.taskMetrics.inputMetrics.recordsRead)
    recordsWritten.addAndGet(taskEnd.taskMetrics.outputMetrics.recordsWritten)
    bytesRead.addAndGet(taskEnd.taskMetrics.inputMetrics.bytesRead)
    bytesWritten.addAndGet(taskEnd.taskMetrics.outputMetrics.bytesWritten)
  }

  def produceLog(jobId:Int, jobEndTime:Long): Unit = {
    // Upload the log after each job
    // S3 object key is timestamp + UUID to avoid that multiple jobs finish the same time
    val objectKey = bucketPrefix + "/" + jobEndTime + "-" + UUID.randomUUID()
    // Prepare the log record
    val log = new JsonObject
    log.addProperty("userid", userid)
    log.addProperty("type", "io")
    log.addProperty("timestamp", jobEndTime)
    log.addProperty("product", product)
    log.addProperty("jobId", jobId)
    log.addProperty("jobsCompleted", jobsCompleted.get())
    log.addProperty("stagesCompleted", stagesCompleted.get())
    log.addProperty("tasksCompleted", tasksCompleted.get())
    log.addProperty("executorRuntime", executorRuntime.get())
    log.addProperty("recordsRead", recordsRead.get())
    log.addProperty("recordsWritten", recordsWritten.get())
    log.addProperty("bytesRead", bytesRead.get())
    log.addProperty("bytesWritten", bytesWritten.get())
    S3Utils.putObject(s3client, s3bucket, objectKey, log.toString);
//    println(log)
  }
  def getPropertyAsString(prop: Properties): String = {
    val writer = new StringWriter()
    prop.list(new PrintWriter(writer))
    writer.getBuffer.toString
  }
}
