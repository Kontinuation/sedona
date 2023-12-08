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
package org.apache.sedona.core.monitoring

import com.codahale.metrics.Timer
import org.apache.log4j.Logger
import org.apache.spark.metrics.source.SedonaMetrics
import org.apache.spark.scheduler._

import java.util.concurrent.ConcurrentHashMap

class IoListener(sedonaMetrics: SedonaMetrics) extends SparkListener {
  private val logger = Logger.getLogger("Sedona IO Metrics Monitor")
  private val jobTimers = new ConcurrentHashMap[Int, Timer.Context]()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobTimers.put(jobStart.jobId, sedonaMetrics.jobExecutiontimeMetric.time())
  }
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobTimer = jobTimers.remove(jobEnd.jobId)
    jobTimer.stop()
    sedonaMetrics.jobsCompleteMetric.inc()
    jobEnd.jobResult match {
      case JobSucceeded => sedonaMetrics.jobSuccessMetric.inc()
      case _ => sedonaMetrics.jobFailedMetric.inc()
    }
    logger.info("Reported Sedona IO metrics")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    sedonaMetrics.stagesCompleteMetric.inc()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    sedonaMetrics.tasksCompleteMetric.inc()
    val taskMetrics = taskEnd.taskMetrics
    if (taskMetrics != null) {
      sedonaMetrics.executorRuntimeMetric.inc(taskMetrics.executorRunTime)
      sedonaMetrics.recordsReadMetric.inc(taskMetrics.inputMetrics.recordsRead)
      sedonaMetrics.recordsWrittenMetric.inc(taskMetrics.outputMetrics.recordsWritten)
      sedonaMetrics.bytesReadMetric.inc(taskMetrics.inputMetrics.bytesRead)
      sedonaMetrics.bytesWrittenMetric.inc(taskMetrics.outputMetrics.bytesWritten)
    }
  }
}
