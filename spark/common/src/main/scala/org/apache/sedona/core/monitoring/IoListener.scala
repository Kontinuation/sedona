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

import org.apache.log4j.Logger
import org.apache.spark.metrics.source.SedonaMetrics
import org.apache.spark.scheduler._

class IoListener(sedonaMetrics: SedonaMetrics) extends SparkListener {
  private val logger = Logger.getLogger("Sedona IO Metrics Monitor")

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    sedonaMetrics.jobsCompleteMetric.inc()
    logger.info("Reported Sedona IO metrics")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    sedonaMetrics.stagesCompleteMetric.inc()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    sedonaMetrics.tasksCompleteMetric.inc()
    sedonaMetrics.executorRuntimeMetric.inc(taskEnd.taskMetrics.executorRunTime)
    sedonaMetrics.recordsReadMetric.inc(taskEnd.taskMetrics.inputMetrics.recordsRead)
    sedonaMetrics.recordsWrittenMetric.inc(taskEnd.taskMetrics.outputMetrics.recordsWritten)
    sedonaMetrics.bytesReadMetric.inc(taskEnd.taskMetrics.inputMetrics.bytesRead)
    sedonaMetrics.bytesWrittenMetric.inc(taskEnd.taskMetrics.outputMetrics.bytesWritten)
  }
}
