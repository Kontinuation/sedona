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
package org.apache.spark.metrics.source
import com.codahale.metrics.{Counter, MetricRegistry}
import org.apache.sedona.sql.UDF.Catalog

import scala.collection.mutable

class SedonaMetrics extends Source {

  override val sourceName: String = "SedonaMetricSource"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val functionNames: mutable.Set[String] = getFunctionNames()
  val jobsCompleteMetric: Counter = createCounterMetric("jobsCompleted", "Number of Sedona jobs completed")
  val stagesCompleteMetric: Counter = createCounterMetric("stagesCompleted", "Number of Sedona stages completed")
  val tasksCompleteMetric: Counter = createCounterMetric("tasksCompleted", "Number of Sedona tasks completed")
  val executorRuntimeMetric: Counter = createCounterMetric("executorRuntime", "Total executor runtime in milliseconds")
  val recordsReadMetric: Counter = createCounterMetric("recordsRead", "Total records read")
  val recordsWrittenMetric: Counter = createCounterMetric("recordsWritten", "Total records written")
  val bytesReadMetric: Counter = createCounterMetric("bytesRead", "Total bytes read")
  val bytesWrittenMetric: Counter = createCounterMetric("bytesWritten", "Total bytes written")
  val functionCallMetrics: mutable.Map[String, Counter] = createCounterMetricMap()

  /**
   * Get all function names from Catalog
   *
   * @return
   */
  private def getFunctionNames(): mutable.HashSet[String] = {
    val functionNameSetBuilder = mutable.HashSet.newBuilder[String]

    Catalog.expressions.foreach(f => functionNameSetBuilder += f._1.funcName.toLowerCase)
    Catalog.aggregateExpressions.foreach(f => functionNameSetBuilder += f.getClass.getSimpleName.toLowerCase)

    functionNameSetBuilder ++= List(
      "BroadcastIndexJoin",
      "RangeJoinExec",
      "DistanceJoinExec",
      "geoparquet",
      "raster",
      "binaryFile"
    ).map(_.toLowerCase)

    functionNameSetBuilder.result()
  }

  private def createCounterMetric(name: String, description: String): Counter = {
    metricRegistry.counter(MetricRegistry.name(name))
  }

  private def createCounterMetricMap(): mutable.HashMap[String, Counter] = {
    val counterMap = new mutable.HashMap[String, Counter]()
    functionNames.foreach(f => counterMap.put(f, createCounterMetric(f, "Number of " + f + " function calls")))
    counterMap
  }
}
