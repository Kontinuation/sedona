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
package org.apache.spark.metrics.source

import com.codahale.metrics.{Counter, MetricRegistry}

class StatsMetrics extends Source {
  private val localMetricRegistry = new MetricRegistry

  var SpatiallyStratifiedSamplePerform: Counter = createCounterMetric(
    "SpatiallyStratifiedSamplePerform")
  var DBScanFitPerform: Counter = createCounterMetric("DBScanFitPerform")
  var GLocalPerform: Counter = createCounterMetric("GLocalPerform")
  var LOFPerform: Counter = createCounterMetric("LOFPerform")

  /**
   * Create a new instance of GeostatsMetrics from another instance
   *
   * @param another
   *   Another instance of GeostatsMetrics
   */
  def this(another: StatsMetrics) {
    this()
    this.SpatiallyStratifiedSamplePerform = another.SpatiallyStratifiedSamplePerform
    this.DBScanFitPerform = another.DBScanFitPerform
    this.LOFPerform = another.LOFPerform
    this.GLocalPerform = another.GLocalPerform
  }

  override def sourceName = "StatsSource"

  override def metricRegistry: MetricRegistry = this.localMetricRegistry

  private def createCounterMetric(name: String) =
    this.localMetricRegistry.counter(MetricRegistry.name(name))
}
