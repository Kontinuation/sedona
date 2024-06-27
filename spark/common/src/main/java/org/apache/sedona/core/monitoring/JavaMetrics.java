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
package org.apache.sedona.core.monitoring;

import org.apache.spark.SparkContext;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

/** This class provides a Java-friendly API for the Scala class Metrics. */
public class JavaMetrics {

  /**
   * Create a LongAccumulator metric.
   *
   * @param sc SparkContext
   * @param name Metric name
   * @return LongAccumulator
   */
  public static LongAccumulator createMetric(SparkContext sc, String name) {
    return Metrics.createMetric(sc, name);
  }

  /**
   * Create a DoubleAccumulator metric.
   *
   * @param sc SparkContext
   * @param name Metric name
   * @return DoubleAccumulator
   */
  public static DoubleAccumulator createDoubleMetric(SparkContext sc, String name) {
    return Metrics.createDoubleMetric(sc, name);
  }
}
