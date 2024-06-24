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
package org.apache.sedona.core.monitoring

import org.apache.log4j.Logger
import org.apache.spark.metrics.source.SedonaMetrics
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class SqlListener(sedonaMetrics: SedonaMetrics) extends QueryExecutionListener {
  private val logger = Logger.getLogger("Sedona SQL Metrics Monitor")

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    produceLog(qe, null)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    produceLog(qe, exception)
  }

  def produceLog(qe: QueryExecution, exception: Exception): Unit = {
    val functionCalls = TreeTraversal.execute(qe)

    val functionCallMap = sedonaMetrics.functionCallMetrics
    functionCalls.foreach(f => {
      functionCallMap.get(f) match {
        case Some(_) => {
          functionCallMap(f).inc()
        }
        case None =>
      }
    })

    logger.info("Reported Sedona SQL metrics")
  }
}
