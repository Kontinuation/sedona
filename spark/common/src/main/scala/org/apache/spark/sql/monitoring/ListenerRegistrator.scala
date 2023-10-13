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
package org.apache.spark.sql.monitoring

import org.apache.log4j.Logger
import org.apache.sedona.core.monitoring.{IoListener, SqlListener}
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.SedonaMetrics
import org.apache.spark.scheduler.SparkListenerInterface
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object ListenerRegistrator {
  val logger = Logger.getLogger(getClass.getName)

  def registerAll(sparkSession: SparkSession):Unit = {
    // Check if the IoListener is already registered
    val ioListeners:java.util.List[SparkListenerInterface] = sparkSession.sparkContext.listenerBus.listeners
    var ioListenerRegistered = false
    ioListeners.foreach(listener => {
      if (listener.isInstanceOf[IoListener]) {
        ioListenerRegistered = true
      }
    })
    // Check if the SqlListener is already registered
    val sqlListeners: Array[QueryExecutionListener] = sparkSession.listenerManager.listListeners()
    var sqlListenerRegistered = false
    sqlListeners.foreach(listener => {
      if (listener.isInstanceOf[SqlListener]) {
        sqlListenerRegistered = true
      }
    })
    var sedonaMetrics: SedonaMetrics = new SedonaMetrics
    val currentMetrics = SparkEnv.get.metricsSystem.getSourcesByName(sedonaMetrics.sourceName)
    if (currentMetrics.isEmpty) {
      SparkEnv.get.metricsSystem.registerSource(sedonaMetrics)
    }
    else sedonaMetrics = currentMetrics.head.asInstanceOf[SedonaMetrics]
    val listeners = createListeners(sedonaMetrics)
    if (ioListenerRegistered) {
      logger.info("IoListener is already registered!")
    }
    else {
      sparkSession.sparkContext.addSparkListener(listeners._1)
      logger.info("Registering IoListener")
    }

    if (sqlListenerRegistered) {
      logger.info("SqlListener is already registered!")
    }
    else {
      sparkSession.listenerManager.register(listeners._2)
      logger.info("Registering SqlListener")
    }
  }

  def unregisterAll(sparkSession: SparkSession): Unit = {
    val sedonaMetrics: SedonaMetrics = new SedonaMetrics
    SparkEnv.get.metricsSystem.removeSource(sedonaMetrics)
    val listeners = createListeners(sedonaMetrics)
    sparkSession.sparkContext.removeSparkListener(listeners._1)
    sparkSession.listenerManager.unregister(listeners._2)
  }

  def createListeners(sedonaMetrics: SedonaMetrics): (IoListener, SqlListener) = {
    (new IoListener(sedonaMetrics), new SqlListener(sedonaMetrics))
  }
}
