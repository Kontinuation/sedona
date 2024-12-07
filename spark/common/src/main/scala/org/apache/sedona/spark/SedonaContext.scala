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
package org.apache.sedona.spark

import org.apache.log4j.Logger
import org.apache.sedona.common.utils.TelemetryCollector
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.UDF.UdfRegistrator
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.sedona.sql.{ParserRegistrator, RasterRegistrator}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.deploy.PythonRunner
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.monitoring.ListenerRegistrator
import org.apache.spark.sql.sedona_sql.optimization.{ExtractGeoStatsFunctions, GetReverseGeocodeLayersFunction, ReverseGeocodingFunction, SpatialFilterPushDownForGeoParquet, UsePreparedPredicate}
import org.apache.spark.sql.sedona_sql.strategy.geostats.EvalGeoStatsFunctionStrategy
import org.apache.spark.sql.sedona_sql.optimization.{GetReverseGeocodeLayersFunction, OrderByOptimization, ReverseGeocodingFunction, SpatialFilterPushDownForGeoParquet, UsePreparedPredicate}
import org.apache.spark.sql.sedona_sql.strategy.join.JoinQueryDetector
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.annotation.StaticAnnotation
import scala.util.Try

class InternalApi(
    description: String = "This method is for internal use only and may change without notice.")
    extends StaticAnnotation

object SedonaContext {
  val logger: Logger = Logger.getLogger("SedonaContext")
  var jsc: JavaSparkContext = _
  var jconf: SparkConf = _
  def create(sqlContext: SQLContext): SQLContext = {
    create(sqlContext.sparkSession)
    sqlContext
  }

  /**
   * This is the entry point of the entire Sedona system
   * @param sparkSession
   * @return
   */
  def create(sparkSession: SparkSession): SparkSession = {
    create(sparkSession, "java")
  }

  @InternalApi
  def create(sparkSession: SparkSession, language: String): SparkSession = {
    TelemetryCollector.send("spark", language)
    if (!sparkSession.experimental.extraStrategies.exists(_.isInstanceOf[JoinQueryDetector])) {
      sparkSession.experimental.extraStrategies ++= Seq(new JoinQueryDetector(sparkSession))
    }
    if (!sparkSession.experimental.extraOptimizations.exists(
        _.isInstanceOf[UsePreparedPredicate])) {
      sparkSession.experimental.extraOptimizations ++= Seq(new UsePreparedPredicate)
    }
    if (!sparkSession.experimental.extraOptimizations.exists(
        _.isInstanceOf[SpatialFilterPushDownForGeoParquet])) {
      sparkSession.experimental.extraOptimizations ++= Seq(
        new SpatialFilterPushDownForGeoParquet(sparkSession))
    }

    // Support reverse geocoding functions
    if (!sparkSession.experimental.extraOptimizations.contains(ReverseGeocodingFunction)) {
      sparkSession.experimental.extraOptimizations ++= Seq(
        // Processing GetReverseGeocodeLayers before ST_ReverseGeocode so that the GetReverseGeocodeLayers call does not
        // wind up in the Join clause of ST_ReverseGeocode when nested.
        GetReverseGeocodeLayersFunction,
        ReverseGeocodingFunction)
    }

    // Support geostats functions
    if (!sparkSession.experimental.extraOptimizations.contains(ExtractGeoStatsFunctions)) {
      sparkSession.experimental.extraOptimizations ++= Seq(ExtractGeoStatsFunctions)
    }
    if (!sparkSession.experimental.extraStrategies.exists(
        _.isInstanceOf[EvalGeoStatsFunctionStrategy])) {
      sparkSession.experimental.extraStrategies ++= Seq(
        new EvalGeoStatsFunctionStrategy(sparkSession))
    }

    // Support order by optimization
    if (!sparkSession.experimental.extraOptimizations.contains(OrderByOptimization)) {
      sparkSession.experimental.extraOptimizations ++= Seq(OrderByOptimization)
    }

    addGeoParquetToSupportNestedFilterSources(sparkSession)
    RasterRegistrator.registerAll(sparkSession)
    UdtRegistrator.registerAll()
    UdfRegistrator.registerAll(sparkSession)
    ListenerRegistrator.registerAll(sparkSession)
    if (sparkSession.conf.get("spark.sedona.enableParserExtensions", "false").toBoolean) {
      ParserRegistrator.register(sparkSession)
    }
    jsc = new JavaSparkContext(sparkSession.sparkContext)
    jconf = jsc.getConf
    try {
      val pythonRunnerInputs = ListBuffer.empty[String]
      // This is the path to the Python entrance. It should contain the main() method. This is a mandatory configuration.
      val pythonEntrancePath = sparkSession.conf.get("spark.wherobots.inference.entrance")
      pythonRunnerInputs += pythonEntrancePath
      // This is the path to the Python files. This is an optional configuration.
      // If no dependencies are provided, this will use the Python entrance path.
      var pythonFilesPath = sparkSession.conf.get("spark.wherobots.inference.files", "")
      if (pythonFilesPath == "") {
        pythonFilesPath = pythonEntrancePath
      }
      pythonRunnerInputs += pythonFilesPath
      // This is a list of comma separated arguments to be passed to the Python entrance. This is an optional configuration.
      val pythonFilesArgs = sparkSession.conf.get("spark.wherobots.inference.args", "")
      if (pythonFilesArgs != "") {
        pythonFilesArgs.split(",").foreach(arg => pythonRunnerInputs += arg)
      }
      PythonRunner.main(pythonRunnerInputs.toArray)
    } catch {
      case e: NoSuchElementException =>
        logger.warn("Python files are not set. Sedona will not pre-load Python UDFs.")
    }

    // Set checkpoint dir by default for WBC
    val checkpointDir = sparkSession.conf.getOption("spark.wherobots.checkpoint.dir")
    if (checkpointDir.isDefined) {
      sparkSession.sparkContext.setCheckpointDir(checkpointDir.get)
    }

    sparkSession
  }

  /**
   * This method adds the basic Sedona configurations to the SparkSession Usually the user does
   * not need to call this method directly This is only needed when the user needs to manually
   * configure Sedona
   * @return
   */
  def builder(): SparkSession.Builder = {
    SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
  }

  private def addGeoParquetToSupportNestedFilterSources(session: SparkSession): Unit = {
    // File formats that support nested predicate pushdown is configured by
    // spark.sql.optimizer.nestedPredicatePushdown.supportedFileSources, which is a comma-separated list of data source
    // names. We need to append "geoparquet" to the list to enable nested predicate pushdown for GeoParquet.
    val sources =
      Try(session.conf.get("spark.sql.optimizer.nestedPredicatePushdown.supportedFileSources"))
        .getOrElse("")
    if (!sources.contains("geoparquet")) {
      val newSources = if (sources.isEmpty) "geoparquet" else sources + ",geoparquet"
      session.conf.set(
        "spark.sql.optimizer.nestedPredicatePushdown.supportedFileSources",
        newSources)
    }
  }
}
