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
import org.apache.sedona.sql.RasterRegistrator
import org.apache.sedona.sql.UDF.Catalog
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaSparkContext.toSparkContext
import org.apache.spark.deploy.PythonRunner
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.monitoring.ListenerRegistrator
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.sedona_sql.optimization._
import org.apache.spark.sql.sedona_sql.strategy.join.JoinQueryDetector
import org.apache.spark.sql.sedona_sql.strategy.physical.function.EvalPhysicalFunctionStrategy
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.annotation.StaticAnnotation
import scala.collection.mutable.ListBuffer
import scala.util.Try

class InternalApi(
    description: String = "This method is for internal use only and may change without notice.")
    extends StaticAnnotation

object SedonaContext {
  val logger: Logger = Logger.getLogger("SedonaContext")
  var jsc: JavaSparkContext = _
  var jconf: SparkConf = _

  private val customOptimizations = Seq(
    // Do these 2 before UsePreparedPredicate so Use PreparedPredicate is used against the revised plan
    ReplaceSingleRowJoinsWithScalarSubqueries,
    OneRowRelationJoin,
    UsePreparedPredicate,
    GetReverseGeocodeLayersFunction,
    ReverseGeocodingFunction,
    ExtractPhysicalFunctions)

  private def customOptimizationsWithSession(sparkSession: SparkSession) =
    Seq(
      new SpatialFilterPushDownForGeoParquet(sparkSession),
      new SpatialTemporalFilterPushDownForStacScan(sparkSession),
      new OptimizeOutDbRasterLoading(sparkSession),
      new LogicalRepartitionBeforeExpensiveOperation(toSparkContext(sparkSession.sparkContext)))

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

    if (!sparkSession.experimental.extraStrategies.exists(
        _.isInstanceOf[EvalPhysicalFunctionStrategy])) {
      sparkSession.experimental.extraStrategies ++= Seq(
        new EvalPhysicalFunctionStrategy(sparkSession))
    }

    val sedonaArrowStrategy = Try(
      Class
        .forName("org.apache.spark.sql.udf.SedonaArrowStrategy")
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[SparkStrategy])

    val extractSedonaUDFRule =
      Try(
        Class
          .forName("org.apache.spark.sql.udf.ExtractSedonaUDFRule")
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[Rule[LogicalPlan]])

    if (sedonaArrowStrategy.isSuccess && extractSedonaUDFRule.isSuccess) {
      sparkSession.experimental.extraStrategies =
        sparkSession.experimental.extraStrategies :+ sedonaArrowStrategy.get
      sparkSession.experimental.extraOptimizations =
        sparkSession.experimental.extraOptimizations :+ extractSedonaUDFRule.get
    }

    val orderByOptimization =
      Try(
        Class
          .forName("org.apache.spark.sql.sedona_sql.optimization.OrderByOptimization")
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[Rule[LogicalPlan]])

    if (orderByOptimization.isSuccess) {
      sparkSession.experimental.extraOptimizations =
        sparkSession.experimental.extraOptimizations :+ orderByOptimization.get
    }

    customOptimizationsWithSession(sparkSession).foreach { opt =>
      if (!sparkSession.experimental.extraOptimizations.exists {
          case _: opt.type => true
          case _ => false
        }) {
        sparkSession.experimental.extraOptimizations ++= Seq(opt)
      }
    }

    customOptimizations.foreach { opt =>
      if (!sparkSession.experimental.extraOptimizations.contains(opt)) {
        sparkSession.experimental.extraOptimizations ++= Seq(opt)
      }
    }

    addGeoParquetToSupportNestedFilterSources(sparkSession)
    RasterRegistrator.registerAll(sparkSession)
    UdtRegistrator.registerAll()
    Catalog.registerAll(sparkSession)
    ListenerRegistrator.registerAll(sparkSession)
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
      .withExtensions(new SedonaSparkSessionExtensions())
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
