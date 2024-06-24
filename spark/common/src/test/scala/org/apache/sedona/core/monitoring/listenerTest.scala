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

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.SparkEnv
import org.junit.Assert.assertEquals

class listenerTest extends TestBaseScala {
  val logger = java.util.logging.Logger.getLogger(getClass.getName)

  it("Read a single relation") {
    val iterations = 10
    val startTimeMillis = System.currentTimeMillis()
    for (i <- 1 to iterations) {
      var df = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      df = df.selectExpr(
        "ST_Point(cast(_c0 as Decimal(24,20)), cast(_c1 as Decimal(24,20))) as geom")
      df = df.filter("ST_Area(geom) >=0")
      df = df
        .as("df1")
        .join(df.as("df2"))
        .filter("ST_Distance(df1.geom, df2.geom) <= 0") // Return the points themselves
      assert(df.count() == 1000)
    }
    val endTimeMillis = System.currentTimeMillis()
    val avgDurationSeconds = (endTimeMillis - startTimeMillis) * 1.0 / (1000 * iterations)
    SparkEnv.get.metricsSystem.report
    println(s"avgDuration ${avgDurationSeconds}")
  }

  it("Should find function calls and join") {
    var df = sparkSession.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "false")
      .load(csvPointInputLocation)
    df =
      df.selectExpr("ST_Point(cast(_c0 as Decimal(24,20)), cast(_c1 as Decimal(24,20))) as geom")
    df = df.filter("ST_Area(geom) >=0")
    df = df
      .as("df1")
      .join(df.as("df2"))
      .filter("ST_Distance(df1.geom, df2.geom) <= 0") // Return the points themselves
    val functions = TreeTraversal.execute(df.queryExecution)
    val funcStat = functions.toList.groupBy(identity).mapValues(_.size)
    assertEquals(2, funcStat.getOrElse("st_point", 0))
    assertEquals(2, funcStat.getOrElse("st_area", 0))
    assertEquals(1, funcStat.getOrElse("st_distance", 0))
    assertEquals(1, funcStat.getOrElse("distancejoinexec", 0))
  }

  it("Should find aggregate functions") {
    var df = sparkSession.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "false")
      .load(csvPointInputLocation)
    df =
      df.selectExpr("ST_Point(cast(_c0 as Decimal(24,20)), cast(_c1 as Decimal(24,20))) as geom")
    df = df.groupBy("geom").agg("geom" -> "ST_Envelope_Aggr")
    val functions = TreeTraversal.execute(df.queryExecution)
    val funcStat = functions.toList.groupBy(identity).mapValues(_.size)
    assertEquals(1, funcStat.getOrElse("st_point", 0))
    assertEquals(1, funcStat.getOrElse("st_envelope_aggr", 0))
  }

  it("Should find Sedona data source") {
    var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
    df = df.selectExpr("RS_Envelope(RS_FromGeoTiff(content)) as geom")
    val functions = TreeTraversal.execute(df.queryExecution)
    val funcStat = functions.toList.groupBy(identity).mapValues(_.size)
    SparkEnv.get.metricsSystem.report
    assertEquals(1, funcStat.getOrElse("binaryfile", 0))
    assertEquals(0, funcStat.getOrElse("rs_fromgeotiff", 0))
    assertEquals(1, funcStat.getOrElse("rs_envelope", 0))
  }
}
