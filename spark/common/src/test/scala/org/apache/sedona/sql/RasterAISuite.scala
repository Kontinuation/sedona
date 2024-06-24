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
package org.apache.sedona.sql

import org.apache.spark.sql.functions.{expr, lit}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

class RasterAISuite extends TestBaseScala with BeforeAndAfter with GivenWhenThen {
  import sparkSession.implicits._

  describe("Raster AI helper functions should work") {
    it("Passed RS_SEGMENT_TO_GEOMS") {
      val confidenceDf = Seq(
        Seq(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.9, 0.1, 0.1, 0.9, 0.1, 0.9, 0.1, 0.1, 0.9,
          0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)).toDF("confidence_array")
      val df = confidenceDf
        .withColumn("rast", expr("RS_MakeEmptyRaster(1, 'B', 5, 5, 0, 5, 1)"))
        .withColumn("class_map", expr("map('building', 10)"))
        .withColumn("labels", expr("array(10)"))
        .withColumn("threshold", lit(0.5))

      val row = df
        .selectExpr(
          "RS_SEGMENT_TO_GEOMS(rast, confidence_array, labels, class_map, threshold) as result")
        .first()
      val result = row.getStruct(0)
      val geometries = result.getSeq(0)
      val averageScores = result.getSeq(1)
      val labels = result.getSeq(2)
      val classNames = result.getSeq(3)
      assert(geometries.size == 1)
      assert(averageScores.size == 1)
      assert(labels.size == 1)
      assert(classNames.size == 1)
      val wktReader = new WKTReader()
      assert(
        geometries.head.asInstanceOf[Geometry] == wktReader.read(
          "MULTIPOLYGON (((3 4, 3 2, 4 2, 4 4, 3 4)), ((1 3, 1 1, 2 1, 2 3, 1 3)))"))
      assert(averageScores.head.asInstanceOf[Double] == 0.9)
      assert(labels.head.asInstanceOf[Int] == 10)
      assert(classNames.head.toString == "building")
    }

    it("Passed RS_SEGMENT_TO_GEOMS for empty results") {
      val confidenceDf = Seq(
        Seq(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.2, 0.1, 0.1, 0.2, 0.1, 0.2, 0.1, 0.1, 0.2,
          0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)).toDF("confidence_array")
      val df = confidenceDf
        .withColumn("rast", expr("RS_MakeEmptyRaster(1, 'B', 5, 5, 0, 5, 1)"))
        .withColumn("class_map", expr("map('building', 10)"))
        .withColumn("labels", expr("array(10)"))
        .withColumn("threshold", lit(0.5))

      val row = df
        .selectExpr(
          "RS_SEGMENT_TO_GEOMS(rast, confidence_array, labels, class_map, threshold) as result")
        .first()
      val result = row.getStruct(0)
      val geometries = result.getSeq(0)
      val averageScores = result.getSeq(1)
      val labels = result.getSeq(2)
      val classNames = result.getSeq(3)
      assert(geometries.isEmpty)
      assert(averageScores.isEmpty)
      assert(labels.isEmpty)
      assert(classNames.isEmpty)
    }

    it("Passed RS_SEGMENT_TO_GEOMS for multiple classes") {
      val confidenceDf = Seq(
        Seq(
          // confidence array for class 10
          0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.9, 0.9, 0.1, 0.1, 0.1, 0.9, 0.9, 0.8, 0.1, 0.1, 0.1,
          0.8, 0.8, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,

          // confidence array for class 7
          0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.8, 0.8, 0.1, 0.1, 0.1, 0.8, 0.8, 0.9, 0.1, 0.1, 0.1,
          0.9, 0.9, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)).toDF("confidence_array")
      val df = confidenceDf
        .withColumn("rast", expr("RS_MakeEmptyRaster(1, 'B', 5, 5, 0, 5, 1)"))
        .withColumn("class_map", expr("map('class_7', 7, 'class_10', 10)"))
        .withColumn("labels", expr("array(10, 7)"))
        .withColumn("threshold", lit(0.5))

      val row = df
        .selectExpr(
          "RS_SEGMENT_TO_GEOMS(rast, confidence_array, labels, class_map, threshold) as result")
        .first()
      val result = row.getStruct(0)
      val geometries = result.getSeq(0)
      val averageScores = result.getSeq(1)
      val labels = result.getSeq(2)
      val classNames = result.getSeq(3)
      assert(geometries.size == 2)
      assert(averageScores.size == 2)
      assert(labels.size == 2)
      assert(classNames.size == 2)

      val wktReader = new WKTReader()
      assert(
        geometries.head.asInstanceOf[Geometry] == wktReader.read(
          "POLYGON ((1 4, 1 2, 3 2, 3 4, 1 4))"))
      assert(averageScores.head.asInstanceOf[Double] == 0.9)
      assert(labels.head.asInstanceOf[Int] == 10)
      assert(classNames.head.toString == "class_10")

      assert(
        geometries(1).asInstanceOf[Geometry] == wktReader.read(
          "POLYGON ((3 3, 3 2, 2 2, 2 1, 4 1, 4 3, 3 3))"))
      assert(averageScores(1).asInstanceOf[Double] == 0.9)
      assert(labels(1).asInstanceOf[Int] == 7)
      assert(classNames(1).toString == "class_7")
    }
  }
}
