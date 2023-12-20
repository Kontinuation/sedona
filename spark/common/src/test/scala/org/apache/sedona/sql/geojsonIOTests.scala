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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.locationtech.jts.geom.{MultiLineString, Polygon}
import org.scalatest.BeforeAndAfterAll

import java.io.File

class geojsonIOTests extends TestBaseScala with BeforeAndAfterAll {
  val geojsondatalocation1: String = resourceFolder + "geojson/test1.json"
  val geojsondatalocation2: String = resourceFolder + "geojson/geojson_feature-collection.json"
  val geojsondatalocation3: String = resourceFolder + "geojson/core-item.json"
  val geojsondatalocation4: String = resourceFolder + "geojson/test2.json"
  val geojsonoutputlocation: String = resourceFolder + "geojson/geojson_output/"

  override def afterAll(): Unit = FileUtils.deleteDirectory(new File(geojsonoutputlocation))

  describe("GeoJSON IO tests") {
    it("GeoJSON Test - Read and Write multiline GeoJSON file") {
      val dfR = sparkSession.read.format("geojson").option("multiLine", true).load(geojsondatalocation1)
      val rowsR = dfR.collect()(0)
      assert(rowsR.getAs[String]("type") == "Feature")
      assert(rowsR.getAs[GenericRowWithSchema]("properties").getString(0) == "2020-12-12T01:48:13.725Z")
      assert(rowsR.getAs[GenericRowWithSchema]("properties").getString(1) == "A sample STAC Item that includes examples of all common metadata")
      assert(rowsR.getAs[GenericRowWithSchema]("properties").getString(5) == "Core Item")
      assert(rowsR.getAs[Polygon]("geometry").toString == "POLYGON ((172.91173669923782 1.3438851951615003, 172.95469614953714 1.3438851951615003, 172.95469614953714 1.3690476620161975, 172.91173669923782 1.3690476620161975, 172.91173669923782 1.3438851951615003))")

      dfR.write.format("geojson").option("multiLine", true).mode(SaveMode.Overwrite).save(geojsonoutputlocation + "/geojson_write.json")

      val dfW = sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")
      val rowsW = dfW.collect()(0)
      assert(rowsR.getAs[GenericRowWithSchema]("properties") == rowsW.getAs[GenericRowWithSchema]("properties"))
      assert(rowsR.getAs[Polygon]("geometry") == rowsW.getAs[Polygon]("geometry"))
      assert(rowsR.getAs[String]("type") == rowsW.getAs[String]("type"))
    }
    it("GeoJSON Test - Read and Write MultilineString geometry") {
      val dfR = sparkSession.read.format("geojson").option("multiLine", true).load(geojsondatalocation4)
      val rowsR = dfR.collect()(0)
      assert(rowsR.getAs[String]("type") == "Feature")
      assert(rowsR.getAs[GenericRowWithSchema]("properties").getString(0) == "2020-12-12T01:48:13.725Z")
      assert(rowsR.getAs[GenericRowWithSchema]("properties").getString(1) == "A sample STAC Item that includes examples of all common metadata")
      assert(rowsR.getAs[GenericRowWithSchema]("properties").getString(5) == "Core Item")
      assert(rowsR.getAs[MultiLineString]("geometry").toString == "MULTILINESTRING ((170 45, 180 45), (-180 45, -170 45))")

      dfR.write.format("geojson").option("multiLine", true).mode(SaveMode.Overwrite).save(geojsonoutputlocation + "/geojson_write.json")

      val dfW = sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")
      val rowsW = dfW.collect()(0)
      assert(rowsR.getAs[GenericRowWithSchema]("properties") == rowsW.getAs[GenericRowWithSchema]("properties"))
      assert(rowsR.getAs[MultiLineString]("geometry") == rowsW.getAs[MultiLineString]("geometry"))
      assert(rowsR.getAs[String]("type") == rowsW.getAs[String]("type"))
    }
    it("GeoJSON Test - feature collection test") {
      val dfR = sparkSession.read.format("geojson").option("multiLine", true).load(geojsondatalocation2)
      val rowsR = dfR.collect()(0)

      assert(rowsR.getAs[Seq[Row]]("features")(0).get(0).toString == "POINT (102 0.5)")
      assert(rowsR.getAs[Seq[Row]]("features")(1).get(0).toString == "LINESTRING (102 0, 103 1, 104 0, 105 1)")
      assert(rowsR.getAs[Seq[Row]]("features")(2).get(0).toString == "POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))")
      assert(rowsR.getAs[Seq[Row]]("features")(3).get(0).toString == "MULTILINESTRING ((170 45, 180 45), (-180 45, -170 45))")
      assert(rowsR.getAs[Seq[Row]]("features")(4).get(0).toString == "MULTIPOLYGON (((180 40, 180 50, 170 50, 170 40, 180 40)), ((-170 40, -170 50, -180 50, -180 40, -170 40)))")

      dfR.write.format("geojson").option("multiLine", true).mode(SaveMode.Overwrite).save(geojsonoutputlocation + "/geojson_write.json")

      val dfW = sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")
      val rowsW = dfW.collect()(0)
      assert(rowsR.get(0).equals(rowsW.get(0)))
      assert(rowsR.getAs[String]("type") == rowsW.getAs[String]("type"))
    }
    it("GeoJSON Test - read and write single line STAC item") {
      val dfR = sparkSession.read.format("geojson").load(geojsondatalocation3)
      val rowsR = dfR.collect()(0)
      assert(rowsR.getAs[String]("type") == "Feature")
      assert(rowsR.getAs[String]("stac_version") == "1.0.0")
      assert(rowsR.getAs[Polygon]("geometry").toString == "POLYGON ((172.91173669923782 1.3438851951615003, 172.95469614953714 1.3438851951615003, 172.95469614953714 1.3690476620161975, 172.91173669923782 1.3690476620161975, 172.91173669923782 1.3438851951615003))")

      dfR.write.format("geojson").mode(SaveMode.Overwrite).save(geojsonoutputlocation + "/geojson_write.json")

      val dfW = sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")

      val rowsW = dfW.collect()(0)
      assert(rowsR.getAs[GenericRowWithSchema]("assets") == rowsW.getAs[GenericRowWithSchema]("assets"))
      assert(rowsR.getAs[String]("stac_version") == rowsW.getAs[String]("stac_version"))
      assert(rowsR.getAs[Polygon]("geometry") == rowsW.getAs[Polygon]("geometry"))
    }
  }
}
