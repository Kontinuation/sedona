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
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.locationtech.jts.geom.Geometry
import org.scalatest.BeforeAndAfterAll
import java.io.File

class stacIOTests extends TestBaseScala with BeforeAndAfterAll {
  val stacdatalocation1: String = resourceFolder + "stac/sentinel-2-item.json"
  val stacdatalocation2: String = resourceFolder + "stac/testStac.json"
  val stacdatalocation3: String = resourceFolder + "stac/core-item.json"
  val stacdatalocation4: String = resourceFolder + "stac/core-item_multiline.json"
  val stacdatalocation5: String = resourceFolder + "stac/proj-example.json"
  val stacdatalocation6: String = resourceFolder + "stac/item.json"
  val stacoutputlocation: String = resourceFolder + "stac/stac_output/"

  override def afterAll(): Unit = FileUtils.deleteDirectory(new File(stacoutputlocation))

  describe("STAC IO tests") {
    it("STAC Test - read and write single line STAC item") {
      val dfR = sparkSession.read.format("stac").load(stacdatalocation3)
      val rowsR = dfR.collect()(0)
      assert(rowsR.getAs[String]("id") == "20201211_223832_CS2")
      assert(rowsR.getAs[String]("title") == "Core Item")
      assert(rowsR.getAs[Double]("gsd") == 0.512)

      dfR.write.format("stac").mode(SaveMode.Overwrite).save(stacoutputlocation + "/stac_write.json")

      val dfW = sparkSession.read.format("stac").load(stacoutputlocation + "/stac_write.json")

      val rowsW = dfW.collect()(0)
      assert(rowsR.getAs[GenericRowWithSchema]("assets") == rowsW.getAs[GenericRowWithSchema]("assets"))
      assert(rowsR.getAs[String]("stac_version") == rowsW.getAs[String]("stac_version"))
      assert(rowsR.getAs[GenericRowWithSchema]("geometry") == rowsW.getAs[GenericRowWithSchema]("geometry"))
    }
    it("STAC Test - read and write multiline STAC item") {
      val dfR = sparkSession.read.format("stac").option("multiLine", true).load(stacdatalocation4)
      val rowsR = dfR.collect()(0)
      assert(rowsR.getAs[String]("id") == "20201211_223832_CS2")
      assert(rowsR.getAs[String]("title") == "Core Item")
      assert(rowsR.getAs[Double]("gsd") == 0.512)

      dfR.write.format("stac").mode(SaveMode.Overwrite).save(stacoutputlocation + "/stac_write.json")

      val dfW = sparkSession.read.format("stac").load(stacoutputlocation + "/stac_write.json")

      val rowsW = dfW.collect()(0)
      assert(rowsR.getAs[GenericRowWithSchema]("assets") == rowsW.getAs[GenericRowWithSchema]("assets"))
      assert(rowsR.getAs[String]("stac_version") == rowsW.getAs[String]("stac_version"))
      assert(rowsR.getAs[GenericRowWithSchema]("geometry") == rowsW.getAs[GenericRowWithSchema]("geometry"))
    }
    it("STAC Test - read and write STAC extensions") {
      val dfR = sparkSession.read.format("stac").option("multiLine", true).load(stacdatalocation6)
      val rowsR = dfR.collect()(0)
      assert(rowsR.getAs[String]("id") == "20201211_223832_CS2")
      assert(rowsR.getAs[Long]("eo:snow_cover") == 0)
      assert(rowsR.getAs[Double]("eo:cloud_cover") == 1.2)

      dfR.write.format("stac").mode(SaveMode.Overwrite).save(stacoutputlocation + "/stac_write.json")

      val dfW = sparkSession.read.format("stac").load(stacoutputlocation + "/stac_write.json")

      val rowsW = dfW.collect()(0)
      assert(rowsR.getAs[GenericRowWithSchema]("assets") == rowsW.getAs[GenericRowWithSchema]("assets"))
      assert(rowsR.getAs[String]("stac_version") == rowsW.getAs[String]("stac_version"))
      assert(rowsR.getAs[GenericRowWithSchema]("geometry") == rowsW.getAs[GenericRowWithSchema]("geometry"))
    }
    it("STAC Test - read single line STAC item with polygon geometry") {
      val df = sparkSession.read.format("stac").option("multiLine", true).load(stacdatalocation5)
      val rows = df.collect()(0)
      assert(rows.getAs[String]("id") == "proj-example")
      assert(rows.getAs[String]("collection") == "landsat-8-l1")
      assert(rows.getAs[Geometry]("geometryUDT").toString == "POLYGON ((152.52758 60.63437, 149.1755 61.19016, 148.13933 59.51584, 151.33786 58.97792, 152.52758 60.63437))")
    }
  }
}
