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

import org.scalatest.{BeforeAndAfter, GivenWhenThen}
import org.junit.Assert._
import org.locationtech.jts.geom.Geometry

class RasterPixelFunctions extends TestBaseScala with BeforeAndAfter with GivenWhenThen {

  import sparkSession.implicits._

  describe("should pass all the pixel functions") {
    it("Passed RS_Polygonize") {
      // Simple 2x2 raster with one column of 1s and one column of 2s
      val inputDf = Seq(Seq(1, 2, 1, 2)).toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'd', 2, 2, 0, 0, 1, -1, 0, 0, 0), band, 1, null) as raster")

      // RS_Polygonize returns array of structs with fields (value, geom)
      val results = df.selectExpr("explode(RS_Polygonize(raster, 1)) as polygon").collect()

      assertEquals(2, results.length)

      // First row - value 1.0
      val row0 = results(0).getStruct(0)
      val geom0 = row0.getAs[Geometry]("geom")
      val value0 = row0.getAs[Double]("value")
      assertEquals(1.0, value0, 0.0)
      assertEquals("POLYGON ((0 0, 0 -2, 1 -2, 1 0, 0 0))", geom0.toString)

      // Second row - value 2.0
      val row1 = results(1).getStruct(0)
      val geom1 = row1.getAs[Geometry]("geom")
      val value1 = row1.getAs[Double]("value")

      assertEquals(2.0, value1, 0.0)
      assertEquals("POLYGON ((1 0, 1 -2, 2 -2, 2 0, 1 0))", geom1.toString)
    }
  }
}
