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

import org.apache.spark.sql.DataFrame
import org.locationtech.jts.geom.Geometry
import org.scalatest.prop.TableDrivenPropertyChecks

class SubdividedSpatialJoinSuite extends TestBaseScala with TableDrivenPropertyChecks {
  override def beforeAll(): Unit = {
    super.beforeAll()
    prepareTempViewsForTestData()
  }

  describe("Subdivided Sedona-SQL Spatial Join Test") {
    val joinedTables = Table(
      ("left", "right"),
      ("rivers_centerlines", "us_railroads"),
      ("rivers_centerlines", "urban_areas"),
      ("urban_areas", "us_railroads"))

    val subdivideConfigs = Seq(
      // subdivideLeft, subdivideRight, localSubdivideLeft, localSubdivideRight, keepRowDataLeft, keepRowDataRight
      ("always", "always", "always", "always", "true", "true"),
      ("always", "always", "always", "always", "false", "false"),
      ("always", "never", "always", "always", "true", "false"),
      ("always", "never", "always", "always", "false", "false"),
      ("never", "always", "always", "always", "false", "true"),
      ("never", "always", "always", "always", "false", "false"),
      ("auto", "auto", "auto", "auto", "false", "false"),
      ("auto", "auto", "auto", "auto", "true", "true"))

    forAll(joinedTables) { (left, right) =>
      subdivideConfigs.foreach {
        case (
              subdivideLeft,
              subdivideRight,
              localSubdivideLeft,
              localSubdivideRight,
              keepRowDataLeft,
              keepRowDataRight) =>
          it(
            s"$left - $right, left: $subdivideLeft, right: $subdivideRight, keep-left: $keepRowDataLeft, keep-right: $keepRowDataRight") {
            val expected = withConf(
              Map(
                "spark.sedona.join.subdivideLeft" -> "never",
                "spark.sedona.join.subdivideRight" -> "never",
                "spark.sedona.join.subdivideLeftInLocalJoin" -> "never",
                "spark.sedona.join.subdivideRightInLocalJoin" -> "never")) {
              sparkSession
                .sql(s"SELECT $left.id, $right.id, $left.geometry, $right.geometry " +
                  s"FROM $left JOIN $right ON ST_Intersects($left.geometry, $right.geometry)")
                .collect()
            }

            withConf(
              Map(
                "spark.sedona.join.debug.enableMetricsForSpatialPartitioning" -> "true",
                "sedona.join.numpartition" -> "20",
                "spark.sedona.join.subdivideLeft" -> subdivideLeft,
                "spark.sedona.join.subdivideLeft.maxWidth" -> "0.1",
                "spark.sedona.join.subdivideLeft.maxHeight" -> "0.1",
                "spark.sedona.join.subdivideRight" -> subdivideRight,
                "spark.sedona.join.subdivideRight.maxWidth" -> "0.1",
                "spark.sedona.join.subdivideRight.maxHeight" -> "0.1",
                "spark.sedona.join.subdivideLeftInLocalJoin" -> localSubdivideLeft,
                "spark.sedona.join.subdivideLeftInLocalJoin.maxWidth" -> "0.05",
                "spark.sedona.join.subdivideLeftInLocalJoin.maxHeight" -> "0.05",
                "spark.sedona.join.subdivideRightInLocalJoin" -> localSubdivideRight,
                "spark.sedona.join.subdivideRightInLocalJoin.maxWidth" -> "0.05",
                "spark.sedona.join.subdivideRightInLocalJoin.maxHeight" -> "0.05",
                "spark.sedona.join.subdivideLeft.keepRowData" -> keepRowDataLeft,
                "spark.sedona.join.subdivideRight.keepRowData" -> keepRowDataRight)) {

              // Result has both geometry columns
              var result = sparkSession
                .sql(s"SELECT $left.id, $right.id, $left.geometry, $right.geometry " +
                  s"FROM $left JOIN $right ON ST_Intersects($left.geometry, $right.geometry)")
                .collect()
              validateResult(expected, result, Seq(0, 1, 2, 3))

              // Result has geometry column from left side
              result = sparkSession
                .sql(s"SELECT $left.id, $right.id, $left.geometry " +
                  s"FROM $left JOIN $right ON ST_Intersects($left.geometry, $right.geometry)")
                .collect()
              validateResult(expected, result, Seq(0, 1, 2))

              // Result has geometry column from right side
              result = sparkSession
                .sql(s"SELECT $left.id, $right.id, $right.geometry " +
                  s"FROM $left JOIN $right ON ST_Intersects($left.geometry, $right.geometry)")
                .collect()
              validateResult(expected, result, Seq(0, 1, 3))

              // Result has no geometry column
              result = sparkSession
                .sql(s"SELECT $left.id, $right.id " +
                  s"FROM $left JOIN $right ON ST_Intersects($left.geometry, $right.geometry)")
                .collect()
              validateResult(expected, result, Seq(0, 1))
            }
          }
      }
    }
  }

  private def prepareTempViewsForTestData(): Unit = {
    loadGeoJson(
      resourceFolder + "natural_earth/ne_50m_rivers_lake_centerlines_scale_rank.geojson.gz")
      .createOrReplaceTempView("rivers_centerlines")
    loadGeoJson(resourceFolder + "natural_earth/ne_10m_railroads_north_america.geojson.gz")
      .createOrReplaceTempView("us_railroads")
    loadGeoJson(resourceFolder + "natural_earth/ne_50m_urban_areas.geojson.gz")
      .createOrReplaceTempView("urban_areas")
  }

  private def loadGeoJson(path: String): DataFrame = {
    val df = sparkSession.read.format("geojson").load(path)
    df.selectExpr("inline(features)")
      .selectExpr("geometry", "properties.*", "monotonically_increasing_id() as id")
      .repartition(10)
  }

  private def validateResult(
      expected: Array[org.apache.spark.sql.Row],
      result: Array[org.apache.spark.sql.Row],
      indices: Seq[Int]): Unit = {
    assert(expected.length != 0)
    expected.foreach { r =>
      assert(r.getAs[Geometry](2) != null)
      assert(r.getAs[Geometry](3) != null)
    }
    assert(expected.length == result.length)
    val expectedSorted = expected.sortBy(r => (r.getLong(0), r.getLong(1)))
    val resultSorted = result.sortBy(r => (r.getLong(0), r.getLong(1)))
    expectedSorted.zip(resultSorted).foreach { case (expectedRow, resultRow) =>
      indices.zipWithIndex.foreach { case (i, ord) =>
        assert(expectedRow.get(i) == resultRow.get(ord))
      }
    }
  }
}
