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

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_GeomFromText
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastIndexJoinExec, DistanceJoinExec, RangeJoinExec}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.scalatest.prop.TableDrivenPropertyChecks

class SpatialJoinSuite extends TestBaseScala with TableDrivenPropertyChecks {

  val testDataDelimiter = "\t"
  val spatialJoinPartitionSideConfKey = "sedona.join.spatitionside"
  val advancedSpatialJoinConfKey = "spark.sedona.join.advanced"

  override def sparkConfig: Map[String, String] =
    defaultSparkConfig ++ Map(
      // Being explicit about subdivided spatial join in tests.
      "spark.sedona.join.subdivideLeft" -> "never",
      "spark.sedona.join.subdivideRight" -> "never",
      "spark.sedona.join.subdivideLeftInLocalJoin" -> "never",
      "spark.sedona.join.subdivideRightInLocalJoin" -> "never")

  override def beforeAll(): Unit = {
    super.beforeAll()
    prepareTempViewsForTestData()
  }

  describe("Sedona-SQL Spatial Join Test") {
    val joinConditions = Table(
      "join condition",
      "ST_Contains(df1.geom, df2.geom)",
      "ST_Intersects(df1.geom, df2.geom)",
      "ST_Within(df1.geom, df2.geom)",
      "ST_Covers(df1.geom, df2.geom)",
      "ST_CoveredBy(df1.geom, df2.geom)",
      "ST_Touches(df1.geom, df2.geom)",
      "ST_Crosses(df1.geom, df2.geom)",
      "ST_Overlaps(df1.geom, df2.geom)",
      "ST_Equals(df1.geom, df2.geom)",
      "ST_Contains(df2.geom, df1.geom)",
      "ST_Intersects(df2.geom, df1.geom)",
      "ST_Within(df2.geom, df1.geom)",
      "ST_Covers(df2.geom, df1.geom)",
      "ST_CoveredBy(df2.geom, df1.geom)",
      "ST_Touches(df2.geom, df1.geom)",
      "ST_Crosses(df2.geom, df1.geom)",
      "ST_Overlaps(df2.geom, df1.geom)",
      "ST_Equals(df2.geom, df1.geom)",
      "ST_Distance(df1.geom, df2.geom) < 1.0",
      "ST_Distance(df1.geom, df2.geom) <= 1.0",
      "ST_Distance(df2.geom, df1.geom) < 1.0",
      "ST_Distance(df2.geom, df1.geom) <= 1.0",
      "ST_Distance(df1.geom, df2.geom) < df1.dist",
      "ST_Distance(df1.geom, df2.geom) < df2.dist",
      "ST_Distance(df2.geom, df1.geom) < df1.dist",
      "ST_Distance(df2.geom, df1.geom) < df2.dist",
      "1.0 > ST_Distance(df1.geom, df2.geom)",
      "1.0 >= ST_Distance(df1.geom, df2.geom)")

    forAll(joinConditions) { joinCondition =>
      it(s"should join two dataframes with $joinCondition") {
        withConf(
          Map(spatialJoinPartitionSideConfKey -> "left", advancedSpatialJoinConfKey -> "false")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)
        }
      }
      it(s"should join two dataframes with $joinCondition, with right side as dominant side") {
        withConf(
          Map(
            spatialJoinPartitionSideConfKey -> "right",
            advancedSpatialJoinConfKey -> "false")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)
        }
      }
      it(s"should join two dataframes with $joinCondition, broadcast the left side") {
        val result = sparkSession.sql(
          s"SELECT /*+ BROADCAST(df1) */ df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
        val expected = buildExpectedResult(joinCondition)
        verifyResult(expected, result)
      }
      it(s"should join two dataframes with $joinCondition, broadcast the right side") {
        val result = sparkSession.sql(
          s"SELECT /*+ BROADCAST(df2) */ df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
        val expected = buildExpectedResult(joinCondition)
        verifyResult(expected, result)
      }

      it(s"should join two dataframes with $joinCondition with auto-broadcasting enabled") {
        withConf(Map("sedona.join.autoBroadcastJoinThreshold" -> "100mb")) {
          var result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          assert(isUsingBroadcastIndexJoin(result))
          var expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)

          result = sparkSession.sql(
            s"SELECT df1.id, df2.id FROM df1 LEFT OUTER JOIN df2 ON $joinCondition")
          assert(isUsingBroadcastIndexJoin(result))
          expected = buildExpectedResult(joinCondition, LeftOuter)
          verifyResult(expected, result)

          result = sparkSession.sql(
            s"SELECT df1.id, df2.id FROM df1 RIGHT OUTER JOIN df2 ON $joinCondition")
          assert(isUsingBroadcastIndexJoin(result))
          expected = buildExpectedResult(joinCondition, RightOuter)
          verifyResult(expected, result)
        }
      }

      it(s"should join two dataframes with $joinCondition, using advanced spatial join") {
        withConf(Map(advancedSpatialJoinConfKey -> "true")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)
          val result2 =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 INNER JOIN df2 ON $joinCondition")
          verifyResult(expected, result2)
        }
      }
      it(s"should left-outer join two dataframe with $joinCondition, using advanced spatial join") {
        withConf(Map(advancedSpatialJoinConfKey -> "true")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 LEFT JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition, LeftOuter)
          verifyResult(expected, result)
          val result2 =
            sparkSession.sql(
              s"SELECT df1.id, df2.id FROM df1 LEFT OUTER JOIN df2 ON $joinCondition")
          verifyResult(expected, result2)
        }
      }
      it(
        s"should right-outer join two dataframe with $joinCondition, using advanced spatial join") {
        withConf(Map(advancedSpatialJoinConfKey -> "true")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 RIGHT JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition, RightOuter)
          verifyResult(expected, result)
          val result2 =
            sparkSession.sql(
              s"SELECT df1.id, df2.id FROM df1 RIGHT OUTER JOIN df2 ON $joinCondition")
          verifyResult(expected, result2)
        }
      }
      it(s"should join two dataframes with $joinCondition, using subdivided join") {
        withConf(
          Map(
            advancedSpatialJoinConfKey -> "true",
            "spark.sedona.join.subdivideLeft" -> "always",
            "spark.sedona.join.subdivideRight" -> "always",
            "spark.sedona.join.subdivideLeftInLocalJoin" -> "always",
            "spark.sedona.join.subdivideRightInLocalJoin" -> "always")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)
        }
      }
      it(s"should join two dataframes with $joinCondition, using auto tuned subdivided join") {
        withConf(
          Map(
            advancedSpatialJoinConfKey -> "true",
            "spark.sedona.join.subdivideLeft" -> "auto",
            "spark.sedona.join.subdivideRight" -> "auto",
            "spark.sedona.join.subdivideLeftInLocalJoin" -> "auto",
            "spark.sedona.join.subdivideRightInLocalJoin" -> "auto")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)
        }
      }

      it(
        s"should left-outer join two dataframes with $joinCondition, using local subdivided join") {
        withConf(
          Map(
            advancedSpatialJoinConfKey -> "true",
            "spark.sedona.join.subdivideLeft" -> "never",
            "spark.sedona.join.subdivideRight" -> "never",
            "spark.sedona.join.subdivideLeftInLocalJoin" -> "always",
            "spark.sedona.join.subdivideRightInLocalJoin" -> "always")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 LEFT JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition, LeftOuter)
          verifyResult(expected, result)
        }
      }
      it(
        s"should right-outer join two dataframes with $joinCondition, using local subdivided join") {
        withConf(
          Map(
            advancedSpatialJoinConfKey -> "true",
            "spark.sedona.join.subdivideLeft" -> "never",
            "spark.sedona.join.subdivideRight" -> "never",
            "spark.sedona.join.subdivideLeftInLocalJoin" -> "always",
            "spark.sedona.join.subdivideRightInLocalJoin" -> "always")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 RIGHT JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition, RightOuter)
          verifyResult(expected, result)
        }
      }

      it(s"should left-outer join two dataframes with $joinCondition, using subdivided join") {
        withConf(
          Map(
            advancedSpatialJoinConfKey -> "true",
            "spark.sedona.join.subdivideLeft" -> "always",
            "spark.sedona.join.subdivideRight" -> "always",
            "spark.sedona.join.subdivideLeftInLocalJoin" -> "always",
            "spark.sedona.join.subdivideRightInLocalJoin" -> "always")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 LEFT JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition, LeftOuter)
          verifyResult(expected, result)
        }
      }
      it(s"should right-outer join two dataframes with $joinCondition, using subdivided join") {
        withConf(
          Map(
            advancedSpatialJoinConfKey -> "true",
            "spark.sedona.join.subdivideLeft" -> "always",
            "spark.sedona.join.subdivideRight" -> "always",
            "spark.sedona.join.subdivideLeftInLocalJoin" -> "always",
            "spark.sedona.join.subdivideRightInLocalJoin" -> "always")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 RIGHT JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition, RightOuter)
          verifyResult(expected, result)
        }
      }

      it(
        s"should join two dataframe with $joinCondition, using advanced spatial join with auto broadcast enabled") {
        withConf(
          Map(
            advancedSpatialJoinConfKey -> "true",
            "sedona.join.autoBroadcastJoinThreshold" -> "100m",
            "spark.sedona.join.allowPlanBroadcastJoin" -> "false")) {
          var result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          var expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)
          result = sparkSession.sql(s"SELECT df1.id, df2.id FROM df2 JOIN df1 ON $joinCondition")
          verifyResult(expected, result)

          result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 LEFT JOIN df2 ON $joinCondition")
          expected = buildExpectedResult(joinCondition, LeftOuter)
          verifyResult(expected, result)

          result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df2 LEFT JOIN df1 ON $joinCondition")
          expected = buildExpectedResult(joinCondition, RightOuter)
          verifyResult(expected, result)

          result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 RIGHT JOIN df2 ON $joinCondition")
          expected = buildExpectedResult(joinCondition, RightOuter)
          verifyResult(expected, result)

          result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df2 RIGHT JOIN df1 ON $joinCondition")
          expected = buildExpectedResult(joinCondition, LeftOuter)
          verifyResult(expected, result)
        }
      }
    }
  }

  describe("Sedona-SQL Spatial Join Test with SELECT * and SELECT COUNT(*)") {
    val joinConditions = Table(
      "join condition",
      "ST_Contains(df1.geom, df2.geom)",
      "ST_Contains(df2.geom, df1.geom)",
      "ST_Distance(df1.geom, df2.geom) < 1.0",
      "ST_Distance(df2.geom, df1.geom) < 1.0",
      "ST_Distance(df1.geom, df2.geom) < df1.dist",
      "ST_Distance(df1.geom, df2.geom) < df2.dist")

    forAll(joinConditions) { joinCondition =>
      it(s"should SELECT * in join query with $joinCondition produce correct result") {
        val resultAll =
          sparkSession.sql(s"SELECT * FROM df1 JOIN df2 ON $joinCondition").collect()
        val result = resultAll.map(row => (Option(row.getInt(0)), Option(row.getInt(3)))).sorted
        val expected = buildExpectedResult(joinCondition)
        assert(result.nonEmpty)
        assert(result === expected)
      }

      it(s"should SELECT COUNT(*) in join query with $joinCondition produce correct result") {
        val result = sparkSession
          .sql(s"SELECT COUNT(*) FROM df1 JOIN df2 ON $joinCondition")
          .collect()
          .head
          .getLong(0)
        val expected = buildExpectedResult(joinCondition).length
        assert(result === expected)
      }

      it(
        s"should SELECT * in join query with $joinCondition produce correct result, broadcast the left side") {
        val resultAll = sparkSession
          .sql(s"SELECT /*+ BROADCAST(df1) */ * FROM df1 JOIN df2 ON $joinCondition")
          .collect()
        val result = resultAll.map(row => (Option(row.getInt(0)), Option(row.getInt(3)))).sorted
        val expected = buildExpectedResult(joinCondition)
        assert(result.nonEmpty)
        assert(result === expected)
      }

      it(
        s"should SELECT COUNT(*) in join query with $joinCondition produce correct result, broadcast the left side") {
        val result = sparkSession
          .sql(s"SELECT /*+ BROADCAST(df1) */ COUNT(*) FROM df1 JOIN df2 ON $joinCondition")
          .collect()
          .head
          .getLong(0)
        val expected = buildExpectedResult(joinCondition).length
        assert(result === expected)
      }

      it(
        s"should SELECT * in join query with $joinCondition produce correct result, broadcast the right side") {
        val resultAll = sparkSession
          .sql(s"SELECT /*+ BROADCAST(df2) */ * FROM df1 JOIN df2 ON $joinCondition")
          .collect()
        val result = resultAll.map(row => (Option(row.getInt(0)), Option(row.getInt(3)))).sorted
        val expected = buildExpectedResult(joinCondition)
        assert(result.nonEmpty)
        assert(result === expected)
      }

      it(
        s"should SELECT COUNT(*) in join query with $joinCondition produce correct result, broadcast the right side") {
        val result = sparkSession
          .sql(s"SELECT /*+ BROADCAST(df2) */ COUNT(*) FROM df1 JOIN df2 ON $joinCondition")
          .collect()
          .head
          .getLong(0)
        val expected = buildExpectedResult(joinCondition).length
        assert(result === expected)
      }
    }
  }

  describe(
    "Spatial join in Sedona SQL should be configurable using sedona.join.optimizationmode") {
    it("Optimize all spatial joins when sedona.join.optimizationmode = all") {
      withOptimizationMode("all") {
        val df = sparkSession.sql(
          "SELECT df1.id, df2.id FROM df1 JOIN df2 ON df1.id = df2.id AND ST_Intersects(df1.geom, df2.geom)")
        assert(isUsingOptimizedSpatialJoin(df))
        val expectedResult = buildExpectedResult("ST_Intersects(df1.geom, df2.geom)")
          .filter { case (id1, id2) => id1 == id2 }
        verifyResult(expectedResult, df)
      }
    }

    it("Only optimize non-equi-joins when sedona.join.optimizationmode = nonequi") {
      withOptimizationMode("nonequi") {
        val df = sparkSession.sql(
          "SELECT df1.id, df2.id FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom)")
        assert(isUsingOptimizedSpatialJoin(df))
        val df2 = sparkSession.sql(
          "SELECT df1.id, df2.id FROM df1 JOIN df2 ON df1.id = df2.id AND ST_Intersects(df1.geom, df2.geom)")
        assert(!isUsingOptimizedSpatialJoin(df2))
      }
    }

    it("Won't optimize spatial joins when sedona.join.optimizationmode = none") {
      withOptimizationMode("none") {
        val df = sparkSession.sql(
          "SELECT df1.id, df2.id FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom)")
        assert(!isUsingOptimizedSpatialJoin(df))
      }
    }
  }

  describe("Advanced spatial join on datasets with disjoint extents") {
    it("Should give empty result") {
      withConf(Map(advancedSpatialJoinConfKey -> "true")) {
        val result = sparkSession.sql(
          "SELECT df1.id, df2.id FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom) " +
            "WHERE ST_Within(df1.geom, ST_PolygonFromEnvelope(-100, -100, 0, 100)) AND " +
            "ST_Within(df2.geom, ST_PolygonFromEnvelope(0, -100, 100, 100))")
        assert(result.isEmpty)
      }
    }
    it("Should give correct results for left outer join") {
      val query = """
          |WITH sdf1 AS (SELECT * FROM df1 WHERE ST_Within(df1.geom, ST_PolygonFromEnvelope(-100, -100, 0, 100))),
          |     sdf2 AS (SELECT * FROM df2 WHERE ST_Within(df2.geom, ST_PolygonFromEnvelope(0, -100, 100, 100)))
          |SELECT sdf1.id, sdf2.id FROM sdf1 LEFT JOIN sdf2 ON ST_Intersects(sdf1.geom, sdf2.geom)
          |""".stripMargin
      val actual = withConf(Map(advancedSpatialJoinConfKey -> "true")) {
        sparkSession.sql(query)
      }
      val expected = withOptimizationMode("none") {
        sparkSession.sql(query)
      }
      verifyResult(collectQueryResult(expected), actual)
    }
    it("Should give correct results for right outer join") {
      val query = """
                   |WITH sdf1 AS (SELECT * FROM df1 WHERE ST_Within(df1.geom, ST_PolygonFromEnvelope(-100, -100, 0, 100))),
                   |     sdf2 AS (SELECT * FROM df2 WHERE ST_Within(df2.geom, ST_PolygonFromEnvelope(0, -100, 100, 100)))
                   |SELECT sdf1.id, sdf2.id FROM sdf1 RIGHT JOIN sdf2 ON ST_Intersects(sdf1.geom, sdf2.geom)
                   |""".stripMargin
      val actual = withConf(Map(advancedSpatialJoinConfKey -> "true")) {
        sparkSession.sql(query)
      }
      val expected = withOptimizationMode("none") {
        sparkSession.sql(query)
      }
      verifyResult(collectQueryResult(expected), actual)
    }
  }

  describe("Spatial join optimizer should work with complex join conditions") {
    it("Optimize spatial join with complex join conditions") {
      withOptimizationMode("all") {
        val df = sparkSession.sql("""
            |SELECT df1.id, df2.id FROM df1 JOIN df2 ON
            |ST_Intersects(df1.geom, df2.geom) AND df1.id > df2.id AND df1.id < df2.id + 100""".stripMargin)
        assert(isUsingOptimizedSpatialJoin(df))
        val expectedResult = buildExpectedResult("ST_Intersects(df1.geom, df2.geom)")
          .filter {
            case (Some(id1), Some(id2)) => id1 > id2 && id1 < id2 + 100
            case _ => false
          }
        verifyResult(expectedResult, df)
      }
    }
  }

  describe("Spatial outer join should handle null geometries and duplicates correctly") {
    val joinClauses = Table(
      ("join type", "condition"),
      ("LEFT JOIN", "ST_Intersects(df1.geom, df2.geom)"),
      ("RIGHT JOIN", "ST_Intersects(df1.geom, df2.geom)"),
      ("LEFT JOIN", "ST_Distance(df1.geom, df2.geom) < 1.0"),
      ("RIGHT JOIN", "ST_Distance(df1.geom, df2.geom) < 1.0"))

    def buildExpectedResultWithNulls(
        condition: String,
        joinClause: String): Seq[(Option[Int], Option[Int])] = {
      withOptimizationMode("none") {
        val df = sparkSession.sql(
          s"SELECT df1.id, df2.id FROM df1WithNullAndDup df1 $joinClause df2WithNullAndDup df2 ON $condition")
        collectQueryResult(df)
      }
    }

    forAll(joinClauses) { case (joinClause, condition) =>
      it(s"Should correctly handle null values in $joinClause, join condition: $condition") {
        withConf(Map(advancedSpatialJoinConfKey -> "true")) {
          val result = sparkSession.sql(
            s"SELECT df1.id, df2.id FROM df1WithNullAndDup df1 $joinClause df2WithNullAndDup df2 ON $condition")
          val expected = buildExpectedResultWithNulls(condition, joinClause)
          verifyResult(expected, result)
          val count = result.count()
          assert(count == expected.size)
        }
      }

      it(
        s"Should correctly handle null values in $joinClause, join condition: $condition, local subdivided join") {
        withConf(
          Map(
            advancedSpatialJoinConfKey -> "true",
            "spark.sedona.join.subdivideLeft" -> "never",
            "spark.sedona.join.subdivideRight" -> "never",
            "spark.sedona.join.subdivideLeftInLocalJoin" -> "always",
            "spark.sedona.join.subdivideRightInLocalJoin" -> "always")) {
          val result = sparkSession.sql(
            s"SELECT df1.id, df2.id FROM df1WithNullAndDup df1 $joinClause df2WithNullAndDup df2 ON $condition")
          val expected = buildExpectedResultWithNulls(condition, joinClause)
          verifyResult(expected, result)
          val count = result.count()
          assert(count == expected.size)
        }
      }

      it(
        s"Should correctly handle null values in $joinClause, join condition: $condition, subdivided join") {
        withConf(
          Map(
            advancedSpatialJoinConfKey -> "true",
            "spark.sedona.join.subdivideLeft" -> "always",
            "spark.sedona.join.subdivideRight" -> "always",
            "spark.sedona.join.subdivideLeftInLocalJoin" -> "always",
            "spark.sedona.join.subdivideRightInLocalJoin" -> "always")) {
          val result = sparkSession.sql(
            s"SELECT df1.id, df2.id FROM df1WithNullAndDup df1 $joinClause df2WithNullAndDup df2 ON $condition")
          val expected = buildExpectedResultWithNulls(condition, joinClause)
          verifyResult(expected, result)
          val count = result.count()
          assert(count == expected.size)
        }
      }

      it(
        s"Should correctly handle null values in $joinClause, join condition: $condition, broadcast left") {
        withConf(Map(advancedSpatialJoinConfKey -> "true")) {
          val result = sparkSession.sql(
            s"SELECT /*+ BROADCAST(df1) */ df1.id, df2.id FROM df1WithNullAndDup df1 $joinClause df2WithNullAndDup df2 ON $condition")
          val expected = buildExpectedResultWithNulls(condition, joinClause)
          val shouldOptimized = joinClause == "RIGHT JOIN"
          verifyResult(expected, result, shouldOptimized)
          val count = result.count()
          assert(count == expected.size)
        }
      }

      it(
        s"Should correctly handle null values in $joinClause, join condition: $condition, broadcast right") {
        withConf(Map(advancedSpatialJoinConfKey -> "true")) {
          val result = sparkSession.sql(
            s"SELECT /*+ BROADCAST(df2) */ df1.id, df2.id FROM df1WithNullAndDup df1 $joinClause df2WithNullAndDup df2 ON $condition")
          val expected = buildExpectedResultWithNulls(condition, joinClause)
          val shouldOptimized = joinClause == "LEFT JOIN"
          verifyResult(expected, result, shouldOptimized)
          val count = result.count()
          assert(count == expected.size)
        }
      }
    }
  }

  describe("Spatial outer join should handle empty datasets correctly") {
    val joinClauses = Table(
      "query",
      "SELECT df1.id, dfEmpty.id FROM df1 LEFT JOIN dfEmpty ON ST_Distance(df1.geom, dfEmpty.geom) < 1",
      "SELECT df1.id, dfEmptyWithPartitions.id FROM df1 LEFT JOIN dfEmptyWithPartitions ON ST_Distance(df1.geom, dfEmptyWithPartitions.geom) < 1",
      "SELECT dfEmpty.id, df1.id FROM dfEmpty LEFT JOIN df1 ON ST_Distance(df1.geom, dfEmpty.geom) < 1",
      "SELECT dfEmptyWithPartitions.id, df1.id FROM dfEmptyWithPartitions LEFT JOIN df1 ON ST_Distance(df1.geom, dfEmptyWithPartitions.geom) < 1",
      "SELECT df1.id, dfEmpty.id FROM df1 RIGHT JOIN dfEmpty ON ST_Distance(df1.geom, dfEmpty.geom) < 1",
      "SELECT df1.id, dfEmptyWithPartitions.id FROM df1 RIGHT JOIN dfEmptyWithPartitions ON ST_Distance(df1.geom  , dfEmptyWithPartitions.geom) < 1",
      "SELECT dfEmpty.id, df1.id FROM dfEmpty RIGHT JOIN df1 ON ST_Distance(df1.geom, dfEmpty.geom) < 1",
      "SELECT dfEmptyWithPartitions.id, df1.id FROM dfEmptyWithPartitions RIGHT JOIN df1 ON ST_Distance(df1.geom, dfEmptyWithPartitions.geom) < 1")

    forAll(joinClauses) { case (query) =>
      it(s"query: $query") {
        val actual = withConf(Map(advancedSpatialJoinConfKey -> "true")) {
          val df = sparkSession.sql(query)
          assert(isUsingOptimizedSpatialJoin(df))
          df
        }
        val expected = withOptimizationMode("none") {
          sparkSession.sql(query)
        }
        val expectedResult = collectQueryResult(expected)
        val actualResult = collectQueryResult(actual)
        assert(actualResult === expectedResult)
      }
    }
  }

  describe("Spatial join should produce results with correct geometry values") {
    def verifyGeometries(result: DataFrame): Unit = {
      val resultRows = result.collect()
      val leftRows = sparkSession.sql("SELECT id, geom FROM df1").collect()
      val rightRows = sparkSession.sql("SELECT id, geom FROM df2").collect()
      val leftGeomMap = leftRows.map(row => (row.getInt(0), row.getAs[Geometry](1))).toMap
      val rightGeomMap = rightRows.map(row => (row.getInt(0), row.getAs[Geometry](1))).toMap
      resultRows.foreach { row =>
        val id1 = row.getInt(0)
        val id2 = row.getInt(1)
        val geom1 = row.getAs[Geometry](2)
        val geom2 = row.getAs[Geometry](3)
        assert(geom1.equals(leftGeomMap(id1)))
        assert(geom2.equals(rightGeomMap(id2)))
      }
    }

    it("Should produce correct geometry values") {
      val configs = Seq(
        Map(
          advancedSpatialJoinConfKey -> "true",
          "spark.sedona.join.subdivideLeft" -> "never",
          "spark.sedona.join.subdivideRight" -> "never"),
        Map(
          advancedSpatialJoinConfKey -> "true",
          "spark.sedona.join.subdivideLeft" -> "always",
          "spark.sedona.join.subdivideRight" -> "always",
          "spark.sedona.join.subdivideLeft.keepRowData" -> "true",
          "spark.sedona.join.subdivideRight.keepRowData" -> "true"),
        Map(
          advancedSpatialJoinConfKey -> "true",
          "spark.sedona.join.subdivideLeft" -> "always",
          "spark.sedona.join.subdivideRight" -> "always",
          "spark.sedona.join.subdivideLeft.keepRowData" -> "false",
          "spark.sedona.join.subdivideRight.keepRowData" -> "false"))
      configs.foreach { conf =>
        withConf(conf) {
          val result = sparkSession.sql(
            "SELECT df1.id, df2.id, df1.geom AS geom1, df2.geom AS geom2 " +
              "FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom)")
          val expected = buildExpectedResult("ST_Intersects(df1.geom, df2.geom)")
          verifyResult(expected, result)
          verifyGeometries(result)
        }
      }
    }

    it("Should produce correct geometry values when left side is broadcasted") {
      val result = sparkSession.sql(
        "SELECT /*+ BROADCAST(df1) */ df1.id, df2.id, df1.geom AS geom1, df2.geom AS geom2 " +
          "FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom)")
      val expected = buildExpectedResult("ST_Intersects(df1.geom, df2.geom)")
      verifyResult(expected, result)
      verifyGeometries(result)
    }

    it("Should produce correct geometry values when right side is broadcasted") {
      val result = sparkSession.sql(
        "SELECT /*+ BROADCAST(df2) */ df1.id, df2.id, df1.geom AS geom1, df2.geom AS geom2 " +
          "FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom)")
      val expected = buildExpectedResult("ST_Intersects(df1.geom, df2.geom)")
      verifyResult(expected, result)
      verifyGeometries(result)
    }
  }

  describe("Spatial join should work with dataframe containing various number of partitions") {
    val queries = Table(
      "join queries",
      "SELECT * FROM df1 JOIN dfEmpty WHERE ST_Intersects(df1.geom, dfEmpty.geom)",
      "SELECT * FROM dfEmpty JOIN df1 WHERE ST_Intersects(df1.geom, dfEmpty.geom)",
      "SELECT /*+ BROADCAST(df1) */ * FROM df1 JOIN dfEmpty WHERE ST_Intersects(df1.geom, dfEmpty.geom)",
      "SELECT /*+ BROADCAST(dfEmpty) */ * FROM df1 JOIN dfEmpty WHERE ST_Intersects(df1.geom, dfEmpty.geom)",
      "SELECT /*+ BROADCAST(df1) */ * FROM dfEmpty JOIN df1 WHERE ST_Intersects(df1.geom, dfEmpty.geom)",
      "SELECT /*+ BROADCAST(dfEmpty) */ * FROM dfEmpty JOIN df1 WHERE ST_Intersects(df1.geom, dfEmpty.geom)")

    forAll(queries) { query =>
      it(s"empty dataframes: $query") {
        withConf(
          Map(spatialJoinPartitionSideConfKey -> "left", advancedSpatialJoinConfKey -> "false")) {
          val resultRows = sparkSession.sql(query).collect()
          assert(resultRows.isEmpty)
        }
        withConf(
          Map(
            spatialJoinPartitionSideConfKey -> "right",
            advancedSpatialJoinConfKey -> "false")) {
          val resultRows = sparkSession.sql(query).collect()
          assert(resultRows.isEmpty)
        }
      }

      it(s"Advanced join: $query") {
        withConf(Map(advancedSpatialJoinConfKey -> "true")) {
          val resultRows = sparkSession.sql(query).collect()
          assert(resultRows.isEmpty)
        }
      }

      it(s"Subdivided join: $query") {
        withConf(
          Map(
            advancedSpatialJoinConfKey -> "true",
            "spark.sedona.join.subdivideLeft" -> "always",
            "spark.sedona.join.subdivideRight" -> "always",
            "spark.sedona.join.subdivideLeft.keepRowData" -> "false",
            "spark.sedona.join.subdivideRight.keepRowData" -> "false")) {
          val resultRows = sparkSession.sql(query).collect()
          assert(resultRows.isEmpty)
        }
      }
    }

    it("non-empty dataframe has lots of partitions") {
      val df = sparkSession
        .range(0, 4)
        .toDF("id")
        .withColumn("geom", expr("ST_Point(id, id)"))
        .repartition(10)
      df.createOrReplaceTempView("df10parts")

      val query =
        "SELECT * FROM df10parts JOIN dfEmpty WHERE ST_Intersects(df10parts.geom, dfEmpty.geom)"
      withConf(
        Map(spatialJoinPartitionSideConfKey -> "left", advancedSpatialJoinConfKey -> "false")) {
        val resultRows = sparkSession.sql(query).collect()
        assert(resultRows.isEmpty)
      }
      withConf(
        Map(spatialJoinPartitionSideConfKey -> "right", advancedSpatialJoinConfKey -> "false")) {
        val resultRows = sparkSession.sql(query).collect()
        assert(resultRows.isEmpty)
      }
      withConf(Map(advancedSpatialJoinConfKey -> "true")) {
        val resultRows = sparkSession.sql(query).collect()
        assert(resultRows.isEmpty)
      }
      withConf(
        Map(
          advancedSpatialJoinConfKey -> "true",
          "spark.sedona.join.subdivideLeft" -> "always",
          "spark.sedona.join.subdivideRight" -> "always",
          "spark.sedona.join.subdivideLeft.keepRowData" -> "false",
          "spark.sedona.join.subdivideRight.keepRowData" -> "false")) {
        val resultRows = sparkSession.sql(query).collect()
        assert(resultRows.isEmpty)
      }
    }

    it("should alias complex expressions in SortOrder") {
      sparkSession.sqlContext.udf.register("customFunction", (x: Int) => x * x)

      val df = sparkSession.range(0, 10).toDF("id")
      df.createOrReplaceTempView("test_table")

      // Query with multiple ORDER BY clauses using a CTE
      val query =
        """
          WITH cte AS (
            SELECT id, customFunction(id) AS squared_value
            FROM test_table
            ORDER BY customFunction(id) DESC
          )
          SELECT id, squared_value, id + squared_value AS sum_value
          FROM cte
          ORDER BY (id + squared_value) ASC
        """

      val queryDf = sparkSession.sql(query)

      val analyzedPlan = queryDf.queryExecution.analyzed
      val optimizedPlan = queryDf.queryExecution.optimizedPlan

      // Ensure the final schema matches the analyzed schema
      val analyzedSchema = analyzedPlan.output.map(_.name)
      val optimizedSchema = optimizedPlan.output.map(_.name)
      assert(
        analyzedSchema == optimizedSchema,
        "Schema mismatch between analyzed and optimized plans")

      // Collect and validate results
      val results = queryDf.collect()
      val expectedResults = (0 until 10)
        .map(i => (i, i * i, i + i * i))
        .sortBy(_._3)
      assert(results.map(r =>
        (r.getLong(0).toInt, r.getInt(1), r.getLong(2).toInt)) === expectedResults)
    }

    it("ST_Distance involving empty geometries should work as a predicate") {
      // ST_Distance returns null when either arg is an empty geometry,
      // while this test doesn't involve an actual spatial join, it tests that
      // a distance-based spatial join doesn't fail due to this edge case.
      val result1 = sparkSession.sql(
        "SELECT * FROM df1 WHERE ST_Distance(df1.geom, ST_GeomFromText('POINT EMPTY')) < 1")
      assert(result1.count() == 0)
      val result2 = sparkSession.sql(
        "SELECT * FROM df2 WHERE ST_Distance(df2.geom, ST_GeomFromText('POINT EMPTY')) < 1")
      assert(result2.count() == 0)
    }
  }

  private def withOptimizationMode[T](mode: String)(body: => T): T = {
    withConf(Map("sedona.join.optimizationmode" -> mode))(body)
  }

  private def prepareTempViewsForTestData(): (DataFrame, DataFrame) = {
    val df1 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(spatialJoinLeftInputLocation)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c2")))
      .select("id", "geom")
      .withColumn("dist", expr("ST_Area(geom)"))
      .repartition(4)
      .cache()
    val df2 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(spatialJoinRightInputLocation)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c2")))
      .select("id", "geom")
      .withColumn("dist", expr("ST_Area(geom)"))
      .repartition(4)
      .cache()
    val emptyRdd = sparkSession.sparkContext.emptyRDD[Row]
    val emptyDfWithNoPartitions = sparkSession.createDataFrame(
      emptyRdd,
      StructType(Seq(StructField("id", IntegerType), StructField("geom", GeometryUDT))))
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    sparkSession
      .sql("SELECT id, CASE WHEN MOD(id, 2) = 0 THEN NULL ELSE geom END AS geom FROM df1")
      .createOrReplaceTempView("df1WithNull")
    sparkSession
      .sql("SELECT id, CASE WHEN MOD(id, 2) = 0 THEN NULL ELSE geom END AS geom FROM df2")
      .createOrReplaceTempView("df2WithNull")
    sparkSession
      .sql("SELECT id, geom, explode(array_repeat(0,2)) dup FROM df1WithNull")
      .createOrReplaceTempView("df1WithNullAndDup")
    sparkSession
      .sql("SELECT id, geom, explode(array_repeat(0,3)) dup FROM df1WithNull")
      .createOrReplaceTempView("df2WithNullAndDup")
    emptyDfWithNoPartitions.createOrReplaceTempView("dfEmpty")
    val emptyDfWithPartitions = df1.where("id > 100000")
    emptyDfWithPartitions.createOrReplaceTempView("dfEmptyWithPartitions")
    (df1, df2)
  }

  private def buildExpectedResult(
      joinCondition: String,
      joinType: JoinType = Inner): Seq[(Option[Int], Option[Int])] = {
    val left = loadTestData(spatialJoinLeftInputLocation)
    val right = loadTestData(spatialJoinRightInputLocation)
    val udf = joinCondition.split('(')(0)
    val swapped = joinCondition.contains("df2.geom, df1.geom")
    val eval = udf match {
      case "ST_Contains" => (l: Geometry, r: Geometry) => l.contains(r)
      case "ST_CoveredBy" => (l: Geometry, r: Geometry) => l.coveredBy(r)
      case "ST_Covers" => (l: Geometry, r: Geometry) => l.covers(r)
      case "ST_Crosses" => (l: Geometry, r: Geometry) => l.crosses(r)
      case "ST_Equals" => (l: Geometry, r: Geometry) => l.equals(r)
      case "ST_Intersects" => (l: Geometry, r: Geometry) => l.intersects(r)
      case "ST_Overlaps" => (l: Geometry, r: Geometry) => l.overlaps(r)
      case "ST_Touches" => (l: Geometry, r: Geometry) => l.touches(r)
      case "ST_Within" => (l: Geometry, r: Geometry) => l.within(r)
      case "ST_Distance" =>
        if (joinCondition contains "df1.dist")
          (l: Geometry, r: Geometry) => l.distance(r) < (if (!swapped) l.getArea else r.getArea)
        else if (joinCondition contains "df2.dist")
          (l: Geometry, r: Geometry) => l.distance(r) < (if (!swapped) r.getArea else l.getArea)
        else {
          if (joinCondition.contains("<=")) { (l: Geometry, r: Geometry) =>
            l.distance(r) <= 1.0
          } else { (l: Geometry, r: Geometry) =>
            l.distance(r) < 1.0
          }
        }
      case _ =>
        if (udf.contains(">=")) { (l: Geometry, r: Geometry) =>
          l.distance(r) <= 1.0
        } else { (l: Geometry, r: Geometry) =>
          l.distance(r) < 1.0
        }
    }

    // The following code assumes that rows in both sides has unique IDs.
    val leftIds = left.map(_._1).toSet
    val rightIds = right.map(_._1).toSet
    val innerJoinResults = left.flatMap { case (id, geom) =>
      right
        .filter { case (_, geom2) =>
          if (swapped) eval(geom2, geom) else eval(geom, geom2)
        }
        .map { case (id2, _) => (id, id2) }
    }
    var joinResults: Seq[(Option[Int], Option[Int])] = innerJoinResults.map { case (id1, id2) =>
      (Some(id1), Some(id2))
    }
    joinType match {
      case LeftOuter =>
        val leftUnmatchedIds = leftIds -- innerJoinResults.map(_._1).toSet
        joinResults = joinResults ++ leftUnmatchedIds.map(id => (Some(id), None))
      case RightOuter =>
        val rightUnmatchedIds = rightIds -- innerJoinResults.map(_._2).toSet
        joinResults = joinResults ++ rightUnmatchedIds.map(id => (None, Some(id)))
      case _ => ()
    }
    joinResults.sorted
  }

  private def loadTestData(path: String): Seq[(Int, Geometry)] = {
    val wktReader = new WKTReader()
    val bufferedSource = scala.io.Source.fromFile(path)
    try {
      bufferedSource
        .getLines()
        .map { line =>
          val Array(id, _, geom) = line.split(testDataDelimiter)
          (id.toInt, wktReader.read(geom))
        }
        .toList
    } finally {
      bufferedSource.close()
    }
  }

  def verifyResult(
      expected: Seq[(Option[Int], Option[Int])],
      result: DataFrame,
      shouldBeOptimized: Boolean = true): Unit = {
    if (shouldBeOptimized) {
      assert(isUsingOptimizedSpatialJoin(result))
    }
    val actual = collectQueryResult(result)
    assert(actual.nonEmpty)
    assert(actual === expected)
  }

  def collectQueryResult(df: DataFrame): Seq[(Option[Int], Option[Int])] = {
    df.collect()
      .map { row =>
        val id0 = if (row.isNullAt(0)) None else Some(row.getInt(0))
        val id1 = if (row.isNullAt(1)) None else Some(row.getInt(1))
        (id0, id1)
      }
      .sorted
  }

  def isUsingOptimizedSpatialJoin(df: DataFrame): Boolean = {
    val actualPlan = df.queryExecution.executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan
      case plan: SparkPlan => plan
    }
    actualPlan.collect {
      case _: BroadcastIndexJoinExec | _: DistanceJoinExec | _: RangeJoinExec => true
    }.nonEmpty
  }

  def isUsingBroadcastIndexJoin(df: DataFrame): Boolean = {
    val actualPlan = df.queryExecution.executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan
      case plan: SparkPlan => plan
    }
    actualPlan.collect { case _: BroadcastIndexJoinExec =>
      true
    }.nonEmpty
  }
}
