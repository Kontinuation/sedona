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

import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_GeomFromText
import org.apache.spark.sql.sedona_sql.expressions.{ST_PreparedContains, ST_PreparedWithin}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.BeforeAndAfter

class ReplaceSingleRowJoinsWithScalarSubqueriesSuite extends TestBaseScala with BeforeAndAfter {
  val testDataDelimiter = "\t"

  lazy private val df1 = loadTestDataFrame()

  private def loadTestDataFrame(): DataFrame = {
    val df1 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(spatialJoinLeftInputLocation)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c2")))
      .select("id", "geom")

    df1.createOrReplaceTempView("df1")

    df1
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    loadTestDataFrame()
  }

  private def testJoin(query: String, keepDf2: Boolean): Unit = {
    val df2 = sparkSession.sql(query)
    df2.createOrReplaceTempView("df2")

    for ((l, r) <- Seq(("df1", "df2"), ("df2", "df1"))) {
      var expectedResult = (keepDf2, l) match {
        case (true, "df1") => df1.select(df1("*"), expr(f"($query) as geom2"))
        case (true, "df2") => df1.select(expr(f"($query) as geom2"), df1("*"))
        case (false, _) => df1
      }
      expectedResult = expectedResult.where(f"ST_CONTAINS(($query), geom)")

      val actualResult = sparkSession.sql(
        f"SELECT ${if (keepDf2) "" else "df1."}* FROM $l, $r WHERE ST_Contains(geom2, geom)")
      assert(actualResult.queryExecution.optimizedPlan.find(_.isInstanceOf[Join]).isEmpty)
      if (actualResult.queryExecution.optimizedPlan.maxRows.nonEmpty && actualResult.queryExecution.optimizedPlan.maxRows.get == 0) {
        assert(expectedResult.queryExecution.optimizedPlan.maxRows.get == 0)
        assert(expectedResult.columns sameElements actualResult.columns)
      } else {
        val condition = actualResult.queryExecution.optimizedPlan
          .find(_.isInstanceOf[Filter])
          .get
          .asInstanceOf[Filter]
          .condition
        assert(
          condition.isInstanceOf[ST_PreparedWithin] || condition
            .isInstanceOf[ST_PreparedContains])
        assert(actualResult.collect() sameElements expectedResult.collect())
      }
    }
  }

  private def testCrossJoin(query: String, keepDf2: Boolean): Unit = {
    val df2 = sparkSession.sql(query)
    df2.createOrReplaceTempView("df2")

    for ((l, r) <- Seq(("df1", "df2"), ("df2", "df1"))) {
      val expectedResult = (keepDf2, l) match {
        case (true, "df1") => df1.select(df1("*"), expr(f"($query) as geom2"))
        case (true, "df2") => df1.select(expr(f"($query) as geom2"), df1("*"))
        case (false, _) => df1
      }
      val actualResult = sparkSession.sql(f"SELECT ${if (keepDf2) "" else "df1."}* FROM $l, $r")
      assert(actualResult.queryExecution.optimizedPlan.find(_.isInstanceOf[Join]).isEmpty)
      if (actualResult.queryExecution.optimizedPlan.maxRows.nonEmpty && actualResult.queryExecution.optimizedPlan.maxRows.get == 0) {
        assert(expectedResult.queryExecution.optimizedPlan.maxRows.get == 0)
        assert(expectedResult.columns sameElements actualResult.columns)
      } else {
        assert(actualResult.queryExecution.optimizedPlan.find(_.isInstanceOf[Filter]).isEmpty)
        assert(actualResult.collect() sameElements expectedResult.collect())
      }

      assert(actualResult.count() == df1.count() * df2.count())
    }
  }

  describe("OneRowRelation Join") {
    it("should use prepared predicate when the scalar is discarded") {
      testJoin(
        "SELECT ST_GEOMFROMTEXT('POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))') AS geom2",
        keepDf2 = false)
    }

    it("should use prepared predicate when the scalar is kept") {
      testJoin(
        "SELECT ST_GEOMFROMTEXT('POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))') AS geom2",
        keepDf2 = true)
    }

    it("should optimize with a null result when the scalar is discarded") {
      testJoin("SELECT null AS geom2", keepDf2 = false)
    }

    it("should optimize with a null result when the scalar is kept") {
      testJoin("SELECT null AS geom2", keepDf2 = true)
    }
  }

  describe("Limit 1 Join") {
    it("should use prepared predicate when the scalar is discarded") {
      testJoin("SELECT geom AS geom2 FROM df1 LIMIT 1", keepDf2 = false)
    }

    it("should use prepared predicate when the scalar is kept") {
      testJoin("SELECT geom AS geom2 FROM df1 LIMIT 1", keepDf2 = true)
    }

    it("should use prepared predicate when the scalar is discarded with 0 rows") {
      val query = "SELECT geom AS geom2 FROM df1 WHERE geom IS NULL LIMIT 1"
      assert(sparkSession.sql(query).count() == 0)
      testJoin(query, keepDf2 = false)
    }

    it("should use prepared predicate when the scalar is kept with 0 rows") {
      val query = "SELECT geom AS geom2 FROM df1 WHERE geom IS NULL LIMIT 1"
      assert(sparkSession.sql(query).count() == 0)
      testJoin(query, keepDf2 = true)
    }

    it("should optimize with a null result when the scalar is discarded") {
      df1
        .select("geom")
        .union(sparkSession.sql("SELECT null AS geom"))
        .select(col("geom").as("geom2"))
        .checkpoint
        .createOrReplaceTempView("df2")
      testJoin("SELECT geom2 FROM df2 ORDER BY geom2 NULLS FIRST LIMIT 1", keepDf2 = false)
    }

    it("should optimize with a null result when the scalar is kept") {
      df1
        .select("geom")
        .union(sparkSession.sql("SELECT null AS geom"))
        .select(col("geom").as("geom2"))
        .checkpoint
        .createOrReplaceTempView("df2")
      testJoin("SELECT geom2 FROM df2 ORDER BY geom2 NULLS FIRST LIMIT 1", keepDf2 = true)
    }
  }

  describe("1 row Aggregate Join") {
    it("should use prepared predicate when the scalar is discarded") {
      val query = "SELECT MAX(geom) AS geom2 FROM df1"
      testJoin(query, keepDf2 = false)
    }
    it("should use prepared predicate when the scalar is kept") {
      val query = "SELECT MAX(geom) AS geom2 FROM df1"
      testJoin(query, keepDf2 = true)
    }
  }

  describe("One Row Cross Join") {
    it("should optimize when the scalar is discarded") {
      testCrossJoin("SELECT 1 as geom2", keepDf2 = false)
    }
    it("should optimize when the scalar is kept") {
      testCrossJoin("SELECT 1 as geom2", keepDf2 = true)
    }
    it("should optimize when the scalar is null and discarded") {
      testCrossJoin("SELECT null as geom2", keepDf2 = false)
    }
    it("should optimize when the scalar is null and kept") {
      testCrossJoin("SELECT null as geom2", keepDf2 = true)
    }
    it("do not optimize when there are no rows on 'scalar' side and the scalar is discarded") {
      // In this case there is a project with an empty output, so the join is not optimized
      // There is probably some possibility to optimize this case, but it is not implemented yet

      val query = "SELECT geom AS geom2 FROM df1 WHERE geom IS NULL LIMIT 1"
      assert(sparkSession.sql(query).count() == 0)
      val actualResult = df1.join(sparkSession.sql(query)).select(df1("*"))

      assert(actualResult.queryExecution.optimizedPlan.find(_.isInstanceOf[Join]).nonEmpty)
      assert(actualResult.collect().isEmpty)

    }
    it("should return no rows when there are no rows on 'scalar' side and the scalar is kept") {
      testCrossJoin("SELECT geom AS geom2 FROM df1 WHERE geom IS NULL LIMIT 1", keepDf2 = true)
    }
  }
}
