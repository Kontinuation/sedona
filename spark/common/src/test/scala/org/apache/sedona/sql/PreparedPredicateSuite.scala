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

import org.apache.sedona.sql.PreparedPredicateSuite.{LeftSide, QueryWindowSide, RightSide}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.sedona_sql.expressions.ST_PreparedPredicate
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_GeomFromText
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame}
import org.locationtech.jts.io.WKTReader
import org.scalatest.prop.TableDrivenPropertyChecks

/**
 * Test if prepared predicates were correctly populated by the optimizer, and if they evaluates to
 * the correct result.
 */
class PreparedPredicateSuite extends TestBaseScala with TableDrivenPropertyChecks {

  val testDataDelimiter = "\t"

  describe("Sedona-SQL prepared predicate test") {
    val predicates = Table(
      "predicate",
      ("ST_Contains", "POINT (10 10)", RightSide),
      ("ST_Contains", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", LeftSide),
      ("ST_Within", "POINT (10 10)", LeftSide),
      ("ST_Within", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", RightSide),
      ("ST_Covers", "POINT (10 10)", RightSide),
      ("ST_Covers", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", LeftSide),
      ("ST_CoveredBy", "POINT (10 10)", LeftSide),
      ("ST_CoveredBy", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", RightSide),
      ("ST_Intersects", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", LeftSide),
      ("ST_Intersects", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", RightSide),
      ("ST_Touches", "POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))", RightSide),
      ("ST_Crosses", "LINESTRING (9 9, 11 11)", RightSide),
      ("ST_Overlaps", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", RightSide),
      ("ST_Equals", "POINT (10 10)", RightSide),
      ("ST_OrderingEquals", "POINT (10 10)", RightSide))

    forAll(predicates) { case (predicate, queryWindow, queryWindowSide) =>
      it(
        s"should correctly evaluate $predicate when query window is on the ${queryWindowSide.side} side") {
        val df = loadTestDataFrame()
        runSpatialQuery(df, predicate, queryWindow, queryWindowSide)
      }
    }

    forAll(predicates) { case (predicate, queryWindow, queryWindowSide) =>
      it(
        s"should correctly evaluate $predicate when scalar subquery window is on the ${queryWindowSide.side} side") {
        val df = loadTestDataFrame()
        runSpatialQueryWithSubquery(df, predicate, queryWindow, queryWindowSide)
      }
    }
  }

  private def loadTestDataFrame(): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(spatialJoinLeftInputLocation)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c2")))
      .select("id", "geom")
  }

  private def createRunSpatialQuery(leftQuery: String, rightQuery: String)(
      df: DataFrame,
      predicate: String,
      queryWindow: String,
      queryWindowSide: QueryWindowSide) = {
    val (condition, joinCondition) = queryWindowSide match {
      case LeftSide =>
        (leftQuery, s"$predicate(q, geom)")
      case RightSide =>
        (rightQuery, s"$predicate(geom, q)")
    }
    val queryDf = df.where(condition).select(col("id"))
    val oneRowDf =
      sparkSession.createDataFrame(Seq((1, new WKTReader().read(queryWindow)))).toDF("id2", "q")
    val joinDf = df.join(oneRowDf, expr(joinCondition)).select(col("id"))
    assert(collectPreparedPredicates(queryDf).nonEmpty)
    assert(collectPreparedPredicates(joinDf).isEmpty)
    val queryResult = queryDf.collect().map(_.getInt(0)).toSet
    val joinResult = joinDf.collect().map(_.getInt(0)).toSet
    assert(queryResult.nonEmpty)
    assert(queryResult == joinResult)
  }

  private def runSpatialQuery(
      df: DataFrame,
      predicate: String,
      queryWindow: String,
      queryWindowSide: QueryWindowSide) = createRunSpatialQuery(
    s"$predicate(ST_GeomFromText('$queryWindow'), geom)",
    s"$predicate(geom, ST_GeomFromText('$queryWindow'))")(
    df,
    predicate,
    queryWindow,
    queryWindowSide)

  private def runSpatialQueryWithSubquery(
      df: DataFrame,
      predicate: String,
      queryWindow: String,
      queryWindowSide: QueryWindowSide) = createRunSpatialQuery(
    // these are getting optimized away, need a not one line subquery
    s"$predicate((SELECT ST_GeomFromText(FIRST(geom)) FROM VALUES ('$queryWindow'), ('POINT(0 0)') AS (geom)), geom)",
    s"$predicate(geom, (SELECT ST_GeomFromText(FIRST(geom)) FROM VALUES ('$queryWindow'), ('POINT(0 0)') AS (geom)))")(
    df,
    predicate,
    queryWindow,
    queryWindowSide)

  private def collectPreparedPredicates(df: DataFrame): Seq[ST_PreparedPredicate] = {
    df.queryExecution.optimizedPlan.collect { case Filter(condition, _) =>
      condition.collect { case p: ST_PreparedPredicate => p }
    }.flatten
  }
}

object PreparedPredicateSuite {
  sealed trait QueryWindowSide {
    def side: String
  }

  case object LeftSide extends QueryWindowSide {
    val side: String = "left"
  }

  case object RightSide extends QueryWindowSide {
    val side: String = "right"
  }
}
