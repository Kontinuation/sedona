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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_GeomFromText
import org.apache.spark.sql.sedona_sql.strategy.join.KNNJoinExec
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalatest.matchers.must.Matchers.{be, include}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.TableDrivenPropertyChecks

/**
 * Test suite for KNN spatial join SQLs
 *
 * This test suite validates the KNN spatial join SQLs for Sedona-SQL module. The main purpose of
 * this test suite is to validate the KNN spatial join SQLs but not the validation of the join
 * results.
 *
 * The join results are validated in the Sedona core module with the unit tests.
 */
class KnnJoinSuite extends TestBaseScala with TableDrivenPropertyChecks {

  val testDataDelimiter = "\t"
  val knnPointsLocationQueries: String = resourceFolder + "knn/queries.csv"
  val knnPointsLocationObjects: String = resourceFolder + "knn/objects.csv"
  val numPartitions = 4

  override def beforeAll(): Unit = {
    super.beforeAll()
    prepareTempViewsForTestData()
  }

  describe("KNN spatial join SQLs should be parsed correctly") {
    it("KNN Join with approximate algorithms based on euclidean distance") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false)")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 5,
        isGeography = false,
        mustInclude = "")
    }

    it(
      "KNN Join with approximate algorithms based on euclidean distance using join-where clause and apply ST_Distance") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.GEOM, OBJECTS.GEOM, ST_Distance(QUERIES.GEOM, OBJECTS.GEOM) FROM QUERIES, OBJECTS WHERE ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false)")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 5,
        isGeography = false,
        mustInclude = "")
    }

    it(
      "KNN Join with approximate algorithms based on euclidean distance using join-where clause and select gem") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.GEOM, OBJECTS.GEOM FROM QUERIES, OBJECTS WHERE ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false)")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 5,
        isGeography = false,
        mustInclude = "")
    }

    it(
      "KNN Join with approximate algorithms based on euclidean distance using join-where clause and select all") {
      val df = sparkSession.sql(
        s"SELECT * FROM QUERIES, OBJECTS WHERE ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false)")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 5,
        isGeography = false,
        mustInclude = "")
    }

    it("KNN Join with exact algorithms based on euclidean distance") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 3, true)")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = false,
        expressionSize = 5,
        isGeography = true,
        mustInclude = "")
    }

    it("KNN Join based on single point on left side should not be supported") {
      val exception = intercept[UnsupportedOperationException] {
        val df = sparkSession.sql(
          s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(ST_MakePoint(100, 100, 1), OBJECTS.GEOM, 3, false)")
        validateQueryPlan(
          df,
          numNeighbors = 3,
          useApproximate = true,
          expressionSize = 5,
          isGeography = false,
          mustInclude = "")
      }
      exception.getMessage should include("ST_AKNN filter is not yet supported in the join query")
    }

    it("KNN Join based on single point on right side should not be supported") {
      val exception = intercept[UnsupportedOperationException] {
        val df = sparkSession.sql(
          s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(OBJECTS.GEOM, ST_MakePoint(100, 100, 1), 3, false)")
        validateQueryPlan(
          df,
          numNeighbors = 3,
          useApproximate = true,
          expressionSize = 5,
          isGeography = false,
          mustInclude = "")
      }
      exception.getMessage should include("ST_AKNN filter is not yet supported in the join query")
    }

    it("KNN Join based with complex join conditions using integer columns") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false) AND QUERIES.ID <= 88")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 5,
        isGeography = false,
        mustInclude = "as int) <= 88))")
    }

    it("KNN Join based with complex join conditions using text columns") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false) AND QUERIES.SHAPE = 'point'")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 5,
        isGeography = false,
        mustInclude = "= point))")
    }

    it("KNN Join based with complex join conditions using text columns and using where clause") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false) WHERE QUERIES.SHAPE = 'point'")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 5,
        isGeography = false,
        mustInclude = "= point))")
    }

    it("KNN Join should work with dataframe containing 0 partitions") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, EMPTYTABLE.ID FROM QUERIES JOIN EMPTYTABLE ON ST_AKNN(QUERIES.GEOM, EMPTYTABLE.GEOM, 3, false)")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 5,
        isGeography = false,
        mustInclude = "")
    }

    it("KNN Join should not support broadcast join hint on left side") {
      val exception = intercept[UnsupportedOperationException] {
        val df = sparkSession.sql(
          s"SELECT /*+ BROADCAST(QUERIES) */ QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false)")
        validateQueryPlan(
          df,
          numNeighbors = 3,
          useApproximate = true,
          expressionSize = 5,
          isGeography = false,
          mustInclude = "")
      }
      exception.getMessage should include("KNN joins are not supported with broadcast hint")
    }

    it("KNN Join should not support broadcast join hint on right side") {
      val exception = intercept[UnsupportedOperationException] {
        val df = sparkSession.sql(
          s"SELECT /*+ BROADCAST(OBJECTS) */ QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false)")
        validateQueryPlan(
          df,
          numNeighbors = 3,
          useApproximate = true,
          expressionSize = 5,
          isGeography = false,
          mustInclude = "")
      }
      exception.getMessage should include("KNN joins are not supported with broadcast hint")
    }
  }

  describe("KNN spatial join SQLs should be executed correctly") {
    it("KNN Join with approximate algorithms based on EUCLIDEAN distance") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 4, false)")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(3 * 4) // 3 queries and 4 neighbors each
      resultAll.mkString should be(
        "[1,3][1,6][1,13][1,16][2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]"
      ) // validate for both AKNN and KNN
    }

    it("KNN Join with approximate algorithms based on SPHEROID distance") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 4, true)")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(3 * 4) // 3 queries and 4 neighbors each
      resultAll.mkString should be(
        "[1,3][1,6][1,13][1,16][2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]"
      ) // validate for both AKNN and KNN
    }

    it("KNN Join with approximate algorithms with additional join conditions on id") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, 4, false) AND QUERIES.ID > 1")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(8) // 2 queries (filtered out 1) and 4 neighbors each
      resultAll.mkString should be(
        "[2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]"
      ) // validate for both AKNN and KNN
    }

    it("KNN Join with exact algorithms are not supported yet") {
      val exception = intercept[UnsupportedOperationException] {
        val df = sparkSession.sql(
          s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, false)")
        df.collect().sortBy(_.getInt(0))
      }
      exception.getMessage should include(
        "Exact KNN join is not supported"
      ) // remove this once the actual join is supported
    }
  }

  def validateQueryPlan(
      df: DataFrame,
      numNeighbors: Int,
      useApproximate: Boolean,
      expressionSize: Int,
      isGeography: Boolean,
      mustInclude: String): Unit = {
    print(df.queryExecution.executedPlan.toString)
    df.queryExecution.executedPlan.toString should include("KNNJoin")
    Option(mustInclude).filter(_.nonEmpty).foreach { text =>
      df.queryExecution.executedPlan.toString should include(text)
    }
    df.queryExecution.executedPlan.collect { case p: KNNJoinExec =>
      p.k should be(Literal(numNeighbors))
      p.useApproximate should be(useApproximate)
      p.isGeography should be(isGeography)
      p.expressions.size should be(expressionSize)
    }
  }

  private def prepareTempViewsForTestData(): (DataFrame, DataFrame) = {
    val df1 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(knnPointsLocationQueries)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c1")))
      .withColumn("shape", col("_c1"))
      .select("id", "geom", "shape")
    val df2 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(knnPointsLocationObjects)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c1")))
      .withColumn("shape", col("_c1"))
      .select("id", "geom", "shape")
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    sparkSession.table("df1").repartition(numPartitions).createOrReplaceTempView("queries")
    sparkSession.table("df2").repartition(numPartitions).createOrReplaceTempView("objects")

    val emptyRdd = sparkSession.sparkContext.emptyRDD[Row]
    val emptyDf = sparkSession.createDataFrame(
      emptyRdd,
      StructType(Seq(StructField("id", IntegerType), StructField("geom", GeometryUDT))))
    emptyDf.createOrReplaceTempView("EMPTYTABLE")

    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    (df1, df2)
  }
}
