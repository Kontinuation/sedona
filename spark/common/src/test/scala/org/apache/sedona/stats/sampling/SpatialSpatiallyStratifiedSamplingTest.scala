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
package org.apache.sedona.stats.sampling

import org.apache.sedona.sql.TestBaseScala
import org.apache.sedona.stats.MetricsRegistrator
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_Point
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_X, ST_Y}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class SpatialSpatiallyStratifiedSamplingTest extends TestBaseScala {

  def getData(dim: Int): Dataset[Row] = {
    val spark: SparkSession = sparkSession
    import spark.implicits._

    (0 until dim)
      .map(_.toDouble)
      .toDF("x")
      .withColumn("y", explode(lit((0 until dim).map(_.toDouble).toArray)))
      .withColumn("geometry", ST_Point(col("x"), col("y")))
  }

  describe("getSpatiallyStratifiedSamples") {
    it("returned sampled data when valid parameters are provided") {
      val initialMetricCount =
        MetricsRegistrator.getOrCreate.SpatiallyStratifiedSamplePerform.getCount
      val dim = 300
      val inputData = getData(dim)
      val inputDataCount = inputData.count()

      val outputDf = SpatiallyStratifiedSampling.spatiallyStratifiedSample(inputData, 0.5, 2)
      assert(outputDf.schema == inputData.schema, "Schema should be the same as input")

      assert(
        (outputDf.count() - inputDataCount * 5) < inputDataCount * .5 * .03,
        "Output count should be close to 50% of input count")

      val expected_count = inputDataCount * .5 * .25
      for (filter <- Seq(
          ST_X(col("geometry")) < dim / 2 and ST_Y(col("geometry")) < dim / 2,
          ST_X(col("geometry")) >= dim / 2 and ST_Y(col("geometry")) < dim / 2,
          ST_X(col("geometry")) < dim / 2 and ST_Y(col("geometry")) >= dim / 2,
          ST_X(col("geometry")) >= dim / 2 and ST_Y(col("geometry")) >= dim / 2)) {
        // fuzzy match because sampling has some imprecision
        assert((outputDf.where(filter).count - expected_count).abs < expected_count * .03)

        // Because tests are run in parallel we can't guarantee some specific value of the metric
        assert(
          (MetricsRegistrator.getOrCreate.SpatiallyStratifiedSamplePerform.getCount - initialMetricCount) == 1)
      }

    }

    it("threw an exception when sampling rate is out of range") {
      assertThrows[IllegalArgumentException] {
        SpatiallyStratifiedSampling.spatiallyStratifiedSample(getData(3), 1.5, 2, "geometry")
      }
    }

    it("threw an exception when partition count is less than or equal to zero") {
      assertThrows[IllegalArgumentException] {
        SpatiallyStratifiedSampling.spatiallyStratifiedSample(getData(3), 0.5, 0, "geometry", 44)
      }
    }

    it("threw an exception when geometry column is not present") {
      assertThrows[IllegalArgumentException] {
        SpatiallyStratifiedSampling.spatiallyStratifiedSample(
          getData(3),
          0.5,
          2,
          "nonexistent",
          49)
      }
    }
  }
}
