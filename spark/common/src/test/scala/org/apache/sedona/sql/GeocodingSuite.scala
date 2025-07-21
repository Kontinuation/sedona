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

import org.apache.spark.sql.sedona_sql.expressions.st_functions.ST_AsText
import org.apache.spark.sql.sedona_sql.expressions.st_functions.ST_Geocode
import org.apache.spark.sql.{functions => f}

class GeocodingSuite extends AbstractGeocodingSuite {
  it("returns the correct geocodes for exact address") {
    val resultDf =
      spark.sql(
        "SELECT ST_AsText(ST_Geocode('1600 Amphitheatre Parkway, Mountain View, CA').geometry) as geocode")
    val result = resultDf.collect().head.get(0)
    assert(result == "POINT (-122.084068 37.422408)")
  }

  it("maintains existing columns and adds geocode column") {
    val resultDf =
      spark.sql("""
          |SELECT id,
          |ST_AsText(ST_Geocode(location).geometry) as geocode,
          | ST_Geocode(location).score as score,
          | ST_Geocode(location).score_algorithm as score_algorithm
          |FROM geocodeTest""".stripMargin)
    val result =
      resultDf
        .collect()
        .map(row =>
          (
            row.getAs[Int]("id"),
            row.getAs[String]("geocode"),
            Option(row.getAs[Long]("score")),
            row.getAs[String]("score_algorithm")))
    assert(result.contains((1, "POINT (-122.084068 37.422408)", Some(0), "levenshtein")))
    assert(result.contains((7, "POINT (-122.080068 37.442408)", Some(0), "levenshtein")))
    assert(result.contains((12, "POINT (-122.084068 37.422408)", Some(0), "levenshtein")))
  }

  it("returns empty geocode for non-existing address") {
    val resultDf =
      spark.sql("SELECT ST_AsText(ST_Geocode('Non-existing address').geometry) as geocode")
    val result = resultDf.collect().head.get(0)
    assert(result == null)
    assert(resultDf.count() == 1)
  }

  it("works with dataframe API") {
    val resultDf = spark
      .range(1)
      .select(
        ST_AsText(ST_Geocode(f.lit("1600 Amphitheatre Parkway, Mountain View, CA"))("geometry"))
          .alias("geocode"))
    val result = resultDf.collect().head.get(0)
    assert(result == "POINT (-122.084068 37.422408)")
  }
}
