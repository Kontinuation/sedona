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

import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, rank}
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_GetReverseGeocodingLayers, ST_ReverseGeocode}
import org.apache.spark.sql.{DataFrame, functions => f}
import org.scalatest.BeforeAndAfterAll

case class Geocode(layer: String, location: String, x: Double, y: Double)

case class Query(id: Int, x: Double, y: Double, layer: String)

class ReverseGeocodeSuite extends TestBaseScala with BeforeAndAfterAll {

  private val spark = sparkSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    create_geocode_table()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.catalog.dropTempView("geocodeTest")
  }

  private def create_geocode_table(): Unit = {
    val geocode_df = spark
      .createDataFrame(
        Seq(
          Geocode(
            "address",
            "1600 Amphitheatre Parkway, Mountain View, CA",
            -122.084068,
            37.422408),
          Geocode(
            "address",
            "1620 Amphitheatre Parkway, Mountain View, CA",
            -122.080068,
            37.442408),
          Geocode("poi", "Google", -122.084068, 37.422408)))
      .withColumn("geometry", f.expr("ST_Point(x, y)"))
      .drop("x", "y")

    geocode_df.createOrReplaceTempView("geocodeTest")
    spark.conf.set("spark.sedona.reverse.geocode.table", "geocodeTest")
  }

  private def get_input_data(): DataFrame = {
    spark
      .createDataFrame(
        Seq(
          Query(0, -122.084068, 37.422408, "poi"),
          Query(1, -122.084078, 37.422418, "address"),
          Query(2, -122.084063, 37.422398, "poi"),
          Query(3, -123.0, 37.0, "address"), // should match nothing
          Query(4, -122.084068 - 0.00031, 37.422408, "address") // should match only poi
        ))
      .withColumn("geometry", f.expr("ST_Point(x, y)"))
      .drop("x", "y")
  }

  it("test reverse geocoding function with literal layer") {

    val df = get_input_data()
      .drop("layer") // avoid debugging confusion

    val geocodedDf = df
      .withColumn("reverse_geocoded", f.expr("ST_ReverseGeocode(geometry, 'poi')"))

    // return every row, every column
    assert(geocodedDf.count() == df.count())
    assert(geocodedDf.columns.length == df.columns.length + 1)

    assert(geocodedDf.where("reverse_geocoded.location is not null").count() == df.count() - 1)
    assert(geocodedDf.where("reverse_geocoded.location = 'Google'").count() == df.count() - 1)
    assert(
      geocodedDf
        .where("reverse_geocoded.geometry = ST_Point(-122.084068, 37.422408)")
        .count() == df.count() - 1)
    assert(geocodedDf.where("reverse_geocoded.layer = 'poi'").count() == df.count())

  }

  it("test reverse geocoding function with col layer") {

    val df = get_input_data()

    val geocodedDf = df
      .withColumn("reverse_geocoded", f.expr("ST_ReverseGeocode(geometry, layer)"))

    // return every row, every column
    assert(geocodedDf.count() == df.count())
    assert(geocodedDf.columns.length == df.columns.length + 1)

    assert(geocodedDf.where("reverse_geocoded.location is not null").count() == 3)

    // The poi and address layers share a geom
    assert(
      geocodedDf
        .where("reverse_geocoded.geometry = ST_Point(-122.084068, 37.422408)")
        .count() == 3)

    assert(geocodedDf.where("reverse_geocoded.location = 'Google'").count() == 2)
    assert(geocodedDf.where("reverse_geocoded.layer = 'poi'").count() == 2)

    assert(
      geocodedDf
        .where("reverse_geocoded.location = '1600 Amphitheatre Parkway, Mountain View, CA'")
        .cache()
        .count() == 1)
    assert(geocodedDf.where("reverse_geocoded.layer = 'address'").count() == 3)

  }

  it("test reverse geocoding Rule works when ST_ReverseGeocode is not top level") {

    val df = get_input_data()

    val geocodedDf = df
      .withColumn("reverse_geocoded", f.expr("ST_ReverseGeocode(geometry, layer)"))
      .withColumn("anotherColumn", f.rand())

    // if we didn't optimize away the ST_ReverseGeocode call, this will throw an exception
    // if we failed to preserve the anotherColumn, this will throw an exception
    geocodedDf.collect()
  }

  it("test parallel reverse geocoding function calls") {

    val df = get_input_data().drop("layer")

    val geocodedDf = df.select(
      f.col("*"),
      f.expr("ST_ReverseGeocode(geometry, 'address')").alias("reverse_geocoded"),
      f.expr("ST_ReverseGeocode(geometry, 'poi')").alias("reverse_geocoded2"))

    assert(
      geocodedDf
        .where("reverse_geocoded.layer = 'address' and reverse_geocoded2.layer = 'poi'")
        .count() == 5)

    // Don't confuse the results
    assert(geocodedDf.where("reverse_geocoded.location = 'Google'").count() == 0)
    assert(
      geocodedDf
        .where("reverse_geocoded2.location = '1600 Amphitheatre Parkway, Mountain View, CA'")
        .count() == 0)

    assert(geocodedDf.where("reverse_geocoded2.location = 'Google'").count() == 4)
    assert(
      geocodedDf
        .where("reverse_geocoded.location = '1600 Amphitheatre Parkway, Mountain View, CA'")
        .count() == 3)

  }

  it("test nested reverse geocoding function calls") {

    val df = get_input_data()

    val geocodedDf = df.withColumn(
      "reverse_geocoded",
      f.expr("ST_ReverseGeocode(geometry, ST_ReverseGeocode(geometry, 'poi').layer)"))
    val exception = intercept[IllegalArgumentException] {
      geocodedDf.collect()
    }

    assert(exception.getMessage == "ST_ReverseGeocode calls cannot be nested")
  }

  it("test reverse geocoding function calls in where clause") {

    val df = get_input_data()

    val geocodedDf = df.where("ST_ReverseGeocode(geometry, 'poi') IS NOT NULL")

    geocodedDf.collect().length == df.count()
  }

  it("test reverse geocoding function calls in subquery") {

    val df = get_input_data()

    val geocodedDf = df
      .withColumn("reverse_geocoded", f.expr("ST_ReverseGeocode(geometry, 'address')"))

    geocodedDf.createOrReplaceTempView("geocodedDf")

    spark.sql("SELECT * FROM geocodedDf WHERE reverse_geocoded.location is not null").collect()
  }

  it("test can call through df functions") {

    val df = get_input_data()

    val geocodedDf =
      df.withColumn("reverse_geocoded", ST_ReverseGeocode(f.col("geometry"), f.lit("poi")))

    geocodedDf.collect()
  }

  it("test support dfs with map column") {
    val df = get_input_data()

    val geocodedDf = df
      .withColumn("myMap", f.map(lit("key"), lit("value")))
      .withColumn("reverse_geocoded", ST_ReverseGeocode(f.col("geometry"), f.lit("address")))

    geocodedDf.collect()

  }

  it("test when table has wrong schema exception is thrown") {
    val geocode_df = spark
      .createDataFrame(Seq(
        Geocode(
          "address",
          "1600 Amphitheatre Parkway, Mountain View, CA",
          37.422408,
          -122.084068),
        Geocode(
          "address",
          "1620 Amphitheatre Parkway, Mountain View, CA",
          37.442408,
          -122.080068),
        Geocode("poi", "Google", 37.422408, -122.084068)))
      .withColumn("geometry", f.expr("ST_Point(x, y)"))
      .drop("x", "y", "layer")

    geocode_df.createOrReplaceTempView("badGeocodeTest")
    spark.conf.set("spark.sedona.reverse.geocode.table", "badGeocodeTest")
    try {
      val df = get_input_data()

      val geocodedDf = df.withColumn("rgc", f.expr("ST_ReverseGeocode(geometry, 'poi')"))
      val exception = intercept[IllegalArgumentException] {
        geocodedDf.collect()
      }

      assert(
        exception.getMessage == "requirement failed: spark.sedona.reverse.geocode.table set to badGeocodeTest. badGeocodeTest does not have a layer column")
    } finally { // always set this back for other tests
      spark.conf.set("spark.sedona.reverse.geocode.table", "geocodeTest")
    }
  }

  it("ST_GetReverseGeocodingLayers function gives expected result") {
    val df = spark.sql("SELECT ST_GetReverseGeocodingLayers() AS layers")

    df.collect() // make sure it doesn't throw an exception
    assert(df.count() == 1)
    assert(df.where("size(layers) = 2").count() == 1)
  }

  it("can use ST_GetReverseGeocodingLayers in ST_ReverseGeocode") {
    val df = get_input_data().drop("layer")

    val geocodedDf = df
      .withColumn("layers", ST_GetReverseGeocodingLayers())
      .withColumn("reverse_geocoded", ST_ReverseGeocode(f.col("geometry"), f.expr("layers[0]")))
    assert(geocodedDf.where("reverse_geocoded.layer = layers[0]").count() == df.count())

  }

  it("test rewritten function inside of a stdlib function") {
    get_input_data()
      .select(sql.functions.size(ST_GetReverseGeocodingLayers()).alias("layerSize"))
      .collect()
      .map(_(0))
      .map(r => assert(r == 2))
  }

  it("test rewritten function inside of a stdlib function optimized into a filter is supported") {
    spark
      .sql("SELECT size(ST_GetReverseGeocodingLayers()) = 1 AS isSize")
      .where("isSize")
      .collect()
      .length == 1
  }

  it("test RewriteLogicalPlan supports sort") {
    // collect proves columns are correct and ordered correctly
    get_input_data().sort(ST_GetReverseGeocodingLayers()).collect()

    // sort where there is a function call passed through
    get_input_data().withColumn("myLayers", ST_GetReverseGeocodingLayers()).sort("id").collect()
  }

  it("test RewriteLogicalPlan supports window") {
    // just testing that this doesn't blow up
    get_input_data()
      .select(
        f.col("*"),
        rank().over(Window.partitionBy(ST_GetReverseGeocodingLayers()).orderBy(f.col("id"))))
      .collect()

    get_input_data()
      .select(
        f.col("*"),
        rank().over(Window.partitionBy("layer").orderBy(ST_GetReverseGeocodingLayers())))
      .collect()
  }

  it("test RewriteLogicalPlan supports aggregate in groupBy statement") {
    // just testing that this doesn't blow up
    get_input_data()
      .groupBy(ST_GetReverseGeocodingLayers())
      .count()
      .collect()

    get_input_data()
      .groupBy(ST_GetReverseGeocodingLayers().alias("abc123"))
      .count()
      .collect()
  }

  it("test RewriteLogicalPlan does not support aggregate in agg statement") {
    val exception = intercept[IllegalArgumentException] {
      get_input_data()
        .groupBy(f.col("layer"))
        .agg(ST_GetReverseGeocodingLayers())
        .collect()
    }

    assert(
      exception.getMessage == "Unsupported call to ST_GetReverseGeocodingLayers in aggregate expression. If this is not the case, report a bug.")

  }

}
