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

import org.apache.sedona.util.GeocodingUtils.createAddressIndex
import org.apache.spark.sql.{SparkSession, functions => f}
import org.scalatest.BeforeAndAfterAll

case class Geocode(id: Int, layer: String, location: String, x: Double, y: Double)

class AbstractGeocodingSuite extends TestBaseScala with BeforeAndAfterAll {
  protected lazy val spark: SparkSession = sparkSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    create_geocode_tables()

    // defaults are set for overture so we set these for our custom test layer names
    spark.conf.set("spark.sedona.reverse.geocode.distance.address", "0.0003")
    spark.conf.set("spark.sedona.reverse.geocode.distance.poi", "0.0006")

  }

  override def afterAll(): Unit = {
    spark.catalog.dropTempView("geocodeTest")
    super.afterAll()
  }

  private def create_geocode_tables(): Unit = {
    val geocode_df = spark
      .createDataFrame(
        Seq(
          Geocode(
            1,
            "address",
            "1600 Amphitheatre Parkway, Mountain View, CA",
            -122.084068,
            37.422408),
          Geocode(
            7,
            "address",
            "1620 Amphitheatre Parkway, Mountain View, CA",
            -122.080068,
            37.442408),
          Geocode(12, "poi", "Google", -122.084068, 37.422408)))
      .withColumn("geometry", f.expr("ST_Point(x, y)"))
      .drop("x", "y")

    geocode_df.createOrReplaceTempView("geocodeTest")
    spark.conf.set("spark.sedona.geocode.table", "geocodeTest")

    createAddressIndex(geocode_df).createOrReplaceTempView("geocodeIndexTest")
    spark.conf.set("spark.sedona.geocode.index.table", "geocodeIndexTest")
  }
}
