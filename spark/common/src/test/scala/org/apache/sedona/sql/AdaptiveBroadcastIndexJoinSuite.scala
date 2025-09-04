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

import org.apache.sedona.core.utils.ExecutorResourceUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory}

class AdaptiveBroadcastIndexJoinSuite extends TestBaseScala {

  override def sparkConfig: Map[String, String] =
    defaultSparkConfig ++ Map(
      "spark.sedona.join.advanced" -> "true",
      "sedona.join.autoBroadcastJoinThreshold" -> "100m",
      "spark.sedona.join.allowPlanBroadcastJoin" -> "false",
      "spark.sedona.join.streamSideSkewScoreThreshold" -> "2.0")

  private val schema = StructType(
    Seq(StructField("id", StringType, false), StructField("geom", GeometryUDT, false)))

  describe("Auto re-balancing stream side") {
    it("should not re-balance the stream side when stream side is not skewed") {
      val df2PartitionSizes = Seq(10000L, 11000L, 12000L)
      prepareTempViewsForTestData(df2PartitionSizes)
      verifyQuery(
        "SELECT df1.id, df2.id FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom)",
        Some(df2PartitionSizes))
      verifyQuery(
        "SELECT df2.id, df1.id FROM df2 JOIN df1 ON ST_Intersects(df2.geom, df1.geom)",
        Some(df2PartitionSizes))
    }

    it("should re-balance the stream side when stream side is skewed") {
      val df2PartitionSizes = Seq(10000L, 10000L, 50000L)
      prepareTempViewsForTestData(df2PartitionSizes)
      verifyQuery("SELECT df1.id, df2.id FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom)")
      verifyQuery("SELECT df2.id, df1.id FROM df2 JOIN df1 ON ST_Intersects(df2.geom, df1.geom)")
    }

    it("should re-balance the stream side when the stream side is underpartitioned") {
      val partitionSize = 150000L
      val df2PartitionSizes = Seq(partitionSize)
      prepareTempViewsForTestData(df2PartitionSizes)
      verifyQuery(
        "SELECT df1.id, df2.id FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom)",
        None,
        Some(partitionSize),
        Some(df2PartitionSizes.length))
      verifyQuery(
        "SELECT df2.id, df1.id FROM df2 JOIN df1 ON ST_Intersects(df2.geom, df1.geom)",
        None,
        Some(partitionSize),
        Some(df2PartitionSizes.length))
    }
  }

  private def prepareTempViewsForTestData(df2PartitionSizes: Seq[Long]): Unit = {
    import scala.collection.JavaConverters._
    val factory = new GeometryFactory()
    val df1Rows = df2PartitionSizes.indices.map { index =>
      Row(s"id_$index", factory.toGeometry(new Envelope(0, 100000, index - 0.1, index + 0.1)))
    }
    val df1 = sparkSession.createDataFrame(df1Rows.asJava, schema)
    val df2 = createTestDataset(df2PartitionSizes)
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
  }

  private def createTestDataset(partitionSizes: Seq[Long]): DataFrame = {
    // Generate a test dataset with the given partition sizes.
    // Each partition will have a different number of rows.
    // The number of partitions will be the length of the partitionSizes sequence.
    // The number of rows in each partition will be the corresponding value in the partitionSizes sequence.
    // The dataset will be created in a way that the partition sizes are skewed.
    val rdd = sparkSession.sparkContext
      .parallelize(partitionSizes)
      .repartition(partitionSizes.size)
      .mapPartitionsWithIndex { (index, _) =>
        val factory = new GeometryFactory()
        (0L until partitionSizes(index)).map { i =>
          val geom = factory.createPoint(new Coordinate(i, index))
          val id = s"id_${index}_$i"
          Row(id, geom)
        }.iterator
      }

    sparkSession.createDataFrame(rdd, schema)
  }

  private def verifyQuery(
      query: String,
      expectedPartitionSizes: Option[Seq[Long]] = None,
      expectedPartitionCount: Option[Long] = None,
      originalNumPartitions: Option[Int] = None): Unit = {
    val result = sparkSession.sql(query)
    val expected = withConf(Map("sedona.join.optimizationmode" -> "none")) {
      sparkSession.sql(query)
    }
    val resultRows = result.collect().map(row => (row.getString(0), row.getString(1))).sorted
    val expectedRows = expected.collect().map(row => (row.getString(0), row.getString(1))).sorted
    assert(resultRows === expectedRows)

    expectedPartitionSizes match {
      case Some(sizes) =>
        // Check if the partition sizes are the same as the expected partition sizes
        assert(result.rdd.partitions.length == sizes.length)
        assert(result.rdd.mapPartitions(iter => Iterator(iter.size)).collect() === sizes)
      case None =>
        // Check if the partition sizes are balanced
        val partitionSizes = result.rdd.mapPartitions(iter => Iterator(iter.size))
        val maxSize = partitionSizes.max()
        val minSize = partitionSizes.min()
        val meanSize = partitionSizes.mean()
        assert(maxSize - minSize <= 0.01 * meanSize)
    }

    if (expectedPartitionCount.nonEmpty) {
      assert(
        ExecutorResourceUtils.getTargetPartitionCount(
          sparkSession.sparkContext,
          10000,
          expectedPartitionCount.get,
          originalNumPartitions.get) == result.rdd.getNumPartitions)
    }
  }
}
