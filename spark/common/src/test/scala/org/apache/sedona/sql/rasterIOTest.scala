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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.sedona.common.raster.outdb.LazyLoadOutDbGridCoverage2D
import org.apache.sedona.common.raster.outdb.OutDbGridCoverage2D
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions.expr
import org.junit.Assert.assertEquals
import org.scalatest.BeforeAndAfter
import org.scalatest.GivenWhenThen

import java.awt.image.DataBuffer
import java.io.File
import java.nio.file.Files

class rasterIOTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen {

  var rasterdatalocation: String = resourceFolder + "raster/"
  val tempDir: String = Files.createTempDirectory("sedona_raster_io_test_").toFile.getAbsolutePath

  describe("Raster IO test") {
    it(
      "should read geotiff using binary source and write geotiff back to disk using raster source") {
      var rasterDf = sparkSession.read.format("binaryFile").load(rasterdatalocation)
      val rasterCount = rasterDf.count()
      rasterDf.write.format("raster").mode(SaveMode.Overwrite).save(tempDir + "/raster-written")
      rasterDf = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = rasterDf.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterDf.count() == rasterCount)
    }

    it("should read and write geotiff using given options") {
      var rasterDf = sparkSession.read.format("binaryFile").load(rasterdatalocation)
      val rasterCount = rasterDf.count()
      rasterDf.write
        .format("raster")
        .option("rasterField", "content")
        .option("fileExtension", ".tiff")
        .option("pathField", "path")
        .option("useDirectCommitter", "false")
        .mode(SaveMode.Overwrite)
        .save(tempDir + "/raster-written")
      rasterDf = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = rasterDf.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterDf.count() == rasterCount)
    }

    it("should read and write via RS_FromGeoTiff and RS_AsGeoTiff") {
      var df = sparkSession.read.format("binaryFile").load(rasterdatalocation)
      var rasterDf = df
        .selectExpr("RS_FromGeoTiff(content) as raster", "path")
        .selectExpr("RS_AsGeoTiff(raster) as content", "path")
      val rasterCount = rasterDf.count()
      rasterDf.write
        .format("raster")
        .option("rasterField", "content")
        .option("fileExtension", ".tiff")
        .option("pathField", "path")
        .mode(SaveMode.Overwrite)
        .save(tempDir + "/raster-written")
      df = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = df.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterDf.count() == rasterCount)
    }

    it("should handle null") {
      var df = sparkSession.read.format("binaryFile").load(rasterdatalocation)
      var rasterDf = df
        .selectExpr("RS_FromGeoTiff(null) as raster", "length")
        .selectExpr("RS_AsGeoTiff(raster) as content", "length")
      val rasterCount = rasterDf.count()
      rasterDf.write.format("raster").mode(SaveMode.Overwrite).save(tempDir + "/raster-written")
      df = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = df.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterCount == 6)
      assert(rasterDf.count() == 0)
    }

    it("Passed RS_AsRaster with empty raster") {
      val df = sparkSession.sql(
        "SELECT RS_MakeEmptyRaster(2, 255, 255, 3, 215, 2, -2, 0, 0, 4326) as raster, ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))') as geom")
      var rasterized =
        df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 255, 0d) as rasterized")
      var actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      var expected =
        "Array(255.0, 255.0, 255.0, 255.0, 0.0, 0.0, 255.0, 255.0, 0.0, 0.0, 0.0, 255.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 3093151) as rasterized")
      actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      expected =
        "Array(3093151.0, 3093151.0, 3093151.0, 3093151.0, 0.0, 0.0, 3093151.0, 3093151.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd') as rasterized")
      actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      expected =
        "Array(1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)"
      assertEquals(expected, actual)
    }

    it("Passed RS_AsRaster LineString") {
      val df = sparkSession.sql(
        "SELECT RS_MakeEmptyRaster(2, 255, 255, 3, 215, 2, -2, 0, 0, 4326) as raster, ST_GeomFromWKT('LINESTRING(1 1, 2 1, 10 1)') as geom")
      var rasterized =
        df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 255, 0d) as rasterized")
      var actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      var expected = "Array(0.0, 0.0, 0.0, 0.0, 255.0, 255.0, 255.0, 255.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr(
        "RS_AsRaster(ST_GeomFromWKT('LINESTRING(4 1, 4 2, 4 10)'), raster, 'd', false, 255, 0d) as rasterized")
      actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      expected = "Array(255.0, 255.0, 255.0, 255.0, 255.0)"
      assertEquals(expected, actual)
    }

    it("Passed RS_AsRaster with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr(
        "ST_GeomFromText('POINT (-13085817.809482181 3993868.8560156375)', 3857) as geom",
        "RS_FromGeoTiff(content) as raster")
      var rasterized =
        df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 61784, 0d) as rasterized")
      var actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      var expected = "Array(61784.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 255) as rasterized")
      actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      expected = "Array(255.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd') as rasterized")
      actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      expected = "Array(1.0)"
      assertEquals(expected, actual)
    }

    it("Passed RS_AsRaster with raster extent") {
      var df = sparkSession.sql(
        "SELECT RS_MakeEmptyRaster(2, 255, 255, 3, 215, 2, -2, 0, 0, 0) as raster, ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))') as geom")
      var rasterized =
        df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 255, 0d, false) as rasterized")
      var actualSeq =
        rasterized.selectExpr("RS_BandAsArray(rasterized, 1)").first().getSeq[Double](0)
      var actualMax = actualSeq.max
      var actualSum = actualSeq.sum
      var expectedMax = 255.0d
      var expectedSum = 255.0 * 7
      assertEquals(expectedMax, actualMax, 1e-5)
      assertEquals(expectedSum, actualSum, 1e-5)

      var actualWidth = rasterized.selectExpr("RS_Width(rasterized)").first().getInt(0)
      var actualHeight = rasterized.selectExpr("RS_Height(rasterized)").first().getInt(0)
      assertEquals(255, actualWidth)
      assertEquals(255, actualHeight)
    }

    it("should read RS_FromGeoTiff and write RS_AsArcGrid") {
      var df =
        sparkSession.read.format("binaryFile").load(resourceFolder + "raster_geotiff_color/*")
      var rasterDf = df
        .selectExpr("RS_FromGeoTiff(content) as raster", "path")
        .selectExpr("RS_AsArcGrid(raster, 1) as content", "path")
      val rasterCount = rasterDf.count()
      rasterDf.write
        .format("raster")
        .option("rasterField", "content")
        .option("fileExtension", ".asc")
        .option("pathField", "path")
        .mode(SaveMode.Overwrite)
        .save(tempDir + "/raster-written")
      df = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = df.selectExpr("RS_FromArcInfoAsciiGrid(content)")
      assert(rasterDf.count() == rasterCount)
    }

    it(
      "should read geotiff using binary source and write geotiff back to hdfs using raster source") {
      val miniHDFS: (MiniDFSCluster, String) = creatMiniHdfs()
      var rasterDf =
        sparkSession.read.format("binaryFile").load(rasterdatalocation).repartition(3)
      val rasterCount = rasterDf.count()
      rasterDf.write
        .format("raster")
        .mode(SaveMode.Overwrite)
        .save(miniHDFS._2 + "/raster-written")
      rasterDf = sparkSession.read.format("binaryFile").load(miniHDFS._2 + "/raster-written/*")
      rasterDf = rasterDf.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterDf.count() == rasterCount)
      miniHDFS._1.shutdown()
    }
  }

  describe("Raster read test") {
    it("should read geotiff using raster source with explicit tiling") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "true", "tileWidth" -> "64"))
        .load(rasterdatalocation)
      assert(rasterDf.count() > 100)
      rasterDf.collect().foreach { row =>
        val raster = row.getAs[Object](0).asInstanceOf[OutDbGridCoverage2D]
        assert(raster.getGridGeometry.getGridRange2D.width <= 64)
        assert(raster.getGridGeometry.getGridRange2D.height <= 64)
        val x = row.getInt(1)
        val y = row.getInt(2)
        assert(x >= 0 && y >= 0)
        raster.dispose(true)
      }

      // Check the execution plan to see if the repartitioning is actually happening
      val plan = rasterDf.queryExecution.executedPlan match {
        case adaptive: AdaptiveSparkPlanExec => adaptive.initialPlan
        case plan: SparkPlan => plan
      }
      assert(plan.collect { case _: Exchange => true }.size == 1)

      // Check if auto-repartitioning is actually working
      val partitions = rasterDf.rdd.getNumPartitions
      assert(partitions >= sparkSession.sparkContext.defaultParallelism)

      // Test projection push-down
      rasterDf.selectExpr("y", "rast as r").collect().foreach { row =>
        val raster = row.getAs[Object](1).asInstanceOf[OutDbGridCoverage2D]
        assert(raster.getGridGeometry.getGridRange2D.width <= 64)
        assert(raster.getGridGeometry.getGridRange2D.height <= 64)
        val y = row.getInt(0)
        assert(y >= 0)
        raster.dispose(true)
      }
    }

    it("should tile geotiff using raster source with padding enabled") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "true", "tileWidth" -> "64", "padWithNoData" -> "true"))
        .load(rasterdatalocation)
      assert(rasterDf.count() > 100)
      rasterDf.collect().foreach { row =>
        val raster = row.getAs[Object](0).asInstanceOf[OutDbGridCoverage2D]
        assert(raster.getGridGeometry.getGridRange2D.width == 64)
        assert(raster.getGridGeometry.getGridRange2D.height == 64)
        val x = row.getInt(1)
        val y = row.getInt(2)
        assert(x >= 0 && y >= 0)
        raster.dispose(true)
      }
    }

    it("auto repartitioning should work with dynamic allocation enabled") {
      withConf(
        Map(
          "spark.wherobots.testing.dynamicAllocation" -> "true",
          "spark.sedona.raster.load.perPartitionSize" -> "100kb")) {
        val rasterDf = sparkSession.read
          .format("raster")
          .options(Map("retile" -> "true", "tileWidth" -> "64"))
          .load(rasterdatalocation)

        val plan = rasterDf.queryExecution.executedPlan match {
          case adaptive: AdaptiveSparkPlanExec => adaptive.initialPlan
          case plan: SparkPlan => plan
        }
        assert(plan.collect { case _: Exchange => true }.size == 1)

        val partitions = rasterDf.rdd.getNumPartitions
        assert(partitions >= 4)
        assert(rasterDf.count() > 100)
      }
    }

    it("should not auto-repartition when limit or show is used") {
      var rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "false"))
        .load(rasterdatalocation)
        .limit(3)
      // Check the execution plan to see if the repartitioning is actually happening
      val plan = rasterDf.queryExecution.executedPlan match {
        case adaptive: AdaptiveSparkPlanExec => adaptive.initialPlan
        case plan: SparkPlan => plan
      }
      assert(plan.collect { case _: Exchange => true }.isEmpty)
    }

    it("should read geotiff using raster source without tiling") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "false"))
        .load(rasterdatalocation)
      assert(rasterDf.schema.fields.length == 1)
      rasterDf.collect().foreach { row =>
        val raster = row.getAs[Object](0).asInstanceOf[OutDbGridCoverage2D]
        // Should not load metadata eagerly
        assert(raster.isInstanceOf[LazyLoadOutDbGridCoverage2D])
        raster.dispose(true)
      }
    }

    it("should read geotiff using raster source with eager metadata loading") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "false", "loadMetadata" -> "true"))
        .load(rasterdatalocation)
      assert(rasterDf.schema.fields.length == 1)
      rasterDf.collect().foreach { row =>
        val raster = row.getAs[Object](0).asInstanceOf[OutDbGridCoverage2D]
        assert(!raster.isInstanceOf[LazyLoadOutDbGridCoverage2D])
        raster.dispose(true)
      }
    }

    it("should use eager metadata loading when RS function is called") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "false"))
        .load(rasterdatalocation)
        .withColumn("width", expr("RS_Width(rast)"))
        .withColumn("height", expr("RS_Height(rast)"))
      assert(rasterDf.schema.fields.length == 3)
      rasterDf.collect().foreach { row =>
        val raster = row.getAs[Object](0).asInstanceOf[OutDbGridCoverage2D]
        // RS_Width and RS_Height were called, should load metadata early in the raster data source
        assert(!raster.isInstanceOf[LazyLoadOutDbGridCoverage2D])
        raster.dispose(true)
      }
    }

    it("should not use eager metadata loading when user explicitly set loadMetadata to false") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "false", "loadMetadata" -> "false"))
        .load(rasterdatalocation)
        .withColumn("width", expr("RS_Width(rast)"))
        .withColumn("height", expr("RS_Height(rast)"))
      assert(rasterDf.schema.fields.length == 3)
      rasterDf.collect().foreach { row =>
        val raster = row.getAs[Object](0).asInstanceOf[OutDbGridCoverage2D]
        // RS_Width and RS_Height were called, but user explicitly set loadMetadata to false.
        // We should not load metadata early in the raster data source
        assert(raster.isInstanceOf[LazyLoadOutDbGridCoverage2D])
        raster.dispose(true)
      }
    }

    it("should read geotiff using raster source with non-parallel eager metadata loading") {
      withConf(Map("spark.sedona.raster.load.parallelism" -> "0")) {
        val rasterDf = sparkSession.read
          .format("raster")
          .options(Map("retile" -> "false", "loadMetadata" -> "true"))
          .load(rasterdatalocation)
        assert(rasterDf.schema.fields.length == 1)
        rasterDf.collect().foreach { row =>
          val raster = row.getAs[Object](0).asInstanceOf[OutDbGridCoverage2D]
          assert(!raster.isInstanceOf[LazyLoadOutDbGridCoverage2D])
          raster.dispose(true)
        }
      }
    }

    it("should read geotiff using raster source with auto-tiling") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "true"))
        .load(rasterdatalocation)
      val rasterDfNoTiling = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "false"))
        .load(rasterdatalocation)
      assert(rasterDf.count() > rasterDfNoTiling.count())
    }

    it("should throw exception when only tileHeight is specified") {
      assertThrows[IllegalArgumentException] {
        val df = sparkSession.read
          .format("raster")
          .options(Map("retile" -> "true", "tileHeight" -> "64"))
          .load(rasterdatalocation)
        df.collect()
      }
    }

    it("should throw exception when the geotiff is badly tiled") {
      val exception = intercept[Exception] {
        val rasterDf = sparkSession.read
          .format("raster")
          .options(Map("retile" -> "true"))
          .load(resourceFolder + "raster_geotiff_color/*")
        rasterDf.collect()
      }
      assert(exception.getMessage.contains("Please set tileWidth and tileHeight explicitly"))
    }

    it("should support geotiff rescaling") {
      Seq("false", "true").foreach { loadMetadata =>
        var dfRasters = sparkSession.read
          .format("raster")
          .options(
            Map("retile" -> "false", "loadMetadata" -> loadMetadata, "autoRescale" -> "false"))
          .load(resourceFolder + "raster_geotiff_rescale/test.tif")
        dfRasters.collect().foreach { row =>
          val raster = row.getAs[OutDbGridCoverage2D]("rast")
          assert(raster.getRenderedImage.getSampleModel.getDataType == DataBuffer.TYPE_USHORT)
          raster.dispose(true)
        }

        dfRasters = sparkSession.read
          .format("raster")
          .options(
            Map("retile" -> "false", "loadMetadata" -> loadMetadata, "autoRescale" -> "true"))
          .load(resourceFolder + "raster_geotiff_rescale/test.tif")
        dfRasters.collect().foreach { row =>
          val raster = row.getAs[OutDbGridCoverage2D]("rast")
          assert(raster.getRenderedImage.getSampleModel.getDataType == DataBuffer.TYPE_DOUBLE)
          raster.dispose(true)
        }
      }
    }

    it("should support AsciiGrid") {
      val rasterDf = sparkSession.read
        .format("raster")
        .load(resourceFolder + "raster_asc/")
      assert(rasterDf.count() == 1)
      rasterDf.collect().foreach { row =>
        val raster = row.getAs[Object](0).asInstanceOf[OutDbGridCoverage2D]
        assert(raster.getGridGeometry.getGridRange2D.width == 2)
        assert(raster.getGridGeometry.getGridRange2D.height == 2)
        raster.dispose(true)
        val x = row.getInt(1)
        val y = row.getInt(2)
        assert(x == 0 && y == 0)
      }
    }

    it("read partitioned directory") {
      FileUtils.cleanDirectory(new File(tempDir))
      Files.createDirectory(new File(tempDir + "/part=1").toPath)
      Files.createDirectory(new File(tempDir + "/part=2").toPath)
      FileUtils.copyFile(
        new File(resourceFolder + "raster/test1.tiff"),
        new File(tempDir + "/part=1/test1.tiff"))
      FileUtils.copyFile(
        new File(resourceFolder + "raster/test2.tiff"),
        new File(tempDir + "/part=1/test2.tiff"))
      FileUtils.copyFile(
        new File(resourceFolder + "raster/test4.tiff"),
        new File(tempDir + "/part=2/test4.tiff"))
      FileUtils.copyFile(
        new File(resourceFolder + "raster/test4.tiff"),
        new File(tempDir + "/part=2/test5.tiff"))

      val shapefileDf = sparkSession.read
        .format("raster")
        .load(tempDir)
      val rows = shapefileDf.collect()
      assert(rows.length >= 4)
      rows.foreach { row =>
        val raster = row.getAs[OutDbGridCoverage2D]("rast")
        val path = raster.getOutDbPath.toString
        if (path.endsWith("test1.tiff") || path.endsWith("test2.tiff")) {
          assert(row.getAs[Int]("part") == 1)
        } else {
          assert(row.getAs[Int]("part") == 2)
        }
      }
    }
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(tempDir))
    super.afterAll()
  }
}
