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
package org.apache.sedona.common.raster.outdb;

import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sedona.common.raster.RasterTestBase;
import org.apache.sedona.common.raster.inputstream.HadoopImageInputStreamFactory;
import org.apache.sedona.common.raster.outdb.OutDbResourcePool.ResourceKey;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

@RunWith(Parameterized.class)
public class OutDbGridCoverage2DTest extends RasterTestBase {

  // Parameterized test using various geotiff files
  @Parameterized.Parameters(name = "file: {0}, no local cache: {1}")
  public static Object[][] testFiles() {
    return new Object[][] {
      {resourceFolder + "/raster/test1.tiff", false},
      {resourceFolder + "/raster/test2.tiff", false},
      {resourceFolder + "/raster/test3.tif", false},
      {resourceFolder + "/raster/raster_with_no_data/test5.tiff", false},
      {resourceFolder + "/raster_geotiff_color/FAA_UTM18N_NAD83.tif", false},
      {resourceFolder + "/raster/test1.tiff", true},
      {resourceFolder + "/raster/test2.tiff", true},
      {resourceFolder + "/raster/test3.tif", true},
      {resourceFolder + "/raster/raster_with_no_data/test5.tiff", true},
      {resourceFolder + "/raster_geotiff_color/FAA_UTM18N_NAD83.tif", true},
    };
  }

  private final String testFilePath;
  private final Configuration conf;

  public OutDbGridCoverage2DTest(String filePath, boolean disableCacheForLocalFile) {
    this.testFilePath = filePath;
    this.conf = new Configuration();
    conf.setBoolean(
        HadoopImageInputStreamFactory.DONT_CACHE_LOCAL_FILE_CONF_KEY, disableCacheForLocalFile);
  }

  @Test
  public void testGeoTiff() throws IOException {
    // Run the test multiple times to see if the result is consistent, since the OutDbGridCoverage2D
    // involves resource reuse and pooling.
    for (int k = 0; k < 3; k++) {
      testUsingGeoTiffFile(testFilePath);
      testTileUsingGeoTiffFile(testFilePath);
    }
  }

  @Test
  public void testHugeGeoTiff() throws IOException {
    String path = resourceFolder + "/raster_huge/huge.tif";
    GeoTiffReader reader = new GeoTiffReader(new File(path));
    GridCoverage2D gridCoverage2D = reader.read(null);
    GridCoverage2D outDbGridCoverage2D = OutDbGridCoverage2D.create("test", new Path(path), conf);
    assertSameCoverage(gridCoverage2D, outDbGridCoverage2D, 1);
  }

  private void testUsingGeoTiffFile(String path) throws IOException {
    // Construct a GridCoverage2D object from the GeoTiff file
    GeoTiffReader reader = new GeoTiffReader(new File(path));
    GridCoverage2D gridCoverage2D = reader.read(null);

    // Construct an OutDbGridCoverage2D from the same GeoTiff file
    GridCoverage2D outDbGridCoverage2D = OutDbGridCoverage2D.create("test", new Path(path), conf);

    // Verify that they are the same coverage
    assertSameCoverage(gridCoverage2D, outDbGridCoverage2D);
  }

  private void testTileUsingGeoTiffFile(String path) throws IOException {
    // Construct a GridCoverage2D object from the GeoTiff file
    GeoTiffReader reader = new GeoTiffReader(new File(path));
    GridCoverage2D gridCoverage2D = reader.read(null);
    GridGeometry2D gridGeometry = gridCoverage2D.getGridGeometry();
    AffineTransform2D affine = (AffineTransform2D) gridGeometry.getGridToCRS2D();
    CoordinateReferenceSystem crs = gridGeometry.getCoordinateReferenceSystem();
    GridSampleDimension[] bands = gridCoverage2D.getSampleDimensions();
    int width = gridGeometry.getGridRange2D().width;
    int height = gridGeometry.getGridRange2D().height;

    // Construct geo-referencing information of the out-db raster, which references a small portion
    // of the original GeoTiff image.
    int outDbWidth = width / 3;
    int outDbHeight = height / 3;
    GridEnvelope gridEnvelope = new GridEnvelope2D(0, 0, outDbWidth, outDbHeight);
    double scaleX = affine.getScaleX();
    double scaleY = affine.getScaleY();
    double ipX = affine.getTranslateX() + affine.getScaleX() * outDbWidth * 0.5;
    double ipY = affine.getTranslateY() + affine.getScaleY() * outDbHeight * 0.5;
    AffineTransform2D outDbTransform =
        new AffineTransform2D(scaleX, affine.getShearY(), affine.getShearX(), scaleY, ipX, ipY);
    GridGeometry2D outDbGridGeometry = new GridGeometry2D(gridEnvelope, outDbTransform, crs);

    // Revert the bands for out-db raster
    int[] outDbBandIndices = new int[bands.length];
    for (int k = 0; k < bands.length; k++) {
      outDbBandIndices[k] = bands.length - k - 1;
    }
    GridSampleDimension[] outDbBands = ArrayUtils.clone(bands);
    ArrayUtils.reverse(outDbBands);

    // Construct the out-db raster
    Path outDbPath = new Path(path);
    OutDbGridCoverage2D outDbGridCoverage2D =
        OutDbGridCoverage2D.create(
            "test", outDbGridGeometry, outDbBands, outDbBandIndices, outDbPath, conf);
    try {
      // Evaluate values on some world coordinates to see if the values are correct
      double[] outDbValues = new double[bands.length];
      double[] values = new double[bands.length];
      for (int y = 0; y < outDbHeight; y += 10) {
        for (int x = 0; x < outDbWidth; x += 10) {
          double worldX = ipX + x * scaleX;
          double worldY = ipY + y * scaleY;
          DirectPosition worldPos = new DirectPosition2D(worldX, worldY);
          outDbGridCoverage2D.evaluate(worldPos, outDbValues);
          gridCoverage2D.evaluate(worldPos, values);
          for (int k = 0; k < bands.length; k++) {
            Assert.assertEquals(values[k], outDbValues[bands.length - k - 1], 1e-6);
          }
        }
      }
    } finally {
      outDbGridCoverage2D.dispose(true);
    }
  }

  @Test
  public void testOutDbRasterWithPadding() throws IOException {
    String path = testFilePath;

    OutDbGridCoverage2D fullRaster = OutDbGridCoverage2D.create("full", new Path(path), conf);
    GridGeometry2D gridGeometry = fullRaster.getGridGeometry();
    GridEnvelope2D gridRange = gridGeometry.getGridRange2D();
    int width = gridRange.width;
    int height = gridRange.height;
    int offsetX = (int) (width * 0.7);
    int offsetY = (int) (height * 0.7);

    GridEnvelope partialGridEnvelope = new GridEnvelope2D(0, 0, width, height);

    AffineTransform2D affine = (AffineTransform2D) gridGeometry.getGridToCRS2D();
    double scaleX = affine.getScaleX();
    double scaleY = affine.getScaleY();
    double skewX = affine.getShearX();
    double skewY = affine.getShearY();
    double upperLeftX = affine.getTranslateX() + offsetX * scaleX;
    double upperLeftY = affine.getTranslateY() + offsetY * scaleY;
    AffineTransform2D affinePartial =
        new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);

    GridGeometry2D partialGridGeometry =
        new GridGeometry2D(
            partialGridEnvelope, affinePartial, gridGeometry.getCoordinateReferenceSystem());

    ResourceKey resourceKey = new ResourceKey(new Path(path), conf);
    OutDbGridCoverage2D partialRaster =
        OutDbGridCoverage2D.create(
            "cropped", // name
            partialGridGeometry, // gridGeometry
            -1, // dataType
            fullRaster.getSampleDimensions(), // bands
            null, // bandIndices
            resourceKey); // resourceKey

    Assert.assertEquals(width, partialRaster.getRenderedImage().getWidth());
    Assert.assertEquals(height, partialRaster.getRenderedImage().getHeight());

    double[] paddedValues = new double[fullRaster.getNumSampleDimensions()];
    for (int i = 0; i < paddedValues.length; i++) {
      double noDataValue = RasterUtils.getNoDataValue(fullRaster.getSampleDimension(i));
      if (Double.isNaN(noDataValue)) {
        paddedValues[i] = 0;
      } else {
        paddedValues[i] = noDataValue;
      }
    }

    // Verify pixel values
    Raster data = fullRaster.getRenderedImage().getData();
    Raster paddedData = partialRaster.getRenderedImage().getData();
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        for (int b = 0; b < fullRaster.getNumSampleDimensions(); b++) {
          double expected;
          if (x + offsetX >= width || y + offsetY >= height) {
            expected = paddedValues[b];
          } else {
            expected = data.getSampleDouble(x + offsetX, y + offsetY, b);
          }
          double actual = paddedData.getSampleDouble(x, y, b);
          Assert.assertEquals(expected, actual, 1e-6);
        }
      }
    }

    fullRaster.dispose(true);
    partialRaster.dispose(true);
  }
}
