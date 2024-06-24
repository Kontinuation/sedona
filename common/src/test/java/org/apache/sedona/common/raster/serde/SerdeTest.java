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
package org.apache.sedona.common.raster.serde;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sedona.common.raster.RasterConstructors;
import org.apache.sedona.common.raster.RasterTestBase;
import org.apache.sedona.common.raster.outdb.HadoopConfigSerializer;
import org.apache.sedona.common.raster.outdb.LazyLoadOutDbGridCoverage2D;
import org.apache.sedona.common.raster.outdb.OutDbGridCoverage2D;
import org.apache.sedona.common.raster.outdb.OutDbResourcePool;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class SerdeTest extends RasterTestBase {

  private static final String[] testFilePaths = {
    resourceFolder + "/raster/test1.tiff",
    resourceFolder + "/raster/test2.tiff",
    resourceFolder + "/raster/test3.tif",
    resourceFolder + "/raster_geotiff_color/FAA_UTM18N_NAD83.tif"
  };

  @Test
  public void testRoundTripSerdeSingleBandRaster() throws IOException, ClassNotFoundException {
    testRoundTrip(oneBandRaster);
  }

  @Test
  public void testRoundTripSerdeMultiBandRaster() throws IOException, ClassNotFoundException {
    testRoundTrip(multiBandRaster);
  }

  @Test
  public void testInDbRaster() throws IOException, ClassNotFoundException {
    for (String testFilePath : testFilePaths) {
      GeoTiffReader reader = new GeoTiffReader(new File(testFilePath));
      GridCoverage2D raster = reader.read(null);
      testRoundTrip(raster);
    }
  }

  @Test
  public void testInDbNorthPoleRaster()
      throws IOException, ClassNotFoundException, FactoryException {
    // If we are not using non-strict mode to serializing CRS, this will raise an exception:
    // org.geotools.referencing.wkt.UnformattableObjectException: This "AxisDirection" object is too
    // complex for
    // WKT syntax.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(
            1, "B", 256, 256, -345000.000, 345000.000, 2000, -2000, 0, 0, 3996);
    testRoundTrip(raster);
  }

  @Test
  public void testOutDbRaster() throws IOException, ClassNotFoundException {
    for (String testFilePath : testFilePaths) {
      // Out-DB raster referencing the entire GeoTiff file
      GridCoverage2D raster =
          OutDbGridCoverage2D.create("test", new Path(testFilePath), new Configuration());
      GridCoverage2D roundTripRaster = testRoundTrip(raster);
      Assert.assertTrue(roundTripRaster instanceof OutDbGridCoverage2D);

      // Out-DB raster referencing only a small portion of the entire GeoTiff file
      raster = createOutDbRasterTileFromGeoTiff(testFilePath);
      roundTripRaster = testRoundTrip(raster);
      Assert.assertTrue(roundTripRaster instanceof OutDbGridCoverage2D);
    }
  }

  @Test
  public void testLazyOutDbRaster() throws IOException, ClassNotFoundException {
    Configuration conf = new Configuration();
    byte[] serializedConf = HadoopConfigSerializer.serialize(conf);
    for (String testFilePath : testFilePaths) {
      // Out-DB raster referencing the entire GeoTiff file
      GridCoverage2D raster = new LazyLoadOutDbGridCoverage2D("test", new Path(testFilePath), conf);

      // Single round-trip
      byte[] bytes = Serde.serialize(raster);
      GridCoverage2D roundTripRaster = Serde.deserialize(bytes);
      assertNotNull(roundTripRaster);
      Assert.assertTrue(roundTripRaster instanceof LazyLoadOutDbGridCoverage2D);

      // Multiple round-trip with lazy-loading (reading raster data)
      raster = new LazyLoadOutDbGridCoverage2D("test", new Path(testFilePath), conf);
      roundTripRaster = testRoundTrip(raster);
      Assert.assertTrue(roundTripRaster instanceof OutDbGridCoverage2D);

      // Serialize without configuration
      raster = new LazyLoadOutDbGridCoverage2D("test", new Path(testFilePath), conf);
      bytes = Serde.serialize(raster, false);
      roundTripRaster = Serde.deserialize(bytes, serializedConf);
      assertNotNull(roundTripRaster);
      Assert.assertTrue(roundTripRaster instanceof LazyLoadOutDbGridCoverage2D);
      assertSameCoverage(raster, roundTripRaster);
    }
  }

  @Test
  public void testHugeOutDbRaster() throws IOException, ClassNotFoundException {
    String path = resourceFolder + "/raster_huge/huge.tif";
    GridCoverage2D raster = OutDbGridCoverage2D.create("test", new Path(path), new Configuration());
    GridCoverage2D roundTripRaster = testRoundTrip(raster, 1);
    Assert.assertTrue(roundTripRaster instanceof OutDbGridCoverage2D);
  }

  @Test
  public void testSerdeOutDbWithoutConfiguration() throws IOException, ClassNotFoundException {
    String testFilePath = resourceFolder + "/raster/test1.tiff";
    Configuration conf = new Configuration();
    Map<String, String> params = new HashMap<>();
    params.put("test_key", "test_value");
    OutDbResourcePool.ResourceKey resourceKey =
        new OutDbResourcePool.ResourceKey(new Path(testFilePath), conf, params);
    GridCoverage2D raster = OutDbGridCoverage2D.create("test", resourceKey);
    byte[] withConf = Serde.serialize(raster);
    byte[] withoutConf = Serde.serialize(raster, false);
    // The serialized bytes without configuration should be much smaller than the one with
    // configuration
    Assert.assertTrue(withConf.length > 2 * withoutConf.length);
    // Deserialize with configuration
    byte[] serializedConf = HadoopConfigSerializer.serialize(conf);
    GridCoverage2D roundTripRaster = Serde.deserialize(withoutConf, serializedConf);
    Assert.assertTrue(roundTripRaster instanceof OutDbGridCoverage2D);
    assertSameCoverage(raster, roundTripRaster, 10);
    // Check that the params were restored
    Map<String, String> newParams = ((OutDbGridCoverage2D) roundTripRaster).getOutDbParams();
    Assert.assertEquals(params, newParams);
  }

  private GridCoverage2D testRoundTrip(GridCoverage2D raster)
      throws IOException, ClassNotFoundException {
    return testRoundTrip(raster, 10);
  }

  private GridCoverage2D testRoundTrip(GridCoverage2D raster, int density)
      throws IOException, ClassNotFoundException {
    byte[] bytes = Serde.serialize(raster);
    GridCoverage2D roundTripRaster = Serde.deserialize(bytes);
    assertNotNull(roundTripRaster);
    assertSameCoverage(raster, roundTripRaster, density);
    bytes = Serde.serialize(roundTripRaster);
    roundTripRaster = Serde.deserialize(bytes);
    assertSameCoverage(raster, roundTripRaster, density);
    return roundTripRaster;
  }

  private GridCoverage2D createOutDbRasterTileFromGeoTiff(String path) throws IOException {
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

    // Construct the out-db raster
    Path outDbPath = new Path(path);
    Configuration conf = new Configuration();
    return OutDbGridCoverage2D.create("test", outDbGridGeometry, bands, null, outDbPath, conf);
  }
}
