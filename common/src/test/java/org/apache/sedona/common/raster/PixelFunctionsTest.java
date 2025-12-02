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
package org.apache.sedona.common.raster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.sedona.common.utils.RasterPolygonizer;
import org.geotools.api.referencing.FactoryException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;

public class PixelFunctionsTest extends RasterTestBase {

  @Test
  public void testGetPolygonizeDataTypes() throws FactoryException {
    // Double, Float, Int, Short, Unsigned Short, Byte
    String[] dataTypes = {"D", "F", "I", "S", "US", "B"};
    for (String datatype : dataTypes) {
      GridCoverage2D testRaster = makeTestRaster(datatype);
      List<RasterPolygonizer.PolygonWithValue> polygons =
          PixelFunctions.getPolygonize(testRaster, 1);
      validatePolygonize(polygons, datatype);
    }
  }

  @Test
  public void testGetPolygonizeOnEmpty() throws FactoryException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 1000, 1000, 0, 0, 1);
    List<RasterPolygonizer.PolygonWithValue> polys = PixelFunctions.getPolygonize(emptyRaster, 1);
    Assert.assertEquals(1, polys.size());
  }

  @Test
  public void testGetPolygonizeWithRepeats() throws FactoryException {
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "I", 3, 3, 0, 0, 1);
    double[] values =
        new double[] {
          1, 2, 1,
          1, 2, 1,
          1, 2, 1
        };
    raster = PixelFunctionEditors.setValues(raster, 1, 1, 1, 3, 3, values);
    List<RasterPolygonizer.PolygonWithValue> polys = PixelFunctions.getPolygonize(raster, 1);
    Assert.assertEquals(3, polys.size());
    RasterPolygonizer.PolygonWithValue poly1 = polys.get(0);
    RasterPolygonizer.PolygonWithValue poly2 = polys.get(1);
    RasterPolygonizer.PolygonWithValue poly3 = polys.get(2);
    Assert.assertEquals(1, poly1.value, 0.0);
    Assert.assertEquals(2, poly2.value, 0.0);
    Assert.assertEquals(1, poly3.value, 0.0);
  }

  @Test
  public void testGetPolygonizeWithMultipleBands() throws FactoryException {
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(2, "I", 1, 1, 0, 0, 1);
    // Band 1 has val 1 and band 2 has val 2
    raster = PixelFunctionEditors.setValues(raster, 1, 1, 1, 1, 1, new double[] {1.0});
    raster = PixelFunctionEditors.setValues(raster, 2, 1, 1, 1, 1, new double[] {2.0});
    List<RasterPolygonizer.PolygonWithValue> polys = PixelFunctions.getPolygonize(raster, 2);
    Assert.assertEquals(1, polys.size());
    RasterPolygonizer.PolygonWithValue poly1 = polys.get(0);
    Assert.assertEquals(2, poly1.value, 0.0); // ensure band 2 value is returned
  }

  @Test
  public void testGetPolygonizeWithNoDataValue() throws FactoryException {
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "F", 3, 2, 0, 0, 1);
    double nodatavalue = 5.2;

    double[] values =
        new double[] {
          nodatavalue, 2.7, nodatavalue,
          nodatavalue, 2.7, nodatavalue
        };
    raster = PixelFunctionEditors.setValues(raster, 1, 1, 1, 3, 2, values);
    raster = RasterBandEditors.setBandNoDataValue(raster, 1, nodatavalue);
    List<RasterPolygonizer.PolygonWithValue> polys = PixelFunctions.getPolygonize(raster, 1);
    Assert.assertEquals(1, polys.size());
    RasterPolygonizer.PolygonWithValue poly = polys.get(0);
    Assert.assertEquals(2.7, poly.value, 0.000001);

    // Check the polygon shape - should be a rectangle covering the middle column
    // Grid is 3x2 with pixel size 1, upper-left at (0,0)
    // Column 2 (x=1) has value 2.7 for both rows (y=0 and y=1)
    // Expected coordinates: (1,0), (1,-2), (2,-2), (2,0), (1,0)
    Polygon polygon = poly.polygon;
    Coordinate[] coords = polygon.getExteriorRing().getCoordinates();
    Assert.assertEquals("Polygon should have 5 coordinates", 5, coords.length);
    assertCoordinate(coords[0], 1, 0);
    assertCoordinate(coords[1], 1, -2);
    assertCoordinate(coords[2], 2, -2);
    assertCoordinate(coords[3], 2, 0);
    assertCoordinate(coords[4], 1, 0);
  }

  @Test
  public void testGetPolygonizeWithULPsChecks() throws FactoryException {
    // Test ULP-based floating point comparison with MAX_ULPS=2
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "D", 3, 2, 0, 0, 1);

    double baseValue = 2.7;
    double closeValue = Math.nextAfter(baseValue, Double.POSITIVE_INFINITY);
    double farValue =
        Math.nextAfter(
            Math.nextAfter(
                Math.nextAfter(closeValue, Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY),
            Double.POSITIVE_INFINITY);

    // Checkered shape between close value and base value should all match as one polygon
    // Far Value will have its own value.
    double[] values =
        new double[] {
          baseValue, closeValue, baseValue,
          closeValue, baseValue, farValue
        };

    raster = PixelFunctionEditors.setValues(raster, 1, 1, 1, 3, 2, values);
    raster = RasterBandEditors.setBandNoDataValue(raster, 1, -9999.0);
    List<RasterPolygonizer.PolygonWithValue> polys = PixelFunctions.getPolygonize(raster, 1);

    Assert.assertEquals("Should have 2 polygons", 2, polys.size());
    // Only checking the far polygon (since we have enough tests to validate the other shape)
    RasterPolygonizer.PolygonWithValue polyWithValue = polys.get(1);
    Assert.assertEquals("Found far value", farValue, polyWithValue.value, 0.0);
    Polygon farPolygonGeom = polyWithValue.polygon;
    Coordinate[] coords = farPolygonGeom.getExteriorRing().getCoordinates();
    Assert.assertEquals("farValue polygon should have 5 coordinates", 5, coords.length);
    assertCoordinate(coords[0], 2, -1);
    assertCoordinate(coords[1], 2, -2);
    assertCoordinate(coords[2], 3, -2);
    assertCoordinate(coords[3], 3, -1);
    assertCoordinate(coords[4], 2, -1);
  }

  /*
   * Creates a test raster with a band of the specified data type.
   * For testability, the raster's upper left corner will be at (0,0) and
   * have a pixel size of 0.5,0.5.
   */
  private GridCoverage2D makeTestRaster(String datatype) throws FactoryException {
    GridCoverage2D testRaster = RasterConstructors.makeEmptyRaster(1, datatype, 5, 4, 0, 0, 0.5);

    // Define the pixel values row by row
    double[] values =
        new double[] {
          1, 3, 3, 3, 6,
          1, 4, 4, 4, 6,
          2, 4, 4, 5, 6,
          2, 5, 5, 5, 6
        };

    // when testing doubles use decimal values
    double[] valueDoubles =
        new double[] {
          1.1, 3.3, 3.3, 3.3, 6.6,
          1.1, 4.4, 4.4, 4.4, 6.6,
          2.2, 4.4, 4.4, 5.5, 6.6,
          2.2, 5.5, 5.5, 5.5, 6.6
        };

    // Parameters: raster, band (1), colX (1), rowY (1), width (5), height (4), values array
    if (useIntegral(datatype)) {
      testRaster = PixelFunctionEditors.setValues(testRaster, 1, 1, 1, 5, 4, values);
    } else {
      testRaster = PixelFunctionEditors.setValues(testRaster, 1, 1, 1, 5, 4, valueDoubles);
    }

    return testRaster;
  }

  private boolean useIntegral(String datatype) {
    // Note, using integral for floats to avoid floating point errors in the returned value keys
    return !datatype.equals("D");
  }

  private void validatePolygonize(
      List<RasterPolygonizer.PolygonWithValue> polygonWithValues, String datatype) {
    // Expected number of polygons is 6
    int expectedNumPolygons = 6;
    Assert.assertEquals(
        "Number of polygons for datatype " + datatype,
        expectedNumPolygons,
        polygonWithValues.size());

    // Create a map of value -> polygon for easier validation
    Map<Number, RasterPolygonizer.PolygonWithValue> polyMap = new HashMap<>();
    for (RasterPolygonizer.PolygonWithValue pwv : polygonWithValues) {
      polyMap.put(pwv.value, pwv);
    }

    // Poly 1: value = 1, coordinates: (0,0), (0, -1), (0.5, -1), (0.5, 0), (0,0)
    Number key1 = useIntegral(datatype) ? 1 : 1.1;
    Assert.assertTrue("Polygon 1 not found", polyMap.containsKey(key1));
    Polygon poly1 = polyMap.get(key1).polygon;
    Coordinate[] coords1 = poly1.getExteriorRing().getCoordinates();
    Assert.assertEquals("Poly 1 should have 5 coordinates", 5, coords1.length);
    assertCoordinate(coords1[0], 0, 0);
    assertCoordinate(coords1[1], 0, -1);
    assertCoordinate(coords1[2], 0.5, -1);
    assertCoordinate(coords1[3], 0.5, 0);
    assertCoordinate(coords1[4], 0, 0);

    // Poly 2: value = 2, coordinates: (0, -1), (0, -2), (0.5, -2), (0.5, -1), (0, -1)
    Number key2 = useIntegral(datatype) ? 2 : 2.2;
    Assert.assertTrue("Polygon 2 not found", polyMap.containsKey(key2));
    Polygon poly2 = polyMap.get(key2).polygon;
    Coordinate[] coords2 = poly2.getExteriorRing().getCoordinates();
    Assert.assertEquals("Poly 2 should have 5 coordinates", 5, coords2.length);
    assertCoordinate(coords2[0], 0, -1);
    assertCoordinate(coords2[1], 0, -2);
    assertCoordinate(coords2[2], 0.5, -2);
    assertCoordinate(coords2[3], 0.5, -1);
    assertCoordinate(coords2[4], 0, -1);

    // Poly 3: value = 3, coordinates: (0.5, 0), (0.5, -0.5), (2, -0.5), (2, 0), (0.5, 0)
    Number key3 = useIntegral(datatype) ? 3 : 3.3;
    Assert.assertTrue("Polygon 3 not found", polyMap.containsKey(key3));
    Polygon poly3 = polyMap.get(key3).polygon;
    Coordinate[] coords3 = poly3.getExteriorRing().getCoordinates();
    Assert.assertEquals("Poly 3 should have 5 coordinates", 5, coords3.length);
    assertCoordinate(coords3[0], 0.5, 0);
    assertCoordinate(coords3[1], 0.5, -0.5);
    assertCoordinate(coords3[2], 2, -0.5);
    assertCoordinate(coords3[3], 2, 0);
    assertCoordinate(coords3[4], 0.5, 0);

    // Poly 4: value = 4, coordinates: (0.5, -0.5), (0.5, -1.5), (1.5, -1.5), (1.5, -1), (2, -1), ,
    // (2, -0.5), (0.5, -0.5)
    Number key4 = useIntegral(datatype) ? 4 : 4.4;
    Assert.assertTrue("Polygon 4 not found", polyMap.containsKey(key4));
    Polygon poly4 = polyMap.get(key4).polygon;
    Coordinate[] coords4 = poly4.getExteriorRing().getCoordinates();
    Assert.assertEquals("Poly 4 should have 7 coordinates", 7, coords4.length);
    assertCoordinate(coords4[0], 0.5, -0.5);
    assertCoordinate(coords4[1], 0.5, -1.5);
    assertCoordinate(coords4[2], 1.5, -1.5);
    assertCoordinate(coords4[3], 1.5, -1);
    assertCoordinate(coords4[4], 2, -1);
    assertCoordinate(coords4[5], 2, -0.5);
    assertCoordinate(coords4[6], 0.5, -0.5);

    // Poly 5: value = 5, coordinates: (1.5, -1), (1.5, -1.5), (0.5, -1.5), (0.5, -2), (2, -2), (2,
    // -1), (1.5, -1)
    Number key5 = useIntegral(datatype) ? 5 : 5.5;
    Assert.assertTrue("Polygon 5 not found", polyMap.containsKey(key5));
    Polygon poly5 = polyMap.get(key5).polygon;
    Coordinate[] coords5 = poly5.getExteriorRing().getCoordinates();
    Assert.assertEquals("Poly 5 should have 7 coordinates", 7, coords5.length);
    assertCoordinate(coords5[0], 1.5, -1);
    assertCoordinate(coords5[1], 1.5, -1.5);
    assertCoordinate(coords5[2], 0.5, -1.5);
    assertCoordinate(coords5[3], 0.5, -2);
    assertCoordinate(coords5[4], 2, -2);
    assertCoordinate(coords5[5], 2, -1);
    assertCoordinate(coords5[6], 1.5, -1);

    // Poly 6: value = 6, coordinates: (2, 0), (2, -2), (2.5, -2), (2.5, 0), (2, 0)
    Number key6 = useIntegral(datatype) ? 6 : 6.6;
    Assert.assertTrue("Polygon 6 not found", polyMap.containsKey(key6));
    Polygon poly6 = polyMap.get(key6).polygon;
    Coordinate[] coords6 = poly6.getExteriorRing().getCoordinates();
    Assert.assertEquals("Poly 6 should have 5 coordinates", 5, coords6.length);
    assertCoordinate(coords6[0], 2, 0);
    assertCoordinate(coords6[1], 2, -2);
    assertCoordinate(coords6[2], 2.5, -2);
    assertCoordinate(coords6[3], 2.5, 0);
    assertCoordinate(coords6[4], 2, 0);
  }

  private void assertCoordinate(Coordinate actual, double expectedX, double expectedY) {
    double tolerance = 0.0001;
    Assert.assertEquals("X coordinate mismatch", expectedX, actual.x, tolerance);
    Assert.assertEquals("Y coordinate mismatch", expectedY, actual.y, tolerance);
  }
}
