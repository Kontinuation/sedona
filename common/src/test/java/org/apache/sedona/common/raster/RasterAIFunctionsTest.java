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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class RasterAIFunctionsTest extends RasterTestBase {
  private static final WKTReader wktReader = new WKTReader();

  @Test
  public void testEmpty() throws FactoryException, TransformException, ParseException {
    GridCoverage2D ref = RasterConstructors.makeEmptyRaster(1, "B", 5, 5, 0, 5, 1);
    double[] confidenceArray = {
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1
    };
    int[] labelArray = {10};
    List<RasterAIFunctions.ExtractedRegionInfo> results =
        RasterAIFunctions.segmentToGeoms(ref, confidenceArray, labelArray, 0.8);
    assertEquals(0, results.size());
  }

  @Test
  public void testSimple() throws FactoryException, TransformException, ParseException {
    GridCoverage2D ref = RasterConstructors.makeEmptyRaster(1, "B", 5, 5, 0, 5, 1);
    double[] confidenceArray = {
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.9, 0.9, 0.1,
      0.1, 0.9, 0.9, 0.9, 0.1,
      0.1, 0.9, 0.9, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1
    };
    int[] labelArray = {10};
    List<RasterAIFunctions.ExtractedRegionInfo> results =
        RasterAIFunctions.segmentToGeoms(ref, confidenceArray, labelArray, 0.8);
    assertEquals(1, results.size());
    RasterAIFunctions.ExtractedRegionInfo result = results.get(0);
    assertEquals(
        wktReader.read("POLYGON ((2 4, 2 3, 1 3, 1 1, 3 1, 3 2, 4 2, 4 4, 2 4))"), result.geometry);
    assertEquals(10, result.label);
    assertEquals(0.9, result.averageConfidenceScore, 1e-6);
  }

  @Test
  public void testTwoDisjoint() throws FactoryException, TransformException, ParseException {
    GridCoverage2D ref = RasterConstructors.makeEmptyRaster(1, "B", 5, 5, 0, 5, 1);
    double[] confidenceArray = {
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.9, 0.9, 0.1,
      0.1, 0.9, 0.1, 0.9, 0.1,
      0.1, 0.9, 0.9, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1
    };
    int[] labelArray = {10};
    List<RasterAIFunctions.ExtractedRegionInfo> results =
        RasterAIFunctions.segmentToGeoms(ref, confidenceArray, labelArray, 0.8);
    assertEquals(1, results.size());
    RasterAIFunctions.ExtractedRegionInfo result = results.get(0);
    assertEquals(
        wktReader.read(
            "MULTIPOLYGON (((2 4, 2 3, 3 3, 3 2, 4 2, 4 4, 2 4)), ((1 3, 1 1, 3 1, 3 2, 2 2, 2 3, 1 3)))"),
        result.geometry);
    assertEquals(10, result.label);
    assertEquals(0.9, result.averageConfidenceScore, 1e-6);
  }

  @Test
  public void testTwoDisjoint2() throws FactoryException, TransformException, ParseException {
    GridCoverage2D ref = RasterConstructors.makeEmptyRaster(1, "B", 5, 5, 0, 5, 1);
    double[] confidenceArray = {
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.9, 0.1,
      0.1, 0.9, 0.1, 0.9, 0.1,
      0.1, 0.9, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1
    };
    int[] labelArray = {10};
    List<RasterAIFunctions.ExtractedRegionInfo> results =
        RasterAIFunctions.segmentToGeoms(ref, confidenceArray, labelArray, 0.8);
    assertEquals(1, results.size());
    RasterAIFunctions.ExtractedRegionInfo result = results.get(0);
    assertEquals(
        result.geometry,
        wktReader.read("MULTIPOLYGON (((3 4, 3 2, 4 2, 4 4, 3 4)), ((1 3, 1 1, 2 1, 2 3, 1 3)))"));
    assertEquals(10, result.label);
    assertEquals(0.9, result.averageConfidenceScore, 1e-6);
  }

  @Test
  public void testHole() throws FactoryException, TransformException, ParseException {
    GridCoverage2D ref = RasterConstructors.makeEmptyRaster(1, "B", 5, 5, 0, 5, 1);
    double[] confidenceArray = {
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.9, 0.9, 0.9, 0.1,
      0.1, 0.9, 0.1, 0.9, 0.1,
      0.1, 0.9, 0.9, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1
    };
    int[] labelArray = {10};
    List<RasterAIFunctions.ExtractedRegionInfo> results =
        RasterAIFunctions.segmentToGeoms(ref, confidenceArray, labelArray, 0.8);
    assertEquals(1, results.size());
    RasterAIFunctions.ExtractedRegionInfo result = results.get(0);
    assertEquals(
        wktReader.read("POLYGON ((1 4, 1 1, 3 1, 3 2, 4 2, 4 4, 1 4), (2 3, 3 3, 3 2, 2 2, 2 3))"),
        result.geometry);
    assertEquals(10, result.label);
    assertEquals(0.9, result.averageConfidenceScore, 1e-6);
  }

  @Test
  public void testHole2() throws FactoryException, TransformException, ParseException {
    GridCoverage2D ref = RasterConstructors.makeEmptyRaster(1, "B", 5, 5, 0, 5, 1);
    double[] confidenceArray = {
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.9, 0.9, 0.9, 0.1,
      0.1, 0.9, 0.1, 0.9, 0.1,
      0.1, 0.9, 0.9, 0.9, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1
    };
    int[] labelArray = {10};
    List<RasterAIFunctions.ExtractedRegionInfo> results =
        RasterAIFunctions.segmentToGeoms(ref, confidenceArray, labelArray, 0.8);
    assertEquals(1, results.size());
    RasterAIFunctions.ExtractedRegionInfo result = results.get(0);
    assertEquals(
        wktReader.read("POLYGON ((1 4, 1 1, 4 1, 4 4, 1 4), (2 3, 3 3, 3 2, 2 2, 2 3))"),
        result.geometry);
    assertEquals(10, result.label);
    assertEquals(0.9, result.averageConfidenceScore, 1e-6);
  }

  @Test
  public void testMultipleBands() throws FactoryException, TransformException, ParseException {
    GridCoverage2D ref = RasterConstructors.makeEmptyRaster(1, "B", 5, 5, 0, 5, 1);
    double[] confidenceArray = {
      // confidence array for class 10
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.9, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.9,
      0.1, 0.1, 0.1, 0.9, 0.9,

      // confidence array for class 7
      0.8, 0.1, 0.1, 0.1, 0.1,
      0.8, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1,
    };
    int[] labelArray = {10, 7};
    List<RasterAIFunctions.ExtractedRegionInfo> results =
        RasterAIFunctions.segmentToGeoms(ref, confidenceArray, labelArray, 0.7);
    assertEquals(2, results.size());
    RasterAIFunctions.ExtractedRegionInfo result1 = results.get(0);
    assertEquals(
        wktReader.read(
            "MULTIPOLYGON (((1 4, 1 3, 2 3, 2 4, 1 4)), ((4 2, 4 1, 3 1, 3 0, 5 0, 5 2, 4 2)))"),
        result1.geometry);
    assertEquals(10, result1.label);
    assertEquals(0.9, result1.averageConfidenceScore, 1e-6);
    RasterAIFunctions.ExtractedRegionInfo result2 = results.get(1);
    assertEquals(wktReader.read("POLYGON ((0 5, 0 3, 1 3, 1 5, 0 5))"), result2.geometry);
    assertEquals(7, result2.label);
    assertEquals(0.8, result2.averageConfidenceScore, 1e-6);
  }

  @Test
  public void testMultipleBandsWithOverlappingArea()
      throws FactoryException, TransformException, ParseException {
    GridCoverage2D ref = RasterConstructors.makeEmptyRaster(1, "B", 5, 5, 0, 5, 1);
    double[] confidenceArray = {
      // confidence array for class 10
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.9, 0.9, 0.1, 0.1,
      0.1, 0.9, 0.9, 0.8, 0.1,
      0.1, 0.1, 0.8, 0.8, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1,

      // confidence array for class 7
      0.1, 0.1, 0.1, 0.1, 0.1,
      0.1, 0.8, 0.8, 0.1, 0.1,
      0.1, 0.8, 0.8, 0.9, 0.1,
      0.1, 0.1, 0.9, 0.9, 0.1,
      0.1, 0.1, 0.1, 0.1, 0.1,
    };
    int[] labelArray = {10, 7};
    List<RasterAIFunctions.ExtractedRegionInfo> results =
        RasterAIFunctions.segmentToGeoms(ref, confidenceArray, labelArray, 0.7);
    assertEquals(2, results.size());
    RasterAIFunctions.ExtractedRegionInfo result1 = results.get(0);
    assertEquals(wktReader.read("POLYGON ((1 4, 1 2, 3 2, 3 4, 1 4))"), result1.geometry);
    assertEquals(10, result1.label);
    assertEquals(0.9, result1.averageConfidenceScore, 1e-6);
    RasterAIFunctions.ExtractedRegionInfo result2 = results.get(1);
    assertEquals(wktReader.read("POLYGON ((3 3, 3 2, 2 2, 2 1, 4 1, 4 3, 3 3))"), result2.geometry);
    assertEquals(7, result2.label);
    assertEquals(0.9, result2.averageConfidenceScore, 1e-6);
  }

  @Test
  public void testGDALPolygonizeCase2()
      throws FactoryException, TransformException, ParseException, IOException {
    // "B" shaped raster
    byte[] bytes = Files.readAllBytes(Paths.get(resourceFolder + "polygonize_in_2.grd"));
    GridCoverage2D ref = RasterConstructors.fromArcInfoAsciiGrid(bytes);

    int width = ref.getRenderedImage().getWidth();
    int height = ref.getRenderedImage().getHeight();
    double[] confidenceArray = new double[width * height];
    RasterUtils.getRaster(ref.getRenderedImage())
        .getDataElements(0, 0, width, height, confidenceArray);

    for (int i = 0; i < confidenceArray.length; i++) {
      confidenceArray[i] = 1 - (confidenceArray[i] / 255.0);
    }
    int[] labelArray = {3};

    List<RasterAIFunctions.ExtractedRegionInfo> results =
        RasterAIFunctions.segmentToGeoms(ref, confidenceArray, labelArray, 1e-8);
    assertEquals(1, results.size());
    assertEquals(
        wktReader.read(
            "POLYGON ((6 -3, 6 -40, 26 -40, 26 -39, 28 -39, 28 -38, 29 -38, 29 -37, 30 -37, 30 -36, 31 -36, 31 -35, 32 -35, 32 -25, 31 -25, 31 -23, 30 -23, 30 -22, 29 -22, 29 -21, 28 -21, 28 -20, 27 -20, 27 -19, 28 -19, 28 -18, 29 -18, 29 -17, 30 -17, 30 -7, 29 -7, 29 -6, 28 -6, 28 -5, 27 -5, 27 -4, 25 -4, 25 -3, 6 -3), (11 -7, 18 -7, 18 -8, 22 -8, 22 -9, 23 -9, 23 -10, 24 -10, 24 -15, 23 -15, 23 -16, 22 -16, 22 -17, 18 -17, 18 -18, 11 -18, 11 -7), (11 -22, 19 -22, 19 -23, 23 -23, 23 -24, 25 -24, 25 -25, 26 -25, 26 -33, 25 -33, 25 -34, 23 -34, 23 -35, 19 -35, 19 -36, 11 -36, 11 -22))"),
        results.get(0).geometry);
    assertEquals(3, results.get(0).label);
  }
}
