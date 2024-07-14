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

import java.util.ArrayList;
import java.util.List;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.utils.RasterPolygonEnumerator;
import org.apache.sedona.common.utils.RasterPolygonizer;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

/** Functions for WherobotsAI */
public class RasterAIFunctions {
  private RasterAIFunctions() {}

  public static class ExtractedRegionInfo {
    public final Geometry geometry;
    public final double averageConfidenceScore;
    public final int label;

    public ExtractedRegionInfo(Geometry geometry, double averageConfidenceScore, int label) {
      this.geometry = geometry;
      this.averageConfidenceScore = averageConfidenceScore;
      this.label = label;
    }
  }

  public static List<ExtractedRegionInfo> segmentToGeoms(
      GridCoverage2D ref, double[] confidenceArray, int[] labelArray, double threshold)
      throws FactoryException, TransformException {
    GridEnvelope2D gridRange = ref.getGridGeometry().getGridRange2D();
    int width = gridRange.width;
    int height = gridRange.height;
    int numClasses = labelArray.length;
    if (confidenceArray.length != width * height * numClasses) {
      throw new IllegalArgumentException("Confidence array length does not match the raster size");
    }
    AffineTransform2D affine = RasterUtils.getGDALAffineTransform(ref);
    MathTransform transform =
        CRS.findMathTransform(ref.getCoordinateReferenceSystem(), DefaultGeographicCRS.WGS84, true);

    GeometryFactory factory = new GeometryFactory();
    List<ExtractedRegionInfo> results = new ArrayList<>();

    int[] grid = new int[width * height];
    double[] sumScores = new double[numClasses];
    double[] averageScores = new double[numClasses];
    int[] numCells = new int[numClasses];

    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        int labelWithHighestScoreIndex = 0;
        double highestScore = Double.MIN_VALUE;
        for (int c = 0; c < numClasses; c++) {
          double score = confidenceArray[c * width * height + y * width + x];
          if (score > highestScore) {
            highestScore = score;
            labelWithHighestScoreIndex = c;
          }
        }
        if (highestScore >= threshold) {
          grid[y * width + x] = labelWithHighestScoreIndex;
          sumScores[labelWithHighestScoreIndex] += highestScore;
          numCells[labelWithHighestScoreIndex]++;
        } else {
          grid[y * width + x] = RasterPolygonEnumerator.GP_NODATA_MARKER;
        }
      }
    }

    for (int labelIndex = 0; labelIndex < numClasses; labelIndex++) {
      int numCellsForLabel = numCells[labelIndex];
      double sumScore = sumScores[labelIndex];
      averageScores[labelIndex] = (numCellsForLabel > 0 ? sumScore / numCellsForLabel : 0);
    }

    // Detect polygons in the grid
    List<RasterPolygonizer.PolygonWithValue> polygonsWithValues =
        RasterPolygonizer.polygonize(grid, width, 4, affine);

    // Collect polygons for each class
    ArrayList<ArrayList<Polygon>> polygonsByLabel = new ArrayList<>(numClasses);
    for (int i = 0; i < numClasses; i++) {
      polygonsByLabel.add(null);
    }
    for (RasterPolygonizer.PolygonWithValue polygonWithValue : polygonsWithValues) {
      Polygon polygon = polygonWithValue.polygon;
      int labelIndex = polygonWithValue.value;
      if (polygonsByLabel.get(labelIndex) == null) {
        polygonsByLabel.set(labelIndex, new ArrayList<>());
      }
      polygonsByLabel.get(labelIndex).add(polygon);
    }

    // Generate results
    for (int labelIndex = 0; labelIndex < numClasses; labelIndex++) {
      // Transform the polygons to WGS84 and put them in the result list
      ArrayList<Polygon> polygons = polygonsByLabel.get(labelIndex);
      if (polygons == null || polygons.isEmpty()) {
        continue;
      }
      Geometry geom;
      if (polygons.size() > 1) {
        geom = factory.createMultiPolygon(polygons.toArray(new Polygon[0]));
      } else {
        geom = polygons.get(0);
      }
      Geometry transformed = JTS.transform(geom, transform);
      transformed = Functions.setSRID(transformed, 4326);
      results.add(
          new ExtractedRegionInfo(transformed, averageScores[labelIndex], labelArray[labelIndex]));
    }

    return results;
  }
}
