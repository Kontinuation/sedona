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
package org.apache.sedona.core.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.knnJudgement.EuclideanItemDistance;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.sedona.core.index.ExternalSpatialIndex;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement.DataObjectWithId;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItem;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItemFormat;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

public class NearestNeighborSearchTest extends ExternalIndexTestBase {
  private static final GeometryFactory FACTORY = new GeometryFactory();

  @Test
  public void testEmpty() throws IOException {
    try (ExternalSpatialIndexWithRefinement<GeometryDataItem> index = create()) {
      index.build();
      Geometry queryPoint = FACTORY.createPoint(new Coordinate(1, 2));
      List<DataObjectWithId<GeometryDataItem>> results =
          index.nearestNeighbours(
              queryPoint.getEnvelopeInternal(), queryPoint, new EuclideanItemDistance(), 3);
      assertTrue(results.isEmpty());
    }
  }

  @Test
  public void testKEqualsZero() throws IOException {
    try (ExternalSpatialIndexWithRefinement<GeometryDataItem> index = create()) {
      // Index grids of geometries
      for (int i = 0; i < 10; i++) {
        double x = i * 0.1;
        double y = i * 0.1;
        Point geom = FACTORY.createPoint(new Coordinate(x, y));
        GeometryDataItem item = new GeometryDataItem(geom);
        index.insert(item);
      }

      index.build();

      Geometry queryPoint = FACTORY.createPoint(new Coordinate(0, 0));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              index.nearestNeighbours(
                  queryPoint.getEnvelopeInternal(), queryPoint, new EuclideanItemDistance(), 0));
    }
  }

  @Test
  public void testSearchAll() throws IOException {
    try (ExternalSpatialIndexWithRefinement<GeometryDataItem> index = create()) {
      // Index grids of geometries
      for (int i = 0; i < 10; i++) {
        double x = i * 0.1;
        double y = i * 0.1;
        Point geom = FACTORY.createPoint(new Coordinate(x, y));
        GeometryDataItem item = new GeometryDataItem(geom);
        index.insert(item);
      }

      index.build();

      Geometry queryPoint = FACTORY.createPoint(new Coordinate(0, 0));
      for (int topK = 10; topK < 20; topK++) {
        List<DataObjectWithId<GeometryDataItem>> results =
            index.nearestNeighbours(
                queryPoint.getEnvelopeInternal(), queryPoint, new EuclideanItemDistance(), topK);
        assertEquals(10, results.size());
        for (int i = 0; i < 10; i++) {
          double x = i * 0.1;
          double y = i * 0.1;
          Point geom = FACTORY.createPoint(new Coordinate(x, y));
          assertEquals(geom, results.get(i).dataObject.geometry);
        }
      }
    }
  }

  @Test
  public void testNearestNeighborSearch() throws IOException {
    int[] topKs = {1, 10, 100};
    for (int topK : topKs) {
      testNearestNeighborSearch(topK);
    }
  }

  private void testNearestNeighborSearch(int topK) throws IOException {
    STRtree tree = new STRtree();
    try (ExternalSpatialIndexWithRefinement<GeometryDataItem> index = create()) {
      // Index grids of geometries
      for (int i = 0; i < 100; i++) {
        for (int j = 0; j < 100; j++) {
          double x = i * 0.1;
          double y = j * 0.1;
          Point geom = FACTORY.createPoint(new Coordinate(x, y));
          GeometryDataItem item = new GeometryDataItem(geom);
          index.insert(item);
          tree.insert(geom.getEnvelopeInternal(), geom);
        }
      }

      index.build();
      tree.build();

      // Query the top-K nearest neighbors
      for (int trials = 0; trials < 2; trials++) {
        for (int i = 0; i < 10; i++) {
          for (int j = 0; j < 10; j++) {
            double x = -5.1 + i * 2;
            double y = -5.2 + j * 2;
            Geometry queryPoint = FACTORY.createPoint(new Coordinate(x, y));
            List<DataObjectWithId<GeometryDataItem>> results =
                index.nearestNeighbours(
                    queryPoint.getEnvelopeInternal(),
                    queryPoint,
                    new EuclideanItemDistance(),
                    topK);
            assertEquals(topK, results.size());
            Object[] expected =
                tree.nearestNeighbour(
                    queryPoint.getEnvelopeInternal(),
                    queryPoint,
                    new EuclideanItemDistance(),
                    topK);
            for (int k = 0; k < expected.length; k++) {
              double dist1 = results.get(k).dataObject.geometry.distance(queryPoint);
              double dist2 = ((Geometry) expected[k]).distance(queryPoint);
              assertEquals(dist1, dist2, 1e-8);
            }
          }
        }
        index.spill();
      }
    }
  }

  private ExternalSpatialIndexWithRefinement<GeometryDataItem> create() {
    ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();
    ExternalSpatialIndex innerIndex =
        new ExternalSpatialIndex(
            taskContext, taskMemoryManager, blockManager, pageSizeBytes, 10, 10, writeMetrics);
    return new ExternalSpatialIndexWithRefinement<>(
        innerIndex, new GeometryDataItemFormat(), ExecutionMode.PREPARE_NONE, null, null);
  }
}
