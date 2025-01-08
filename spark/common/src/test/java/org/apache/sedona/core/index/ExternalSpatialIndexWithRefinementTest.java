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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.sedona.core.index.ExternalSpatialIndex;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement.DataObjectWithId;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItem;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItemFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.index.strtree.STRtree;

@RunWith(Parameterized.class)
public class ExternalSpatialIndexWithRefinementTest extends ExternalIndexTestBase {
  private static final GeometryFactory FACTORY = new GeometryFactory();
  private static final SubdivideOptions SUBDIVIDE_OPTIONS = new SubdivideOptions(0.1, 0.1);
  private static final Function2<Geometry, Geometry, Boolean> FILTER =
      (Geometry geom1, Geometry geom2) -> geom1.getCentroid().distance(geom2.getCentroid()) < 0.1;

  private static final List<Geometry> DATASET_BUILD = generateRandomGeometries(10000, 1);
  private static final List<Geometry> DATASET_STREAM = generateRandomGeometries(1000, 2);

  private final ExecutionMode executionMode;
  private final Function2<Geometry, Geometry, Boolean> extraFilter;
  private final SubdivideOptions subdivideBuildOptions;
  private final SubdivideOptions subdivideStreamOptions;

  @Parameters(
      name = "exec mode: {0}, has extra filter: {1}, subdivide build: {2}, subdivide stream: {3}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(
        new Object[][] {
          {ExecutionMode.PREPARE_NONE, false, false, false},
          {ExecutionMode.PREPARE_NONE, true, false, false},
          {ExecutionMode.PREPARE_BUILD, false, false, false},
          {ExecutionMode.PREPARE_BUILD, true, false, false},
          {ExecutionMode.PREPARE_STREAM, false, false, false},
          {ExecutionMode.PREPARE_STREAM, true, false, false},
          {ExecutionMode.PREPARE_BUILD, true, true, true},
          {ExecutionMode.PREPARE_STREAM, true, true, true},
        });
  }

  public ExternalSpatialIndexWithRefinementTest(
      ExecutionMode executionMode,
      boolean hasExtraFilter,
      boolean subdivideLeft,
      boolean subdivideRight) {
    this.executionMode = executionMode;
    this.extraFilter = hasExtraFilter ? FILTER : null;
    this.subdivideBuildOptions = subdivideLeft ? SUBDIVIDE_OPTIONS : null;
    this.subdivideStreamOptions = subdivideRight ? SUBDIVIDE_OPTIONS : null;
  }

  @Test
  public void testEmpty() throws IOException {
    ExternalSpatialIndexWithRefinement<GeometryDataItem> index =
        create(executionMode, subdivideBuildOptions, subdivideStreamOptions);
    index.build();

    // Querying an empty index should return an empty iterator
    Geometry queryGeom = FACTORY.createPoint(new Coordinate(0, 0));
    Iterator<DataObjectWithId<GeometryDataItem>> iter =
        index.query(
            queryGeom, SpatialPredicateEvaluators.create(SpatialPredicate.INTERSECTS), extraFilter);
    assertFalse(iter.hasNext());

    index.close();
  }

  @Test
  public void testBuildAndQueryIndex() throws IOException {
    ExternalSpatialIndexWithRefinement<GeometryDataItem> index =
        create(executionMode, subdivideBuildOptions, subdivideStreamOptions);
    STRtree refIndex = new STRtree();
    for (Geometry geom : DATASET_BUILD) {
      index.insert(new GeometryDataItem(geom));
      refIndex.insert(geom.getEnvelopeInternal(), geom);
    }
    index.build();
    refIndex.build();

    for (int trial = 0; trial < 2; trial++) {
      for (Geometry geom : DATASET_STREAM) {
        Iterator<DataObjectWithId<GeometryDataItem>> actualIter =
            index.query(
                geom, SpatialPredicateEvaluators.create(SpatialPredicate.INTERSECTS), extraFilter);
        List<Geometry> actual = new ArrayList<>();
        while (actualIter.hasNext()) {
          actual.add(actualIter.next().dataObject.geometry);
        }
        actual.sort(Comparator.comparingInt(g -> (int) g.getUserData()));
        List<Geometry> expected = queryRefIndex(refIndex, geom);
        verifyResults(expected, actual);
      }

      index.spill();
      index.invalidateCache();
    }

    index.close();
  }

  @Test
  public void testFetchDataObjects() throws IOException {
    ExternalSpatialIndexWithRefinement<GeometryDataItem> index =
        create(executionMode, subdivideBuildOptions, subdivideStreamOptions);
    for (Geometry geom : DATASET_BUILD) {
      index.insert(new GeometryDataItem(geom));
    }
    index.build();

    Random random = new Random(0);
    for (int trial = 0; trial < 2; trial++) {
      for (int k = 0; k < 100; k++) {
        // Randomly select 100 item ids
        IntArrayList itemIds = new IntArrayList(100);
        List<Geometry> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
          int itemId = random.nextInt(DATASET_BUILD.size());
          itemIds.add(itemId);
          expected.add(DATASET_BUILD.get(itemId));
        }
        Iterator<DataObjectWithId<GeometryDataItem>> iter = index.fetchDataObjects(itemIds);
        List<Geometry> results = new ArrayList<>();
        while (iter.hasNext()) {
          results.add(iter.next().dataObject.geometry);
        }
        expected.sort(Comparator.comparingInt(geom -> (int) geom.getUserData()));
        results.sort(Comparator.comparingInt(geom -> (int) geom.getUserData()));
        verifyResults(expected, results);
      }

      index.spill();
      index.invalidateCache();
    }

    index.close();
  }

  private static List<Geometry> generateRandomGeometries(int numGeometries, int seed) {
    Random random = new Random(seed);
    List<Geometry> geometries = new ArrayList<>(numGeometries);
    // Generate random geometries in the range of [0, 10] x [0, 10].
    // The generated geometries are line strings of length 0.5, each line string has 5 points.
    for (int i = 0; i < numGeometries; i++) {
      double x = random.nextDouble() * 10;
      double y = random.nextDouble() * 10;
      double theta = random.nextDouble() * 2 * Math.PI;
      double cosTheta = Math.cos(theta);
      double sinTheta = Math.sin(theta);
      Coordinate[] coordinates = new Coordinate[5];
      for (int j = 0; j < 5; j++) {
        coordinates[j] = new Coordinate(x + j * 0.1 * cosTheta, y + j * 0.1 * sinTheta);
      }
      Geometry geom = FACTORY.createLineString(coordinates);
      geom.setUserData(i);
      geometries.add(geom);
    }
    return geometries;
  }

  @SuppressWarnings("unchecked")
  private List<Geometry> queryRefIndex(STRtree refIndex, Geometry queryGeom) {
    List<Geometry> candidates = (List<Geometry>) refIndex.query(queryGeom.getEnvelopeInternal());
    List<Geometry> results = new ArrayList<>();

    try {
      for (Geometry geom : candidates) {
        if (queryGeom.intersects(geom)
            && (extraFilter == null || extraFilter.call(queryGeom, geom))) {
          results.add(geom);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    results.sort(Comparator.comparingInt(geom -> (int) geom.getUserData()));
    return results;
  }

  private void verifyResults(List<Geometry> expected, List<Geometry> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Geometry expectedGeom = expected.get(i);
      Geometry actualGeom = actual.get(i);
      assertEquals(expectedGeom.getUserData(), actualGeom.getUserData());
      assertTrue(expectedGeom.equalsExact(actualGeom, 1e-6));
    }
  }

  private ExternalSpatialIndexWithRefinement<GeometryDataItem> create(
      ExecutionMode executionMode,
      SubdivideOptions subdivideBuildOptions,
      SubdivideOptions subdivideStreamOptions) {
    ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();
    ExternalSpatialIndex innerIndex =
        new ExternalSpatialIndex(
            taskContext, taskMemoryManager, blockManager, pageSizeBytes, 20, 10, writeMetrics);
    return new ExternalSpatialIndexWithRefinement<>(
        innerIndex,
        new GeometryDataItemFormat(),
        executionMode,
        subdivideBuildOptions,
        subdivideStreamOptions);
  }
}
