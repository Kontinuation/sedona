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
package org.apache.sedona.core.joinJudgement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.commons.collections4.iterators.SingletonIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.geometryObjects.UniqueGeometry;
import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.sedona.core.index.ExternalIndexTestBase;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.util.LongAccumulator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.mockito.Mock;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class KnnJoinIndexJudgementTest extends ExternalIndexTestBase {

  @Parameterized.Parameters(name = "use external spatial index: {0}")
  public static Collection<Boolean> testParams() {
    return Arrays.asList(false, true);
  }

  private final boolean useExternalSpatialIndex;

  public KnnJoinIndexJudgementTest(boolean useExternalSpatialIndex) {
    this.useExternalSpatialIndex = useExternalSpatialIndex;
  }

  @Mock(answer = RETURNS_SMART_NULLS)
  SedonaConf sedonaConf;

  @Mock(answer = RETURNS_SMART_NULLS)
  SparkEnv sparkEnv;

  @Before
  public void setUpMock() {
    when(sparkEnv.blockManager()).thenReturn(blockManager);
    when(sedonaConf.useExternalSpatialIndex()).thenReturn(useExternalSpatialIndex);
    when(sedonaConf.getExternalSpatialIndexLeafPageCapacity()).thenReturn(10);
    when(sedonaConf.getExternalSpatialIndexInternalNodeCapacity()).thenReturn(10);
    when(sedonaConf.forceSpillExternalSpatialIndex()).thenReturn(false);
    buildCount = Mockito.mock(LongAccumulator.class);
    streamCount = Mockito.mock(LongAccumulator.class);
    resultCount = Mockito.mock(LongAccumulator.class);
    candidateCount = Mockito.mock(LongAccumulator.class);
  }

  private LongAccumulator buildCount;
  private LongAccumulator streamCount;
  private LongAccumulator resultCount;
  private LongAccumulator candidateCount;
  private final GeometryFactory factory = new GeometryFactory();

  private KnnJoinIndexJudgement<Geometry, Geometry> createTestJudgement(
      int k, Double searchRadius, DistanceMetric distanceMetric, boolean includeTies) {
    KnnJoinIndexJudgement<Geometry, Geometry> judgement =
        new KnnJoinIndexJudgement<>(
            k,
            searchRadius,
            distanceMetric,
            includeTies,
            null,
            null,
            buildCount,
            streamCount,
            resultCount,
            candidateCount,
            sedonaConf);
    judgement.setSparkEnv(sparkEnv);
    judgement.setTaskContext(taskContext);
    return judgement;
  }

  private KnnJoinIndexJudgement<Geometry, Geometry> createTestJudgement() {
    return createTestJudgement(5, null, DistanceMetric.EUCLIDEAN, false);
  }

  @Test
  public void testCallWithEmptyIterators() throws Exception {
    KnnJoinIndexJudgement<Geometry, Geometry> judgement = createTestJudgement();
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(Collections.emptyIterator(), Collections.emptyIterator());
    assertFalse(resultIterator.hasNext());
  }

  @Test
  public void testCallWithNonEmptyIterators() throws Exception {
    List<Geometry> testObjects = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      testObjects.add(factory.createPoint(new Coordinate(i, i)));
    }

    // Create a test point
    Geometry testPoint = factory.createPoint(new Coordinate(0, 0));

    // Perform a KNN search using the test point
    KnnJoinIndexJudgement<Geometry, Geometry> judgement = createTestJudgement();
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(new SingletonIterator<>(testPoint), testObjects.iterator());

    // Assert that there are results
    assertTrue(resultIterator.hasNext());

    // Assert that the results are correct
    for (int i = 0; i < 5; i++) {
      Pair<Geometry, Geometry> pair = resultIterator.next();
      assertEquals(testPoint, pair.getKey());
      assertEquals(factory.createPoint(new Coordinate(i, i)), pair.getValue());
    }

    // Assert that there are no more results
    assertFalse(resultIterator.hasNext());
  }

  @Test
  public void testCallWithRandomSpatialIndexData() throws Exception {
    List<Geometry> testObjects = new ArrayList<>();

    // Create a Random object
    Random random = new Random();

    // Insert multiple random points into the spatial index
    for (int i = 0; i < 100; i++) {
      double x = random.nextDouble() * 10; // generate random x-coordinate within range [0, 10)
      double y = random.nextDouble() * 10; // generate random y-coordinate within range [0, 10)
      Geometry point = factory.createPoint(new Coordinate(x, y));
      testObjects.add(point);
    }

    // Create a list of test points
    List<Geometry> testPoints = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      double x = random.nextDouble() * 10; // generate random x-coordinate within range [0, 10)
      double y = random.nextDouble() * 10; // generate random y-coordinate within range [0, 10)
      testPoints.add(factory.createPoint(new Coordinate(x, y)));
    }

    // Perform a KNN search using each test point
    KnnJoinIndexJudgement<Geometry, Geometry> judgement = createTestJudgement();
    for (Geometry testPoint : testPoints) {
      Iterator<Pair<Geometry, Geometry>> resultIterator =
          judgement.call(new SingletonIterator<>(testPoint), testObjects.iterator());

      // Assert that there are results
      assertTrue(resultIterator.hasNext());

      // Assert that the results are correct
      while (resultIterator.hasNext()) {
        Pair<Geometry, Geometry> pair = resultIterator.next();
        assertEquals(testPoint, pair.getKey());
        assertNotNull(pair.getValue());
      }
    }
  }

  @Test
  public void testCase1() throws Exception {
    KnnJoinIndexJudgement<Geometry, Geometry> thisJudgement =
        createTestJudgement(4, null, DistanceMetric.EUCLIDEAN, false);

    // Points forming a grid
    List<Geometry> testObjects = new ArrayList<>();
    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j <= 4; j++) {
        testObjects.add(factory.createPoint(new Coordinate(i, j)));
      }
    }

    // Create a test point
    Geometry testPoint = factory.createPoint(new Coordinate(3.3, 4.4));

    // Perform a KNN search using the test point
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        thisJudgement.call(new SingletonIterator<>(testPoint), testObjects.iterator());

    // Assert that the results are correct
    int count = 0;
    while (resultIterator.hasNext()) {
      resultIterator.next();
      count++;
    }
    // without search radius, the result should be 4
    assertEquals(4, count);
  }

  @Test
  public void testCase2() throws Exception {
    KnnJoinIndexJudgement<Geometry, Geometry> thisJudgement =
        createTestJudgement(4, 1.4, DistanceMetric.EUCLIDEAN, false);

    // Points forming a grid
    List<Geometry> testObjects = new ArrayList<>();
    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j <= 4; j++) {
        testObjects.add(factory.createPoint(new Coordinate(i, j)));
      }
    }

    // Create a test point
    Geometry testPoint = factory.createPoint(new Coordinate(3.3, 4.4));

    // Perform a KNN search using the test point
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        thisJudgement.call(new SingletonIterator<>(testPoint), testObjects.iterator());

    // Assert that the results are correct
    int count = 0;
    while (resultIterator.hasNext()) {
      resultIterator.next();
      count++;
    }
    // with search radius 1.4, the result should be 3
    assertEquals(3, count);
  }

  @Test
  public void testSpill() throws Exception {
    if (!useExternalSpatialIndex) {
      return;
    }

    KnnJoinIndexJudgement<Geometry, Geometry> thisJudgement =
        createTestJudgement(4, 1.4, DistanceMetric.EUCLIDEAN, false);

    // Points forming a grid
    List<Geometry> testObjects = new ArrayList<>();
    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j <= 4; j++) {
        testObjects.add(factory.createPoint(new Coordinate(i, j)));
      }
    }

    // Create a test points
    Geometry testPoint = factory.createPoint(new Coordinate(3.3, 4.4));
    List<Geometry> testPoints = new ArrayList<>();
    for (int k = 0; k < 10; k++) {
      testPoints.add(testPoint);
    }

    // Perform a KNN search using the test point
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        thisJudgement.call(testPoints.iterator(), testObjects.iterator());
    ((ExternalKNNJoinIterator<Geometry, Geometry>) resultIterator).forceSpill();

    // Assert that the results are correct
    int count = 0;
    while (resultIterator.hasNext()) {
      resultIterator.next();
      count++;
    }
    // with search radius 1.4, the result should be 3
    assertEquals(30, count);
  }

  @Test
  public void testSpillWithUniqueQueryGeometry() throws Exception {
    if (!useExternalSpatialIndex) {
      return;
    }

    KnnJoinIndexJudgement<Geometry, Geometry> thisJudgement =
        createTestJudgement(4, 1.4, DistanceMetric.EUCLIDEAN, false);

    // Points forming a grid
    List<Geometry> testObjects = new ArrayList<>();
    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j <= 4; j++) {
        testObjects.add(factory.createPoint(new Coordinate(i, j)));
      }
    }

    // Create test points as UniqueGeometry
    Geometry testPoint = factory.createPoint(new Coordinate(3.3, 4.4));
    List<Geometry> testPoints = new ArrayList<>();
    for (int k = 0; k < 10; k++) {
      testPoints.add(new UniqueGeometry<>(k, testPoint));
    }

    // Perform a KNN search using the test point
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        thisJudgement.call(testPoints.iterator(), testObjects.iterator());
    ((ExternalKNNJoinIterator<Geometry, Geometry>) resultIterator).forceSpill();

    // Assert that the results are correct
    int count = 0;
    while (resultIterator.hasNext()) {
      resultIterator.next();
      count++;
    }
    // with search radius 1.4, the result should be 3
    assertEquals(30, count);
  }
}
