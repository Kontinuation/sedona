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

import static org.junit.Assert.*;

import java.util.*;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.spark.util.LongAccumulator;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.index.strtree.STRtree;
import org.mockito.Mockito;

public class KnnJoinIndexJudgementTest {

  private KnnJoinIndexJudgement<Geometry, Geometry> judgement;
  private LongAccumulator buildCount;
  private LongAccumulator streamCount;
  private LongAccumulator resultCount;
  private LongAccumulator candidateCount;
  private GeometryFactory factory;

  @Before
  public void setUp() {
    buildCount = Mockito.mock(LongAccumulator.class);
    streamCount = Mockito.mock(LongAccumulator.class);
    resultCount = Mockito.mock(LongAccumulator.class);
    candidateCount = Mockito.mock(LongAccumulator.class);
    factory = new GeometryFactory();
    judgement =
        new KnnJoinIndexJudgement<>(
            5,
            DistanceMetric.EUCLIDEAN,
            false,
            null,
            buildCount,
            streamCount,
            resultCount,
            candidateCount);
  }

  @Test
  public void testCallWithEmptyIterators() throws Exception {
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(Collections.emptyIterator(), Collections.emptyIterator());
    assertFalse(resultIterator.hasNext());
  }

  @Test
  public void testCallWithNonEmptyIterators() throws Exception {
    // Create an STRtree spatial index
    STRtree strTree = new STRtree();

    // Insert multiple points into the spatial index
    strTree.insert(
        factory.createPoint(new Coordinate(0, 0)).getEnvelopeInternal(),
        factory.createPoint(new Coordinate(0, 0)));
    strTree.insert(
        factory.createPoint(new Coordinate(1, 1)).getEnvelopeInternal(),
        factory.createPoint(new Coordinate(1, 1)));
    strTree.insert(
        factory.createPoint(new Coordinate(2, 2)).getEnvelopeInternal(),
        factory.createPoint(new Coordinate(2, 2)));
    strTree.insert(
        factory.createPoint(new Coordinate(3, 3)).getEnvelopeInternal(),
        factory.createPoint(new Coordinate(3, 3)));
    strTree.insert(
        factory.createPoint(new Coordinate(4, 4)).getEnvelopeInternal(),
        factory.createPoint(new Coordinate(4, 4)));

    // Create a test point
    Geometry testPoint = factory.createPoint(new Coordinate(0, 0));

    // Perform a KNN search using the test point
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(new SingletonIterator(testPoint), new SingletonIterator(strTree));

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
    // Create an STRtree spatial index
    STRtree strTree = new STRtree();

    // Create a Random object
    Random random = new Random();

    // Insert multiple random points into the spatial index
    for (int i = 0; i < 100; i++) {
      double x = random.nextDouble() * 10; // generate random x-coordinate within range [0, 10)
      double y = random.nextDouble() * 10; // generate random y-coordinate within range [0, 10)
      Geometry point = factory.createPoint(new Coordinate(x, y));
      strTree.insert(point.getEnvelopeInternal(), point);
    }

    // Create a list of test points
    List<Geometry> testPoints = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      double x = random.nextDouble() * 10; // generate random x-coordinate within range [0, 10)
      double y = random.nextDouble() * 10; // generate random y-coordinate within range [0, 10)
      testPoints.add(factory.createPoint(new Coordinate(x, y)));
    }

    // Perform a KNN search using each test point
    for (Geometry testPoint : testPoints) {
      Iterator<Pair<Geometry, Geometry>> resultIterator =
          judgement.call(new SingletonIterator(testPoint), new SingletonIterator(strTree));

      // Assert that there are results
      assertTrue(resultIterator.hasNext());

      // Assert that the results are correct
      while (resultIterator.hasNext()) {
        Pair<Geometry, Geometry> pair = resultIterator.next();
        assertEquals(testPoint, pair.getKey());
        assertTrue(pair.getValue() instanceof Geometry);
      }

      // Assert that there are no more results
      assertFalse(resultIterator.hasNext());
    }
  }

  @Test
  public void testCase1() throws Exception {
    // Create an STRtree spatial index
    STRtree strTree = new STRtree();

    KnnJoinIndexJudgement thisJudgement =
        new KnnJoinIndexJudgement<>(
            4,
            DistanceMetric.EUCLIDEAN,
            false,
            null,
            buildCount,
            streamCount,
            resultCount,
            candidateCount);
    // Points forming a grid
    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j <= 4; j++) {
        strTree.insert(
            factory.createPoint(new Coordinate(i, j)).getEnvelopeInternal(),
            factory.createPoint(new Coordinate(i, j)));
      }
    }

    // Create a test point
    Geometry testPoint = factory.createPoint(new Coordinate(3.3, 4.4));

    // Perform a KNN search using the test point
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        thisJudgement.call(new SingletonIterator(testPoint), new SingletonIterator(strTree));

    // Assert that there are results
    assertTrue(resultIterator.hasNext());
  }
}
