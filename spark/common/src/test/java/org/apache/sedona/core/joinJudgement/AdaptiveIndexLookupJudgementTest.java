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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.ExecutionMode;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.IndexBuildSide;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.LocalSpatialJoinExecParams;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class AdaptiveIndexLookupJudgementTest {

  private static final GeometryFactory factory = new GeometryFactory();
  private static final List<Geometry> datasetA = generateRandomGeometries(1);
  private static final List<Geometry> datasetB = generateRandomGeometries(2);

  @SuppressWarnings("unchecked")
  @Test
  public void testEmptyDatasets() {
    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_STREAM, null);
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        new AdaptiveIndexLookupJudgement<>(
            SpatialPredicate.INTERSECTS, Collections.singletonList(param));

    // Both sides are empty
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(0, Collections.emptyIterator(), Collections.emptyIterator());
    assertFalse(resultIterator.hasNext());

    // Left side is empty
    resultIterator =
        judgement.call(
            0,
            Collections.emptyIterator(),
            new SingletonIterator(factory.createPoint(new Coordinate(0, 0))));
    assertFalse(resultIterator.hasNext());

    // Right side is empty
    resultIterator =
        judgement.call(
            0,
            new SingletonIterator(factory.createPoint(new Coordinate(0, 0))),
            Collections.emptyIterator());
    assertFalse(resultIterator.hasNext());
  }

  @Test
  public void testPrepareBuildSide() {
    testExecutionMode(ExecutionMode.PREPARE_BUILD);
  }

  @Test
  public void testPrepareStreamSide() {
    testExecutionMode(ExecutionMode.PREPARE_STREAM);
  }

  @Test
  public void testPrepareNone() {
    testExecutionMode(ExecutionMode.PREPARE_NONE);
  }

  @Test
  public void testSubdividing() {
    SubdivideOptions subdivideOptions = new SubdivideOptions(0.2, 0.2);
    ExecutionMode[] executionModes = {
      ExecutionMode.PREPARE_BUILD, ExecutionMode.PREPARE_STREAM, ExecutionMode.PREPARE_NONE
    };
    for (ExecutionMode executionMode : executionModes) {
      testExecutionMode(
          executionMode,
          SpatialPredicate.INTERSECTS,
          IndexType.RTREE,
          IndexBuildSide.LEFT,
          subdivideOptions,
          null);
      testExecutionMode(
          executionMode,
          SpatialPredicate.INTERSECTS,
          IndexType.RTREE,
          IndexBuildSide.LEFT,
          null,
          subdivideOptions);
      testExecutionMode(
          executionMode,
          SpatialPredicate.INTERSECTS,
          IndexType.RTREE,
          IndexBuildSide.LEFT,
          subdivideOptions,
          subdivideOptions);
    }
  }

  private void testExecutionMode(ExecutionMode executionMode) {
    SpatialPredicate[] predicates =
        new SpatialPredicate[] {
          SpatialPredicate.CONTAINS, SpatialPredicate.INTERSECTS, SpatialPredicate.WITHIN
        };
    IndexType[] indexTypes = new IndexType[] {IndexType.RTREE, IndexType.QUADTREE};
    IndexBuildSide[] indexBuildSides =
        new IndexBuildSide[] {IndexBuildSide.LEFT, IndexBuildSide.RIGHT};

    for (SpatialPredicate predicate : predicates) {
      for (IndexType indexType : indexTypes) {
        for (IndexBuildSide indexBuildSide : indexBuildSides) {
          testExecutionMode(executionMode, predicate, indexType, indexBuildSide, null, null);
        }
      }
    }
  }

  private void testExecutionMode(
      ExecutionMode executionMode,
      SpatialPredicate predicate,
      IndexType indexType,
      IndexBuildSide indexBuildSide,
      SubdivideOptions subdivideBuildOptions,
      SubdivideOptions subdivideStreamOptions) {
    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            indexType,
            indexBuildSide,
            executionMode,
            null,
            subdivideBuildOptions,
            subdivideStreamOptions);
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        new AdaptiveIndexLookupJudgement<>(predicate, Collections.singletonList(param));
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(0, datasetA.iterator(), datasetB.iterator());
    verifyResult(resultIterator, datasetA, datasetB, predicate);
    resultIterator = judgement.call(0, datasetB.iterator(), datasetA.iterator());
    verifyResult(resultIterator, datasetB, datasetA, predicate);
  }

  private static List<Geometry> generateRandomGeometries(int seed) {
    List<Geometry> geoms = new ArrayList<>();
    Random random = new Random(seed);
    for (int k = 0; k < 1000; k++) {
      double minX = random.nextDouble() * 10;
      double minY = random.nextDouble() * 10;
      double width = random.nextDouble();
      double height = random.nextDouble();
      if (random.nextBoolean()) {
        Envelope env = new Envelope(minX, minX + width, minY, minY + height);
        geoms.add(factory.toGeometry(env));
      } else {
        double centerX = minX + width / 2;
        double centerY = minY + height / 2;
        double radius = 0.5 * (width + height);
        geoms.add(factory.createPoint(new Coordinate(centerX, centerY)).buffer(radius, 2));
      }
    }
    return geoms;
  }

  private static void verifyResult(
      Iterator<Pair<Geometry, Geometry>> resultIterator,
      List<Geometry> datasetA,
      List<Geometry> datasetB,
      SpatialPredicate predicate) {
    Set<Pair<Geometry, Geometry>> expected = new HashSet<>();
    SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator =
        SpatialPredicateEvaluators.create(predicate);
    for (Geometry geomA : datasetA) {
      for (Geometry geomB : datasetB) {
        if (evaluator.eval(geomA, geomB)) {
          expected.add(Pair.of(geomA, geomB));
        }
      }
    }
    long count = 0;
    while (resultIterator.hasNext()) {
      Pair<Geometry, Geometry> result = resultIterator.next();
      assertTrue(expected.contains(result));
      count++;
    }
    assertTrue(count > 0);
    assertEquals(expected.size(), count);
  }
}
