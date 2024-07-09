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
import org.apache.sedona.core.enums.JoinType;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.ExecutionMode;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.IndexBuildSide;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.LocalSpatialJoinExecParams;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.sedona.core.spatialPartitioning.OuterJoinSpatialPartitioner.OuterJoinUserData;
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
            SpatialPredicate.INTERSECTS, JoinType.INNER, Collections.singletonList(param));

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
    JoinType[] joinTypes = {JoinType.INNER, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER};
    ExecutionMode[] executionModes = {
      ExecutionMode.PREPARE_BUILD, ExecutionMode.PREPARE_STREAM, ExecutionMode.PREPARE_NONE
    };

    for (JoinType joinType : joinTypes) {
      for (ExecutionMode executionMode : executionModes) {
        testExecutionMode(
            executionMode,
            SpatialPredicate.INTERSECTS,
            joinType,
            IndexType.RTREE,
            IndexBuildSide.LEFT,
            subdivideOptions,
            null);
        testExecutionMode(
            executionMode,
            SpatialPredicate.INTERSECTS,
            joinType,
            IndexType.RTREE,
            IndexBuildSide.LEFT,
            null,
            subdivideOptions);
        testExecutionMode(
            executionMode,
            SpatialPredicate.INTERSECTS,
            joinType,
            IndexType.RTREE,
            IndexBuildSide.LEFT,
            subdivideOptions,
            subdivideOptions);
      }
    }
  }

  private void testExecutionMode(ExecutionMode executionMode) {
    JoinType[] joinTypes = {JoinType.INNER, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER};
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
          testExecutionMode(
              executionMode, predicate, JoinType.INNER, indexType, indexBuildSide, null, null);
        }
      }
    }
    for (JoinType joinType : joinTypes) {
      for (IndexType indexType : indexTypes) {
        for (IndexBuildSide indexBuildSide : indexBuildSides) {
          testExecutionMode(
              executionMode,
              SpatialPredicate.INTERSECTS,
              joinType,
              indexType,
              indexBuildSide,
              null,
              null);
        }
      }
    }
  }

  private void testExecutionMode(
      ExecutionMode executionMode,
      SpatialPredicate predicate,
      JoinType joinType,
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
        new AdaptiveIndexLookupJudgement<>(predicate, joinType, Collections.singletonList(param));
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(0, datasetA.iterator(), datasetB.iterator());
    verifyResult(resultIterator, datasetA, datasetB, predicate, joinType);
    resultIterator = judgement.call(0, datasetB.iterator(), datasetA.iterator());
    verifyResult(resultIterator, datasetB, datasetA, predicate, joinType);
  }

  private static List<Geometry> generateRandomGeometries(int seed) {
    List<Geometry> geoms = new ArrayList<>();
    Random random = new Random(seed);
    for (int k = 0; k < 1000; k++) {
      double minX = random.nextDouble() * 10;
      double minY = random.nextDouble() * 10;
      double width = random.nextDouble();
      double height = random.nextDouble();
      Geometry geom;
      if (random.nextBoolean()) {
        Envelope env = new Envelope(minX, minX + width, minY, minY + height);
        geom = factory.toGeometry(env);
      } else {
        double centerX = minX + width / 2;
        double centerY = minY + height / 2;
        double radius = 0.5 * (width + height);
        geom = factory.createPoint(new Coordinate(centerX, centerY)).buffer(radius, 2);
      }
      geom.setUserData(new OuterJoinUserData(k, true));
      geoms.add(geom);
    }
    return geoms;
  }

  private static void verifyResult(
      Iterator<Pair<Geometry, Geometry>> resultIterator,
      List<Geometry> datasetA,
      List<Geometry> datasetB,
      SpatialPredicate predicate,
      JoinType joinType) {
    Set<Pair<Geometry, Geometry>> expected = new HashSet<>();
    SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator =
        SpatialPredicateEvaluators.create(predicate);
    if (joinType == JoinType.INNER || joinType == JoinType.LEFT_OUTER) {
      for (Geometry geomA : datasetA) {
        boolean hasResults = false;
        for (Geometry geomB : datasetB) {
          if (evaluator.eval(geomA, geomB)) {
            expected.add(Pair.of(geomA, geomB));
            hasResults = true;
          }
        }
        if (!hasResults && joinType == JoinType.LEFT_OUTER) {
          expected.add(Pair.of(geomA, null));
        }
      }
    } else if (joinType == JoinType.RIGHT_OUTER) {
      for (Geometry geomB : datasetB) {
        boolean hasResults = false;
        for (Geometry geomA : datasetA) {
          if (evaluator.eval(geomA, geomB)) {
            expected.add(Pair.of(geomA, geomB));
            hasResults = true;
          }
        }
        if (!hasResults) {
          expected.add(Pair.of(null, geomB));
        }
      }
    } else {
      throw new UnsupportedOperationException("Unsupported join type: " + joinType);
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
