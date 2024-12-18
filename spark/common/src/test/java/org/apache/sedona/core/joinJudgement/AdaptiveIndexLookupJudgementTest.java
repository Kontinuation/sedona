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
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinType;
import org.apache.sedona.core.index.ExternalIndexTestBase;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.IndexBuildSide;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.LocalSpatialJoinExecParams;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.sedona.core.spatialPartitioning.OuterJoinSpatialPartitioner.OuterJoinUserData;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.SparkEnv;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.mockito.Mock;

@RunWith(Parameterized.class)
public class AdaptiveIndexLookupJudgementTest extends ExternalIndexTestBase {

  private static final GeometryFactory factory = new GeometryFactory();
  private static final List<Geometry> datasetA = generateRandomGeometries(1, 1000);
  private static final List<Geometry> datasetB = generateRandomGeometries(2, 1000);

  @Parameterized.Parameters(name = "use external spatial index: {0}")
  public static Collection<Boolean> testParams() {
    return Arrays.asList(false, true);
  }

  private final boolean useExternalSpatialIndex;

  public AdaptiveIndexLookupJudgementTest(boolean useExternalSpatialIndex) {
    this.useExternalSpatialIndex = useExternalSpatialIndex;
  }

  @Mock(answer = RETURNS_SMART_NULLS)
  SedonaConf sedonaConf;

  @Mock(answer = RETURNS_SMART_NULLS)
  SparkEnv sparkEnv;

  @Before
  public void setUpMock() {
    when(sparkEnv.blockManager()).thenReturn(blockManager);
    when(sedonaConf.useExternalSpatialIndex()).thenReturn(true);
    when(sedonaConf.getExternalSpatialIndexLeafPageCapacity()).thenReturn(10);
    when(sedonaConf.getExternalSpatialIndexInternalNodeCapacity()).thenReturn(10);
    when(sedonaConf.forceSpillExternalSpatialIndex()).thenReturn(false);
  }

  private AdaptiveIndexLookupJudgement<Geometry, Geometry> createJudgement(
      SpatialPredicate spatialPredicate,
      JoinType joinType,
      List<LocalSpatialJoinExecParams> localSpatialJoinExecParamsList) {
    if (useExternalSpatialIndex) {
      AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
          new AdaptiveIndexLookupJudgement<>(
              spatialPredicate, joinType, localSpatialJoinExecParamsList, sedonaConf);
      judgement.setSparkEnv(sparkEnv);
      judgement.setTaskContext(taskContext);
      return judgement;
    } else {
      return new AdaptiveIndexLookupJudgement<>(
          spatialPredicate, joinType, localSpatialJoinExecParamsList, null);
    }
  }

  private void cleanUpTaskResource() {
    if (useExternalSpatialIndex) {
      // The external spatial index should be closed when the iterator was drained.
      long consumption = memoryManager.executionMemoryUsed();
      assertEquals(0, consumption);
      taskContext.runTaskCompletionListeners();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testEmptyDatasets() {
    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_STREAM, null);
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        createJudgement(
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
    cleanUpTaskResource();

    // Right side is empty
    resultIterator =
        judgement.call(
            0,
            new SingletonIterator(factory.createPoint(new Coordinate(0, 0))),
            Collections.emptyIterator());
    assertFalse(resultIterator.hasNext());
    cleanUpTaskResource();
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

  @Test
  public void testSpilling() throws IOException {
    if (!useExternalSpatialIndex) {
      return;
    }

    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_STREAM, null, null, null);

    SpatialPredicate predicate = SpatialPredicate.INTERSECTS;
    JoinType joinType = JoinType.RIGHT_OUTER;
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        createJudgement(predicate, joinType, Collections.singletonList(param));
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(0, datasetA.iterator(), datasetB.iterator());

    List<Pair<Geometry, Geometry>> results = new ArrayList<>();
    for (int k = 0; resultIterator.hasNext(); k++) {
      results.add(resultIterator.next());
      if (k == 100) {
        ((ExternalSpatialJoinIterator<Geometry, Geometry>) resultIterator).forceSpill();
      }
    }
    // At least 100 results were generated after spilling
    assertTrue(results.size() > 200);
    verifyResult(results.iterator(), datasetA, datasetB, predicate, joinType);
  }

  @Test
  public void testSpillingWithLimitedMemory() throws IOException {
    if (!useExternalSpatialIndex) {
      return;
    }

    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_STREAM, null, null, null);
    SpatialPredicate predicate = SpatialPredicate.INTERSECTS;
    JoinType joinType = JoinType.RIGHT_OUTER;
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        createJudgement(predicate, joinType, Collections.singletonList(param));

    List<Geometry> datasetBLarge = generateRandomGeometries(2, 200000);
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(0, datasetA.iterator(), datasetBLarge.iterator());

    List<Pair<Geometry, Geometry>> results = new ArrayList<>();
    for (int k = 0; resultIterator.hasNext(); k++) {
      results.add(resultIterator.next());
      if (k == 100) {
        ExternalSpatialJoinIterator<Geometry, Geometry> iter =
            ((ExternalSpatialJoinIterator<Geometry, Geometry>) resultIterator);
        iter.forceSpill();
        iter.setEqualityValidation(true);
        memoryManager.limit(8 * 1024 * 1024);
      }
    }
    // At least 100 results were generated after spilling
    assertTrue(results.size() > 200);
    verifyResult(results.iterator(), datasetA, datasetBLarge, predicate, joinType);
  }

  @Test
  public void testSpillingWithoutEqualityValidation() throws IOException {
    if (!useExternalSpatialIndex) {
      return;
    }

    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_STREAM, null, null, null);
    SpatialPredicate predicate = SpatialPredicate.INTERSECTS;
    JoinType joinType = JoinType.RIGHT_OUTER;
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        createJudgement(predicate, joinType, Collections.singletonList(param));

    List<Geometry> datasetBLarge = generateRandomGeometries(2, 200000);
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(0, datasetA.iterator(), datasetBLarge.iterator());

    List<Pair<Geometry, Geometry>> results = new ArrayList<>();
    for (int k = 0; resultIterator.hasNext(); k++) {
      results.add(resultIterator.next());
      if (k == 100) {
        ExternalSpatialJoinIterator<Geometry, Geometry> iter =
            ((ExternalSpatialJoinIterator<Geometry, Geometry>) resultIterator);
        iter.forceSpill();
        iter.setEqualityValidation(false);
        memoryManager.limit(8 * 1024 * 1024);
      }
    }
    // At least 100 results were generated after spilling
    assertTrue(results.size() > 200);
    verifyResult(results.iterator(), datasetA, datasetBLarge, predicate, joinType);
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
        createJudgement(predicate, joinType, Collections.singletonList(param));
    Iterator<Pair<Geometry, Geometry>> resultIterator =
        judgement.call(0, datasetA.iterator(), datasetB.iterator());
    verifyResult(resultIterator, datasetA, datasetB, predicate, joinType);
    cleanUpTaskResource();
    resultIterator = judgement.call(0, datasetB.iterator(), datasetA.iterator());
    verifyResult(resultIterator, datasetB, datasetA, predicate, joinType);
    cleanUpTaskResource();
  }

  private static List<Geometry> generateRandomGeometries(int seed, int count) {
    List<Geometry> geoms = new ArrayList<>();
    Random random = new Random(seed);
    for (int k = 0; k < count; k++) {
      double minX = random.nextDouble() * 10;
      double minY = random.nextDouble() * 10;
      double width = random.nextDouble() * 0.2;
      double height = random.nextDouble() * 0.2;
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
