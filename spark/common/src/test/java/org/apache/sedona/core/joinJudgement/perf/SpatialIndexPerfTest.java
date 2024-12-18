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
package org.apache.sedona.core.joinJudgement.perf;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinType;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.IndexBuildSide;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.LocalSpatialJoinExecParams;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

/** Performance test for spatial index based join using randomly generated data. */
@Ignore
public class SpatialIndexPerfTest extends SpatialIndexPerfTestBase {

  // 1m rectangular polygons, measured size: 492.86MB (492 bytes per geometry)
  private static final List<Geometry> rectangle1m = generateRandomRectangles(1_000_000, 0, 0.2);

  // 1m circle polygons, estimated size: 1GB (1024 bytes per geometry)
  private static final List<Geometry> polygon1m = generateRandomPolygons(1_000_000, 0, 0.2, 16);

  // 1m points, measured size: 212.86MB (212 bytes per geometry)
  private static final List<Geometry> point1m = generateRandomPoints(1_000_000, 1);

  @Test
  public void inMemRectanglePoint() {
    perfInMemorySpatialIndex(rectangle1m, point1m);
  }

  @Test
  public void externalNoSpillingRectanglePoint() {
    perfExternalSpatialIndexNoSpilling(rectangle1m, point1m);
  }

  @Test
  public void inMemPolygonPoint() {
    perfInMemorySpatialIndex(polygon1m, point1m);
  }

  @Test
  public void externalNoSpillingPolygonPoint() {
    perfExternalSpatialIndexNoSpilling(polygon1m, point1m);
  }

  private void perfInMemorySpatialIndex(List<Geometry> left, List<Geometry> right) {
    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_BUILD, null, null, null);
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        new AdaptiveIndexLookupJudgement<>(
            SpatialPredicate.INTERSECTS, JoinType.INNER, Collections.singletonList(param), null);

    for (int i = 0; i < 10; i++) {
      int count = 0;
      long start = System.nanoTime();
      Iterator<Pair<Geometry, Geometry>> results =
          judgement.call(0, left.iterator(), right.iterator());
      while (results.hasNext()) {
        results.next();
        count += 1;
      }
      long end = System.nanoTime();
      System.out.printf("Time taken: %f seconds, count: %d\n", (end - start) / 1e9, count);
    }
  }

  private void perfExternalSpatialIndexNoSpilling(List<Geometry> left, List<Geometry> right) {
    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_BUILD, null, null, null);
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        new AdaptiveIndexLookupJudgement<>(
            SpatialPredicate.INTERSECTS,
            JoinType.INNER,
            Collections.singletonList(param),
            sedonaConf);
    judgement.setSparkEnv(sparkEnv);
    judgement.setTaskContext(taskContext);

    for (int i = 0; i < 10; i++) {
      int count = 0;
      long start = System.nanoTime();
      Iterator<Pair<Geometry, Geometry>> results =
          judgement.call(0, left.iterator(), right.iterator());
      while (results.hasNext()) {
        results.next();
        count += 1;
      }
      long end = System.nanoTime();
      System.out.printf("Time taken: %f seconds, count: %d\n", (end - start) / 1e9, count);
      taskContext.runTaskCompletionListeners();
    }
  }
}
