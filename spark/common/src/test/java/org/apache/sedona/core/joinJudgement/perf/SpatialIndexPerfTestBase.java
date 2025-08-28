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

import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.sedona.core.index.ExternalIndexTestBase;
import org.apache.sedona.core.spatialPartitioning.OuterJoinSpatialPartitioner.OuterJoinUserData;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.SparkEnv;
import org.junit.Before;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.mockito.Mock;

public class SpatialIndexPerfTestBase extends ExternalIndexTestBase {
  protected static final GeometryFactory factory = new GeometryFactory();

  @Mock(answer = RETURNS_SMART_NULLS)
  protected SedonaConf sedonaConf;

  @Mock(answer = RETURNS_SMART_NULLS)
  protected SparkEnv sparkEnv;

  @Before
  public void setUpMock() {
    when(sparkEnv.blockManager()).thenReturn(blockManager);
    when(sedonaConf.useExternalSpatialIndex()).thenReturn(true);
    when(sedonaConf.getExternalSpatialIndexLeafPageCapacity()).thenReturn(10);
    when(sedonaConf.getExternalSpatialIndexInternalNodeCapacity()).thenReturn(10);
    when(sedonaConf.forceSpillExternalSpatialIndex()).thenReturn(false);
  }

  protected static List<Geometry> generateRandomRectangles(int count, long seed, double objSize) {
    List<Geometry> geometries = new ArrayList<>();
    Random random = new Random(seed);
    for (int k = 0; k < count; k++) {
      double minX = random.nextDouble() * 100;
      double minY = random.nextDouble() * 100;
      double width = random.nextDouble() * objSize;
      double height = random.nextDouble() * objSize;
      Envelope env = new Envelope(minX, minX + width, minY, minY + height);
      Geometry geom = factory.toGeometry(env);
      geom.setUserData(new OuterJoinUserData(k, true, k));
      geometries.add(geom);
    }
    return geometries;
  }

  protected static List<Geometry> generateRandomPolygons(
      int count, long seed, double objSize, int segments) {
    List<Geometry> geometries = new ArrayList<>();
    Random random = new Random(seed);
    for (int k = 0; k < count; k++) {
      double centerX = random.nextDouble() * 100;
      double centerY = random.nextDouble() * 100;
      double radius = 0.5 * objSize * random.nextDouble();
      Geometry geom =
          factory.createPoint(new Coordinate(centerX, centerY)).buffer(radius, segments / 4);
      geom.setUserData(new OuterJoinUserData(k, true, k));
      geometries.add(geom);
    }
    return geometries;
  }

  protected static List<Geometry> generateRandomPoints(int count, long seed) {
    List<Geometry> geometries = new ArrayList<>();
    Random random = new Random(seed);
    for (int k = 0; k < count; k++) {
      double x = random.nextDouble() * 100;
      double y = random.nextDouble() * 100;
      Geometry geom = factory.createPoint(new Coordinate(x, y));
      geom.setUserData(new OuterJoinUserData(k, true, k));
      geometries.add(geom);
    }
    return geometries;
  }
}
