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
package org.apache.sedona.core.spatialPartitioning;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections.IteratorUtils;
import org.apache.sedona.common.geometryObjects.NullGeometry;
import org.apache.sedona.core.enums.GridType;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

public class OuterJoinSpatialPartitionerTest {

  @Test
  public void testPlacingGeometries() throws Exception {
    SpatialPartitioner partitioner = buildPartitioner();
    OuterJoinSpatialPartitioner outerJoinSpatialPartitioner =
        new OuterJoinSpatialPartitioner(partitioner, 4, true);
    assertEquals(partitioner.numPartitions() + 4, outerJoinSpatialPartitioner.numPartitions());

    // Placing geometries that are within the partitioned space
    GeometryFactory factory = new GeometryFactory();
    Geometry geometry = factory.createPoint(new Coordinate(50, 50));
    Iterator<Tuple2<Integer, Geometry>> iter = outerJoinSpatialPartitioner.placeObject(geometry);
    Iterator<Tuple2<Integer, Geometry>> iter2 = partitioner.placeObject(geometry);
    assertIteratorEquals(iter2, iter);
    geometry = factory.toGeometry(new Envelope(90, 110, 90, 110));
    iter = outerJoinSpatialPartitioner.placeObject(geometry);
    iter2 = partitioner.placeObject(geometry);
    assertIteratorEquals(iter2, iter);

    // Placing geometries that are out of the partitioned space
    geometry = factory.createPoint(new Coordinate(150, 150));
    iter = outerJoinSpatialPartitioner.placeObject(geometry);
    assertInOutOfBoundsPartition(iter, partitioner.numPartitions());

    // Place empty geometry
    geometry = factory.createPoint();
    iter = outerJoinSpatialPartitioner.placeObject(geometry);
    assertInOutOfBoundsPartition(iter, partitioner.numPartitions());

    // Place null geometry
    iter = outerJoinSpatialPartitioner.placeObject(new NullGeometry());
    assertInOutOfBoundsPartition(iter, partitioner.numPartitions());
  }

  @SuppressWarnings("unchecked")
  private void assertIteratorEquals(
      Iterator<Tuple2<Integer, Geometry>> iter1, Iterator<Tuple2<Integer, Geometry>> iter2) {
    List<Tuple2<Integer, Geometry>> list1 = IteratorUtils.toList(iter1);
    List<Tuple2<Integer, Geometry>> list2 = IteratorUtils.toList(iter2);
    assertEquals(list1, list2);
  }

  private void assertInOutOfBoundsPartition(
      Iterator<Tuple2<Integer, Geometry>> iter, int baseOutOfBoundsPartitionId) {
    assertTrue(iter.hasNext());
    Tuple2<Integer, Geometry> tuple = iter.next();
    assertTrue(tuple._1 >= baseOutOfBoundsPartitionId);
    assertFalse(iter.hasNext());
  }

  @Test
  public void getDedupParams() {
    SpatialPartitioner partitioner = buildPartitioner();
    OuterJoinSpatialPartitioner outerJoinSpatialPartitioner =
        new OuterJoinSpatialPartitioner(partitioner, 4, true);
    assertEquals(partitioner.getDedupParams(), outerJoinSpatialPartitioner.getDedupParams());
  }

  @Test
  public void getGrids() {
    SpatialPartitioner partitioner = buildPartitioner();
    OuterJoinSpatialPartitioner outerJoinSpatialPartitioner =
        new OuterJoinSpatialPartitioner(partitioner, 4, true);
    assertEquals(partitioner.getGrids(), outerJoinSpatialPartitioner.getGrids());
  }

  @Test
  public void compatibleWith() {
    SpatialPartitioner partitioner = buildPartitioner();
    OuterJoinSpatialPartitioner outerJoinSpatialPartitioner =
        new OuterJoinSpatialPartitioner(partitioner, 4, true);
    OuterJoinSpatialPartitioner outerJoinSpatialPartitioner2 =
        new OuterJoinSpatialPartitioner(partitioner, 4, false);
    SpatialPartitioner partitioner2 = buildPartitioner();
    OuterJoinSpatialPartitioner outerJoinSpatialPartitioner3 =
        new OuterJoinSpatialPartitioner(partitioner2, 4, false);
    assertTrue(outerJoinSpatialPartitioner.compatibleWith(outerJoinSpatialPartitioner2));
    assertFalse(outerJoinSpatialPartitioner.compatibleWith(outerJoinSpatialPartitioner3));
  }

  private SpatialPartitioner buildPartitioner() {
    List<Envelope> samples = new ArrayList<>();
    for (int k = 0; k < 100; k++) {
      double x = Math.random() * 100;
      double y = Math.random() * 100;
      samples.add(new Envelope(x, x, y, y));
    }
    Envelope boundary = new Envelope(0, 100, 0, 100);
    SpatialPartitionerBuilder builder =
        new SpatialPartitionerBuilder(GridType.KDBTREE, 5, 100, boundary);
    builder.addSamples(samples);
    return builder.build();
  }
}
