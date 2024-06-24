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
import java.util.List;
import org.apache.sedona.core.enums.GridType;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;

public class SpatialPartitionerBuilderTest {

  @Test
  public void buildEqualGrid() {
    SpatialPartitionerBuilder builder =
        new SpatialPartitionerBuilder(GridType.EQUALGRID, 4, 100, new Envelope(0, 100, 0, 100));
    List<Envelope> samples = new ArrayList<>();
    for (int k = -100; k < 200; k++) {
      samples.add(new Envelope(k, k, k, k));
    }
    builder.addSamples(samples);
    SpatialPartitioner partitioner = builder.build();
    assertEquals(4, partitioner.numPartitions());
  }

  @Test
  public void buildQuadTree() {
    SpatialPartitionerBuilder builder =
        new SpatialPartitionerBuilder(GridType.QUADTREE, 4, 100, new Envelope(0, 100, 0, 100));
    List<Envelope> samples = new ArrayList<>();
    for (int k = -100; k < 200; k++) {
      samples.add(new Envelope(k, k, k, k));
    }
    builder.addSamples(samples);
    SpatialPartitioner partitioner = builder.build();
    int numPartitions = partitioner.numPartitions();
    assertTrue(numPartitions >= 4);
  }

  @Test
  public void buildKDBTree() {
    SpatialPartitionerBuilder builder =
        new SpatialPartitionerBuilder(GridType.KDBTREE, 4, 100, new Envelope(0, 100, 0, 100));
    List<Envelope> samples = new ArrayList<>();
    for (int k = -100; k < 200; k++) {
      samples.add(new Envelope(k, k, k, k));
    }
    builder.addSamples(samples);
    SpatialPartitioner partitioner = builder.build();
    assertTrue(partitioner.numPartitions() >= 4);
  }
}
