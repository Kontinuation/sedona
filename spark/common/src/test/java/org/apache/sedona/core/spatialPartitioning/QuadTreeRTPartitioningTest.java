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

import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.sedona.core.enums.DistanceMetric;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

public class QuadTreeRTPartitioningTest extends TestCase {
  private final GeometryFactory factory = new GeometryFactory();

  @Test
  public void testBuildSTRTree() {

    // Create an artificially skewed data set of identical envelopes
    final Point point = factory.createPoint(new Coordinate(0, 0));

    final List<Envelope> samples = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      samples.add(point.getEnvelopeInternal());
    }

    final Envelope extent = new Envelope(0, 1, 0, 1);

    // Make sure Quad-tree is built successfully without throwing
    // java.lang.StackOverflowError
    QuadtreePartitioning partitioning = new QuadtreePartitioning(samples, extent, 10);
    Assert.assertNotNull(partitioning.getPartitionTree());

    QuadTreeRTPartitioning rtreePartitioning = new QuadTreeRTPartitioning(samples, extent, 10);

    int k = 2; // Number of neighbors
    STRtree strTree = rtreePartitioning.buildSTRTree(samples, k, DistanceMetric.EUCLIDEAN, -1);

    // Check the resulting STR tree
    // Here you can add assertions to validate the behavior
    // For simplicity, let's just print out the STR tree details
    System.out.println("STR tree built successfully:");
    strTree.itemsTree().forEach(item -> System.out.println(item));
  }
}
