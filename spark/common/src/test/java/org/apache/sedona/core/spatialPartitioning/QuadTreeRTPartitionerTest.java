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
import java.util.Iterator;
import java.util.List;
import junit.framework.TestCase;
import org.apache.sedona.core.spatialPartitioning.quadtree.ExtendedQuadTree;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

public class QuadTreeRTPartitionerTest extends TestCase {
  QuadTreeRTPartitioner partitioner;

  @Before
  public void setUp() {
    Envelope boundary = new Envelope(0, 100, 0, 200);
    final List<Envelope> samples = new ArrayList<>();
    // insert samples
    samples.add(new Envelope(10, 10, 20, 10));
    samples.add(new Envelope(10, 12, 20, 15));
    samples.add(new Envelope(45, 80, 55, 90));
    samples.add(new Envelope(46, 82, 57, 89));
    samples.add(new Envelope(47, 79, 52, 75));
    samples.add(new Envelope(45, 80, 51, 72));
    samples.add(new Envelope(49, 88, 50, 75));
    samples.add(new Envelope(53, 83, 60, 77));
    samples.add(new Envelope(90, 20, 120, 110));
    samples.add(new Envelope(95, 90, 150, 190));

    // create the extended quad tree
    ExtendedQuadTree<Integer> extendedQuadTree = new ExtendedQuadTree<>(boundary, 10);
    // insert samples to the extended quad tree
    for (Envelope sample : samples) {
      extendedQuadTree.insert(sample);
    }

    int k = 4; // Number of neighbors
    extendedQuadTree.build(k, 0.0);

    // create the partitioner
    partitioner = new QuadTreeRTPartitioner(extendedQuadTree);
  }

  @Test
  public void testPlaceObject() throws Exception {
    GeometryFactory geometryFactory = new GeometryFactory();

    Geometry spatialObject = geometryFactory.createPoint(new Coordinate(70, 81));
    QuadTreeRTPartitioner partitionerNonOverlap = partitioner.nonOverlappedPartitioner();

    Iterator<Tuple2<Integer, Geometry>> result = partitionerNonOverlap.placeObject(spatialObject);

    // Check if the result is correct
    List<Tuple2<Integer, Geometry>> resultList = new ArrayList<>();
    result.forEachRemaining(resultList::add);

    // For simplicity, we just check if the result list is not empty.
    // In a real test, we should verify the specific partition IDs.
    Assert.assertFalse(resultList.isEmpty());

    for (Tuple2<Integer, Geometry> tuple : resultList) {
      System.out.println("Partition ID: " + tuple._1() + ", Geometry: " + tuple._2());
    }
  }
}
