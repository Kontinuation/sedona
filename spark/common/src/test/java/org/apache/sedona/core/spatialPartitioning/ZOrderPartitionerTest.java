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

import org.locationtech.jts.geom.*;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Iterator;

import static org.junit.Assert.*;

public class ZOrderPartitionerTest {

    private ZOrderPartitioner partitioner;

    @Before
    public void setUp() {
        Envelope boundary = new Envelope(0, 100, 0, 200);
        IntervalTree tree = new IntervalTree(boundary, 4);
        // insert samples
        tree.insert(new Envelope(10, 10, 20, 10));
        tree.insert(new Envelope(10, 12, 20, 15));
        tree.insert(new Envelope(45, 80, 55, 90));
        tree.insert(new Envelope(46, 82, 57, 89));
        tree.insert(new Envelope(47, 79, 52, 75));
        tree.insert(new Envelope(45, 80, 51, 72));
        tree.insert(new Envelope(49, 88, 50, 75));
        tree.insert(new Envelope(53, 83, 60, 77));
        tree.insert(new Envelope(90, 20, 120, 110));
        tree.insert(new Envelope(95, 90, 150, 190));
        // build the tree
        tree.build(0, 1.0f);
        // create the partitioner
        partitioner = new ZOrderPartitioner(tree);
    }

    /**
     * Test method for {@link ZOrderPartitioner#placeObject(Geometry)}.
     */
    @Test
    public void testPlaceObject() {
        GeometryFactory geometryFactory = new GeometryFactory();

        // case 1: the object is in the ONE range
        Geometry spatialObject = geometryFactory.createPoint(new Coordinate(70, 81));
        Iterator<Tuple2<Integer, Geometry>> result = partitioner.placeObject(spatialObject);
        assertTrue(result.hasNext());
        Tuple2<Integer, Geometry> tuple = result.next();
        assertEquals(3, tuple._1.intValue());
        assertEquals(spatialObject, tuple._2);

        // case 2: the object is in a range that has no samples
        spatialObject = geometryFactory.createPoint(new Coordinate(-10, 20));
        result = partitioner.placeObject(spatialObject);
        assertTrue(result.hasNext());
    }
}
