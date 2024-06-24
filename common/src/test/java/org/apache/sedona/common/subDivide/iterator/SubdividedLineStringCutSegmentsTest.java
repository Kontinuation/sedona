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
package org.apache.sedona.common.subDivide.iterator;

import static org.junit.Assert.*;

import java.util.Iterator;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.operation.linemerge.LineMerger;

public class SubdividedLineStringCutSegmentsTest {
  private final GeometryFactory factory = new GeometryFactory();
  private final SubdivideOptions options = new SubdivideOptions(0.1, 0.1);
  private final double maxSegLength = 0.1;

  @Test
  public void testSplitAtNodes() {
    LineString lineString =
        factory.createLineString(
            new Coordinate[] {
              new Coordinate(1, 3),
              new Coordinate(1.1, 3),
              new Coordinate(1.2, 3),
              new Coordinate(1.3, 3),
            });
    SubdividedLineStringCutSegments iter = new SubdividedLineStringCutSegments(lineString, options);
    verifySubdivided(iter, lineString);
  }

  @Test
  public void testNoSplit() {
    LineString lineString =
        factory.createLineString(
            new Coordinate[] {
              new Coordinate(1.01, 3),
              new Coordinate(1.02, 3),
              new Coordinate(1.03, 3),
              new Coordinate(1.04, 3),
            });
    SubdividedLineStringCutSegments iter = new SubdividedLineStringCutSegments(lineString, options);
    verifySubdivided(iter, lineString);
  }

  @Test
  public void testCutAtSegments() {
    LineString lineString =
        factory.createLineString(
            new Coordinate[] {
              new Coordinate(1, 3),
              new Coordinate(2.1, 3),
              new Coordinate(3.2, 3),
              new Coordinate(4.3, 3),
            });
    SubdividedLineStringCutSegments iter = new SubdividedLineStringCutSegments(lineString, options);
    verifySubdivided(iter, lineString);
  }

  @Test
  public void testCombineMultipleSegments() {
    LineString lineString =
        factory.createLineString(
            new Coordinate[] {
              new Coordinate(1, 3),
              new Coordinate(1.01, 3),
              new Coordinate(1.02, 3),
              new Coordinate(1.1, 3),
              new Coordinate(1.11, 3),
              new Coordinate(1.12, 3),
              new Coordinate(1.13, 3),
              new Coordinate(1.24, 3),
            });
    SubdividedLineStringCutSegments iter = new SubdividedLineStringCutSegments(lineString, options);
    verifySubdivided(iter, lineString);
  }

  @Test
  public void testNonAxisAlignedSegments() {
    LineString lineString =
        factory.createLineString(
            new Coordinate[] {
              new Coordinate(1, 3),
              new Coordinate(1.01, 3.01),
              new Coordinate(1.02, 3.02),
              new Coordinate(1.1, 3.03),
              new Coordinate(1.11, 3.04),
              new Coordinate(1.12, 2.98),
              new Coordinate(1.13, 2.97),
              new Coordinate(1.24, 2.96),
            });
    SubdividedLineStringCutSegments iter = new SubdividedLineStringCutSegments(lineString, options);
    verifySubdivided(iter, lineString);
  }

  private void verifySubdivided(Iterator<Geometry> iter, LineString lineString) {
    LineMerger merger = new LineMerger();
    while (iter.hasNext()) {
      Geometry sub = iter.next();
      assertTrue(sub instanceof LineString);
      merger.add(sub);
      assertTrue(sub.getLength() <= maxSegLength + 1e-10);
    }
    Object[] merged = merger.getMergedLineStrings().toArray();
    LineString mergedLineString = (LineString) merged[0];
    assertEquals(1, merged.length);
    assertTrue(lineString.buffer(1e-10).covers(mergedLineString));
    assertTrue(mergedLineString.buffer(1e-10).covers(lineString));
  }
}
