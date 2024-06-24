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

public class SubdividedLineStringPreserveSegmentsTest {
  private final GeometryFactory factory = new GeometryFactory();
  private final SubdivideOptions options = new SubdivideOptions(0.1, 0.1);

  @Test
  public void testMinimal() {
    LineString lineString =
        factory.createLineString(
            new Coordinate[] {
              new Coordinate(1, 3), new Coordinate(1.03, 3),
            });
    SubdividedLineStringPreserveSegments iter =
        new SubdividedLineStringPreserveSegments(lineString, options);
    verifySubdivided(iter, lineString);
    lineString =
        factory.createLineString(
            new Coordinate[] {
              new Coordinate(1, 3), new Coordinate(2, 3),
            });
    iter = new SubdividedLineStringPreserveSegments(lineString, options);
    verifySubdivided(iter, lineString);
  }

  @Test
  public void testSplit() {
    LineString lineString =
        factory.createLineString(
            new Coordinate[] {
              new Coordinate(1, 3),
              new Coordinate(1.03, 3),
              new Coordinate(1.06, 3),
              new Coordinate(1.09, 3),
              new Coordinate(1.13, 3),
              new Coordinate(1.16, 3),
              new Coordinate(1.19, 3),
              new Coordinate(1.23, 3),
            });
    SubdividedLineStringPreserveSegments iter =
        new SubdividedLineStringPreserveSegments(lineString, options);
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
    SubdividedLineStringPreserveSegments iter =
        new SubdividedLineStringPreserveSegments(lineString, options);
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
    SubdividedLineStringPreserveSegments iter =
        new SubdividedLineStringPreserveSegments(lineString, options);
    verifySubdivided(iter, lineString);
  }

  private void verifySubdivided(Iterator<Geometry> iter, LineString lineString) {
    LineMerger merger = new LineMerger();
    while (iter.hasNext()) {
      Geometry sub = iter.next();
      assertTrue(sub instanceof LineString);
      merger.add(sub);
      Coordinate[] coords = sub.getCoordinates();
      assertTrue(coords.length >= 2);
      for (int k = 0; k < coords.length - 1; k++) {
        assertNotEquals(coords[k], coords[k + 1]);
      }
    }
    Object[] merged = merger.getMergedLineStrings().toArray();
    LineString mergedLineString = (LineString) merged[0];
    assertEquals(1, merged.length);
    assertTrue(lineString.covers(mergedLineString));
    assertTrue(mergedLineString.covers(lineString));
  }
}
