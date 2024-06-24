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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

public class SubdividedPolygonUsingBoxesTest {
  private final GeometryFactory factory = new GeometryFactory();
  private final SubdivideOptions options = new SubdivideOptions(0.1, 0.1);

  @Test
  public void testZeroExtent() {
    Polygon polygon =
        factory.createPolygon(
            new Coordinate[] {
              new Coordinate(1, 3),
              new Coordinate(1, 3),
              new Coordinate(1, 3),
              new Coordinate(1, 3),
            });
    SubdividedPolygonUsingBoxes iter = new SubdividedPolygonUsingBoxes(polygon, options);
    assertTrue(iter.hasNext());
    Geometry box = iter.next();
    assertEquals(box.getEnvelopeInternal(), polygon.getEnvelopeInternal());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testConcavePolygon() {
    Polygon polygon =
        factory.createPolygon(
            new Coordinate[] {
              new Coordinate(0, 0),
              new Coordinate(2, 1),
              new Coordinate(1, 0),
              new Coordinate(2, -1),
              new Coordinate(0, 0),
            });
    SubdividedPolygonUsingBoxes iter = new SubdividedPolygonUsingBoxes(polygon, options);
    verifySubdivided(iter, polygon, 400);
  }

  @Test
  public void testPolygonWithHoles() {
    LinearRing shell =
        factory.createLinearRing(
            new Coordinate[] {
              new Coordinate(0, 0),
              new Coordinate(2, 0),
              new Coordinate(2, 2),
              new Coordinate(0, 2),
              new Coordinate(0, 0),
            });
    LinearRing hole =
        factory.createLinearRing(
            new Coordinate[] {
              new Coordinate(0.5, 0.5),
              new Coordinate(1.5, 0.5),
              new Coordinate(1.5, 1.5),
              new Coordinate(0.5, 1.5),
              new Coordinate(0.5, 0.5),
            });
    Polygon polygon = factory.createPolygon(shell, new LinearRing[] {hole});
    SubdividedPolygonUsingBoxes iter = new SubdividedPolygonUsingBoxes(polygon, options);
    verifySubdivided(iter, polygon, 100);
  }

  @Test
  public void testBox() {
    Polygon polygon =
        factory.createPolygon(
            new Coordinate[] {
              new Coordinate(0, 0),
              new Coordinate(2, 0),
              new Coordinate(2, 2),
              new Coordinate(0, 2),
              new Coordinate(0, 0),
            });
    SubdividedPolygonUsingBoxes iter = new SubdividedPolygonUsingBoxes(polygon, options);
    verifySubdivided(iter, polygon, 1);
  }

  @Test
  public void testZeroHeight() {
    Polygon polygon =
        factory.createPolygon(
            new Coordinate[] {
              new Coordinate(0, 0),
              new Coordinate(1, 0),
              new Coordinate(1, 0),
              new Coordinate(0, 0),
            });
    SubdividedPolygonUsingBoxes iter = new SubdividedPolygonUsingBoxes(polygon, options);
    verifySubdivided(
        iter,
        factory.createLineString(
            new Coordinate[] {
              new Coordinate(0, 0), new Coordinate(1, 0),
            }),
        1);
  }

  @Test
  public void testZeroWidth() {
    Polygon polygon =
        factory.createPolygon(
            new Coordinate[] {
              new Coordinate(0, 0),
              new Coordinate(0, 1),
              new Coordinate(0, 1),
              new Coordinate(0, 0),
            });
    SubdividedPolygonUsingBoxes iter = new SubdividedPolygonUsingBoxes(polygon, options);
    verifySubdivided(
        iter,
        factory.createLineString(
            new Coordinate[] {
              new Coordinate(0, 0), new Coordinate(0, 1),
            }),
        1);
  }

  private void verifySubdivided(Iterator<Geometry> iter, Geometry original, int maxSubGeoms) {
    List<Geometry> subGeoms = new ArrayList<>();
    int numSubGeoms = 0;
    while (iter.hasNext()) {
      subGeoms.add(iter.next());
      numSubGeoms++;
      assertTrue(numSubGeoms <= maxSubGeoms);
    }
    Geometry allSubGeoms =
        factory.createGeometryCollection(subGeoms.toArray(new Geometry[0])).union();
    assertTrue(allSubGeoms.covers(original));
  }
}
