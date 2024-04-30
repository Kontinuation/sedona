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
package org.apache.sedona.common.subDivide;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.operation.linemerge.LineMerger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExtentBasedGeometrySubDividerTest {
    private final GeometryFactory factory = new GeometryFactory();
    private final SubdivideOptions options = new SubdivideOptions(0.1, 0.1);
    private final ExtentBasedGeometrySubDivider divider = new ExtentBasedGeometrySubDivider(options);

    @Test
    public void testSubdivideEmpty() {
        Iterator<Geometry> iter = divider.subdivide(factory.createLineString());
        List<Geometry> results = toList(iter);
        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof LineString);
        assertTrue(results.get(0).isEmpty());
    }

    @Test
    public void testSubdividePoint() {
        Iterator<Geometry> iter = divider.subdivide(factory.createPoint(new Coordinate(10, 20)));
        List<Geometry> results = toList(iter);
        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof Point);
        assertEquals(results.get(0).getCoordinate(), new Coordinate(10, 20));
    }

    @Test
    public void testSubdivideMultiPoint() {
        Iterator<Geometry> iter = divider.subdivide(factory.createMultiPointFromCoords(
                new Coordinate[] { new Coordinate(10, 20), new Coordinate(30, 40) }));
        List<Geometry> results = toList(iter);
        assertEquals(2, results.size());
        assertTrue(results.get(0) instanceof Point);
        assertEquals(results.get(0).getCoordinate(), new Coordinate(10, 20));
        assertTrue(results.get(1) instanceof Point);
        assertEquals(results.get(1).getCoordinate(), new Coordinate(30, 40));
    }

    @Test
    public void testSubdivideLineString() {
        Coordinate[] coordinates = { new Coordinate(0, 0), new Coordinate(10, 10) };
        LineString lineString = factory.createLineString(coordinates);
        Iterator<Geometry> iter = divider.subdivide(lineString);
        double length = 0;
        while (iter.hasNext()) {
            Geometry subGeom = iter.next();
            assertTrue(subGeom instanceof LineString);
            assertTrue(lineString.covers(subGeom));
            length += subGeom.getLength();
        }
        assertEquals(lineString.getLength(), length, 1e-10);
    }

    @Test
    public void testSubdivideMultiLineString() {
        Coordinate[] coordinates1 = { new Coordinate(0, 0), new Coordinate(10, 10) };
        Coordinate[] coordinates2 = { new Coordinate(10, 10), new Coordinate(20, 20) };
        LineString lineString1 = factory.createLineString(coordinates1);
        LineString lineString2 = factory.createLineString(coordinates2);
        Iterator<Geometry> iter = divider.subdivide(factory.createMultiLineString(
                new LineString[] { lineString1, lineString2 }));
        double length = 0;
        while (iter.hasNext()) {
            Geometry subGeom = iter.next();
            assertTrue(subGeom instanceof LineString);
            assertTrue(lineString1.covers(subGeom) || lineString2.covers(subGeom));
            length += subGeom.getLength();
        }
        assertEquals(lineString1.getLength() + lineString2.getLength(), length, 1e-10);
    }

    @Test
    public void testSubdividePolygon() {
        Geometry polygon = factory.createPoint(new Coordinate(10, 20)).buffer(3);
        Iterator<Geometry> iter = divider.subdivide(polygon);
        List<Polygon> subGeoms = new ArrayList<>();
        while (iter.hasNext()) {
            subGeoms.add((Polygon) iter.next());
        }
        assertTrue(subGeoms.size() > 4);
        Geometry allSubGeoms = factory.createMultiPolygon(subGeoms.toArray(new Polygon[0])).union();
        assertTrue(polygon.coveredBy(allSubGeoms));
    }

    @Test
    public void testSubdividePolygonWithHoles() {
        Polygon shell = (Polygon) factory.createPoint(new Coordinate(10, 20)).buffer(3);
        Polygon hole = (Polygon) factory.createPoint(new Coordinate(10, 20)).buffer(1);
        Polygon polygonWithHole = factory.createPolygon(shell.getExteriorRing(),
                new LinearRing[] { hole.getExteriorRing() });
        Iterator<Geometry> iter = divider.subdivide(polygonWithHole);
        List<Polygon> subGeoms = new ArrayList<>();
        while (iter.hasNext()) {
            subGeoms.add((Polygon) iter.next());
        }
        Geometry allSubGeoms = factory.createMultiPolygon(subGeoms.toArray(new Polygon[0])).union();
        assertTrue(polygonWithHole.coveredBy(allSubGeoms));
        assertFalse(shell.coveredBy(allSubGeoms));
    }

    @Test
    public void testSubdivideMultiPolygon() {
        Geometry polygon1 = factory.createPoint(new Coordinate(10, 20)).buffer(1);
        Geometry polygon2 = factory.createPoint(new Coordinate(30, 40)).buffer(1);
        Iterator<Geometry> iter = divider.subdivide(factory.createMultiPolygon(new Polygon[] {
                (Polygon) polygon1, (Polygon) polygon2
        }));
        List<Polygon> subGeoms = new ArrayList<>();
        while (iter.hasNext()) {
            subGeoms.add((Polygon) iter.next());
        }
        assertTrue(subGeoms.size() > 4);
        Geometry allSubGeoms = factory.createMultiPolygon(subGeoms.toArray(new Polygon[0])).union();
        assertTrue(polygon1.coveredBy(allSubGeoms) && polygon2.coveredBy(allSubGeoms));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSubdivideGeometryCollection() {
        Geometry lineString = factory.createLineString(new Coordinate[] {
                new Coordinate(0, 0), new Coordinate(10, 10)
        });
        Geometry polygon = factory.createPoint(new Coordinate(10, 20)).buffer(3);
        Geometry multiPolygon = factory.createMultiPolygon(new Polygon[] {
                (Polygon) factory.createPoint(new Coordinate(10, 20)).buffer(1)
        });
        Geometry geometryCollection = factory.createGeometryCollection(new Geometry[] {
                lineString, factory.createLineString(),
                polygon, factory.createPolygon(),
                multiPolygon, factory.createMultiPolygon()
        });
        Iterator<Geometry> iter = divider.subdivide(geometryCollection);
        List<Geometry> subGeoms = new ArrayList<>();
        LineMerger lineMerger = new LineMerger();
        while (iter.hasNext()) {
            Geometry subGeom = iter.next();
            if (subGeom instanceof LineString) {
                lineMerger.add(subGeom);
            } else {
                subGeoms.add(subGeom);
            }
        }
        Geometry allSubGeoms = factory.createGeometryCollection(subGeoms.toArray(new Geometry[0])).union();
        Geometry mergedLineStrings = factory.createMultiLineString(
                (LineString[]) lineMerger.getMergedLineStrings().toArray(new LineString[0]));
        assertTrue(coveredByOneOfSubGeometries(lineString, mergedLineStrings));
        assertTrue(coveredByOneOfSubGeometries(polygon, allSubGeoms));
        assertTrue(coveredByOneOfSubGeometries(multiPolygon, allSubGeoms));
    }

    private boolean coveredByOneOfSubGeometries(Geometry a, Geometry b) {
        for (int k = 0; k < b.getNumGeometries(); k++) {
            if (a.coveredBy(b.getGeometryN(k))) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private List<Geometry> toList(Iterator<Geometry> iter) {
        return (List<Geometry>) IteratorUtils.toList(iter);
    }
}
