/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common.subDivide.iterator;

import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class SubdividedMultiPointPackingTest {
    private final GeometryFactory factory = new GeometryFactory();
    private final SubdivideOptions options = new SubdivideOptions(0.3, 0.3);

    @Test
    public void testSubdivideSinglePoint() {
        MultiPoint multiPoint = factory.createMultiPointFromCoords(new Coordinate[] {new Coordinate(10, 20)});
        SubdividedMultiPointPacking iter = new SubdividedMultiPointPacking(multiPoint, options);
        assertTrue(iter.hasNext());
        Geometry geom = iter.next();
        assertTrue(geom.covers(multiPoint));
        assertTrue(geom.coveredBy(multiPoint));
        assertFalse(iter.hasNext());
    }

    @Test
    public void testSubdivide() {
        Coordinate[] coords = new Coordinate[100];
        for (int i = 0; i < 100; i++) {
            int row = i / 10;
            int col = i % 10;
            double y = row * 0.1;
            double x = col * 0.1;
            coords[i] = new Coordinate(x, y);
        }
        MultiPoint multiPoint = factory.createMultiPointFromCoords(coords);
        SubdividedMultiPointPacking iter = new SubdividedMultiPointPacking(multiPoint, options);
        int count = 0;
        List<Coordinate> subdivided = new ArrayList<>();
        while (iter.hasNext()) {
            Geometry geom = iter.next();
            Envelope envelope = geom.getEnvelopeInternal();
            assertTrue(envelope.getWidth() <= options.maxWidth);
            assertTrue(envelope.getHeight() <= options.maxHeight);
            subdivided.addAll(Arrays.asList(geom.getCoordinates()));
            count += 1;
        }
        assertTrue(count < 20);
        assertEquals(100, subdivided.size());
        MultiPoint combinedGeom = factory.createMultiPointFromCoords(subdivided.toArray(new Coordinate[0]));
        assertTrue(combinedGeom.covers(multiPoint));
        assertTrue(combinedGeom.coveredBy(multiPoint));
    }
}
