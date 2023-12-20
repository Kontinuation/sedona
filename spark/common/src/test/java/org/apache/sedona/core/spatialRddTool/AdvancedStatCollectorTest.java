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
package org.apache.sedona.core.spatialRddTool;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class AdvancedStatCollectorTest {
    private static final GeometryFactory factory = new GeometryFactory();

    @Test
    public void construct() {
        AdvancedStatCollector stat = new AdvancedStatCollector(1);
        assertEquals(0, stat.getCount());
        assertTrue(stat.getBoundary().isNull());
        assertEquals(0, stat.getPuntalCount());
        assertEquals(0, stat.getLinealCount());
        assertEquals(0, stat.getPolygonalCount());
        assertEquals(0, stat.getGeometryCollectionCount());
        assertEquals(0, stat.getEstimatedSizeInBytes());
        assertEquals(0, stat.getMeanNumPoints(), 1e-10);
        assertEquals(0, stat.getMeanEnvelopeWidth(), 1e-10);
        assertEquals(0, stat.getMeanEnvelopeHeight(), 1e-10);
        assertEquals(0, stat.getMeanEnvelopeArea(), 1e-10);
        assertTrue(stat.getSampledEnvelopes().isEmpty());
    }

    @Test
    public void update() {
        AdvancedStatCollector stat = new AdvancedStatCollector(1);
        Geometry geom = factory.createPolygon(new Coordinate[] {
                new Coordinate(0, 0),
                new Coordinate(0, 1),
                new Coordinate(1, 1),
                new Coordinate(1, 0),
                new Coordinate(0, 0)
        });
        stat.update(geom);
        assertEquals(1, stat.getCount());
        assertEquals(geom.getEnvelopeInternal(), stat.getBoundary());
        assertEquals(0, stat.getPuntalCount());
        assertEquals(0, stat.getLinealCount());
        assertEquals(1, stat.getPolygonalCount());
        assertEquals(0, stat.getGeometryCollectionCount());
        assertTrue(stat.getEstimatedSizeInBytes() > 64);
        assertEquals(5, stat.getMeanNumPoints(), 1e-10);
        assertEquals(1, stat.getMeanEnvelopeWidth(), 1e-10);
        assertEquals(1, stat.getMeanEnvelopeHeight(), 1e-10);
        assertEquals(1, stat.getMeanEnvelopeArea(), 1e-10);

        geom = factory.createPoint(new Coordinate(5, 10));
        stat.update(geom);
        assertEquals(2, stat.getCount());
        assertEquals(new Envelope(0, 5, 0, 10), stat.getBoundary());
        assertEquals(1, stat.getPuntalCount());
        assertEquals(0, stat.getLinealCount());
        assertEquals(1, stat.getPolygonalCount());
    }

    @Test
    public void updateShouldSampleCorrectly() {
        AdvancedStatCollector stat = new AdvancedStatCollector(100, 1000, 0.01, 1.2, false, 1);
        Random random = new Random(1);
        Envelope boundary = new Envelope();
        for (int x = 0; x < 100; x++) {
            for (int y = 0; y < 100; y++) {
                double width = random.nextDouble() * 10;
                double height = random.nextDouble() * 10;
                int numBytes = 1000 + random.nextInt(100);
                Envelope env = new Envelope(x, x + width, y, y + height);
                boundary.expandToInclude(env);
                Geometry geom = factory.toGeometry(env);
                geom.setUserData(new byte[numBytes]);
                stat.update(geom);
            }
        }

        // Check collected statistics
        assertEquals(10000, stat.getCount());
        assertEquals(boundary, stat.getBoundary());
        assertTrue(stat.getEstimatedSizeInBytes() > 1000);
        assertTrue(stat.getEstimatedSizeInBytes() < 2000);
        assertTrue(stat.getNumEstimatedGeometries() >= 30);
        assertTrue(stat.getNumEstimatedGeometries() <= 60);
        assertEquals(5, stat.getMeanNumPoints(), 1e-10);
        assertTrue(Math.abs(stat.getMeanEnvelopeWidth() - 5) < 1);
        assertTrue(Math.abs(stat.getMeanEnvelopeHeight() - 5) < 1);
        assertTrue(Math.abs(stat.getMeanEnvelopeArea() - 25) < 1);

        // Test if sampled envelopes are distributed evenly
        List<Envelope> sampledEnvelopes = stat.getSampledEnvelopes();
        assertEquals(100, sampledEnvelopes.size());
        int numLeftPlane = 0;
        int numRightPlane = 0;
        for (Envelope sampledEnvelope : sampledEnvelopes) {
            if (sampledEnvelope.getMinX() <= 50) {
                numLeftPlane += 1;
            }
            else if (sampledEnvelope.getMinX() > 50) {
                numRightPlane += 1;
            }
        }
        assertTrue(Math.abs(numLeftPlane - numRightPlane) < 30);
    }

    @Test
    public void updateShouldWorkWithAllStagesOfSampling() {
        AdvancedStatCollector stat = new AdvancedStatCollector(100, 200, 0.01, 1.2, false, 1);
        Random random = new Random(1);
        int count = 0;

        // Stage 1
        for ( ; count < 100; count++) {
            stat.update(factory.createPoint(new Coordinate(random.nextDouble(), random.nextDouble())));
        }
        assertEquals(100, stat.getSampledEnvelopes().size());

        // Stage 2
        for ( ; count < 10000; count++) {
            stat.update(factory.createPoint(new Coordinate(random.nextDouble(), random.nextDouble())));
        }
        assertEquals(100, stat.getSampledEnvelopes().size());

        // Stage 3
        for ( ; count < 15000; count++) {
            stat.update(factory.createPoint(new Coordinate(random.nextDouble(), random.nextDouble())));
        }
        assertTrue(Math.abs(stat.getSampledEnvelopes().size() - 150) < 10);

        for ( ; count < 20000; count++) {
            stat.update(factory.createPoint(new Coordinate(random.nextDouble(), random.nextDouble())));
        }
        assertTrue(Math.abs(stat.getSampledEnvelopes().size() - 200) < 10);

        // Stage 4
        for ( ; count < 40000; count++) {
            stat.update(factory.createPoint(new Coordinate(-random.nextDouble(), random.nextDouble())));
        }
        assertEquals(200, stat.getSampledEnvelopes().size());

        // Should sample evenly
        List<Envelope> sampledEnvelopes = stat.getSampledEnvelopes();
        long leftPlaneCount = sampledEnvelopes.stream().filter(e -> e.getMinX() < 0).count();
        long rightPlaneCount = sampledEnvelopes.stream().filter(e -> e.getMinX() > 0).count();
        assertTrue(Math.abs(leftPlaneCount - rightPlaneCount) < 20);
    }

    @Test
    public void combineEmpty() {
        AdvancedStatCollector stat = new AdvancedStatCollector(1);
        AdvancedStatCollector stat1 = new AdvancedStatCollector(2);
        stat.combineWith(stat1);
        assertEquals(0, stat.getCount());
        assertTrue(stat.getBoundary().isNull());
        assertEquals(0, stat.getPuntalCount());
        assertEquals(0, stat.getLinealCount());
        assertEquals(0, stat.getPolygonalCount());
        assertEquals(0, stat.getGeometryCollectionCount());
        assertEquals(0, stat.getEstimatedSizeInBytes());
        assertEquals(0, stat.getMeanNumPoints(), 1e-10);
        assertEquals(0, stat.getMeanEnvelopeWidth(), 1e-10);
        assertEquals(0, stat.getMeanEnvelopeHeight(), 1e-10);
        assertEquals(0, stat.getMeanEnvelopeArea(), 1e-10);
        assertTrue(stat.getSampledEnvelopes().isEmpty());
    }

    @Test
    public void combineWith() {
        Random random = new Random(1);

        // Collect statistics for points on the 1st quadrant
        AdvancedStatCollector stat = new AdvancedStatCollector(100, 200, 0.01, 1.2, false, 1);
        for (int k = 0; k < 10000; k++) {
            Geometry geom = factory.createPoint(new Coordinate(1e-6 + random.nextDouble(), 1e-6 + random.nextDouble()));
            stat.update(geom);
        }

        // Collect statistics for points on the 2nd quadrant
        AdvancedStatCollector statQ2 = new AdvancedStatCollector(100, 200, 0.01, 1.2, false, 1);
        for (int k = 0; k < 10000; k++) {
            Geometry geom = factory.createPoint(new Coordinate(-1e-6 - random.nextDouble(), 1e-6 + random.nextDouble()));
            statQ2.update(geom);
        }

        stat.combineWith(statQ2);
        assertEquals(20000, stat.getCount());
        assertEquals(20000, stat.getPuntalCount());
        assertTrue(Math.abs(stat.getBoundary().getMinX() + 1) < 0.1);
        assertTrue(Math.abs(stat.getBoundary().getMaxX() - 1) < 0.1);
        assertTrue(Math.abs(stat.getEstimatedSizeInBytes() - 248) < 10);
        assertTrue(Math.abs(stat.getMeanNumPoints() - 1) < 0.1);
        List<Envelope> sampledEnvelopes = stat.getSampledEnvelopes();
        assertEquals(200, sampledEnvelopes.size());
        assertEquals(100, sampledEnvelopes.stream().filter(e -> e.getMinX() < 0).count());
        assertEquals(100, sampledEnvelopes.stream().filter(e -> e.getMinX() > 0).count());
    }

    @Test
    public void combineEmptyWithNonEmpty() {
        combineEmptyWithNonEmpty(true);
        combineEmptyWithNonEmpty(false);
    }

    private void combineEmptyWithNonEmpty(boolean combineNonEmptyIntoEmpty) {
        Random random = new Random(1);

        AdvancedStatCollector emptyStat = new AdvancedStatCollector(100, 200, 0.01, 1.2, false, 1);
        AdvancedStatCollector nonEmptyStat = new AdvancedStatCollector(100, 200, 0.01, 1.2, false, 1);
        for (int k = 0; k < 100; k++) {
            Geometry geom = factory.createPoint(new Coordinate(1e-6 + random.nextDouble(), 1e-6 + random.nextDouble()));
            nonEmptyStat.update(geom);
        }

        AdvancedStatCollector stat;
        if (combineNonEmptyIntoEmpty) {
            stat = emptyStat;
            stat.combineWith(nonEmptyStat);
        } else {
            stat = nonEmptyStat;
            stat.combineWith(emptyStat);
        }

        assertEquals(100, stat.getCount());
        assertEquals(nonEmptyStat.getBoundary(), stat.getBoundary());
        assertEquals(100, stat.getPuntalCount());
        assertEquals(0, stat.getLinealCount());
        assertEquals(0, stat.getPolygonalCount());
        assertEquals(0, stat.getGeometryCollectionCount());
        assertTrue(Math.abs(stat.getEstimatedSizeInBytes() - 248) < 10);
        assertEquals(1, stat.getMeanNumPoints(), 1e-10);
        assertEquals(0, stat.getMeanEnvelopeWidth(), 1e-10);
        assertEquals(0, stat.getMeanEnvelopeHeight(), 1e-10);
        assertEquals(0, stat.getMeanEnvelopeArea(), 1e-10);
        assertEquals(nonEmptyStat.getSampledEnvelopes(), stat.getSampledEnvelopes());
    }

    @Test
    public void combineDifferentSamplingRate() {
        combineDifferentSamplingRate(true);
        combineDifferentSamplingRate(false);
    }

    private void combineDifferentSamplingRate(boolean combineQ2IntoQ1) {
        Random random = new Random(1);

        // Collect statistics for points on the 1st quadrant
        AdvancedStatCollector stat = new AdvancedStatCollector(100, 200, 0.01, 1.2, false, 1);
        for (int k = 0; k < 10000; k++) {
            Geometry geom = factory.createPoint(new Coordinate(1e-6 + random.nextDouble(), 1e-6 + random.nextDouble()));
            stat.update(geom);
        }

        // Collect statistics for points on the 2nd quadrant
        AdvancedStatCollector statQ2 = new AdvancedStatCollector(100, 200, 0.01, 1.2, false, 1);
        for (int k = 0; k < 5000; k++) {
            Geometry geom = factory.createPoint(new Coordinate(-1e-6 - random.nextDouble(), 1e-6 + random.nextDouble()));
            statQ2.update(geom);
        }

        if (combineQ2IntoQ1) {
            stat.combineWith(statQ2);
        } else {
            statQ2.combineWith(stat);
            stat = statQ2;
        }

        assertEquals(15000, stat.getCount());
        assertEquals(15000, stat.getPuntalCount());
        List<Envelope> sampledEnvelopes = stat.getSampledEnvelopes();
        assertTrue(Math.abs(sampledEnvelopes.size() - 150) < 10);
        long q1Count = sampledEnvelopes.stream().filter(e -> e.getMinX() > 0).count();
        long q2Count = sampledEnvelopes.stream().filter(e -> e.getMinX() < 0).count();
        assertEquals(100, q1Count);
        assertTrue(Math.abs(q2Count - 50) < 10);
    }

    @Test
    public void combineExceedingMaxCombinedSamples() {
        Random random = new Random(1);

        // Collect statistics for points on the 1st quadrant
        AdvancedStatCollector stat = new AdvancedStatCollector(100, 120, 0.01, 1.2, false, 1);
        for (int k = 0; k < 10000; k++) {
            Geometry geom = factory.createPoint(new Coordinate(1e-6 + random.nextDouble(), 1e-6 + random.nextDouble()));
            stat.update(geom);
        }

        // Collect statistics for points on the 2nd quadrant
        AdvancedStatCollector statQ2 = new AdvancedStatCollector(100, 120, 0.01, 1.2, false, 1);
        for (int k = 0; k < 5000; k++) {
            Geometry geom = factory.createPoint(new Coordinate(-1e-6 - random.nextDouble(), 1e-6 + random.nextDouble()));
            statQ2.update(geom);
        }

        stat.combineWith(statQ2);
        assertEquals(15000, stat.getCount());
        assertEquals(15000, stat.getPuntalCount());
        List<Envelope> sampledEnvelopes = stat.getSampledEnvelopes();
        assertTrue(Math.abs(sampledEnvelopes.size() - 120) < 10);

        // Expects to sample 80 from Q1 and 40 from Q2
        long q1Count = sampledEnvelopes.stream().filter(e -> e.getMinX() > 0).count();
        long q2Count = sampledEnvelopes.stream().filter(e -> e.getMinX() < 0).count();
        assertTrue(Math.abs(q1Count - 80) < 10);
        assertTrue(Math.abs(q2Count - 40) < 10);
    }
}
