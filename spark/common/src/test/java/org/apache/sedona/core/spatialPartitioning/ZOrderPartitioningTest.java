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

import junit.framework.TestCase;
import org.apache.commons.lang3.Range;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertThrows;

public class ZOrderPartitioningTest extends TestCase {

    class SampleWithZOrder<T> {
        private T sample;
        private long zOrder;

        public SampleWithZOrder(T sample, long zOrder) {
            this.sample = sample;
            this.zOrder = zOrder;
        }

        public T getSample() {
            return sample;
        }

        public long getZOrder() {
            return zOrder;
        }
    }

    @Test
    public void testCalculateZOrder() {
        ZOrderPartitioning zOrderPartitioning = new ZOrderPartitioning(new Envelope(-20, 30, -100, 100), 4);

        long zOrder = zOrderPartitioning.calculateZOrder(new Coordinate(-10, -80));
        assertEquals(-9255005095641088L, zOrder);

        zOrder = zOrderPartitioning.calculateZOrder(new Coordinate(10, 20));
        assertEquals(645698030026752L, zOrder);

        zOrder = zOrderPartitioning.calculateZOrder(new Coordinate(20, 70));
        assertEquals(9303659488968704L, zOrder);

        // Test edge cases where the point is the minimal of the boundary
        zOrder = zOrderPartitioning.calculateZOrder(new Coordinate(-20, -100));
        assertEquals(-10044191934578688L, zOrder);

        // Test edge cases where the point is the maximal of the boundary
        zOrder = zOrderPartitioning.calculateZOrder(new Coordinate(30, 100));
        assertEquals(10126931454803968L, zOrder);
    }

    /**
     * Test the case where the number of partitions is less than the number of samples.
     * The number of ranges should match the number of partitions.
     */
    @Test
    public void testCreateZOrderRangesLessPartitions() {
        ZOrderPartitioning zOrderPartitioning = new ZOrderPartitioning(new Envelope(-20, 30, -100, 100), 2);

        List<Envelope> samples = Arrays.asList(
                new Envelope(10, 20, 10, 20),
                new Envelope(20, 20, 20, 70),
                new Envelope(21, 20, 20, 70),
                new Envelope(-10, 20, -20, 10)
        );
        List<Range<Long>> ranges = zOrderPartitioning.createZOrderRanges(samples, 0);
        assertEquals("Number of ranges should match the requested number.", 2, ranges.size());
    }

    /**
     * Test the case where the number of partitions is more than the number of samples.
     * The number of ranges should match the number of samples.
     */
    @Test
    public void testCreateZOrderRangesLessSamples() {
        ZOrderPartitioning zOrderPartitioning = new ZOrderPartitioning(new Envelope(-20, 30, -100, 100), 4);

        List<Envelope> samples = Arrays.asList(
                new Envelope(10, 20, 10, 20)
        );
        List<Range<Long>> ranges = zOrderPartitioning.createZOrderRanges(samples,0);
        assertEquals("Number of ranges should match the requested number.", 1, ranges.size());
    }

    /**
     * Test the case where samples are empty.
     * The number of ranges should be 1 (including the whole boundary).
     */
    @Test
    public void testCreateZOrderRangesEmptySamples() {
        ZOrderPartitioning zOrderPartitioning = new ZOrderPartitioning(new Envelope(-20, 30, -100, 100), 4);

        List<Envelope> samples = Arrays.asList();
        List<Range<Long>> ranges = zOrderPartitioning.createZOrderRanges(samples,0);
        assertEquals("Number of ranges should match the requested number.", 1, ranges.size());
    }

    /**
     * Test the case where partition is set to 0.
     * The method should throw an IllegalArgumentException.
     */
    @Test
    public void testCreateZOrderRangesInvalidPartitions() {
        ZOrderPartitioning zOrderPartitioning = new ZOrderPartitioning(new Envelope(-20, 30, -100, 100), 0);

        List<Envelope> samples = Arrays.asList();
        assertThrows(IllegalArgumentException.class, () -> {
            zOrderPartitioning.createZOrderRanges(samples,0);
        });
    }

    @Test
    public void testCreateZOrderRangesEqualPartitions() {
        Envelope boundary = new Envelope(0, 7.01, 0, 4.01);
        int numPartitions = 4;
        ZOrderPartitioning zOrderPartitioning = new ZOrderPartitioning(boundary, numPartitions);

        GeometryFactory factory = new GeometryFactory();
        List<Envelope> samples = Arrays.asList(
                factory.createPoint(new Coordinate(0, 0)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(0, 1)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(0, 2)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(0, 3)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(0, 4)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(1, 0)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(1, 1)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(1, 2)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(1, 3)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(1, 4)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(2, 0)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(2, 1)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(2, 2)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(2, 3)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(2, 4)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(3, 0)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(3, 1)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(3, 2)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(3, 3)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(3, 4)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(4, 0)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(4, 1)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(4, 2)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(4, 3)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(4, 4)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(5, 0)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(5, 1)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(5, 2)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(5, 3)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(5, 4)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(6, 0)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(6, 1)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(6, 2)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(6, 3)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(6, 4)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(7, 0)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(7, 1)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(7, 2)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(7, 3)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(7, 4)).getEnvelopeInternal()
        );

        List<SampleWithZOrder<Envelope>> orderredSamples = samples.stream()
                .map(sample -> new SampleWithZOrder<>(sample, zOrderPartitioning.calculateZOrder(sample)))
                .sorted((a, b) -> Long.compare(a.getZOrder(), b.getZOrder()))
                .collect(Collectors.toList());

        assertEquals("Number of ordered sample should match the requested number.", samples.size(), orderredSamples.size());

        IntervalTree tree = new IntervalTree(boundary, numPartitions);
        for (Envelope sample : samples) {
            tree.insert(sample);
        }
        tree.build(numPartitions, 1.0f);
        Iterator<Tuple2<Integer, Geometry>> iterator1 = tree.placeObject(factory.createPoint(new Coordinate(3, 3)));

        IntervalTree nonOverlappedTree = new IntervalTree(tree, true);
        nonOverlappedTree.placeObject(factory.createPoint(new Coordinate(3.3, 4.4)));
        Iterator<Tuple2<Integer, Geometry>> iterator2 = nonOverlappedTree.placeObject(factory.createPoint(new Coordinate(3.3, 4.4)));

        assertEquals("Number of ranges should match the requested number.", 4, tree.getPartitionNum());
    }

    @Test
    public void testCreateZOrderRangesCase1() {
        Envelope boundary = new Envelope(0, 7.01, 0, 4.01);
        int numPartitions = 4;
        ZOrderPartitioning zOrderPartitioning = new ZOrderPartitioning(boundary, numPartitions);

        GeometryFactory factory = new GeometryFactory();
        List<Envelope> samples = Arrays.asList(
                // Hot spot 1
                factory.createPoint(new Coordinate(10.12, 15.33)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(10.22, 15.34)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(10.32, 15.25)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(10.42, 15.78)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(10.52, 15.54)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(10.45, 15.33)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(10.36, 15.33)).getEnvelopeInternal(),
                // Hot spot 2
                factory.createPoint(new Coordinate(15.12, 25.33)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(15.22, 25.34)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(15.32, 25.25)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(15.42, 25.78)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(15.52, 25.54)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(15.45, 25.33)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(15.36, 25.33)).getEnvelopeInternal(),
                // Hot spot 3
                factory.createPoint(new Coordinate(16.12, 25.33)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(16.22, 25.34)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(16.32, 25.25)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(16.42, 25.78)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(16.52, 25.54)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(16.45, 25.33)).getEnvelopeInternal(),
                factory.createPoint(new Coordinate(16.36, 25.33)).getEnvelopeInternal()
        );

        List<SampleWithZOrder<Envelope>> orderredSamples = samples.stream()
                .map(sample -> new SampleWithZOrder<>(sample, zOrderPartitioning.calculateZOrder(sample)))
                .sorted((a, b) -> Long.compare(a.getZOrder(), b.getZOrder()))
                .collect(Collectors.toList());

        assertEquals("Number of ordered sample should match the requested number.", samples.size(), orderredSamples.size());
    }

    @Test
    public void testCreateZOrderRangesWithHotspots() {
        ZOrderPartitioning zOrderPartitioning = new ZOrderPartitioning(new Envelope(-20, 30, -100, 100), 8);

        List<Envelope> samples = generateHotspotEnvelopes(100);
        List<Range<Long>> ranges = zOrderPartitioning.createZOrderRanges(samples,0);

        assertEquals("Number of ranges should match the requested number.", 8, ranges.size());

        // print ranges for debugging
        for (Range<Long> range : ranges) {
            System.out.println(range);
        }

        assertEquals(Long.MIN_VALUE, (long) ranges.get(0).getMinimum());
        assertEquals(-9213978661312649L, (long) ranges.get(0).getMaximum());

        assertEquals(-9213978661312648L, (long) ranges.get(1).getMinimum());
        assertEquals(-3044712793522252L, (long) ranges.get(1).getMaximum());

        assertEquals(-3044712793522251L, (long) ranges.get(2).getMinimum());
        assertEquals(645169553216621L, (long) ranges.get(2).getMaximum());

        assertEquals(645169553216622L, (long) ranges.get(3).getMinimum());
        assertEquals(830625133002065L, (long) ranges.get(3).getMaximum());

        assertEquals(830625133002066L, (long) ranges.get(4).getMinimum());
        assertEquals(2677608172805488L, (long) ranges.get(4).getMaximum());

        assertEquals(2677608172805489L, (long) ranges.get(5).getMinimum());
        assertEquals(9403705122164405L, (long) ranges.get(5).getMaximum());

        assertEquals(9403705122164406L, (long) ranges.get(6).getMinimum());
        assertEquals(9880050638048616L, (long) ranges.get(6).getMaximum());

        assertEquals(9880050638048617L, (long) ranges.get(7).getMinimum());
        assertEquals(Long.MAX_VALUE, (long) ranges.get(7).getMaximum());
    }

    public static List<Envelope> generateHotspotEnvelopes(int totalEnvelopes) {
        List<Envelope> envelopes = new ArrayList<>();
        // Use a fixed seed for reproducibility
        Random random = new Random(0);

        // Define hotspots
        double[][] hotspots = {
                {-10, -80}, // Hotspot 1
                {10, 20},   // Hotspot 2
                {20, 70}    // Hotspot 3
        };

        int envelopesPerHotspot = totalEnvelopes / hotspots.length;

        // Generate envelopes around each hotspot
        for (double[] hotspot : hotspots) {
            for (int i = 0; i < envelopesPerHotspot; i++) {
                double minX = hotspot[0] + random.nextGaussian() * 5;
                double maxX = minX + 5 + random.nextDouble() * 5;
                double minY = hotspot[1] + random.nextGaussian() * 10;
                double maxY = minY + 10 + random.nextDouble() * 10;

                envelopes.add(new Envelope(minX, maxX, minY, maxY));
            }
        }

        // If there are remaining envelopes, add them randomly around the hotspots
        for (int i = 0; i < totalEnvelopes % hotspots.length; i++) {
            double[] hotspot = hotspots[i % hotspots.length];
            double minX = hotspot[0] + random.nextGaussian() * 5;
            double maxX = minX + 5 + random.nextDouble() * 5;
            double minY = hotspot[1] + random.nextGaussian() * 10;
            double maxY = minY + 10 + random.nextDouble() * 10;

            envelopes.add(new Envelope(minX, maxX, minY, maxY));
        }

        return envelopes;
    }

    @Test
    public void testCalculateScaleFactorForRange() {
        ZOrderPartitioning zOrderPartitioning;

        // Test 1: boundary with range 10 in x and y
        zOrderPartitioning = new ZOrderPartitioning(new Envelope(0, 10, 0, 10), 4);
        assertEquals(10_000_000, zOrderPartitioning.calculateScaleFactorForRange(new Envelope(0, 10, 0, 10)));

        // Test 2: boundary with range 100 in x and y
        zOrderPartitioning = new ZOrderPartitioning(new Envelope(0, 100, 0, 100), 4);
        assertEquals(1_000_000, zOrderPartitioning.calculateScaleFactorForRange(new Envelope(0, 100, 0, 100)));

        // Test 3: boundary with range 1000 in x and y
        zOrderPartitioning = new ZOrderPartitioning(new Envelope(0, 1000, 0, 1000), 4);
        assertEquals(100_000, zOrderPartitioning.calculateScaleFactorForRange(new Envelope(0, 1000, 0, 1000)));

        // Test 4: boundary with range 0.1 in x and y
        zOrderPartitioning = new ZOrderPartitioning(new Envelope(0, 0.1, 0, 0.1), 4);
        assertEquals(1_000_000_000, zOrderPartitioning.calculateScaleFactorForRange(new Envelope(0, 0.1, 0, 0.1)));

        // Test 45: boundary with range -180, 180 in x and y
        zOrderPartitioning = new ZOrderPartitioning(new Envelope(-180, 180, -180, 180), 4);
        assertEquals(1_000_000_000, zOrderPartitioning.calculateScaleFactorForRange(new Envelope(0, 0.1, 0, 0.1)));
    }
}
