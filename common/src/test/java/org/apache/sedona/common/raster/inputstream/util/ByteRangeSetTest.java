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
package org.apache.sedona.common.raster.inputstream.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class ByteRangeSetTest {
    @Test
    public void testAddingDisjointRanges() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(0, 4));
        brs.addRange(new ByteRange(8, 10));
        brs.addRange(new ByteRange(5, 7));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 10));
        Assert.assertEquals(3, ranges.size());
        Assert.assertEquals(new ByteRange(0, 4), ranges.get(0));
        Assert.assertEquals(new ByteRange(5, 7), ranges.get(1));
        Assert.assertEquals(new ByteRange(8, 10), ranges.get(2));
    }

    @Test
    public void testAddingContainedRanges() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(0, 5));
        brs.addRange(new ByteRange(10, 15));
        brs.addRange(new ByteRange(20, 25));
        brs.checkConsistency();
        brs.addRange(new ByteRange(21, 24));
        brs.addRange(new ByteRange(1, 4));
        brs.addRange(new ByteRange(10, 15));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(-100, 100));
        Assert.assertEquals(3, ranges.size());
    }

    @Test
    public void testAddingOverlappingRanges() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(0, 10));

        // Keep adding overlapping ranges to the end, this should always trigger the fast path.
        for (int k = 10; k < 20; k++) {
            brs.addRange(new ByteRange(k - 2, k + 2));
            List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(-100, 100));
            Assert.assertEquals(1, ranges.size());
            ByteRange range = ranges.get(0);
            Assert.assertEquals(0, range.inclusiveStart);
            Assert.assertEquals(k + 2, range.exclusiveEnd);
        }
    }

    @Test
    public void testAddingAdjacentRanges() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(0, 10));

        // Keep adding adjacent ranges to the end, this should always trigger the fast path.
        for (int k = 10; k < 20; k++) {
            brs.addRange(new ByteRange(k, k + 1));
            List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(-100, 100));
            Assert.assertEquals(1, ranges.size());
            ByteRange range = ranges.get(0);
            Assert.assertEquals(0, range.inclusiveStart);
            Assert.assertEquals(k + 1, range.exclusiveEnd);
        }
    }

    @Test
    public void testAddingToInterleavingSubRanges() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(0, 10));
        brs.addRange(new ByteRange(100, 110));

        // Adding overlapping ranges to 2 disjoint ranges in an interleaving manner. This should
        // trigger the slow path since it breaks the principle of locality.
        for (int k = 10; k < 20; k++) {
            brs.addRange(new ByteRange(k - 2, k + 2));
            brs.addRange(new ByteRange(98 + k, 102 + k));
            brs.checkConsistency();
            List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(-200, 200));
            Assert.assertEquals(2, ranges.size());
            ByteRange range = ranges.get(0);
            Assert.assertEquals(0, range.inclusiveStart);
            Assert.assertEquals(k + 2, range.exclusiveEnd);
            range = ranges.get(1);
            Assert.assertEquals(100, range.inclusiveStart);
            Assert.assertEquals(k + 102, range.exclusiveEnd);
        }
    }

    @Test
    public void testAddingToInterleavingSubRanges2() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(0, 10));
        brs.addRange(new ByteRange(100, 110));

        // Adding overlapping ranges to 2 disjoint ranges in an interleaving manner. This should
        // trigger the slow path since it breaks the principle of locality.
        for (int k = 10; k < 20; k++) {
            brs.addRange(new ByteRange(k, k + 1));
            brs.addRange(new ByteRange(100 + k, 101 + k));
            brs.checkConsistency();
            List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(-200, 200));
            Assert.assertEquals(2, ranges.size());
            ByteRange range = ranges.get(0);
            Assert.assertEquals(0, range.inclusiveStart);
            Assert.assertEquals(k + 1, range.exclusiveEnd);
            range = ranges.get(1);
            Assert.assertEquals(100, range.inclusiveStart);
            Assert.assertEquals(k + 101, range.exclusiveEnd);
        }
    }

    @Test
    public void testPrependRanges() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        for (int k = 10; k > 0; k--) {
            brs.addRange(new ByteRange(k - 1, k));
            brs.checkConsistency();
            List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
            Assert.assertEquals(1, ranges.size());
            ByteRange range = ranges.get(0);
            Assert.assertEquals(k - 1, range.inclusiveStart);
            Assert.assertEquals(20, range.exclusiveEnd);
        }
    }

    @Test
    public void testFillingTheGapAdjacent() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));
        brs.addRange(new ByteRange(20, 30));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
        Assert.assertEquals(1, ranges.size());
        ByteRange range = ranges.get(0);
        Assert.assertEquals(10, range.inclusiveStart);
        Assert.assertEquals(40, range.exclusiveEnd);
    }

    @Test
    public void testFillingTheGapOverlappingLeft() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));
        brs.addRange(new ByteRange(15, 30));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
        Assert.assertEquals(1, ranges.size());
        ByteRange range = ranges.get(0);
        Assert.assertEquals(10, range.inclusiveStart);
        Assert.assertEquals(40, range.exclusiveEnd);
    }

    @Test
    public void testFillingTheGapOverlappingRight() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));
        brs.addRange(new ByteRange(20, 32));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
        Assert.assertEquals(1, ranges.size());
        ByteRange range = ranges.get(0);
        Assert.assertEquals(10, range.inclusiveStart);
        Assert.assertEquals(40, range.exclusiveEnd);
    }

    @Test
    public void testFillingTheGapOverlappingBoth() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));
        brs.addRange(new ByteRange(15, 32));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
        Assert.assertEquals(1, ranges.size());
        ByteRange range = ranges.get(0);
        Assert.assertEquals(10, range.inclusiveStart);
        Assert.assertEquals(40, range.exclusiveEnd);
    }

    @Test
    public void testFillingTheGapExtendLeft() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));
        brs.addRange(new ByteRange(6, 32));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
        Assert.assertEquals(1, ranges.size());
        ByteRange range = ranges.get(0);
        Assert.assertEquals(6, range.inclusiveStart);
        Assert.assertEquals(40, range.exclusiveEnd);
    }

    @Test
    public void testFillingTheGapExtendRight() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));
        brs.addRange(new ByteRange(15, 50));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
        Assert.assertEquals(1, ranges.size());
        ByteRange range = ranges.get(0);
        Assert.assertEquals(10, range.inclusiveStart);
        Assert.assertEquals(50, range.exclusiveEnd);
    }

    @Test
    public void testFillingTheGapExtendAll() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));
        brs.addRange(new ByteRange(5, 50));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
        Assert.assertEquals(1, ranges.size());
        ByteRange range = ranges.get(0);
        Assert.assertEquals(5, range.inclusiveStart);
        Assert.assertEquals(50, range.exclusiveEnd);
    }

    @Test
    public void testMergeLeft() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));
        brs.addRange(new ByteRange(20, 25));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals(new ByteRange(10, 25), ranges.get(0));
        Assert.assertEquals(new ByteRange(30, 40), ranges.get(1));
    }

    @Test
    public void testMergeRight() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));
        brs.addRange(new ByteRange(25, 30));
        brs.checkConsistency();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals(new ByteRange(10, 20), ranges.get(0));
        Assert.assertEquals(new ByteRange(25, 40), ranges.get(1));
    }

    @Test
    public void testFindOverlappingRangesFastPath() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));

        // Fast path: current range contains queried range
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(10, 20));
        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(new ByteRange(10, 20), ranges.get(0));

        // Fast path: queried range is adjacent with current range, no next range
        ranges = brs.findOverlappingRanges(new ByteRange(20, 30));
        Assert.assertEquals(0, ranges.size());

        // Fast path: current range overlaps with queried range, no next range
        ranges = brs.findOverlappingRanges(new ByteRange(15, 25));
        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(new ByteRange(10, 20), ranges.get(0));

        // Fast path: current range overlaps with queried range, but not overlap with next range
        brs.addRange(new ByteRange(30, 40));
        brs.setCurrentRange(new ByteRange(10, 20));
        ranges = brs.findOverlappingRanges(new ByteRange(15, 25));
        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(new ByteRange(10, 20), ranges.get(0));

        // Fast path: current range and the next range overlaps with the queried range
        brs.setCurrentRange(new ByteRange(10, 20));
        ranges = brs.findOverlappingRanges(new ByteRange(15, 40));
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals(new ByteRange(10, 20), ranges.get(0));
        Assert.assertEquals(new ByteRange(30, 40), ranges.get(1));

        // Fast path: queried range is adjacent to the current range, and does not overlap with the
        // next range
        brs.setCurrentRange(new ByteRange(10, 20));
        ranges = brs.findOverlappingRanges(new ByteRange(20, 30));
        Assert.assertEquals(0, ranges.size());

        // Fast path: queried range is adjacent to the current range, and only overlap with the
        // next page
        brs.setCurrentRange(new ByteRange(10, 20));
        ranges = brs.findOverlappingRanges(new ByteRange(20, 31));
        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(new ByteRange(30, 40), ranges.get(0));
    }

    @Test
    public void testFindOverlappingRangesSlowPath() {
        ByteRangeSet brs = new ByteRangeSet();
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));

        // Slow path: current range and the next range overlaps with the queried range, but the queried
        // range spans through the end of the next range
        brs.setCurrentRange(new ByteRange(10, 20));
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(15, 50));
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals(new ByteRange(10, 20), ranges.get(0));
        Assert.assertEquals(new ByteRange(30, 40), ranges.get(1));

        // Slow path: the start of queried range is smaller than all the ranges
        ranges = brs.findOverlappingRanges(new ByteRange(5, 50));
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals(new ByteRange(10, 20), ranges.get(0));
        Assert.assertEquals(new ByteRange(30, 40), ranges.get(1));

        // Slow path: retrieve 2 or more ranges
        brs.addRange(new ByteRange(45, 50));
        brs.setCurrentRange(new ByteRange(10, 20));
        ranges = brs.findOverlappingRanges(new ByteRange(15, 50));
        Assert.assertEquals(3, ranges.size());
        Assert.assertEquals(new ByteRange(10, 20), ranges.get(0));
        Assert.assertEquals(new ByteRange(30, 40), ranges.get(1));
        Assert.assertEquals(new ByteRange(45, 50), ranges.get(2));

        ranges = brs.findOverlappingRanges(new ByteRange(15, 42));
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals(new ByteRange(10, 20), ranges.get(0));
        Assert.assertEquals(new ByteRange(30, 40), ranges.get(1));

        ranges = brs.findOverlappingRanges(new ByteRange(15, 60));
        Assert.assertEquals(3, ranges.size());
        Assert.assertEquals(new ByteRange(10, 20), ranges.get(0));
        Assert.assertEquals(new ByteRange(30, 40), ranges.get(1));
        Assert.assertEquals(new ByteRange(45, 50), ranges.get(2));

        ranges = brs.findOverlappingRanges(new ByteRange(25, 60));
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals(new ByteRange(30, 40), ranges.get(0));
        Assert.assertEquals(new ByteRange(45, 50), ranges.get(1));

        // Slow path: queried range is adjacent to the current range, and the end exceeds the
        // next range
        brs.addRange(new ByteRange(55, 60));
        brs.setCurrentRange(new ByteRange(10, 20));
        ranges = brs.findOverlappingRanges(new ByteRange(20, 57));
        Assert.assertEquals(3, ranges.size());
        Assert.assertEquals(new ByteRange(30, 40), ranges.get(0));
        Assert.assertEquals(new ByteRange(45, 50), ranges.get(1));
        Assert.assertEquals(new ByteRange(55, 60), ranges.get(2));
    }

    @Test
    public void testFindOverlappingRangesNoResults() {
        ByteRangeSet brs = new ByteRangeSet();
        List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(0, 100));
        Assert.assertEquals(0, ranges.size());
        brs.addRange(new ByteRange(10, 20));
        brs.addRange(new ByteRange(30, 40));
        brs.addRange(new ByteRange(50, 60));
        brs.setCurrentRange(new ByteRange(10, 20));

        ranges = brs.findOverlappingRanges(new ByteRange(0, 10));
        Assert.assertEquals(0, ranges.size());
        ranges = brs.findOverlappingRanges(new ByteRange(0, 9));
        Assert.assertEquals(0, ranges.size());
        ranges = brs.findOverlappingRanges(new ByteRange(20, 30));
        Assert.assertEquals(0, ranges.size());
        ranges = brs.findOverlappingRanges(new ByteRange(21, 29));
        Assert.assertEquals(0, ranges.size());
        ranges = brs.findOverlappingRanges(new ByteRange(40, 50));
        Assert.assertEquals(0, ranges.size());
        ranges = brs.findOverlappingRanges(new ByteRange(41, 49));
        Assert.assertEquals(0, ranges.size());
        ranges = brs.findOverlappingRanges(new ByteRange(60, 70));
        Assert.assertEquals(0, ranges.size());
        ranges = brs.findOverlappingRanges(new ByteRange(61, 70));
        Assert.assertEquals(0, ranges.size());
    }

    private void testRandomRanges(long domain, long rangeLength, int rangeCount, int queryCount) {
        // Randomly generate ranges and add them to the ByteRangeSet.
        ByteRangeSet brs = new ByteRangeSet();
        Set<Long> longSet = new HashSet<>();
        for (int k = 0; k < rangeCount; k++) {
            long start = (long) (Math.random() * domain);
            long len = (long) (Math.random() * rangeLength + 1);
            long end = start + len;
            brs.addRange(new ByteRange(start, end));
            brs.checkConsistency();

            for (long i = start; i < end; i++) {
                longSet.add(i);
            }
        }


        // Randomly generate ranges and find the overlapping ranges.
        for (int k = 0; k < queryCount; k++) {
            long rand0 = (long) (Math.random() * domain);
            long rand1 = (long) (Math.random() * domain);
            if (rand0 == rand1) {
                continue;
            }
            long start = Math.min(rand0, rand1);
            long end = Math.max(rand0, rand1);
            List<ByteRange> ranges = brs.findOverlappingRanges(new ByteRange(start, end));

            // Everything within the found ranges should be present in the longSet.
            for (ByteRange range : ranges) {
                for (long i = range.inclusiveStart; i < range.exclusiveEnd; i++) {
                    Assert.assertTrue(longSet.contains(i));
                }
            }

            // Everything not within the found ranges should not be present in the longSet.
            for (long i = start; i < end; i++) {
                if (longSet.contains(i)) {
                    boolean found = false;
                    for (ByteRange range : ranges) {
                        if (range.contains(i)) {
                            found = true;
                            break;
                        }
                    }
                    Assert.assertTrue(found);
                }
            }
        }
    }

    @Test
    public void testRandomRangesRepeated() {
        for (int k = 0; k < 100; k++) {
            testRandomRanges(500, 10, 200, 200);
        }
    }
}
