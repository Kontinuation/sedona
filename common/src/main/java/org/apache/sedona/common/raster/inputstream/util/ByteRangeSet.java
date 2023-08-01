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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * A set of {@link ByteRange} objects. This is used to track the ranges that have been cached in the
 * disk cache file.
 */
public class ByteRangeSet {

    private static class ByteRangeComparator implements Comparator<ByteRange> {
        @Override
        public int compare(ByteRange r1, ByteRange r2) {
            return Long.compare(r1.inclusiveStart, r2.inclusiveStart);
        }
    }

    private final TreeSet<ByteRange> ranges;

    // These are used to track the current range being read and the byte range next to the current
    // range. These are for optimization purposes to avoid searching the TreeSet on every read,
    // since most of the reads are sequential or close to sequential (principle of locality).
    private ByteRange currentRange;
    private ByteRange nextRange;

    public ByteRangeSet() {
        ranges = new TreeSet<>(new ByteRangeComparator());
        currentRange = null;
        nextRange = null;
    }

    /**
     * Add a range to the set of ranges. This method will merge the range with existing ranges if
     * possible. This method should be called after writing bytes to the disk cache file.
     *
     * @param range the byte range to add
     */
    public void addRange(ByteRange range) {
        if (currentRange == null || !currentRange.containsOrAdjacent(range.inclusiveStart)) {
            // Slow path: the range we're adding has nothing to do with currentRange.
            ByteRange existingRange = ranges.floor(range);
            if (existingRange != null && existingRange.containsOrAdjacent(range.inclusiveStart)) {
                currentRange = existingRange;
            } else {
                ranges.add(range);
                currentRange = range;
            }
            nextRange = ranges.higher(currentRange);
        }

        // Extend currentRange by len, and merge with subsequent ranges if possible.
        currentRange.exclusiveEnd = Math.max(currentRange.exclusiveEnd, range.exclusiveEnd);
        while (nextRange != null && currentRange.overlapsOrAdjacent(nextRange)) {
            currentRange.exclusiveEnd = Math.max(currentRange.exclusiveEnd, nextRange.exclusiveEnd);
            ranges.remove(nextRange);
            nextRange = ranges.higher(currentRange);
        }
    }

    /**
     * Find the ranges that overlap with the given range.
     *
     * @param range     the range to find overlapping ranges for
     * @param maxRanges the maximum number of ranges to return. Specify 0 for no limits.
     * @return a list of ByteRange objects that overlap with the given range. The list is sorted
     */
    public List<ByteRange> findOverlappingRanges(ByteRange range, int maxRanges) {
        long inclusiveStart = range.inclusiveStart;
        long exclusiveEnd = range.exclusiveEnd;
        if (currentRange != null) {
            // Fast path: let's see if current range and next range could cover the requested range.
            if (currentRange.contains(range)) {
                return Collections.singletonList(currentRange);
            } else if (currentRange.contains(inclusiveStart)) {
                // The current range overlaps with the requested range. Let's see if it overlaps with
                // the next range, and if so, return both ranges.
                if (nextRange == null) {
                    return Collections.singletonList(currentRange);
                } else if (nextRange.containsOrAdjacent(exclusiveEnd)) {
                    return Arrays.asList(currentRange, nextRange);
                } else if (exclusiveEnd <= nextRange.inclusiveStart) {
                    return Collections.singletonList(currentRange);
                }
                // There may be more ranges after next range, which overlaps with the requested range.
                // We should go through the slow path to find them.
            } else if (currentRange.exclusiveEnd == inclusiveStart) {
                // The requested range starts at the end of current range. If it does not overlap with
                // the next range, then there's no overlapping ranges.
                if (nextRange == null) {
                    return Collections.emptyList();
                } else if (exclusiveEnd <= nextRange.inclusiveStart) {
                    return Collections.emptyList();
                } else if (nextRange.containsOrAdjacent(exclusiveEnd)) {
                    return Collections.singletonList(nextRange);
                }
                // There may be more ranges after next range, which overlaps with the requested range.
                // We should go through the slow path to find them.
            }
        }

        // Slow path: search the range tree for overlapping ranges.
        List<ByteRange> overlappingRanges = new ArrayList<>(maxRanges);
        ByteRange cur = ranges.floor(range);
        if (cur == null || !cur.overlaps(range)) {
            cur = ranges.higher(range);
        }
        while (cur != null && cur.overlaps(range)) {
            currentRange = cur;
            nextRange = ranges.higher(cur);
            overlappingRanges.add(cur);
            if (maxRanges > 0 && overlappingRanges.size() >= maxRanges) {
                break;
            }
            cur = nextRange;
        }
        return overlappingRanges;
    }

    public List<ByteRange> findOverlappingRanges(ByteRange range) {
        return findOverlappingRanges(range, 0);
    }

    /**
     * Check the consistency of the ByteRanges data structure. This method is for testing only.
     */
    public void checkConsistency() {
        ByteRange prev = null;
        for (ByteRange cur : ranges) {
            if (prev != null) {
                if (prev.exclusiveEnd >= cur.inclusiveStart) {
                    // If this happens, there should be some problem with the range merging code in
                    // addRange().
                    throw new IllegalStateException("ByteRanges are adjacent or overlapping");
                }
            }
            prev = cur;
        }

        if (currentRange != null) {
            ByteRange cur = ranges.floor(currentRange);
            if (cur != currentRange) {
                throw new IllegalStateException("currentRange is not correct");
            }
            ByteRange next = ranges.higher(cur);
            if (next != nextRange) {
                throw new IllegalStateException("nextRange is not correct");
            }
        } else {
            if (nextRange != null) {
                throw new IllegalStateException("nextRange should be null");
            }
        }
    }

    public void setCurrentRange(ByteRange range) {
        if (range != null) {
            ByteRange cur = ranges.floor(range);
            if (!range.equals(cur)) {
                throw new IllegalArgumentException("specified is not in the range set");
            }
            currentRange = cur;
            nextRange = ranges.higher(cur);
        } else {
            currentRange = null;
            nextRange = null;
        }
    }
}
