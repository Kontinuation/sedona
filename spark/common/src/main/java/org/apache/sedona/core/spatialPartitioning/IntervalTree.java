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

import org.apache.commons.lang3.Range;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Implementation of IntervalTree using a linear list of Envelopes.
 * This implementation does not use a tree structure such as QuadTree,
 * but rather a simple 1D List for storing the partition zones.
 */
public class IntervalTree extends PartitioningUtils
        implements Serializable
{
    final private List<Envelope> samples = new ArrayList<>();
    final private Envelope boundary;

    private int numPartitions;
    // ordered list of ranges of the partition zones in 1D space
    private List<Range<Long>> ranges;
    // ordered list of ranges of the partition zones in 1D space
    private List<Range<Long>> nonOverlappedRanges;
    // flag to indicate whether the partition zones are overlapped or not
    private boolean useNonOverlapped = false;

    private ZOrderPartitioning zOrderPartitioning;

    /**
     * Constructor to initialize the partitions list.
     *
     * @param boundary      Envelope object representing the boundary of the partition zones.
     * @param numPartitions Number of partitions to be created.
     */
    public IntervalTree(Envelope boundary, int numPartitions) {
        this.ranges = new ArrayList<>();
        this.nonOverlappedRanges = new ArrayList<>();
        this.boundary = boundary;
        this.numPartitions = numPartitions;
        this.zOrderPartitioning = new ZOrderPartitioning(boundary, numPartitions);
    }

    /**
     * Constructor to initialize the partitions list.
     * @param intervalTree original IntervalTree object
     * @param nonOverlap flag to indicate whether the partition zones are overlapped or not
     */
    public IntervalTree(IntervalTree intervalTree, boolean nonOverlap) {
        this.ranges = intervalTree.ranges;
        this.nonOverlappedRanges = intervalTree.nonOverlappedRanges;
        this.useNonOverlapped = nonOverlap;
        this.boundary = intervalTree.boundary;
        this.numPartitions = intervalTree.numPartitions;
        this.zOrderPartitioning = intervalTree.zOrderPartitioning;
    }

    /**
     * Check the geometry against the partition zones to find the IDs of overlapping ranges.
     * Note that the geometry can be in multiple ranges because ranges can overlap.
     *
     * @param geometry Geometry object to be placed.
     * @return Iterator of Tuple2 containing partition ID and the corresponding geometry.
     */
    @Override
    public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry geometry) {
        List<Tuple2<Integer, Geometry>> results = new ArrayList<>();
        long zOrderValue = this.zOrderPartitioning.calculateZOrder(geometry.getEnvelopeInternal());

        List<Range<Long>> partitionRanges = useNonOverlapped ? nonOverlappedRanges : ranges;
        for (int i = 0; i < partitionRanges.size(); i++) {
            if (partitionRanges.get(i).contains(zOrderValue)) {
                results.add(new Tuple2<>(i, geometry));
            }
        }
        return results.iterator();
    }

    /**
     * Check the geometry against the partition zones to find the IDs of overlapping ranges.
     * Only returns the IDs of the overlapping partitions.
     * Note that the geometry can be in multiple ranges because ranges can overlap.
     *
     * @param geometry Geometry object to be checked.
     * @return Set of integers representing the IDs of the overlapping partitions.
     */
    @Override
    public Set<Integer> getKeys(Geometry geometry) {
        Set<Integer> keys = new HashSet<>();
        long zOrderValue = this.zOrderPartitioning.calculateZOrder(geometry.getEnvelopeInternal());

        List<Range<Long>> partitionRanges = useNonOverlapped ? nonOverlappedRanges : ranges;
        for (int i = 0; i < partitionRanges.size(); i++) {
            if (partitionRanges.get(i).contains(zOrderValue)) {
                keys.add(i);
            }
        }
        return keys;
    }

    /**
     * Traverse the partition list and fetch all the partition zones.
     *
     * @return List of Envelope objects representing all the partition zones.
     */
    @Override
    public List<Envelope> fetchLeafZones() {
        // this is not supported for IntervalTree because it operates in 1D space
        throw new UnsupportedOperationException("Unsupported operation.");
    }

    public int getPartitionNum() {
        List<Range<Long>> partitionRanges = useNonOverlapped ? nonOverlappedRanges : ranges;
        return partitionRanges.size();
    }

    /**
     * Insert a new sample into the sample list.
     *
     * @param sample Envelope object to be inserted.
     */
    public void insert(Envelope sample) {
        samples.add(sample);
    }

    /**
     * Build the IntervalTree by creating the partition zones (ranges).
     */
    public void build(int neighborSampleNumber, double samplingProbability) {
        this.ranges = this.zOrderPartitioning.createZOrderRanges(samples, neighborSampleNumber, samplingProbability);
        // ranges are created based on the samples and now contains the partition zones
        this.nonOverlappedRanges = this.zOrderPartitioning.nonOverlappedRanges;
        // clear the samples list to free up memory and reduce the object size for serialization
        this.samples.clear();
    }
}
