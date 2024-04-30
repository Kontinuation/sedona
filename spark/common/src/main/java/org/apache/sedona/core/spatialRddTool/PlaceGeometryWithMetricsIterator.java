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

import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator that places geometries using a spatial partitioner and accumulates metrics.
 * @param <T> The type of geometries to place
 */
public class PlaceGeometryWithMetricsIterator<T extends Geometry> implements Iterator<Tuple2<Integer, T>> {
    private final Iterator<T> iterator;
    private final SpatialPartitioner partitioner;
    private final LongAccumulator accInputCount;
    private final LongAccumulator accOutputCount;
    private final LongAccumulator accMaxDuplicates;

    private long inputCount = 0;
    private long outputCount = 0;
    private long maxDuplicates = 0;
    private long numDuplicates = 0;
    private boolean metricsUpdated = false;
    Iterator<Tuple2<Integer, T>> placedObjects = Collections.emptyIterator();

    public PlaceGeometryWithMetricsIterator(
            Iterator<T> iterator, SpatialPartitioner partitioner,
            LongAccumulator accInputCount, LongAccumulator accOutputCount,
            LongAccumulator accMaxDuplicates) {
        this.iterator = iterator;
        this.partitioner = partitioner;
        this.accInputCount = accInputCount;
        this.accOutputCount = accOutputCount;
        this.accMaxDuplicates = accMaxDuplicates;
    }

    @Override
    public boolean hasNext() {
        while (!placedObjects.hasNext()) {
            maxDuplicates = Math.max(maxDuplicates, numDuplicates);
            if (!iterator.hasNext()) {
                if (!metricsUpdated) {
                    accInputCount.add(inputCount);
                    accOutputCount.add(outputCount);
                    accMaxDuplicates.add(maxDuplicates);
                    metricsUpdated = true;
                }
                return false;
            }
            T geometry = iterator.next();
            try {
                placedObjects = partitioner.placeObject(geometry);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            inputCount += 1;
            numDuplicates = 0;
        }
        return true;
    }

    @Override
    public Tuple2<Integer, T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        outputCount += 1;
        numDuplicates += 1;
        return placedObjects.next();
    }
}
