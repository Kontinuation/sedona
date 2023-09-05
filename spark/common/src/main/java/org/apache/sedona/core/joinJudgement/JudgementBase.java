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

package org.apache.sedona.core.joinJudgement;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sedona.core.monitoring.Metric;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.spark.TaskContext;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Base class for partition level join implementations.
 * <p>
 * Provides `match` method to test whether a given pair of geometries satisfies join condition.
 * <p>
 */
abstract class JudgementBase<T extends Geometry, U extends Geometry>
        implements Serializable
{
    private static final Logger log = LogManager.getLogger(JudgementBase.class);

    private final SpatialPredicate spatialPredicate;
    protected final Metric buildCount;
    protected final Metric streamCount;
    protected final Metric resultCount;
    protected final Metric candidateCount;
    protected final boolean buildLeft;
    private int shapeCnt;

    // A batch of pre-computed matches
    private List<Pair<U, T>> batch = null;
    // An index of the element from 'batch' to return next
    private int nextIndex = 0;

    /**
     *
     * @param spatialPredicate spatial predicate as join condition
     * @param buildCount num of geometries in build side
     * @param streamCount num of geometries in stream side
     * @param resultCount num of join results
     * @param candidateCount num of candidate pairs to be refined by their real geometries
     */
    protected JudgementBase(SpatialPredicate spatialPredicate, Metric buildCount, Metric streamCount, Metric resultCount, Metric candidateCount, boolean buildLeft)
    {
        this.spatialPredicate = spatialPredicate;
        this.buildCount = buildCount;
        this.streamCount = streamCount;
        this.resultCount = resultCount;
        this.candidateCount = candidateCount;
        this.shapeCnt = 0;
        this.buildLeft = buildLeft;
    }


    /**
     * Iterator model for the index-based join.
     * It checks if there is a next match and populate it to the result.
     * @param spatialIndex
     * @param streamShapes
     * @return
     */
    protected boolean hasNextBase(SpatialIndex spatialIndex, Iterator<? extends Geometry> streamShapes, JoinResultCandidateRefiner.Refiner refiner)
    {
        if (batch != null) {
            return true;
        }
        else {
            return populateNextBatch(spatialIndex, streamShapes, refiner);
        }
    }

    /**
     * Iterator model for the nest loop join.
     * It checks if there is a next match and populate it to the result.
     * @param buildShapes
     * @param streamShapes
     * @return
     */
    protected boolean hasNextBase(List<? extends Geometry> buildShapes, Iterator<? extends Geometry> streamShapes, JoinResultCandidateRefiner.Refiner refiner)
    {
        if (batch != null) {
            return true;
        }
        else {
            return populateNextBatch(buildShapes, streamShapes, refiner);
        }
    }

    /**
     * Iterator model for the index-based join.
     * It returns 1 pair in the current batch.
     * Each batch contains a list of pairs of geometries that satisfy the join condition.
     * The current batch is the result of the current stream shape against all the build shapes.
     * @param spatialIndex
     * @param streamShapes
     * @return
     */
    protected Pair<U, T> nextBase(SpatialIndex spatialIndex, Iterator<? extends Geometry> streamShapes, JoinResultCandidateRefiner.Refiner refiner) {
        if (batch == null) {
            populateNextBatch(spatialIndex, streamShapes, refiner);
        }

        if (batch != null) {
            final Pair<U, T> result = batch.get(nextIndex);
            nextIndex++;
            if (nextIndex >= batch.size()) {
                populateNextBatch(spatialIndex, streamShapes, refiner);
                nextIndex = 0;
            }
            return result;
        }

        throw new NoSuchElementException();
    }

    /**
     * Iterator model for the nest loop join.
     * It returns 1 pair in the current batch.
     * Each batch contains a list of pairs of geometries that satisfy the join condition.
     * The current batch is the result of the current stream shape against all the build shapes.
     * @param buildShapes
     * @param streamShapes
     * @return
     */
    protected Pair<U, T> nextBase(List<? extends Geometry> buildShapes, Iterator<? extends Geometry> streamShapes, JoinResultCandidateRefiner.Refiner refiner) {
        if (batch == null) {
            populateNextBatch(buildShapes, streamShapes, refiner);
        }

        if (batch != null) {
            final Pair<U, T> result = batch.get(nextIndex);
            nextIndex++;
            if (nextIndex >= batch.size()) {
                populateNextBatch(buildShapes, streamShapes, refiner);
                nextIndex = 0;
            }
            return result;
        }

        throw new NoSuchElementException();
    }

    /**
     * Populates the next batch of matches given the current shape in the stream side.
     * It works as follows:
     * 1. If there is no shape left in the stream side, it returns false.
     * 2. If there are shapes left in the stream side, it uses the current shape in the stream side to query the spatial index.
     * The query result is a list of geometries in the build side that overlap with the current shape in the stream side.
     * The query result is flattened to a list of pairs of geometries
     * 3. If there are no results, it returns false.
     *
     * @param spatialIndex spatial index of the build side
     * @param streamShapes stream side geometries
     * @return whether there is a next batch
     */
    private boolean populateNextBatch(SpatialIndex spatialIndex, Iterator<? extends Geometry> streamShapes, JoinResultCandidateRefiner.Refiner refiner)
    {
        if (!streamShapes.hasNext()) {
            if (batch != null) {
                batch = null;
            }
            return false;
        }

        batch = new ArrayList<>();

        while (streamShapes.hasNext()) {
            shapeCnt++;
            streamCount.add(1);
            final Geometry streamShape = streamShapes.next();
            final List candidates = spatialIndex.query(streamShape.getEnvelopeInternal());
            candidateCount.add(candidates.size());
            refiner.refine(streamShape, candidates, batch);
            resultCount.add(batch.size());
            logMilestone(shapeCnt, 100 * 1000, "Streaming shapes");
            if (!batch.isEmpty()) {
                return true;
            }
        }

        batch = null;
        return false;
    }

    /**
     * Populates the next batch of matches given the current shape in the stream side.
     * This is solely used for nested loop join.
     * It works as follows:
     * 1. If there is no shape left in the stream side, it returns false.
     * 2. If there are shapes left in the stream side, it uses the current shape in the stream side to query buildShapes
     * The query result is a list of geometries in the build side that overlap with the current shape in the stream side.
     * The query result is flattened to a list of pairs of geometries
     * 3. If there are no results, it returns false.
     * @param buildShapes
     * @param streamShapes
     * @return
     */
    private boolean populateNextBatch(List<? extends Geometry> buildShapes, Iterator<? extends Geometry> streamShapes, JoinResultCandidateRefiner.Refiner refiner)
    {
        if (!streamShapes.hasNext()) {
            if (batch != null) {
                batch = null;
            }
            return false;
        }

        batch = new ArrayList<>();

        while (streamShapes.hasNext()) {
            shapeCnt++;
            streamCount.add(1);
            final Geometry streamShape = streamShapes.next();
            refiner.refine(streamShape, (List<Geometry>) buildShapes, batch);
            resultCount.add(batch.size());
            logMilestone(shapeCnt, 100 * 1000, "Streaming shapes");
            if (!batch.isEmpty()) {
                return true;
            }
        }

        batch = null;
        return false;
    }

    protected void log(String message, Object... params)
    {
        if (Level.INFO.isGreaterOrEqual(log.getEffectiveLevel())) {
            final int partitionId = TaskContext.getPartitionId();
            final long threadId = Thread.currentThread().getId();
            log.info("[" + threadId + ", PID=" + partitionId + "] " + String.format(message, params));
        }
    }

    private void logMilestone(long cnt, long threshold, String name)
    {
        if (cnt > 1 && cnt % threshold == 1) {
            log("[%s] Reached a milestone: %d", name, cnt);
        }
    }

    protected JoinResultCandidateRefiner.Refiner createRefiner(boolean isStream)
    {
        return JoinResultCandidateRefiner.create(isStream, spatialPredicate);
    }
}
