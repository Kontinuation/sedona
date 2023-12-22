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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.enums.GeometryType;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.HalfOpenRectangle;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.spatialRddTool.AdvancedStatCollector;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Run local spatial join on spatial partitioned dataset RDD[U] and RDD[T]. The join algorithm used for each spatial
 * partition is determined by the statistics of the RDDs. It is adaptive to the distribution of the data so that it
 * can achieve better performance.
 * @param <U> Geometry type of left side RDD
 * @param <T> Geometry type of right side RDD
 */
public class AdaptiveIndexLookupJudgement<U extends Geometry, T extends Geometry>
        implements Function3<Integer, Iterator<U>, Iterator<T>, Iterator<Pair<U, T>>>, Serializable {

    private static final GeometryFactory factory = new GeometryFactory();

    private final SpatialPredicate spatialPredicate;

    // Metrics
    private final SpatialJoinMetric buildCount;
    private final SpatialJoinMetric streamCount;
    private final SpatialJoinMetric resultCount;
    private final SpatialJoinMetric candidateCount;
    private final SpatialJoinMetric buildTime;
    private final SpatialJoinMetric buildLeftTasks;
    private final SpatialJoinMetric buildRightTasks;
    private final SpatialJoinMetric prepareBuildTasks;
    private final SpatialJoinMetric prepareStreamTasks;

    // Per-partition local spatial join exec params
    private transient final List<LocalSpatialJoinExecParams> localSpatialJoinExecParamsList;
    private Broadcast<List<LocalSpatialJoinExecParams>> localSpatialJoinExecParamsListBroadcast = null;

    /**
     * Parameters for local spatial join execution
     */
    public static class LocalSpatialJoinExecParams implements Serializable, KryoSerializable {
        private IndexType indexType;
        private IndexBuildSide indexBuildSide;
        private ExecutionMode executionMode;
        private Envelope extent;  // For de-duplicating join results across spatial partitions

        public LocalSpatialJoinExecParams(IndexType indexType, IndexBuildSide indexBuildSide,
                                          ExecutionMode executionMode, Envelope extent) {
            this.indexType = indexType;
            this.indexBuildSide = indexBuildSide;
            this.executionMode = executionMode;
            this.extent = extent;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeInt(indexType.ordinal());
            output.writeInt(indexBuildSide.ordinal());
            output.writeInt(executionMode.ordinal());
            kryo.writeObject(output, extent);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            indexType = IndexType.values()[input.readInt()];
            indexBuildSide = IndexBuildSide.values()[input.readInt()];
            executionMode = ExecutionMode.values()[input.readInt()];
            extent = kryo.readObject(input, Envelope.class);
        }
    }

    public enum IndexBuildSide {
        /**
         * Build spatial index on the left side
         */
        LEFT,

        /**
         * Build spatial index on the right side
         */
        RIGHT
    }

    public enum ExecutionMode {
        /**
         * Create prepared geometries for the build side
         */
        PREPARE_BUILD,

        /**
         * Create prepared geometries for the stream side
         */
        PREPARE_STREAM,

        /**
         * Don't use prepared geometry for evaluating spatial predicates
         */
        PREPARE_NONE
    }

    public AdaptiveIndexLookupJudgement(
            SpatialPredicate spatialPredicate,
            AdvancedStatCollector leftStat,
            AdvancedStatCollector rightStat,
            SpatialPartitioner partitioner,
            LongAccumulator buildCount,
            LongAccumulator streamCount,
            LongAccumulator resultCount,
            LongAccumulator candidateCount,
            LongAccumulator buildTime,
            LongAccumulator buildLeftTasks,
            LongAccumulator buildRightTasks,
            LongAccumulator prepareBuildTasks,
            LongAccumulator prepareStreamTasks,
            SQLMetric sqlBuildCount,
            SQLMetric sqlStreamCount,
            SQLMetric sqlResultCount,
            SQLMetric sqlCandidateCount,
            SQLMetric sqlBuildTime,
            SQLMetric sqlBuildLeftTasks,
            SQLMetric sqlBuildRightTasks,
            SQLMetric sqlPrepareBuildTasks,
            SQLMetric sqlPrepareStreamTasks) {
        this.spatialPredicate = spatialPredicate;
        this.buildCount = new SpatialJoinMetric(sqlBuildCount, buildCount);
        this.streamCount = new SpatialJoinMetric(sqlStreamCount, streamCount);
        this.resultCount = new SpatialJoinMetric(sqlResultCount, resultCount);
        this.candidateCount = new SpatialJoinMetric(sqlCandidateCount, candidateCount);
        this.buildTime = new SpatialJoinMetric(sqlBuildTime, buildTime);
        this.buildLeftTasks = new SpatialJoinMetric(sqlBuildLeftTasks, buildLeftTasks);
        this.buildRightTasks = new SpatialJoinMetric(sqlBuildRightTasks, buildRightTasks);
        this.prepareBuildTasks = new SpatialJoinMetric(sqlPrepareBuildTasks, prepareBuildTasks);
        this.prepareStreamTasks = new SpatialJoinMetric(sqlPrepareStreamTasks, prepareStreamTasks);
        this.localSpatialJoinExecParamsList = generatePerPartitionPlan(leftStat, rightStat, partitioner,
                spatialPredicate);
    }

    // Constructor for testing
    public AdaptiveIndexLookupJudgement(
            SpatialPredicate spatialPredicate,
            List<LocalSpatialJoinExecParams> localSpatialJoinExecParamsList) {
        this.spatialPredicate = spatialPredicate;
        this.buildCount = new SpatialJoinMetric();
        this.streamCount = new SpatialJoinMetric();
        this.resultCount = new SpatialJoinMetric();
        this.candidateCount = new SpatialJoinMetric();
        this.buildTime = new SpatialJoinMetric();
        this.buildLeftTasks = new SpatialJoinMetric();
        this.buildRightTasks = new SpatialJoinMetric();
        this.prepareBuildTasks = new SpatialJoinMetric();
        this.prepareStreamTasks = new SpatialJoinMetric();
        this.localSpatialJoinExecParamsList = localSpatialJoinExecParamsList;
    }

    /**
     * Broadcast the local spatial join execution parameters. This should be called before running the spark job
     * for spatial join.
     * @param sparkContext The spark context.
     */
    public void prepare(JavaSparkContext sparkContext) {
        if (localSpatialJoinExecParamsListBroadcast != null) {
            localSpatialJoinExecParamsListBroadcast.destroy();
        }
        localSpatialJoinExecParamsListBroadcast = sparkContext.broadcast(localSpatialJoinExecParamsList);
    }

    /**
     * Generate a per-partition execution plan for the local spatial joins.
     * @param leftStat Statistics of the left RDD.
     * @param rightStat Statistics of the right RDD.
     * @param spatialPredicate The spatial predicate.
     * @param partitioner The spatial partitioner.
     */
    private static List<LocalSpatialJoinExecParams> generatePerPartitionPlan(
            AdvancedStatCollector leftStat, AdvancedStatCollector rightStat, SpatialPartitioner partitioner,
            SpatialPredicate spatialPredicate) {
        GeometryType leftGeomType = getDominantGeometryType(leftStat);
        GeometryType rightGeomType = getDominantGeometryType(rightStat);
        long[] leftPerPartitionCount = getPerPartitionGeometryCount(leftStat, partitioner);
        long[] rightPerPartitionCount = getPerPartitionGeometryCount(rightStat, partitioner);
        int numPartitions = partitioner.numPartitions();
        DedupParams dedupParams = partitioner.getDedupParams();
        List<Envelope> grids = (dedupParams != null? dedupParams.getPartitionExtents(): null);
        ArrayList<LocalSpatialJoinExecParams> plans = new ArrayList<>(numPartitions);
        for (int k = 0; k < numPartitions; k++) {
            long leftCount = leftPerPartitionCount[k];
            long rightCount = rightPerPartitionCount[k];
            Envelope extent = (grids != null? grids.get(k): null);
            LocalSpatialJoinExecParams plan = determineSpatialJoinExecParams(
                    leftGeomType, leftCount, leftStat.getMeanNumPoints(),
                    rightGeomType, rightCount, rightStat.getMeanNumPoints(),
                    spatialPredicate, extent);
            plans.add(plan);
        }
        return plans;
    }

    /**
     * Determine the spatial join execution parameters for a partition.
     * @param leftGeomType The dominant geometry type of the left RDD.
     * @param leftCount The estimated number of geometries in the left RDD.
     * @param leftMeanNumPoints The mean number of points of geometries in the left RDD.
     * @param rightGeomType The dominant geometry type of the right RDD.
     * @param rightCount The estimated number of geometries in the right RDD.
     * @param rightMeanNumPoints The mean number of points of geometries in the right RDD.
     * @param predicate The spatial predicate.
     * @param extent The extent of the partition.
     * @return The spatial join execution parameters.
     */
    private static LocalSpatialJoinExecParams determineSpatialJoinExecParams(
            GeometryType leftGeomType, long leftCount, double leftMeanNumPoints,
            GeometryType rightGeomType, long rightCount, double rightMeanNumPoints,
            SpatialPredicate predicate, Envelope extent) {
        // Always use STR-tree since it has better performance for most of the cases
        IndexType indexType = IndexType.RTREE;

        // Use the smaller side as the index build side
        IndexBuildSide buildSide = leftCount <= rightCount? IndexBuildSide.LEFT: IndexBuildSide.RIGHT;
        GeometryType indexGeomType;
        GeometryType streamGeomType;
        double indexMeanNumPoints;
        double streamMeanNumPoints;
        if (buildSide == IndexBuildSide.LEFT) {
            indexGeomType = leftGeomType;
            streamGeomType = rightGeomType;
            indexMeanNumPoints = leftMeanNumPoints;
            streamMeanNumPoints = rightMeanNumPoints;
        } else {
            indexGeomType = rightGeomType;
            streamGeomType = leftGeomType;
            indexMeanNumPoints = rightMeanNumPoints;
            streamMeanNumPoints = leftMeanNumPoints;

            // Make sure that the predicate is always applied as `predicate(indexSide, streamSide)`
            predicate = SpatialPredicate.inverse(predicate);
        }

        // Determine the execution mode
        ExecutionMode executionMode;
        switch (predicate) {
            case CONTAINS:
            case COVERS:
                executionMode = ExecutionMode.PREPARE_BUILD;
                break;

            case WITHIN:
            case COVERED_BY:
                executionMode = ExecutionMode.PREPARE_STREAM;
                break;

            case INTERSECTS:
                if (indexGeomType == GeometryType.POINT && streamGeomType != GeometryType.POINT) {
                    executionMode = ExecutionMode.PREPARE_STREAM;
                } else if (indexGeomType != GeometryType.POINT && streamGeomType == GeometryType.POINT) {
                    executionMode = ExecutionMode.PREPARE_BUILD;
                } else if (indexGeomType != GeometryType.POINT) {
                    // Both sides are not points. We need to determine the execution mode based on the complexity
                    // of the geometries.
                    if (streamMeanNumPoints > 0 && indexMeanNumPoints / streamMeanNumPoints > 10) {
                        // The index side is much more complex than the stream side. We create prepared geometries for
                        // the build side.
                        executionMode = ExecutionMode.PREPARE_BUILD;
                    } else {
                        // In all other cases, we create prepared geometries for the stream side. Preparing the stream
                        // side has better performance in general since it has a higher cache hit rate.
                        executionMode = ExecutionMode.PREPARE_STREAM;
                    }
                } else {
                    executionMode = ExecutionMode.PREPARE_STREAM;
                }
                break;

            default:
                executionMode = ExecutionMode.PREPARE_NONE;
        }
        return new LocalSpatialJoinExecParams(indexType, buildSide, executionMode, extent);
    }

    /**
     * Get the dominant geometry type of the RDD.
     * @param stat The statistics of the RDD.
     * @return The dominant geometry type.
     */
    private static GeometryType getDominantGeometryType(AdvancedStatCollector stat) {
        long numPoints = stat.getPuntalCount();
        long numLines = stat.getLinealCount();
        long numPolygons = stat.getPolygonalCount() + stat.getGeometryCollectionCount();
        if (numPoints > numPolygons && numPoints > numLines) {
            return GeometryType.POINT;
        } else if (numLines > numPoints && numLines > numPolygons) {
            return GeometryType.LINESTRING;
        } else {
            return GeometryType.POLYGON;
        }
    }

    /**
     * Get the number of geometries in each partition.
     * @param stat The statistics of the RDD.
     * @param partitioner The spatial partitioner.
     * @return The number of geometries in each partition.
     */
    private static long[] getPerPartitionGeometryCount(AdvancedStatCollector stat, SpatialPartitioner partitioner) {
        long[] perPartitionCount = new long[partitioner.numPartitions()];
        List<Envelope> samples = stat.getSampledEnvelopes();
        double factor = (samples.isEmpty()? 0.0: (double) stat.getCount() / samples.size());
        for (Envelope sample : samples) {
            try {
                Iterator<Tuple2<Integer, Geometry>> iter = partitioner.placeObject(factory.toGeometry(sample));
                while (iter.hasNext()) {
                    Tuple2<Integer, Geometry> entry = iter.next();
                    perPartitionCount[entry._1] += 1;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        for (int i = 0; i < perPartitionCount.length; i++) {
            perPartitionCount[i] = (long) (perPartitionCount[i] * factor);
        }
        return perPartitionCount;
    }

    @Override
    public Iterator<Pair<U, T>> call(Integer index, Iterator<U> leftIterator, Iterator<T> rightIterator) {
        if (!leftIterator.hasNext() || !rightIterator.hasNext()) {
            return Collections.emptyIterator();
        }
        List<LocalSpatialJoinExecParams> paramsList;
        if (localSpatialJoinExecParamsListBroadcast != null) {
            // This is the path taken by actual Spark job execution
            paramsList = localSpatialJoinExecParamsListBroadcast.value();
        } else if (localSpatialJoinExecParamsList != null) {
            // This path is for running unit tests without launching Spark jobs
            paramsList = localSpatialJoinExecParamsList;
        } else {
            throw new IllegalStateException("No per-partition local spatial join params available.");
        }
        if (index >= paramsList.size()) {
            throw new IllegalStateException("Invalid partition index: " + index +
                    ", total partitions: " + paramsList.size());
        }
        LocalSpatialJoinExecParams params = paramsList.get(index);
        HalfOpenRectangle extent = (params.extent != null? new HalfOpenRectangle(params.extent): null);

        switch (params.executionMode) {
            case PREPARE_BUILD:
                prepareBuildTasks.add(1);
                break;

            case PREPARE_STREAM:
                prepareStreamTasks.add(1);
                break;

            case PREPARE_NONE:
            default:
                break;
        }

        if (params.indexBuildSide == IndexBuildSide.LEFT) {
            buildLeftTasks.add(1);
            return new IndexedSpatialJoinIterator<>(leftIterator, rightIterator,
                    params.indexType, spatialPredicate, params.executionMode, extent,
                    buildCount, streamCount, resultCount, candidateCount, buildTime);
        } else {
            buildRightTasks.add(1);
            SpatialPredicate invSpatialPredicate = SpatialPredicate.inverse(spatialPredicate);
            IndexedSpatialJoinIterator<T, U> swappedJoinResultIterator = new IndexedSpatialJoinIterator<>(
                    rightIterator, leftIterator,
                    params.indexType, invSpatialPredicate, params.executionMode, extent,
                    buildCount, streamCount, resultCount, candidateCount, buildTime);
            return new SwapLeftAndRightIterator<>(swappedJoinResultIterator);
        }
    }

    /**
     * The actual heavy lifting of the local spatial join is done by this iterator.
     * @param <U> The type of the geometries on the build side
     * @param <T> The type of the geometries on the stream side
     */
    private static class IndexedSpatialJoinIterator<U extends Geometry, T extends Geometry>
            implements Iterator<Pair<U, T>> {
        private static final PreparedGeometryFactory PREPARED_GEOMETRY_FACTORY = new PreparedGeometryFactory();

        private final SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator;
        private final ExecutionMode executionMode;
        private final HalfOpenRectangle extent;
        private final SpatialIndex spatialIndex;
        private final Iterator<T> streamIterator;

        // Iterator state
        private final List<Pair<U, T>> batch = new ArrayList<>();
        private int batchIndex;

        // metrics
        private final SpatialJoinMetric metricStreamCount;
        private final SpatialJoinMetric metricResultCount;
        private final SpatialJoinMetric metricCandidateCount;

        IndexedSpatialJoinIterator(Iterator<U> buildIterator, Iterator<T> streamIterator,
                                   IndexType indexType, SpatialPredicate predicate, ExecutionMode executionMode,
                                   HalfOpenRectangle extent,
                                   SpatialJoinMetric buildCount,
                                   SpatialJoinMetric streamCount,
                                   SpatialJoinMetric resultCount,
                                   SpatialJoinMetric candidateCount,
                                   SpatialJoinMetric buildTime) {
            this.evaluator = SpatialPredicateEvaluators.create(predicate);
            this.executionMode = executionMode;
            this.extent = extent;
            long start = System.nanoTime();
            this.spatialIndex = buildSpatialIndex(buildIterator, indexType, executionMode, buildCount);
            buildTime.add(NANOSECONDS.toMillis(System.nanoTime() - start));
            this.streamIterator = streamIterator;
            this.batchIndex = 0;
            this.metricStreamCount = streamCount;
            this.metricResultCount = resultCount;
            this.metricCandidateCount = candidateCount;
        }

        private static <U extends Geometry> SpatialIndex buildSpatialIndex(Iterator<U> buildIterator,
                                                                           IndexType indexType,
                                                                           ExecutionMode executionMode,
                                                                           SpatialJoinMetric buildCount) {
            switch (indexType) {
                case RTREE:
                    return buildRTree(buildIterator, executionMode, buildCount);
                case QUADTREE:
                    return buildQuadTree(buildIterator, executionMode, buildCount);
                default:
                    throw new IllegalArgumentException("Unsupported index type: " + indexType);
            }
        }

        private static <U extends Geometry> SpatialIndex buildRTree(Iterator<U> buildIterator,
                                                                    ExecutionMode executionMode,
                                                                    SpatialJoinMetric buildCount) {
            STRtree spatialIndex = new STRtree();
            long count = 0;
            while (buildIterator.hasNext()) {
                count++;
                U geometry = buildIterator.next();
                Envelope envelope = geometry.getEnvelopeInternal();
                if (executionMode == ExecutionMode.PREPARE_BUILD) {
                    PreparedGeometry preparedGeometry = PREPARED_GEOMETRY_FACTORY.create(geometry);
                    spatialIndex.insert(envelope, preparedGeometry);
                } else {
                    spatialIndex.insert(envelope, geometry);
                }
            }
            spatialIndex.build();
            buildCount.add(count);
            return spatialIndex;
        }

        private static <U extends Geometry> SpatialIndex buildQuadTree(Iterator<U> buildIterator,
                                                                       ExecutionMode executionMode,
                                                                       SpatialJoinMetric buildCount) {
            Quadtree spatialIndex = new Quadtree();
            long count = 0;
            while (buildIterator.hasNext()) {
                count++;
                U geometry = buildIterator.next();
                Envelope envelope = geometry.getEnvelopeInternal();
                if (executionMode == ExecutionMode.PREPARE_BUILD) {
                    PreparedGeometry preparedGeometry = PREPARED_GEOMETRY_FACTORY.create(geometry);
                    spatialIndex.insert(envelope, preparedGeometry);
                } else {
                    spatialIndex.insert(envelope, geometry);
                }
            }
            buildCount.add(count);
            return spatialIndex;
        }

        @Override
        public boolean hasNext() {
            if (batch.isEmpty() || batchIndex >= batch.size()) {
                populateNextBatch();
            }

            return !batch.isEmpty();
        }

        @Override
        public Pair<U, T> next() {
            if (batch.isEmpty() || batchIndex >= batch.size()) {
                populateNextBatch();
            }

            if (batch.isEmpty()) {
                throw new NoSuchElementException();
            }

            Pair<U, T> pair = batch.get(batchIndex);
            batchIndex++;
            return pair;
        }

        private void populateNextBatch() {
            batch.clear();
            batchIndex = 0;
            switch (executionMode) {
                case PREPARE_BUILD:
                    populateNextBatchPrepareBuild();
                    break;

                case PREPARE_STREAM:
                    populateNextBatchPrepareStream();
                    break;

                case PREPARE_NONE:
                default:
                    populateNextBatchWithoutPreparedGeometry();
                    break;
            }
        }

        @SuppressWarnings("unchecked")
        private void populateNextBatchPrepareBuild() {
            long streamCount = 0;
            long candidateCount = 0;
            long resultCount = 0;

            while (streamIterator.hasNext()) {
                streamCount++;
                T geometry = streamIterator.next();
                Envelope envelope = geometry.getEnvelopeInternal();
                List<PreparedGeometry> candidates = (List<PreparedGeometry>) spatialIndex.query(envelope);
                candidateCount += candidates.size();
                for (PreparedGeometry candidate : candidates) {
                    if (evaluator.eval(candidate, geometry)) {
                        if (extent == null || !GeomUtils.isDuplicate(candidate.getGeometry(), geometry, extent)) {
                            batch.add(Pair.of((U) candidate.getGeometry(), geometry));
                        }
                        resultCount++;
                    }
                }
                if (!batch.isEmpty()) {
                    break;
                }
            }

            // Update statistics
            metricStreamCount.add(streamCount);
            metricCandidateCount.add(candidateCount);
            metricResultCount.add(resultCount);
        }

        @SuppressWarnings("unchecked")
        private void populateNextBatchPrepareStream() {
            long streamCount = 0;
            long candidateCount = 0;
            long resultCount = 0;

            while (streamIterator.hasNext()) {
                streamCount++;
                T geometry = streamIterator.next();
                Envelope envelope = geometry.getEnvelopeInternal();
                List<U> candidates = (List<U>) spatialIndex.query(envelope);
                if (candidates.isEmpty()) {
                    continue;
                } else {
                    candidateCount += candidates.size();
                    PreparedGeometry preparedGeometry = PREPARED_GEOMETRY_FACTORY.create(geometry);
                    for (U candidate : candidates) {
                        if (evaluator.eval(candidate, preparedGeometry)) {
                            if (extent == null || !GeomUtils.isDuplicate(candidate, geometry, extent)) {
                                batch.add(Pair.of(candidate, geometry));
                            }
                            resultCount++;
                        }
                    }
                }
                if (!batch.isEmpty()) {
                    break;
                }
            }

            // Update statistics
            metricStreamCount.add(streamCount);
            metricCandidateCount.add(candidateCount);
            metricResultCount.add(resultCount);
        }

        @SuppressWarnings("unchecked")
        private void populateNextBatchWithoutPreparedGeometry() {
            long streamCount = 0;
            long candidateCount = 0;
            long resultCount = 0;

            while (streamIterator.hasNext()) {
                streamCount++;
                T geometry = streamIterator.next();
                Envelope envelope = geometry.getEnvelopeInternal();
                List<U> candidates = (List<U>) spatialIndex.query(envelope);
                candidateCount += candidates.size();
                for (U candidate : candidates) {
                    if (evaluator.eval(candidate, geometry)) {
                        if (extent == null || !GeomUtils.isDuplicate(candidate, geometry, extent)) {
                            batch.add(Pair.of(candidate, geometry));
                        }
                        resultCount++;
                    }
                }
                if (!batch.isEmpty()) {
                    break;
                }
            }

            // Update statistics
            metricStreamCount.add(streamCount);
            metricCandidateCount.add(candidateCount);
            metricResultCount.add(resultCount);
        }
    }

    private static class SwapLeftAndRightIterator<U extends Geometry, T extends Geometry>
            implements Iterator<Pair<T, U>> {
        private final Iterator<Pair<U, T>> iterator;

        public SwapLeftAndRightIterator(Iterator<Pair<U, T>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Pair<T, U> next() {
            Pair<U, T> pair = iterator.next();
            return Pair.of(pair.getRight(), pair.getLeft());
        }
    }

    /**
     * Unify the interfaces of SQLMetric and LongAccumulator.
     * SQLMetric could be displayed in SQL query detail page and won't be displayed in the stage detail page,
     * while LongAccumulator could be displayed in the stage detail page.
     */
    private static class SpatialJoinMetric implements Serializable {
        private final SQLMetric sqlMetric;
        private final LongAccumulator accumulator;

        public SpatialJoinMetric(SQLMetric sqlMetric, LongAccumulator accumulator) {
            this.sqlMetric = sqlMetric;
            this.accumulator = accumulator;
        }

        public SpatialJoinMetric() {
            this(null, null);
        }

        public void add(long delta) {
            if (sqlMetric != null) {
                sqlMetric.add(delta);
            }
            if (accumulator != null) {
                accumulator.add(delta);
            }
        }
    }
}
