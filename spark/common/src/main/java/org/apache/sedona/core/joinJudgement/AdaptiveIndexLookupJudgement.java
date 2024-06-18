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
import org.apache.sedona.common.subDivide.ExtentBasedGeometrySubDivider;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.HalfOpenRectangle;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.spatialRddTool.AdvancedStatCollector;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.util.DoubleAccumulator;
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
import java.util.HashSet;
import java.util.IdentityHashMap;
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

    private final DoubleAccumulator partitionMinX;
    private final DoubleAccumulator partitionMinY;
    private final DoubleAccumulator partitionMaxX;
    private final DoubleAccumulator partitionMaxY;

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
        private SubdivideOptions subdivideBuildOptions;
        private SubdivideOptions subdivideStreamOptions;

        public LocalSpatialJoinExecParams(IndexType indexType, IndexBuildSide indexBuildSide,
                                          ExecutionMode executionMode, Envelope extent) {
            this(indexType, indexBuildSide, executionMode, extent, null, null);
        }

        public LocalSpatialJoinExecParams(IndexType indexType, IndexBuildSide indexBuildSide,
                                          ExecutionMode executionMode, Envelope extent,
                                          SubdivideOptions subdivideBuildOptions,
                                          SubdivideOptions subdivideStreamOptions) {
            this.indexType = indexType;
            this.indexBuildSide = indexBuildSide;
            this.executionMode = executionMode;
            this.extent = extent;
            this.subdivideBuildOptions = subdivideBuildOptions;
            this.subdivideStreamOptions = subdivideStreamOptions;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeInt(indexType.ordinal());
            output.writeInt(indexBuildSide.ordinal());
            output.writeInt(executionMode.ordinal());
            kryo.writeObject(output, extent);
            if (subdivideBuildOptions != null) {
                output.writeBoolean(true);
                subdivideBuildOptions.write(kryo, output);
            } else {
                output.writeBoolean(false);
            }
            if (subdivideStreamOptions != null) {
                output.writeBoolean(true);
                subdivideStreamOptions.write(kryo, output);
            } else {
                output.writeBoolean(false);
            }
        }

        @Override
        public void read(Kryo kryo, Input input) {
            indexType = IndexType.values()[input.readInt()];
            indexBuildSide = IndexBuildSide.values()[input.readInt()];
            executionMode = ExecutionMode.values()[input.readInt()];
            extent = kryo.readObject(input, Envelope.class);
            if (input.readBoolean()) {
                subdivideBuildOptions = new SubdivideOptions();
                subdivideBuildOptions.read(kryo, input);
            } else {
                subdivideBuildOptions = null;
            }
            if (input.readBoolean()) {
                subdivideStreamOptions = new SubdivideOptions();
                subdivideStreamOptions.read(kryo, input);
            } else {
                subdivideStreamOptions = null;
            }
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
            SedonaConf sedonaConf,
            LongAccumulator buildCount,
            LongAccumulator streamCount,
            LongAccumulator resultCount,
            LongAccumulator candidateCount,
            LongAccumulator buildTime,
            LongAccumulator buildLeftTasks,
            LongAccumulator buildRightTasks,
            LongAccumulator prepareBuildTasks,
            LongAccumulator prepareStreamTasks,
            DoubleAccumulator partitionMinX,
            DoubleAccumulator partitionMinY,
            DoubleAccumulator partitionMaxX,
            DoubleAccumulator partitionMaxY,
            SQLMetric sqlBuildCount,
            SQLMetric sqlStreamCount,
            SQLMetric sqlResultCount,
            SQLMetric sqlCandidateCount,
            SQLMetric sqlBuildTime,
            SQLMetric sqlBuildLeftTasks,
            SQLMetric sqlBuildRightTasks,
            SQLMetric sqlPrepareBuildTasks,
            SQLMetric sqlPrepareStreamTasks,
            SubdivideOptions leftSubdivideOptions,
            SubdivideOptions rightSubdivideOptions) {
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
        this.partitionMinX = partitionMinX;
        this.partitionMinY = partitionMinY;
        this.partitionMaxX = partitionMaxX;
        this.partitionMaxY = partitionMaxY;
        this.localSpatialJoinExecParamsList = generatePerPartitionPlan(leftStat, rightStat, partitioner,
                spatialPredicate, leftSubdivideOptions, rightSubdivideOptions);
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
        this.partitionMinX = null;
        this.partitionMinY = null;
        this.partitionMaxX = null;
        this.partitionMaxY = null;
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
     * @param leftSubdivideOptions The subdivide options for the left geometries.
     * @param rightSubdivideOptions The subdivide options for the right geometries.
     */
    private static List<LocalSpatialJoinExecParams> generatePerPartitionPlan(
            AdvancedStatCollector leftStat, AdvancedStatCollector rightStat, SpatialPartitioner partitioner,
            SpatialPredicate spatialPredicate,
            SubdivideOptions leftSubdivideOptions, SubdivideOptions rightSubdivideOptions) {
        GeometryType leftGeomType = leftStat.getDominantGeometryType();
        GeometryType rightGeomType = rightStat.getDominantGeometryType();
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
                    spatialPredicate, extent, leftSubdivideOptions, rightSubdivideOptions);
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
     * @param leftSubdivideOptions The subdivide options for the left geometries.
     * @param rightSubdivideOptions The subdivide options for the right geometries.
     * @return The spatial join execution parameters.
     */
    private static LocalSpatialJoinExecParams determineSpatialJoinExecParams(
            GeometryType leftGeomType, long leftCount, double leftMeanNumPoints,
            GeometryType rightGeomType, long rightCount, double rightMeanNumPoints,
            SpatialPredicate predicate, Envelope extent,
            SubdivideOptions leftSubdivideOptions, SubdivideOptions rightSubdivideOptions) {
        // Always use STR-tree since it has better performance for most of the cases
        IndexType indexType = IndexType.RTREE;

        // Use the smaller side as the index build side
        IndexBuildSide buildSide = leftCount <= rightCount? IndexBuildSide.LEFT: IndexBuildSide.RIGHT;
        GeometryType indexGeomType;
        GeometryType streamGeomType;
        double indexMeanNumPoints;
        double streamMeanNumPoints;
        SubdivideOptions subdivideBuildOptions;
        SubdivideOptions subdivideStreamOptions;
        if (buildSide == IndexBuildSide.LEFT) {
            indexGeomType = leftGeomType;
            streamGeomType = rightGeomType;
            indexMeanNumPoints = leftMeanNumPoints;
            streamMeanNumPoints = rightMeanNumPoints;
            subdivideBuildOptions = leftSubdivideOptions;
            subdivideStreamOptions = rightSubdivideOptions;
        } else {
            indexGeomType = rightGeomType;
            streamGeomType = leftGeomType;
            indexMeanNumPoints = rightMeanNumPoints;
            streamMeanNumPoints = leftMeanNumPoints;
            subdivideBuildOptions = rightSubdivideOptions;
            subdivideStreamOptions = leftSubdivideOptions;

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

        return new LocalSpatialJoinExecParams(indexType, buildSide, executionMode, extent,
                subdivideBuildOptions, subdivideStreamOptions);
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

        if (extent != null) {
            // Add the extent of the partition to metrics for easier identifying straggler tasks.
            // We can know the extent of the spatial partition of slow tasks.
            Envelope env = extent.getEnvelope();
            if (this.partitionMinX != null) {
                this.partitionMinX.add(env.getMinX());
            }
            if (this.partitionMinY != null) {
                this.partitionMinY.add(env.getMinY());
            }
            if (this.partitionMaxX != null) {
                this.partitionMaxX.add(env.getMaxX());
            }
            if (this.partitionMaxY != null) {
                this.partitionMaxY.add(env.getMaxY());
            }
        }

        if (params.indexBuildSide == IndexBuildSide.LEFT) {
            buildLeftTasks.add(1);
            return new IndexedSpatialJoinIterator<>(leftIterator, rightIterator,
                    params.indexType, spatialPredicate, params.executionMode, extent,
                    params.subdivideBuildOptions, params.subdivideStreamOptions,
                    buildCount, streamCount, resultCount, candidateCount, buildTime);
        } else {
            buildRightTasks.add(1);
            SpatialPredicate invSpatialPredicate = SpatialPredicate.inverse(spatialPredicate);
            IndexedSpatialJoinIterator<T, U> swappedJoinResultIterator = new IndexedSpatialJoinIterator<>(
                    rightIterator, leftIterator,
                    params.indexType, invSpatialPredicate, params.executionMode, extent,
                    params.subdivideBuildOptions, params.subdivideStreamOptions,
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
        private final SubdivideOptions subdivideBuildOptions;
        private final SubdivideOptions subdivideStreamOptions;
        private final SpatialIndex spatialIndex;
        private final ArrayList<Object> indexedGeometries;  // only used when indexed geometries are subdivided
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
                                   SubdivideOptions subdivideBuildOptions,
                                   SubdivideOptions subdivideStreamOptions,
                                   SpatialJoinMetric buildCount,
                                   SpatialJoinMetric streamCount,
                                   SpatialJoinMetric resultCount,
                                   SpatialJoinMetric candidateCount,
                                   SpatialJoinMetric buildTime) {
            this.evaluator = SpatialPredicateEvaluators.create(predicate);
            this.executionMode = executionMode;
            this.extent = extent;
            this.subdivideBuildOptions = subdivideBuildOptions;
            this.subdivideStreamOptions = subdivideStreamOptions;
            this.indexedGeometries = new ArrayList<>();
            long start = System.nanoTime();
            this.spatialIndex = buildSpatialIndex(buildIterator, indexType, buildCount);
            buildTime.add(NANOSECONDS.toMillis(System.nanoTime() - start));
            this.streamIterator = streamIterator;
            this.batchIndex = 0;
            this.metricStreamCount = streamCount;
            this.metricResultCount = resultCount;
            this.metricCandidateCount = candidateCount;
        }

        private SpatialIndex buildSpatialIndex(Iterator<U> buildIterator, IndexType indexType,
                                               SpatialJoinMetric buildCount) {
            SpatialIndex spatialIndex = createEmptySpatialIndex(indexType);
            ExtentBasedGeometrySubDivider subDivider = subdivideBuildOptions != null?
                    new ExtentBasedGeometrySubDivider(subdivideBuildOptions): null;
            long count = 0;
            for ( ; buildIterator.hasNext(); count++) {
                U geometry = buildIterator.next();
                if (subDivider == null) {
                    // No subdivision, put the original geometry or prepared geometry into the spatial index
                    Envelope envelope = geometry.getEnvelopeInternal();
                    if (executionMode == ExecutionMode.PREPARE_BUILD) {
                        PreparedGeometry preparedGeometry = PREPARED_GEOMETRY_FACTORY.create(geometry);
                        spatialIndex.insert(envelope, preparedGeometry);
                    } else {
                        spatialIndex.insert(envelope, geometry);
                    }
                } else {
                    // With subdivision, put subdivided parts into the spatial index, with an index into the array
                    // containing the original geometries
                    if (executionMode == ExecutionMode.PREPARE_BUILD) {
                        PreparedGeometry preparedGeometry = PREPARED_GEOMETRY_FACTORY.create(geometry);
                        indexedGeometries.add(preparedGeometry);
                    } else {
                        indexedGeometries.add(geometry);
                    }
                    Iterator<Geometry> subGeomIter = subDivider.subdivide(geometry);
                    while (subGeomIter.hasNext()) {
                        Geometry geom = subGeomIter.next();
                        Envelope envelope = geom.getEnvelopeInternal();
                        spatialIndex.insert(envelope, (int) count);
                    }
                }
            }
            if (indexType == IndexType.RTREE) {
                ((STRtree) spatialIndex).build();
            }
            buildCount.add(count);
            return spatialIndex;
        }

        private static SpatialIndex createEmptySpatialIndex(IndexType indexType) {
            switch (indexType) {
                case RTREE:
                    return new STRtree();
                case QUADTREE:
                    return  new Quadtree();
                default:
                    throw new IllegalArgumentException("Unsupported index type: " + indexType);
            }
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
            if (subdivideBuildOptions != null || subdivideStreamOptions != null) {
                populateNextBatchWithSubdivision();
            } else {
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

        @SuppressWarnings("unchecked")
        private void populateNextBatchWithSubdivision() {
            long streamCount = 0;
            long candidateCount = 0;
            long resultCount = 0;

            ExtentBasedGeometrySubDivider streamSubDivider = subdivideStreamOptions != null?
                    new ExtentBasedGeometrySubDivider(subdivideStreamOptions): null;
            while (streamIterator.hasNext()) {
                streamCount++;
                T geometry = streamIterator.next();
                List<Object> candidates = new ArrayList<>();
                if (streamSubDivider != null) {
                    // If the stream side should be subdivided, we need to first subdivide the geometry and then query
                    // the spatial index.
                    Iterator<Geometry> subGeomIter = streamSubDivider.subdivide(geometry);
                    while (subGeomIter.hasNext()) {
                        Geometry subGeom = subGeomIter.next();
                        Envelope envelope = subGeom.getEnvelopeInternal();
                        List<Object> results = spatialIndex.query(envelope);
                        candidates.addAll(results);
                    }
                } else {
                    Envelope envelope = geometry.getEnvelopeInternal();
                    candidates.addAll(spatialIndex.query(envelope));
                }

                // Now there are 3 cases:
                // 1. The retrieved candidates are prepared geometries
                // 2. The retrieved candidates are ordinary geometries
                // 3. The retrieved candidates are subdivided parts of the original indexed geometries
                // In all cases, we need to deduplicate the candidates before evaluating the spatial predicate.
                List<Object> deduplicatedCandidates = new ArrayList<>();
                if (subdivideBuildOptions != null) {
                    // Case 3: deduplicate the candidate indexes and retrieve the candidates
                    HashSet<Integer> candidateIndexes = new HashSet<>();
                    for (Object candidate : candidates) {
                        candidateIndexes.add((Integer) candidate);
                    }
                    for (int candidateIndex : candidateIndexes) {
                        deduplicatedCandidates.add(indexedGeometries.get(candidateIndex));
                    }
                } else {
                    // Case 1 or 2: deduplicate the candidates by referential equality
                    IdentityHashMap<Object, Boolean> candidateMap = new IdentityHashMap<>();
                    for (Object candidate : candidates) {
                        candidateMap.put(candidate, true);
                    }
                    deduplicatedCandidates.addAll(candidateMap.keySet());
                }
                if (deduplicatedCandidates.isEmpty()) {
                    continue;
                }
                candidateCount += deduplicatedCandidates.size();

                // Evaluate spatial predicate on candidates
                PreparedGeometry preparedGeometry = (executionMode == ExecutionMode.PREPARE_STREAM)?
                        PREPARED_GEOMETRY_FACTORY.create(geometry): null;
                for (Object candidateObj : deduplicatedCandidates) {
                    Geometry candidateGeom;
                    boolean evalResult;
                    if (executionMode == ExecutionMode.PREPARE_BUILD) {
                        PreparedGeometry candidate = (PreparedGeometry) candidateObj;
                        candidateGeom = candidate.getGeometry();
                        evalResult = evaluator.eval(candidate, geometry);
                    } else {
                        Geometry candidate = (Geometry) candidateObj;
                        candidateGeom = candidate;
                        evalResult = preparedGeometry != null?
                                evaluator.eval(candidate, preparedGeometry):
                                evaluator.eval(candidate, geometry);
                    }
                    if (evalResult) {
                        if (extent == null || !GeomUtils.isDuplicate(candidateGeom, geometry, extent)) {
                            batch.add(Pair.of((U) candidateGeom, geometry));
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
