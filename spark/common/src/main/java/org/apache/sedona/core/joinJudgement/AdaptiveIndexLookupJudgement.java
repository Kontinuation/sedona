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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.subDivide.ExtentBasedGeometrySubDivider;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.HalfOpenRectangle;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinType;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.sedona.core.spatialPartitioning.OuterJoinSpatialPartitioner.OuterJoinUserData;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.spatialRddTool.AdvancedStatCollector;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
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

/**
 * Run local spatial join on spatial partitioned dataset RDD[U] and RDD[T]. The join algorithm used
 * for each spatial partition is determined by the statistics of the RDDs. It is adaptive to the
 * distribution of the data so that it can achieve better performance.
 *
 * @param <U> Geometry type of left side RDD
 * @param <T> Geometry type of right side RDD
 */
public class AdaptiveIndexLookupJudgement<U extends Geometry, T extends Geometry>
    implements Function3<Integer, Iterator<U>, Iterator<T>, Iterator<Pair<U, T>>>, Serializable {

  private static final GeometryFactory factory = new GeometryFactory();

  private final SpatialPredicate spatialPredicate;
  private final Function0<Function2<Geometry, Geometry, Boolean>> extraFilterCreator;
  private final JoinType joinType;

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
  private final transient List<LocalSpatialJoinExecParams> localSpatialJoinExecParamsList;
  private Broadcast<List<LocalSpatialJoinExecParams>> localSpatialJoinExecParamsListBroadcast =
      null;

  /** Parameters for local spatial join execution */
  public static class LocalSpatialJoinExecParams implements Serializable, KryoSerializable {
    private IndexType indexType;
    private IndexBuildSide indexBuildSide;
    private ExecutionMode executionMode;
    private Envelope extent; // For de-duplicating join results across spatial partitions
    private SubdivideOptions subdivideBuildOptions;
    private SubdivideOptions subdivideStreamOptions;

    public LocalSpatialJoinExecParams(
        IndexType indexType,
        IndexBuildSide indexBuildSide,
        ExecutionMode executionMode,
        Envelope extent) {
      this(indexType, indexBuildSide, executionMode, extent, null, null);
    }

    public LocalSpatialJoinExecParams(
        IndexType indexType,
        IndexBuildSide indexBuildSide,
        ExecutionMode executionMode,
        Envelope extent,
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
    /** Build spatial index on the left side */
    LEFT,

    /** Build spatial index on the right side */
    RIGHT
  }

  public AdaptiveIndexLookupJudgement(
      SpatialPredicate spatialPredicate,
      Function0<Function2<Geometry, Geometry, Boolean>> extraFilterCreator,
      JoinType joinType,
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
    this.extraFilterCreator = extraFilterCreator;
    this.joinType = joinType;
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
    this.localSpatialJoinExecParamsList =
        generatePerPartitionPlan(
            leftStat,
            rightStat,
            partitioner,
            spatialPredicate,
            leftSubdivideOptions,
            rightSubdivideOptions);
  }

  // Constructor for testing
  public AdaptiveIndexLookupJudgement(
      SpatialPredicate spatialPredicate,
      JoinType joinType,
      List<LocalSpatialJoinExecParams> localSpatialJoinExecParamsList) {
    this.spatialPredicate = spatialPredicate;
    this.extraFilterCreator = null;
    this.joinType = joinType;
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
   * Broadcast the local spatial join execution parameters. This should be called before running the
   * spark job for spatial join.
   *
   * @param sparkContext The spark context.
   */
  public void prepare(JavaSparkContext sparkContext) {
    if (localSpatialJoinExecParamsListBroadcast != null) {
      localSpatialJoinExecParamsListBroadcast.destroy();
    }
    localSpatialJoinExecParamsListBroadcast =
        sparkContext.broadcast(localSpatialJoinExecParamsList);
  }

  /**
   * Generate a per-partition execution plan for the local spatial joins.
   *
   * @param leftStat Statistics of the left RDD.
   * @param rightStat Statistics of the right RDD.
   * @param spatialPredicate The spatial predicate.
   * @param partitioner The spatial partitioner.
   * @param leftSubdivideOptions The subdivide options for the left geometries.
   * @param rightSubdivideOptions The subdivide options for the right geometries.
   */
  private static List<LocalSpatialJoinExecParams> generatePerPartitionPlan(
      AdvancedStatCollector leftStat,
      AdvancedStatCollector rightStat,
      SpatialPartitioner partitioner,
      SpatialPredicate spatialPredicate,
      SubdivideOptions leftSubdivideOptions,
      SubdivideOptions rightSubdivideOptions) {
    long[] leftPerPartitionCount = getPerPartitionGeometryCount(leftStat, partitioner);
    long[] rightPerPartitionCount = getPerPartitionGeometryCount(rightStat, partitioner);
    List<Envelope> grids = partitioner.getGrids();
    ArrayList<LocalSpatialJoinExecParams> plans = new ArrayList<>(grids.size());
    for (int k = 0; k < grids.size(); k++) {
      long leftCount = leftPerPartitionCount[k];
      long rightCount = rightPerPartitionCount[k];
      Envelope extent = grids.get(k);
      LocalSpatialJoinExecParams plan =
          determineSpatialJoinExecParams(
              leftStat,
              leftCount,
              rightStat,
              rightCount,
              spatialPredicate,
              extent,
              leftSubdivideOptions,
              rightSubdivideOptions);
      plans.add(plan);
    }
    return plans;
  }

  /**
   * Determine the spatial join execution parameters for a partition.
   *
   * @param leftStat Statistics of the left RDD.
   * @param leftCount The estimated number of geometries in the left RDD.
   * @param rightStat Statistics of the right RDD.
   * @param rightCount The estimated number of geometries in the right RDD.
   * @param predicate The spatial predicate.
   * @param extent The extent of the partition.
   * @param leftSubdivideOptions The subdivide options for the left geometries.
   * @param rightSubdivideOptions The subdivide options for the right geometries.
   * @return The spatial join execution parameters.
   */
  private static LocalSpatialJoinExecParams determineSpatialJoinExecParams(
      AdvancedStatCollector leftStat,
      long leftCount,
      AdvancedStatCollector rightStat,
      long rightCount,
      SpatialPredicate predicate,
      Envelope extent,
      SubdivideOptions leftSubdivideOptions,
      SubdivideOptions rightSubdivideOptions) {
    // Always use STR-tree since it has better performance for most of the cases
    IndexType indexType = IndexType.RTREE;

    // Use the smaller side as the index build side
    IndexBuildSide buildSide = leftCount <= rightCount ? IndexBuildSide.LEFT : IndexBuildSide.RIGHT;
    AdvancedStatCollector indexedStat;
    AdvancedStatCollector streamStat;
    SubdivideOptions subdivideBuildOptions;
    SubdivideOptions subdivideStreamOptions;
    if (buildSide == IndexBuildSide.LEFT) {
      indexedStat = leftStat;
      streamStat = rightStat;
      subdivideBuildOptions = leftSubdivideOptions;
      subdivideStreamOptions = rightSubdivideOptions;
    } else {
      indexedStat = rightStat;
      streamStat = leftStat;
      subdivideBuildOptions = rightSubdivideOptions;
      subdivideStreamOptions = leftSubdivideOptions;

      // Make sure that the predicate is always applied as `predicate(indexSide, streamSide)`
      predicate = SpatialPredicate.inverse(predicate);
    }

    // Determine the execution mode
    ExecutionMode executionMode =
        ExecutionMode.getOptimalExecutionMode(predicate, indexedStat, streamStat);

    return new LocalSpatialJoinExecParams(
        indexType, buildSide, executionMode, extent, subdivideBuildOptions, subdivideStreamOptions);
  }

  /**
   * Get the number of geometries in each partition.
   *
   * @param stat The statistics of the RDD.
   * @param partitioner The spatial partitioner.
   * @return The number of geometries in each partition.
   */
  private static long[] getPerPartitionGeometryCount(
      AdvancedStatCollector stat, SpatialPartitioner partitioner) {
    long[] perPartitionCount = new long[partitioner.numPartitions()];
    List<Envelope> samples = stat.getSampledEnvelopes();
    double factor = (samples.isEmpty() ? 0.0 : (double) stat.getCount() / samples.size());
    for (Envelope sample : samples) {
      try {
        Iterator<Tuple2<Integer, Geometry>> iter =
            partitioner.placeObject(factory.toGeometry(sample));
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
  public Iterator<Pair<U, T>> call(
      Integer index, Iterator<U> leftIterator, Iterator<T> rightIterator) {
    if (joinType == JoinType.INNER && (!leftIterator.hasNext() || !rightIterator.hasNext())) {
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
      if (joinType == JoinType.LEFT_OUTER) {
        return new OuterJoinIterators.LeftOuterJoinIterator<>(leftIterator);
      } else if (joinType == JoinType.RIGHT_OUTER) {
        return new OuterJoinIterators.RightOuterJoinIterator<>(rightIterator);
      } else if (joinType == JoinType.FULL_OUTER) {
        return new OuterJoinIterators.FullOuterJoinIterator<>(leftIterator, rightIterator);
      } else if (joinType == JoinType.INNER) {
        throw new IllegalStateException(
            "Spatial partitions for inner join should not have out-of-bounds partitions");
      } else {
        throw new UnsupportedOperationException("Unsupported join type: " + joinType);
      }
    }
    LocalSpatialJoinExecParams params = paramsList.get(index);
    HalfOpenRectangle extent =
        (params.extent != null ? new HalfOpenRectangle(params.extent) : null);

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
      Function2<Geometry, Geometry, Boolean> extraFilter = null;
      if (extraFilterCreator != null) {
        try {
          extraFilter = extraFilterCreator.call();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      LocalJoinType localJoinType;
      switch (joinType) {
        case INNER:
          localJoinType = LocalJoinType.INNER;
          break;
        case LEFT_OUTER:
          localJoinType = LocalJoinType.INDEX_OUTER;
          break;
        case RIGHT_OUTER:
          localJoinType = LocalJoinType.STREAM_OUTER;
          break;
        default:
          throw new UnsupportedOperationException("Unsupported join type: " + joinType);
      }
      return new IndexedSpatialJoinIterator<>(
          leftIterator,
          rightIterator,
          localJoinType,
          params.indexType,
          spatialPredicate,
          extraFilter,
          params.executionMode,
          extent,
          params.subdivideBuildOptions,
          params.subdivideStreamOptions,
          buildCount,
          streamCount,
          resultCount,
          candidateCount,
          buildTime);
    } else {
      buildRightTasks.add(1);
      SpatialPredicate invSpatialPredicate = SpatialPredicate.inverse(spatialPredicate);
      Function2<Geometry, Geometry, Boolean> extraFilter = null;
      if (extraFilterCreator != null) {
        try {
          extraFilter = new SwappedExtraFilter(extraFilterCreator.call());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      LocalJoinType localJoinType;
      switch (joinType) {
        case INNER:
          localJoinType = LocalJoinType.INNER;
          break;
        case LEFT_OUTER:
          localJoinType = LocalJoinType.STREAM_OUTER;
          break;
        case RIGHT_OUTER:
          localJoinType = LocalJoinType.INDEX_OUTER;
          break;
        default:
          throw new UnsupportedOperationException("Unsupported join type: " + joinType);
      }
      IndexedSpatialJoinIterator<T, U> swappedJoinResultIterator =
          new IndexedSpatialJoinIterator<>(
              rightIterator,
              leftIterator,
              localJoinType,
              params.indexType,
              invSpatialPredicate,
              extraFilter,
              params.executionMode,
              extent,
              params.subdivideBuildOptions,
              params.subdivideStreamOptions,
              buildCount,
              streamCount,
              resultCount,
              candidateCount,
              buildTime);
      return new SwapLeftAndRightIterator<>(swappedJoinResultIterator);
    }
  }

  private enum LocalJoinType {
    INNER,
    INDEX_OUTER,
    STREAM_OUTER
  }

  /**
   * The actual heavy lifting of the local spatial join is done by this iterator.
   *
   * @param <U> The type of the geometries on the build side
   * @param <T> The type of the geometries on the stream side
   */
  private static class IndexedSpatialJoinIterator<U extends Geometry, T extends Geometry>
      implements Iterator<Pair<U, T>> {
    private static final PreparedGeometryFactory PREPARED_GEOMETRY_FACTORY =
        new PreparedGeometryFactory();

    private final LocalJoinType localJoinType;
    private final SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator;
    private final Function2<Geometry, Geometry, Boolean> extraFilter;
    private final ExecutionMode executionMode;
    private final HalfOpenRectangle extent;
    private final SubdivideOptions subdivideBuildOptions;
    private final SubdivideOptions subdivideStreamOptions;
    private final SpatialIndex spatialIndex;
    private final ArrayList<Object> indexedGeometries;
    private boolean[] indexedGeometryHasJoinResults = null;
    private final Iterator<T> streamIterator;

    // Iterator state
    private final List<Pair<U, T>> batch = new ArrayList<>();
    private boolean populatedIndexOuterBatch = false;
    private int batchIndex;

    // metrics
    private final SpatialJoinMetric metricStreamCount;
    private final SpatialJoinMetric metricResultCount;
    private final SpatialJoinMetric metricCandidateCount;

    IndexedSpatialJoinIterator(
        Iterator<U> buildIterator,
        Iterator<T> streamIterator,
        LocalJoinType localJoinType,
        IndexType indexType,
        SpatialPredicate predicate,
        Function2<Geometry, Geometry, Boolean> extraFilter,
        ExecutionMode executionMode,
        HalfOpenRectangle extent,
        SubdivideOptions subdivideBuildOptions,
        SubdivideOptions subdivideStreamOptions,
        SpatialJoinMetric buildCount,
        SpatialJoinMetric streamCount,
        SpatialJoinMetric resultCount,
        SpatialJoinMetric candidateCount,
        SpatialJoinMetric buildTime) {
      this.localJoinType = localJoinType;
      this.evaluator = SpatialPredicateEvaluators.create(predicate);
      this.extraFilter = extraFilter;
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

    private SpatialIndex buildSpatialIndex(
        Iterator<U> buildIterator, IndexType indexType, SpatialJoinMetric buildCount) {
      SpatialIndex spatialIndex = createEmptySpatialIndex(indexType);
      ExtentBasedGeometrySubDivider subDivider =
          subdivideBuildOptions != null
              ? new ExtentBasedGeometrySubDivider(subdivideBuildOptions)
              : null;
      int count = 0;
      for (; buildIterator.hasNext(); count++) {
        U geometry = buildIterator.next();
        if (executionMode == ExecutionMode.PREPARE_BUILD) {
          PreparedGeometry preparedGeometry = PREPARED_GEOMETRY_FACTORY.create(geometry);
          indexedGeometries.add(preparedGeometry);
        } else {
          indexedGeometries.add(geometry);
        }
        if (subDivider == null) {
          // No subdivision, put the index of the original geometry or prepared geometry in
          // the array into the spatial index
          Envelope envelope = geometry.getEnvelopeInternal();
          spatialIndex.insert(envelope, count);
        } else {
          // With subdivision, put subdivided parts into the spatial index, with an index into the
          // array containing the original geometries
          Iterator<Geometry> subGeomIter = subDivider.subdivide(geometry);
          while (subGeomIter.hasNext()) {
            Geometry geom = subGeomIter.next();
            Envelope envelope = geom.getEnvelopeInternal();
            spatialIndex.insert(envelope, count);
          }
        }
      }
      if (indexType == IndexType.RTREE) {
        ((STRtree) spatialIndex).build();
      }
      indexedGeometryHasJoinResults = new boolean[count];
      Arrays.fill(indexedGeometryHasJoinResults, false);
      buildCount.add(count);
      return spatialIndex;
    }

    private static SpatialIndex createEmptySpatialIndex(IndexType indexType) {
      switch (indexType) {
        case RTREE:
          return new STRtree();
        case QUADTREE:
          return new Quadtree();
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
        List<Integer> candidateIndexes = (List<Integer>) spatialIndex.query(envelope);
        candidateCount += candidateIndexes.size();
        for (int candidateIndex : candidateIndexes) {
          PreparedGeometry candidate = (PreparedGeometry) indexedGeometries.get(candidateIndex);
          if (evaluator.eval(candidate, geometry)) {
            if (extent == null
                || !GeomUtils.isDuplicate(candidate.getGeometry(), geometry, extent)) {
              try {
                if (extraFilter == null || extraFilter.call(candidate.getGeometry(), geometry)) {
                  indexedGeometryHasJoinResults[candidateIndex] = true;
                  batch.add(Pair.of((U) candidate.getGeometry(), geometry));
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
            resultCount++;
          }
        }
        if (!batch.isEmpty()) {
          break;
        } else {
          if (tryPopulateStreamOuterBatch(geometry)) {
            break;
          }
        }
      }

      // When we get here and the batch is still empty, we must have consumed all geometries from
      // the stream side. We may need to populate elements for index outer join.
      if (batch.isEmpty()) {
        populateIndexOuterBatch();
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
        List<Integer> candidateIndexes = (List<Integer>) spatialIndex.query(envelope);
        if (!candidateIndexes.isEmpty()) {
          candidateCount += candidateIndexes.size();
          PreparedGeometry preparedGeometry = PREPARED_GEOMETRY_FACTORY.create(geometry);
          for (int candidateIndex : candidateIndexes) {
            U candidate = (U) indexedGeometries.get(candidateIndex);
            if (evaluator.eval(candidate, preparedGeometry)) {
              if (extent == null || !GeomUtils.isDuplicate(candidate, geometry, extent)) {
                try {
                  if (extraFilter == null || extraFilter.call(candidate, geometry)) {
                    indexedGeometryHasJoinResults[candidateIndex] = true;
                    batch.add(Pair.of(candidate, geometry));
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
              resultCount++;
            }
          }
        }
        if (!batch.isEmpty()) {
          break;
        } else {
          if (tryPopulateStreamOuterBatch(geometry)) {
            break;
          }
        }
      }

      // When we get here and the batch is still empty, we must have consumed all geometries from
      // the stream side. We may need to populate elements for index outer join.
      if (batch.isEmpty()) {
        populateIndexOuterBatch();
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
        List<Integer> candidateIndexes = (List<Integer>) spatialIndex.query(envelope);
        candidateCount += candidateIndexes.size();
        for (int candidateIndex : candidateIndexes) {
          U candidate = (U) indexedGeometries.get(candidateIndex);
          if (evaluator.eval(candidate, geometry)) {
            if (extent == null || !GeomUtils.isDuplicate(candidate, geometry, extent)) {
              try {
                if (extraFilter == null || extraFilter.call(candidate, geometry)) {
                  indexedGeometryHasJoinResults[candidateIndex] = true;
                  batch.add(Pair.of(candidate, geometry));
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
            resultCount++;
          }
        }
        if (!batch.isEmpty()) {
          break;
        } else {
          if (tryPopulateStreamOuterBatch(geometry)) {
            break;
          }
        }
      }

      // When we get here and the batch is still empty, we must have consumed all geometries from
      // the stream side. We may need to populate elements for index outer join.
      if (batch.isEmpty()) {
        populateIndexOuterBatch();
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

      ExtentBasedGeometrySubDivider streamSubDivider =
          subdivideStreamOptions != null
              ? new ExtentBasedGeometrySubDivider(subdivideStreamOptions)
              : null;
      while (streamIterator.hasNext()) {
        streamCount++;
        T geometry = streamIterator.next();
        List<Object> candidates = new ArrayList<>();
        if (streamSubDivider != null) {
          // If the stream side should be subdivided, we need to first subdivide the geometry and
          // then query
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

        if (!candidates.isEmpty()) {
          // Now there are 3 cases:
          // 1. The retrieved candidates are prepared geometries
          // 2. The retrieved candidates are ordinary geometries
          // 3. The retrieved candidates are subdivided parts of the original indexed geometries
          // In all cases, we need to deduplicate the candidates before evaluating the spatial
          // predicate.
          HashSet<Integer> candidateIndexes = new HashSet<>();
          for (Object candidate : candidates) {
            candidateIndexes.add((Integer) candidate);
          }
          candidateCount += candidateIndexes.size();

          // Evaluate spatial predicate on candidates
          PreparedGeometry preparedGeometry =
              (executionMode == ExecutionMode.PREPARE_STREAM)
                  ? PREPARED_GEOMETRY_FACTORY.create(geometry)
                  : null;
          for (int candidateIndex : candidateIndexes) {
            Object candidateObj = indexedGeometries.get(candidateIndex);
            Geometry candidateGeom;
            boolean evalResult;
            if (executionMode == ExecutionMode.PREPARE_BUILD) {
              PreparedGeometry candidate = (PreparedGeometry) candidateObj;
              candidateGeom = candidate.getGeometry();
              evalResult = evaluator.eval(candidate, geometry);
            } else {
              Geometry candidate = (Geometry) candidateObj;
              candidateGeom = candidate;
              evalResult =
                  preparedGeometry != null
                      ? evaluator.eval(candidate, preparedGeometry)
                      : evaluator.eval(candidate, geometry);
            }
            if (evalResult) {
              if (extent == null || !GeomUtils.isDuplicate(candidateGeom, geometry, extent)) {
                try {
                  if (extraFilter == null || extraFilter.call(candidateGeom, geometry)) {
                    indexedGeometryHasJoinResults[candidateIndex] = true;
                    batch.add(Pair.of((U) candidateGeom, geometry));
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
              resultCount++;
            }
          }
        }

        if (!batch.isEmpty()) {
          break;
        } else {
          if (tryPopulateStreamOuterBatch(geometry)) {
            break;
          }
        }
      }

      // When we get here and the batch is still empty, we must have consumed all geometries from
      // the stream side. We may need to populate elements for index outer join.
      if (batch.isEmpty()) {
        populateIndexOuterBatch();
      }

      // Update statistics
      metricStreamCount.add(streamCount);
      metricCandidateCount.add(candidateCount);
      metricResultCount.add(resultCount);
    }

    @SuppressWarnings("unchecked")
    private void populateIndexOuterBatch() {
      if (localJoinType == LocalJoinType.INDEX_OUTER && !populatedIndexOuterBatch) {
        populatedIndexOuterBatch = true;
        // Walk through all primary geometries in the index side that has no emitted join results,
        // and emit a record with stream side = null for them.
        for (int k = 0; k < indexedGeometryHasJoinResults.length; k++) {
          if (!indexedGeometryHasJoinResults[k]) {
            Object indexedObj = indexedGeometries.get(k);
            U geometry;
            if (executionMode == ExecutionMode.PREPARE_BUILD) {
              geometry = (U) ((PreparedGeometry) indexedObj).getGeometry();
            } else {
              geometry = (U) indexedObj;
            }
            OuterJoinUserData userData = (OuterJoinUserData) geometry.getUserData();
            if (userData.isPrimary) {
              batch.add(Pair.of(geometry, null));
            }
          }
        }
      }
    }

    private boolean tryPopulateStreamOuterBatch(T geometry) {
      // Check if the stream side is the primary geometry. if it is, we should emit a
      // record with index side = null
      if (localJoinType != LocalJoinType.STREAM_OUTER) {
        return false;
      }
      OuterJoinUserData userData = (OuterJoinUserData) geometry.getUserData();
      if (!userData.isPrimary) {
        return false;
      }
      batch.add(Pair.of(null, geometry));
      return true;
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

  private static class SwappedExtraFilter implements Function2<Geometry, Geometry, Boolean> {
    final Function2<Geometry, Geometry, Boolean> wrapped;

    SwappedExtraFilter(Function2<Geometry, Geometry, Boolean> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public Boolean call(Geometry v1, Geometry v2) throws Exception {
      return wrapped.call(v2, v1);
    }
  }

  /**
   * Unify the interfaces of SQLMetric and LongAccumulator. SQLMetric could be displayed in SQL
   * query detail page and won't be displayed in the stage detail page, while LongAccumulator could
   * be displayed in the stage detail page.
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
