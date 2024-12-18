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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.common.utils.HalfOpenRectangle;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinType;
import org.apache.sedona.core.enums.LocalJoinType;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.spatialRddTool.AdvancedStatCollector;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
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
  private final SedonaConf sedonaConf;
  private SparkEnv sparkEnv = null;
  private TaskContext taskContext = null;

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
    this.sedonaConf = sedonaConf;
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
      List<LocalSpatialJoinExecParams> localSpatialJoinExecParamsList,
      SedonaConf sedonaConf) {
    this.spatialPredicate = spatialPredicate;
    this.extraFilterCreator = null;
    this.joinType = joinType;
    this.sedonaConf = sedonaConf;
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
    // Short circuit for empty partitions
    switch (joinType) {
      case INNER:
        if (!leftIterator.hasNext() || !rightIterator.hasNext()) {
          return Collections.emptyIterator();
        }
        break;
      case LEFT_OUTER:
        if (!leftIterator.hasNext()) {
          return Collections.emptyIterator();
        }
        break;
      case RIGHT_OUTER:
        if (!rightIterator.hasNext()) {
          return Collections.emptyIterator();
        }
        break;
    }

    // Get the per-partition execution parameters
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
      if (sedonaConf != null && sedonaConf.useExternalSpatialIndex()) {
        return new ExternalSpatialJoinIterator<>(
            leftIterator,
            rightIterator,
            localJoinType,
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
            buildTime,
            sedonaConf,
            sparkEnv,
            taskContext);
      } else {
        return new InMemorySpatialJoinIterator<>(
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
      }
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

      Iterator<Pair<T, U>> swappedJoinResultIterator;
      if (sedonaConf != null && sedonaConf.useExternalSpatialIndex()) {
        swappedJoinResultIterator =
            new ExternalSpatialJoinIterator<>(
                rightIterator,
                leftIterator,
                localJoinType,
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
                buildTime,
                sedonaConf,
                sparkEnv,
                taskContext);
      } else {
        swappedJoinResultIterator =
            new InMemorySpatialJoinIterator<>(
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
      }
      return new SwapLeftAndRightIterator<>(swappedJoinResultIterator);
    }
  }

  /** For setting up a mock SparkEnv object when running unit tests */
  public void setSparkEnv(SparkEnv sparkEnv) {
    this.sparkEnv = sparkEnv;
  }

  /** For setting up a mock TaskContext object when running unit tests */
  public void setTaskContext(TaskContext taskContext) {
    this.taskContext = taskContext;
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
}
