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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.commons.collections4.iterators.EmptyIterator;
import org.apache.commons.collections4.iterators.SingletonIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.HalfOpenRectangle;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.enums.LocalJoinType;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators.SpatialPredicateEvaluator;
import org.apache.sedona.core.spatialPartitioning.OuterJoinSpatialPartitioner.OuterJoinUserData;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sedona.core.index.ExternalSpatialIndex;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement.DataObjectWithId;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItem;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItemFormat;
import org.apache.spark.storage.BlockManager;
import org.locationtech.jts.geom.Geometry;

/**
 * The actual heavy lifting of the local spatial join is done by this iterator. It spills data to
 * disk when the memory is insufficient.
 *
 * @param <U> The type of the geometries on the build side
 * @param <T> The type of the geometries on the stream side
 */
public class ExternalSpatialJoinIterator<U extends Geometry, T extends Geometry>
    implements Iterator<Pair<U, T>>, AutoCloseable {

  private final LocalJoinType localJoinType;
  private Iterator<T> streamIterator;
  private final SpatialPredicateEvaluator spatialPredicateEvaluator;
  private final Function2<Geometry, Geometry, Boolean> extraFilterWithDedup;
  private final ExternalSpatialIndexWithRefinement<GeometryDataItem> externalSpatialIndex;
  private BitSet indexedGeometryHasNoMatches;
  private Iterator<DataObjectWithId<GeometryDataItem>> queryResultIterator;
  private T currentStreamGeometry;
  private boolean populatedIndexOuterBatch = false;
  private boolean isStreamSideSorted = false;

  // For testing purposes
  private boolean validateEqualityInExternalSort = false;

  // metrics
  private final SpatialJoinMetric metricStreamCount;
  private final SpatialJoinMetric metricResultCount;
  private final SpatialJoinMetric metricCandidateCount;

  private final TaskContext taskContext;
  private final SparkEnv sparkEnv;

  ExternalSpatialJoinIterator(
      Iterator<U> buildIterator,
      Iterator<T> streamIterator,
      LocalJoinType localJoinType,
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
      SpatialJoinMetric buildTime,
      SedonaConf sedonaConf,
      SparkEnv sparkEnv,
      TaskContext taskContext) {
    this.localJoinType = localJoinType;
    this.streamIterator = streamIterator;
    this.spatialPredicateEvaluator = SpatialPredicateEvaluators.create(predicate);
    this.extraFilterWithDedup = getExtraFilterWithDedup(extraFilter, extent);
    this.queryResultIterator = EmptyIterator.emptyIterator();
    this.metricStreamCount = streamCount;
    this.metricResultCount = resultCount;
    this.metricCandidateCount = candidateCount;
    if (localJoinType == LocalJoinType.INDEX_OUTER) {
      indexedGeometryHasNoMatches = new BitSet();
    }

    // taskContext is non-null only when running unit tests, where a mock TaskContext object is
    // passed in. In production, this will always be null, and we'll always get the TaskContext
    // object from TaskContext.get().
    if (taskContext == null) {
      this.taskContext = TaskContext.get();
    } else {
      this.taskContext = taskContext;
    }

    // sparkEnv is non-null only when running unit tests, where a mock SparkEnv object is passed
    // in. In production, this will always be null, and we'll always get the SparkEnv object from
    // SparkEnv.get().
    if (sparkEnv == null) {
      this.sparkEnv = SparkEnv.get();
    } else {
      this.sparkEnv = sparkEnv;
    }

    long start = System.nanoTime();
    this.externalSpatialIndex =
        buildExternalSpatialIndex(
            buildIterator,
            executionMode,
            subdivideBuildOptions,
            subdivideStreamOptions,
            buildCount,
            sedonaConf);
    if (sedonaConf.forceSpillExternalSpatialIndex()) {
      try {
        this.externalSpatialIndex.spill();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    buildTime.add(NANOSECONDS.toMillis(System.nanoTime() - start));
  }

  private ExternalSpatialIndexWithRefinement<GeometryDataItem> buildExternalSpatialIndex(
      Iterator<U> buildIterator,
      ExecutionMode executionMode,
      SubdivideOptions subdivideBuildOptions,
      SubdivideOptions subdivideStreamOptions,
      SpatialJoinMetric buildCount,
      SedonaConf conf) {
    BlockManager blockManager = sparkEnv.blockManager();
    TaskMemoryManager taskMemoryManager = taskContext.taskMemoryManager();
    long pageSizeBytes = taskMemoryManager.pageSizeBytes();
    ShuffleWriteMetrics spillMetrics = new ShuffleWriteMetrics();
    int leafPageCapacity = conf.getExternalSpatialIndexLeafPageCapacity();
    int internalNodeCapacity = conf.getExternalSpatialIndexInternalNodeCapacity();
    ExternalSpatialIndex inner =
        new ExternalSpatialIndex(
            taskContext,
            taskMemoryManager,
            blockManager,
            pageSizeBytes,
            leafPageCapacity,
            internalNodeCapacity,
            spillMetrics);

    ExternalSpatialIndexWithRefinement<GeometryDataItem> externalSpatialIndex =
        new ExternalSpatialIndexWithRefinement<>(
            inner,
            new GeometryDataItemFormat(),
            executionMode,
            subdivideBuildOptions,
            subdivideStreamOptions);

    taskContext.addTaskCompletionListener(
        context -> {
          externalSpatialIndex.close();
        });

    int count = 0;
    for (; buildIterator.hasNext(); count++) {
      U geometry = buildIterator.next();
      GeometryDataItem dataItem = new GeometryDataItem(geometry);
      externalSpatialIndex.insert(dataItem);
      if (localJoinType == LocalJoinType.INDEX_OUTER) {
        if (((OuterJoinUserData) geometry.getUserData()).isPrimary) {
          indexedGeometryHasNoMatches.set(count);
        }
      }
    }
    try {
      externalSpatialIndex.build();
    } catch (IOException e) {
      externalSpatialIndex.close();
      throw new RuntimeException(e);
    }

    buildCount.add(count);
    return externalSpatialIndex;
  }

  private static Function2<Geometry, Geometry, Boolean> getExtraFilterWithDedup(
      Function2<Geometry, Geometry, Boolean> extraFilter, HalfOpenRectangle extent) {
    Function2<Geometry, Geometry, Boolean> extraFilterWithDedup = extraFilter;
    if (extent != null) {
      // Add deduplication filter to the extra filter
      if (extraFilter != null) {
        extraFilterWithDedup =
            (geom1, geom2) -> {
              if (GeomUtils.isDuplicate(geom1, geom2, extent)) {
                return false;
              }
              return extraFilter.call(geom1, geom2);
            };
      } else {
        extraFilterWithDedup = (geom1, geom2) -> !GeomUtils.isDuplicate(geom1, geom2, extent);
      }
    }
    return extraFilterWithDedup;
  }

  @Override
  public boolean hasNext() {
    if (queryResultIterator.hasNext()) {
      return true;
    }
    populateNextBatch();
    if (!queryResultIterator.hasNext()) {
      close();
      return false;
    } else {
      return true;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Pair<U, T> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    DataObjectWithId<GeometryDataItem> indexedItem = queryResultIterator.next();
    int indexedItemId = indexedItem.itemId;
    if (indexedItemId >= 0) {
      if (localJoinType == LocalJoinType.INDEX_OUTER) {
        indexedGeometryHasNoMatches.clear(indexedItemId);
      }
      metricResultCount.add(1);
    }
    return Pair.of((U) indexedItem.dataObject.geometry, currentStreamGeometry);
  }

  private void populateNextBatch() {
    while (!queryResultIterator.hasNext()) {
      if (streamIterator.hasNext()) {
        // If the external spatial index is spilled, we'll sort the stream side to improve
        // the locality of the query windows, thus improve the I/O efficiency and cache hit rate
        // of the index.
        if (!isStreamSideSorted && externalSpatialIndex.hasSpilled()) {
          try {
            streamIterator =
                SortedGeometryIterator.sortGeometryIterator(
                    streamIterator,
                    sparkEnv,
                    taskContext,
                    externalSpatialIndex,
                    validateEqualityInExternalSort);
            isStreamSideSorted = true;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        currentStreamGeometry = streamIterator.next();
        metricStreamCount.add(1);
        try {
          queryResultIterator =
              externalSpatialIndex.query(
                  currentStreamGeometry,
                  spatialPredicateEvaluator,
                  extraFilterWithDedup,
                  metricCandidateCount);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        if (!queryResultIterator.hasNext()) {
          if (localJoinType == LocalJoinType.STREAM_OUTER) {
            populateStreamOuterBatch(currentStreamGeometry);
          }
        }
      } else {
        currentStreamGeometry = null;
        if (localJoinType == LocalJoinType.INDEX_OUTER && !populatedIndexOuterBatch) {
          populateIndexOuterBatch();
          populatedIndexOuterBatch = true;
        }
        break;
      }
    }
  }

  private void populateStreamOuterBatch(T geometry) {
    // Check if the stream side is the primary geometry. if it is, we should emit a
    // record with index side = null
    OuterJoinUserData userData = (OuterJoinUserData) geometry.getUserData();
    if (!userData.isPrimary) {
      return;
    }
    queryResultIterator =
        new SingletonIterator<>(new DataObjectWithId<>(-1, new GeometryDataItem(null)));
  }

  private void populateIndexOuterBatch() {
    // Walk through all primary geometries in the index side that has no emitted join results,
    // and emit a record with stream side = null for them.
    IntList itemIds = new IntArrayList();
    for (int i = indexedGeometryHasNoMatches.nextSetBit(0);
        i >= 0;
        i = indexedGeometryHasNoMatches.nextSetBit(i + 1)) {
      if (i == Integer.MAX_VALUE) {
        break;
      }
      itemIds.add(i);
    }
    if (!itemIds.isEmpty()) {
      queryResultIterator = externalSpatialIndex.fetchDataObjects(itemIds);
    }
  }

  /** For testing purposes */
  public void setEqualityValidation(boolean enabled) {
    validateEqualityInExternalSort = enabled;
  }

  /** For testing purposes */
  public void forceSpill() throws IOException {
    externalSpatialIndex.spill();
  }

  @Override
  public void close() {
    externalSpatialIndex.close();
  }
}
