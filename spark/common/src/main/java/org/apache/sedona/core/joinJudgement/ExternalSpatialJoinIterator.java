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
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.strtree.STRtree;

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
            predicate,
            extraFilter,
            executionMode,
            extent,
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
      SpatialPredicate predicate,
      Function2<Geometry, Geometry, Boolean> extraFilter,
      ExecutionMode executionMode,
      HalfOpenRectangle extent,
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

    Function2<Geometry, Geometry, Boolean> extraFilterWithDedup =
        getExtraFilterWithDedup(extraFilter, extent);

    ExternalSpatialIndexWithRefinement<GeometryDataItem> externalSpatialIndex =
        new ExternalSpatialIndexWithRefinement<>(
            inner,
            new GeometryDataItemFormat(),
            predicate,
            extraFilterWithDedup,
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
            sortStreamSide();
            isStreamSideSorted = true;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        currentStreamGeometry = streamIterator.next();
        metricStreamCount.add(1);
        try {
          queryResultIterator =
              externalSpatialIndex.query(currentStreamGeometry, metricCandidateCount);
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

  private void sortStreamSide() throws IOException {
    // Construct a sorter to sort the stream side geometries by the leaf page id of the index side
    BlockManager blockManager = sparkEnv.blockManager();
    TaskMemoryManager taskMemoryManager = taskContext.taskMemoryManager();
    long pageSizeBytes = taskMemoryManager.pageSizeBytes();
    STRtree nonLeafTree = externalSpatialIndex.getNonLeafTree();
    GeometryDataItemFormat format = new GeometryDataItemFormat();
    DataItemComparator comparator;
    if (validateEqualityInExternalSort) {
      comparator = new DataItemComparator(nonLeafTree, format, true);
    } else {
      // Don't hold a reference to nonLeafTree if we don't need to validate equality, since
      // this will make nonLeafTree not eligible for GC until the end of the task.
      comparator = new DataItemComparator(null, null, false);
    }

    UnsafeExternalSorter sorter =
        UnsafeExternalSorter.create(
            taskMemoryManager,
            blockManager,
            blockManager.serializerManager(),
            taskContext,
            () -> comparator,
            PrefixComparators.LONG,
            (int) pageSizeBytes / 100,
            pageSizeBytes,
            Integer.MAX_VALUE,
            true);

    // Load remaining stream side geometries into the sorter
    FirstLeafPageVisitor visitor = new FirstLeafPageVisitor();
    int bufferLength = 0;
    int[] leafPageRanks =
        externalSpatialIndex.getSpatialIndex().getLeafPageIndex().rankLeafPagesBySpatialProximity();
    while (streamIterator.hasNext()) {
      T geometry = streamIterator.next();
      Envelope envelope = geometry.getEnvelopeInternal();
      byte[] serializedGeometry = format.serialize(geometry);
      bufferLength = Math.max(bufferLength, serializedGeometry.length);
      visitor.reset();
      nonLeafTree.query(envelope, visitor);
      long prefix = visitor.leafPageId >= 0 ? leafPageRanks[visitor.leafPageId] : -1;
      sorter.insertRecord(
          serializedGeometry, Platform.BYTE_ARRAY_OFFSET, serializedGeometry.length, prefix, false);
    }

    // Force the stream side sorter to spill, because we need memory for caching the data items
    // when running the spatial join.
    sorter.spill();

    // Construct an iterator to iterate over the sorted stream side geometries. The set of
    // geometries in
    // the iterator should be the same as the initial streamIterator before sorting.
    streamIterator = new SortedStreamGeometryIterator<>(sorter, format, bufferLength);
  }

  /** For testing purposes */
  public void setEqualityValidation(boolean enabled) {
    validateEqualityInExternalSort = enabled;
  }

  /** For testing purposes */
  public void forceSpill() throws IOException {
    externalSpatialIndex.spill();
  }

  private static class SortedStreamGeometryIterator<T> implements Iterator<T> {
    private UnsafeExternalSorter sorter;
    private final UnsafeSorterIterator unsafeSorterIterator;
    private final byte[] serializedGeometry;
    private final GeometryDataItemFormat format;

    SortedStreamGeometryIterator(
        UnsafeExternalSorter sorter, GeometryDataItemFormat format, int bufferLength)
        throws IOException {
      this.sorter = sorter;
      this.unsafeSorterIterator = sorter.getSortedIterator();
      this.serializedGeometry = new byte[bufferLength];
      this.format = format;
    }

    @Override
    public boolean hasNext() {
      boolean hasNext = unsafeSorterIterator.hasNext();
      if (!hasNext && sorter != null) {
        sorter.cleanupResources();
        sorter = null;
      }
      return hasNext;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T next() {
      try {
        unsafeSorterIterator.loadNext();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      Object baseObject = unsafeSorterIterator.getBaseObject();
      long baseOffset = unsafeSorterIterator.getBaseOffset();
      int baseLength = unsafeSorterIterator.getRecordLength();
      if (serializedGeometry.length < baseLength) {
        throw new IllegalStateException("Serialized geometry buffer is too small");
      }
      Platform.copyMemory(
          baseObject, baseOffset, serializedGeometry, Platform.BYTE_ARRAY_OFFSET, baseLength);
      return (T) format.deserializeToGeometry(serializedGeometry);
    }
  }

  private static class FirstLeafPageVisitor implements ItemVisitor {
    int leafPageId = -1;

    @Override
    public void visitItem(Object item) {
      if (leafPageId == -1) {
        leafPageId = (int) item;
      }
    }

    void reset() {
      leafPageId = -1;
    }
  }

  private static class DataItemComparator extends RecordComparator {
    STRtree nonLeafTree;
    FirstLeafPageVisitor visitor;
    GeometryDataItemFormat format;
    boolean validateEquality;

    DataItemComparator(
        STRtree nonLeafTree, GeometryDataItemFormat format, boolean validateEquality) {
      this.nonLeafTree = nonLeafTree;
      this.visitor = new FirstLeafPageVisitor();
      this.format = format;
      this.validateEquality = validateEquality;
    }

    @Override
    public int compare(
        Object leftBaseObject,
        long leftBaseOffset,
        int leftBaseLength,
        Object rightBaseObject,
        long rightBaseOffset,
        int rightBaseLength) {
      // NOTICE: This method is only called when the prefix of the left and right records are
      // the same. We are already using first leafId as prefix for sorting, so we can directly
      // return 0 here.
      if (!validateEquality) {
        return 0;
      }

      // If validateEquality is enabled, we'll decode the geometries and compare their leaf page ids
      // to ensure that the sorting is correct. This only happens when running tests.
      int leftLeafId = recordToLeafPageId(leftBaseObject, leftBaseOffset, leftBaseLength);
      int rightLeafId = recordToLeafPageId(rightBaseObject, rightBaseOffset, rightBaseLength);
      int result = Integer.compare(leftLeafId, rightLeafId);
      if (result != 0) {
        throw new IllegalStateException(
            "leafIds should be equal. leftLeafId: " + leftLeafId + ", rightLeafId: " + rightLeafId);
      }
      return result;
    }

    private int recordToLeafPageId(Object baseObject, long baseOffset, int baseLength) {
      byte[] serializedGeom = new byte[baseLength];
      Platform.copyMemory(
          baseObject, baseOffset, serializedGeom, Platform.BYTE_ARRAY_OFFSET, baseLength);
      Geometry geom = format.deserializeToGeometry(serializedGeom);
      Envelope envelope = geom.getEnvelopeInternal();
      visitor.reset();
      nonLeafTree.query(envelope, visitor);
      return visitor.leafPageId;
    }
  }

  @Override
  public void close() {
    externalSpatialIndex.close();
  }
}
