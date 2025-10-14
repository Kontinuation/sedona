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

import java.io.IOException;
import java.util.Iterator;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItem;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItemFormat;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.strtree.STRtree;

public class SortedGeometryIterator<T> implements Iterator<T> {

  private UnsafeExternalSorter sorter;
  private final UnsafeSorterIterator unsafeSorterIterator;
  private final byte[] serializedGeometry;
  private final GeometryDataItemFormat format;

  public SortedGeometryIterator(
      UnsafeExternalSorter sorter, GeometryDataItemFormat format, int bufferLength)
      throws IOException {
    this.sorter = sorter;
    this.unsafeSorterIterator = sorter.getSortedIterator();
    this.serializedGeometry = new byte[bufferLength];
    this.format = format;
  }

  public void cleanUpResources() {
    if (sorter != null) {
      sorter.cleanupResources();
      sorter = null;
    }
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = unsafeSorterIterator.hasNext();
    if (!hasNext) {
      cleanUpResources();
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

  /**
   * Sort the stream side geometries by the leaf page id of the index side. This will make
   * geometries close with each other spatially being colocated.
   *
   * @param iterator the iterator of geometries to sort
   * @param sparkEnv the SparkEnv
   * @param taskContext the TaskContext
   * @param externalSpatialIndex the external spatial index
   * @param validateEqualityInExternalSort whether to validate the equality of the geometries
   * @return the sorted iterator
   * @param <T> the type of the geometries
   * @throws IOException if an I/O error occurs
   */
  public static <T extends Geometry> SortedGeometryIterator<T> sortGeometryIterator(
      Iterator<T> iterator,
      SparkEnv sparkEnv,
      TaskContext taskContext,
      ExternalSpatialIndexWithRefinement<GeometryDataItem> externalSpatialIndex,
      boolean validateEqualityInExternalSort)
      throws IOException {
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
    while (iterator.hasNext()) {
      T geometry = iterator.next();
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
    // the iterator should be the same as the initial iterator before sorting.
    return new SortedGeometryIterator<>(sorter, format, bufferLength);
  }

  private static final long MIN_MEMORY_REQUIRED_FOR_SORTING = 100 * 1024 * 1024; // 100MB

  /**
   * Check if there is enough memory for sorting. This is a best-effort check, the sorting may still
   * fail even though this test passes.
   *
   * @param consumer the memory consumer
   * @return true if there is enough memory for sorting, false otherwise
   */
  public static boolean hasEnoughMemoryForSorting(MemoryConsumer consumer) {
    try {
      LongArray allocated = consumer.allocateArray(MIN_MEMORY_REQUIRED_FOR_SORTING / 8);
      consumer.freeArray(allocated);
      return true;
    } catch (OutOfMemoryError e) {
      return false;
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
}
