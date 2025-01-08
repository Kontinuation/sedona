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
package org.apache.spark.sedona.core.index;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sedona.core.index.ExternalDataItemIndex.DataItemWithId;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockManager;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * A spatial index class that is capable of storing the spatial index in memory or on disk. It can
 * be adaptive to the memory pressure and spill the index to disk when necessary. This index is
 * immutable: it does not support remove() method; once it is queried, no more insertions are
 * allowed.
 */
public class ExternalSpatialIndex extends SedonaMemoryConsumer implements AutoCloseable {

  private final BlockManager blockManager;

  /** The maximum number of envelopes in a leaf page. */
  private final int leafPageCapacity;
  /** The maximum number of internal nodes in a non-leaf page. */
  private final int internalNodeCapacity;

  /** The id of the next item to be inserted. */
  private int nextItemId = 0;

  /** Whether the spatial index is built. */
  private volatile boolean built = false;

  /**
   * Leaf page index builder for building the lowest level of the spatial index. Each envelope in
   * the leaf page has an item id stored with it.
   */
  private ExternalLeafPageIndexBuilder leafPageIndexBuilder;

  /**
   * Index builder for building the data item index. It will support lookups of the indexed data
   * items using item IDs.
   */
  private ExternalDataItemIndexBuilder dataItemIndexBuilder;

  private ExternalLeafPageIndex leafPageIndex = null;
  private ExternalDataItemIndex dataItemIndex = null;
  private STRtree nonLeafTree = null;

  public ExternalSpatialIndex(
      TaskContext taskContext,
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      long pageSizeBytes,
      int leafPageCapacity,
      int internalNodeCapacity,
      ShuffleWriteMetricsReporter shuffleWriteMetricsReporter) {
    super(taskMemoryManager, pageSizeBytes, taskMemoryManager.getTungstenMemoryMode());
    this.blockManager = blockManager;
    this.leafPageCapacity = leafPageCapacity;
    this.internalNodeCapacity = internalNodeCapacity;
    this.leafPageIndexBuilder =
        new ExternalLeafPageIndexBuilder(
            this,
            taskContext,
            taskMemoryManager,
            blockManager,
            Math.min(pageSizeBytes, 16L * 1024 * 1024),
            shuffleWriteMetricsReporter);
    this.dataItemIndexBuilder =
        new ExternalDataItemIndexBuilder(
            this,
            taskContext,
            taskMemoryManager,
            blockManager,
            pageSizeBytes,
            shuffleWriteMetricsReporter);
    taskContext.addTaskCompletionListener(
        context -> {
          close();
        });
  }

  public ExternalLeafPageIndex getLeafPageIndex() {
    checkIndexIsBuilt();
    return leafPageIndex;
  }

  public ExternalDataItemIndex getDataItemIndex() {
    checkIndexIsBuilt();
    return dataItemIndex;
  }

  /**
   * Insert a data item into the spatial index. The itemEnv is the envelope of the data item.
   *
   * @param itemEnv the envelope of the data item
   * @param dataItem the data item to insert
   * @return the item id of the inserted data item
   */
  public int insert(Envelope itemEnv, byte[] dataItem) {
    checkItemId();
    leafPageIndexBuilder.add(
        nextItemId, itemEnv.getMinX(), itemEnv.getMaxX(), itemEnv.getMinY(), itemEnv.getMaxY());
    int itemId = nextItemId;
    dataItemIndexBuilder.add(itemId, dataItem);
    nextItemId += 1;
    return itemId;
  }

  /**
   * Insert a data item into the spatial index. The itemEnvs are the envelopes that fully cover the
   * data item.
   *
   * @param itemEnvs the envelopes that cover the data item
   * @param dataItem the data item to insert
   * @return the item id of the inserted data item
   */
  public int insert(Iterator<Envelope> itemEnvs, byte[] dataItem) {
    checkItemId();
    while (itemEnvs.hasNext()) {
      Envelope itemEnv = itemEnvs.next();
      leafPageIndexBuilder.add(
          nextItemId, itemEnv.getMinX(), itemEnv.getMaxX(), itemEnv.getMinY(), itemEnv.getMaxY());
    }
    int itemId = nextItemId;
    dataItemIndexBuilder.add(itemId, dataItem);
    nextItemId += 1;
    return itemId;
  }

  /**
   * Insert a data item into the spatial index. The itemEnvs are the envelopes that fully cover the
   * data item.
   *
   * @param itemEnvs the envelopes that cover the data item
   * @param dataItem the data item to insert
   * @return the item id of the inserted data item
   */
  public int insert(List<Envelope> itemEnvs, byte[] dataItem) {
    return insert(itemEnvs.iterator(), dataItem);
  }

  private void checkItemId() {
    if (nextItemId == Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Too many indexed records (2^31). Please split the data into smaller partitions.");
    }
  }

  /**
   * Number of indexed items
   *
   * @return the number of indexed items
   */
  public int count() {
    return nextItemId;
  }

  public boolean hasSpilled() {
    checkIndexIsBuilt();
    return dataItemIndex.isSpilled() || leafPageIndex.isSpilled();
  }

  /**
   * Query the item ids that intersects the given search envelope.
   *
   * @param searchEnv The search envelope.
   * @return A list of deduplicated item ids.
   * @throws IOException If an I/O error occurs.
   */
  public IntList queryItemIds(Envelope searchEnv) throws IOException {
    checkIndexIsBuilt();

    // Query the non-leaf tree to get the leaf ids
    IntList leafIds = queryNonLeafTree(searchEnv);

    // Filter out the envelopes from the leaf pages
    IntList itemIds = leafPageIndex.query(searchEnv, leafIds);

    // Deduplicate the item ids
    deduplicateInPlace(itemIds);

    return itemIds;
  }

  /**
   * Query the item ids that intersects the given search envelopes.
   *
   * @param searchEnvs the search envelopes
   * @return a list of deduplicated item ids
   * @throws IOException if an I/O error occurs
   */
  public IntList queryItemIds(Iterator<Envelope> searchEnvs) throws IOException {
    checkIndexIsBuilt();

    STRtree filterTree = new STRtree();
    List<Envelope> searchEnvsCopy = new ArrayList<>();
    while (searchEnvs.hasNext()) {
      Envelope searchEnv = searchEnvs.next();
      filterTree.insert(searchEnv, null);
      searchEnvsCopy.add(searchEnv);
    }

    // Query the non-leaf tree to get the leaf ids
    IntList leafIds = queryNonLeafTree(searchEnvsCopy.iterator());

    // Filter out the envelopes from the leaf pages
    ExternalLeafPageIndex.EnvelopeFilter filter =
        (id, envelope) -> {
          Envelope queryEnv = new Envelope(envelope[0], envelope[1], envelope[2], envelope[3]);
          return !filterTree.query(queryEnv).isEmpty();
        };
    IntList itemIds = leafPageIndex.query(filter, leafIds);

    // Deduplicate the item ids
    deduplicateInPlace(itemIds);

    return itemIds;
  }

  /**
   * Query the item ids that intersects the given search envelopes.
   *
   * @param searchEnvs the search envelopes
   * @return a list of deduplicated item ids
   * @throws IOException if an I/O error occurs
   */
  public IntList queryItemIds(List<Envelope> searchEnvs) throws IOException {
    return queryItemIds(searchEnvs.iterator());
  }

  /**
   * Query the data items that intersects the given search envelope.
   *
   * @param searchEnv the search envelope
   * @return an iterator of data items
   * @throws IOException if an I/O error occurs
   */
  public Iterator<DataItemWithId> query(Envelope searchEnv) throws IOException {
    IntList itemIds = queryItemIds(searchEnv);

    // Fetch data items from the data item index
    return dataItemIndex.fetch(itemIds);
  }

  /**
   * Query the data items that intersects the given search envelopes.
   *
   * @param searchEnvs the search envelopes
   * @return an iterator of data items
   * @throws IOException if an I/O error occurs
   */
  public Iterator<DataItemWithId> query(List<Envelope> searchEnvs) throws IOException {
    IntList itemIds = queryItemIds(searchEnvs);
    return dataItemIndex.fetch(itemIds);
  }

  private IntList queryNonLeafTree(Envelope queryWindow) {
    class NonLeafTreeVisitor implements ItemVisitor {
      final IntArrayList result = new IntArrayList();

      @Override
      public void visitItem(Object item) {
        result.add((int) item);
      }
    }
    NonLeafTreeVisitor resultVisitor = new NonLeafTreeVisitor();
    nonLeafTree.query(queryWindow, resultVisitor);
    return resultVisitor.result;
  }

  private IntList queryNonLeafTree(Iterator<Envelope> queryWindows) {
    class NonLeafTreeVisitor implements ItemVisitor {
      final IntOpenHashSet result = new IntOpenHashSet();

      @Override
      public void visitItem(Object item) {
        result.add((int) item);
      }
    }
    NonLeafTreeVisitor resultVisitor = new NonLeafTreeVisitor();
    while (queryWindows.hasNext()) {
      Envelope queryWindow = queryWindows.next();
      nonLeafTree.query(queryWindow, resultVisitor);
    }
    return new IntArrayList(resultVisitor.result);
  }

  /**
   * Build the spatial index.
   *
   * @throws IOException if an I/O error occurs
   */
  public void build() throws IOException {
    if (built) {
      return;
    }

    // Force spilling everything to disk if spill has happened before. This is for ensuring that
    // the external sorter has enough memory for sorting envelopes and data items.
    if (leafPageIndexBuilder.hasSpilled()) {
      leafPageIndexBuilder.spill(true);
    }
    if (dataItemIndexBuilder.hasSpilled()) {
      dataItemIndexBuilder.spill(true);
    }

    ExternalLeafPageIndex leafPages = null;
    ExternalDataItemIndex dataItemIndex;
    try {
      leafPages = leafPageIndexBuilder.buildLeafPages(leafPageCapacity);
      dataItemIndex = dataItemIndexBuilder.buildDataItemIndex(leafPages);
    } catch (Exception e) {
      if (leafPages != null) {
        leafPages.close();
      }
      throw e;
    }

    this.leafPageIndex = leafPages;
    this.dataItemIndex = dataItemIndex;

    leafPageIndexBuilder.close();
    leafPageIndexBuilder = null;
    dataItemIndexBuilder.close();
    dataItemIndexBuilder = null;
    nonLeafTree = leafPages.buildNonLeafTree(internalNodeCapacity);
    built = true;
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (built) {
      // Index is already built. This spill can be triggered by other memory consumers.
      // The spill methods of the leaf page index and the data item index are thread-safe to
      // support cooperative spilling.
      DiskBlockManager diskBlockManager = blockManager.diskBlockManager();
      long freedBytes = dataItemIndex.spill(diskBlockManager);
      if (freedBytes < size) {
        freedBytes += leafPageIndex.spill(diskBlockManager);
      }
      return freedBytes;
    } else {
      // Cannot handle spilling triggered by other consumers until the index is built
      if (trigger != this && !isCooperativeConsumer(trigger)) {
        return 0L;
      }

      // Here are the handling of self-spilling:
      // 1. Spill the data items first
      long freedBytes = dataItemIndexBuilder.spill(false);
      if (freedBytes < size) {
        // 2. If spilling the data items does not free enough memory, spill the buffered
        // envelopes
        freedBytes += leafPageIndexBuilder.spill(false);
      }
      return freedBytes;
    }
  }

  public static void deduplicateInPlace(IntList intList) {
    intList.sort(null);
    if (intList instanceof IntArrayList) {
      // Directly work on the underlying array to bypass the bound checks
      int[] elements = ((IntArrayList) intList).elements();
      int newSize = 0;
      for (int i = 0; i < intList.size(); i++) {
        if (i == 0 || elements[i] != elements[i - 1]) {
          elements[newSize] = elements[i];
          newSize += 1;
        }
      }
      intList.size(newSize);
    } else {
      throw new UnsupportedOperationException(
          "Deduplicating a non-array-backed list is not supported");
    }
  }

  @Override
  public void close() {
    if (leafPageIndex != null) {
      leafPageIndex.close();
      leafPageIndex = null;
    }
    if (dataItemIndex != null) {
      dataItemIndex.close();
      dataItemIndex = null;
    }
    if (leafPageIndexBuilder != null) {
      leafPageIndexBuilder.close();
      leafPageIndexBuilder = null;
    }
    if (dataItemIndexBuilder != null) {
      dataItemIndexBuilder.close();
      dataItemIndexBuilder = null;
    }
    nonLeafTree = null;
  }

  public STRtree getNonLeafTree() {
    checkIndexIsBuilt();
    return nonLeafTree;
  }

  private void checkIndexIsBuilt() {
    if (!built) {
      throw new IllegalStateException("The spatial index is not built yet");
    }
  }
}
