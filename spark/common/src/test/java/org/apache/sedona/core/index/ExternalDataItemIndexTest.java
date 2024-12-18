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
package org.apache.sedona.core.index;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.math3.util.MathArrays;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sedona.core.index.ExternalDataItemIndex;
import org.apache.spark.sedona.core.index.ExternalDataItemIndex.DataItemWithId;
import org.apache.spark.sedona.core.index.ExternalDataItemIndexBuilder;
import org.apache.spark.sedona.core.index.ExternalLeafPageIndex;
import org.apache.spark.sedona.core.index.ExternalLeafPageIndex.LeafPageMetadata;
import org.apache.spark.sedona.core.index.SedonaMemoryConsumer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.junit.Test;

public class ExternalDataItemIndexTest extends ExternalIndexTestBase {

  final DummyMemoryConsumer consumer =
      new DummyMemoryConsumer(taskMemoryManager, pageSizeBytes, MemoryMode.ON_HEAP);

  @Test
  public void buildEmptyIndex() throws IOException {
    ExternalDataItemIndexBuilder builder = create();
    ExternalLeafPageIndex leafPages =
        new ExternalLeafPageIndex(taskContext, consumer, taskMemoryManager);
    ExternalDataItemIndex index = builder.buildDataItemIndex(leafPages);
    builder.close();
    index.close();
  }

  @Test
  public void buildSmallIndex() throws IOException {
    ExternalDataItemIndexBuilder builder = create();
    List<byte[]> expected = new ArrayList<>();
    expected.add(new byte[] {10, 20, 30});
    expected.add(new byte[] {40, 50});
    expected.add(new byte[] {});
    expected.add(new byte[] {60, 70, 80, 90});
    for (int k = 0; k < expected.size(); k++) {
      builder.add(k, expected.get(k));
    }
    assertTrue(builder.getMemoryUsage() > 0);
    assertFalse(builder.hasSpilled());
    ExternalLeafPageIndex leafPages = createMockLeafPageIndex(4, 2);
    int[] itemIdToLeafPageId = leafPages.borrowItemIdToLeafPageId();
    ExternalDataItemIndex index = builder.buildDataItemIndex(leafPages);
    assertFalse(index.isSpilled());
    assertEquals(0, builder.getMemoryUsage());
    builder.close();
    verifyDataItemIndex(index, expected, leafPages, itemIdToLeafPageId);
    index.spill(diskBlockManager);
    assertTrue(index.isSpilled());
    verifyDataItemIndex(index, expected, leafPages, itemIdToLeafPageId);
    index.close();
  }

  @Test
  public void buildMultiBlockIndex() throws IOException {
    ExternalDataItemIndexBuilder builder = create();
    int itemSize = 1000;
    int numItems = (int) pageSizeBytes / itemSize * 10;
    List<byte[]> expected = new ArrayList<>(numItems);
    Random random = new Random();
    for (int i = 0; i < numItems; i++) {
      byte[] data = new byte[itemSize];
      random.nextBytes(data);
      builder.add(i, data);
      expected.add(data);
    }
    assertFalse(builder.hasSpilled());
    ExternalLeafPageIndex leafPages = createMockLeafPageIndex(numItems, 10);
    int[] itemIdToLeafPageId = leafPages.borrowItemIdToLeafPageId();
    ExternalDataItemIndex index = builder.buildDataItemIndex(leafPages);
    assertFalse(index.isSpilled());
    builder.close();
    verifyDataItemIndex(index, expected, leafPages, itemIdToLeafPageId);
    index.spill(diskBlockManager);
    assertTrue(index.isSpilled());
    verifyDataItemIndex(index, expected, leafPages, itemIdToLeafPageId);
    index.close();
  }

  @Test
  public void consecutiveSpills() throws IOException {
    ExternalDataItemIndexBuilder builder = create();
    int itemSize = 1000;
    List<byte[]> expected = new ArrayList<>(100);
    Random random = new Random();
    int id = 0;
    for (int spills = 0; spills < 10; spills++) {
      for (int i = 0; i < 100; i++) {
        byte[] data = new byte[itemSize];
        random.nextBytes(data);
        builder.add(id, data);
        expected.add(data);
        id += 1;
      }
      builder.spill(true);
      builder.spill(false);
    }

    ExternalLeafPageIndex leafPages = createMockLeafPageIndex(id, 10);
    int[] itemIdToLeafPageId = leafPages.borrowItemIdToLeafPageId();
    ExternalDataItemIndex index = builder.buildDataItemIndex(leafPages);
    builder.close();

    verifyDataItemIndex(index, expected, leafPages, itemIdToLeafPageId);
    index.spill(diskBlockManager);
    index.spill(diskBlockManager);
    assertTrue(index.isSpilled());
    verifyDataItemIndex(index, expected, leafPages, itemIdToLeafPageId);

    // repeated close should also be fine
    index.close();
    index.close();
  }

  @Test
  public void buildWithAllocationFailure() {
    // If the memory manager keeps throwing OOM, the builder will eventually fail.
    memoryManager.markConsequentOOM(10);
    assertThrows(
        OutOfMemoryError.class,
        () -> {
          try (ExternalDataItemIndexBuilder builder = create()) {
            builder.add(0, new byte[] {10, 20, 30});
          }
        });
  }

  @Test
  public void buildWithSpilling() throws IOException {
    // There's only memory for 3 pages.
    memoryManager.limit(pageSizeBytes * 3);

    // But we'll build an index containing 10 pages of data
    int itemSize = 1000;
    int numItems = (int) pageSizeBytes / itemSize * 10;

    ExternalDataItemIndexBuilder builder = create();
    List<byte[]> expected = new ArrayList<>(numItems);
    Random random = new Random();
    for (int i = 0; i < numItems; i++) {
      byte[] data = new byte[itemSize];
      random.nextBytes(data);
      builder.add(i, data);
      expected.add(data);
    }
    assertTrue(builder.hasSpilled());
    ExternalLeafPageIndex leafPages = createMockLeafPageIndex(numItems, 10);
    int[] itemIdToLeafPageId = leafPages.borrowItemIdToLeafPageId();
    ExternalDataItemIndex index = builder.buildDataItemIndex(leafPages);
    assertTrue(index.isSpilled());
    builder.close();
    verifyDataItemIndex(index, expected, leafPages, itemIdToLeafPageId);
    index.close();
  }

  @Test
  public void fetchDataItemsFromEmptyIndex() throws IOException {
    ExternalDataItemIndexBuilder builder = create();
    ExternalLeafPageIndex leafPages =
        new ExternalLeafPageIndex(taskContext, consumer, taskMemoryManager);
    ExternalDataItemIndex index = builder.buildDataItemIndex(leafPages);
    builder.close();
    Iterator<DataItemWithId> results = index.fetch(IntArrayList.of(0, 1, 2));
    assertFalse(results.hasNext());
    index.close();
  }

  @Test
  public void fetchDataItems() throws IOException {
    ExternalDataItemIndexBuilder builder = create();
    int itemSize = 100;
    int numItems = 1000;
    List<byte[]> expected = new ArrayList<>(numItems);
    Random random = new Random();
    for (int i = 0; i < numItems; i++) {
      byte[] data = new byte[itemSize];
      random.nextBytes(data);
      builder.add(i, data);
      expected.add(data);
    }
    ExternalLeafPageIndex leafPages = createMockLeafPageIndex(numItems, 10);
    ExternalDataItemIndex index = builder.buildDataItemIndex(leafPages);
    builder.close();

    for (int trials = 0; trials < 2; trials++) {
      // Fetch no items
      Iterator<DataItemWithId> iter = index.fetch(IntArrayList.of());
      assertFalse(iter.hasNext());

      // Fetch random items
      int numFetchedItems = random.nextInt(30) + 1;
      IntArrayList itemIds = new IntArrayList();
      for (int i = 0; i < numFetchedItems; i++) {
        itemIds.add(random.nextInt(numItems));
      }
      itemIds = IntArrayList.toList(itemIds.intStream().sorted().distinct());
      iter = index.fetch(itemIds);
      Set<Integer> fetchedItemIds = new HashSet<>();
      while (iter.hasNext()) {
        DataItemWithId item = iter.next();
        assertTrue(itemIds.contains(item.itemId));
        assertTrue(fetchedItemIds.add(item.itemId));
        assertArrayEquals(expected.get(item.itemId), item.dataItem);
      }
      assertEquals(itemIds.size(), fetchedItemIds.size());

      index.spill(diskBlockManager);
    }

    index.close();
  }

  @Test
  public void fetchDataItemsWhileSpilling() throws IOException {
    for (int trial = 0; trial < 100; trial++) {
      ExternalDataItemIndexBuilder builder = create();
      int itemSize = 100;
      int numItems = 1000;
      List<byte[]> expected = new ArrayList<>(numItems);
      Random random = new Random(trial);
      for (int i = 0; i < numItems; i++) {
        byte[] data = new byte[itemSize];
        random.nextBytes(data);
        builder.add(i, data);
        expected.add(data);
      }
      ExternalLeafPageIndex leafPages = createMockLeafPageIndex(numItems, 10);
      ExternalDataItemIndex index = builder.buildDataItemIndex(leafPages);
      builder.close();

      // Fetch random items
      int numFetchedItems = random.nextInt(30) + 1;
      int spillAfter = random.nextInt(numFetchedItems);
      IntList itemIds = new IntArrayList();
      for (int i = 0; i < numFetchedItems; i++) {
        itemIds.add(random.nextInt(numItems));
      }
      itemIds = IntArrayList.toList(itemIds.intStream().sorted().distinct());
      Iterator<DataItemWithId> iter = index.fetch(itemIds);
      Set<Integer> fetchedItemIds = new HashSet<>();
      while (iter.hasNext()) {
        DataItemWithId item = iter.next();
        assertTrue(itemIds.contains(item.itemId));
        assertTrue(fetchedItemIds.add(item.itemId));
        assertArrayEquals(expected.get(item.itemId), item.dataItem);
        if (spillAfter == fetchedItemIds.size()) {
          index.spill(diskBlockManager);
        }
      }
      assertEquals(itemIds.size(), fetchedItemIds.size());
      index.close();
    }
  }

  private ExternalDataItemIndexBuilder create() {
    ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();
    ExternalDataItemIndexBuilder builder =
        new ExternalDataItemIndexBuilder(
            consumer, taskContext, taskMemoryManager, blockManager, pageSizeBytes, writeMetrics);
    consumer.setBuilder(builder);
    return builder;
  }

  private void verifyDataItemIndex(
      ExternalDataItemIndex index,
      List<byte[]> dataItems,
      ExternalLeafPageIndex leafPages,
      int[] itemIdToLeafPageId)
      throws IOException {
    int[] ids = new int[index.positions.length];
    byte[][] actualDataItems = loadDataItems(index, ids);
    assertEquals(dataItems.size(), actualDataItems.length);
    for (int i = 0; i < dataItems.size(); i++) {
      assertArrayEquals(dataItems.get(i), actualDataItems[i]);
    }

    if (index.isSpilled()) {
      // Verify the order of the data items if the data item index has been spilled. The ordering
      // makes sure that the data items referenced by the same leaf page are stored together.
      int[] leafPageRank = leafPages.rankLeafPagesBySpatialProximity();
      int previousRank = 0;
      for (int id : ids) {
        int rank = leafPageRank[itemIdToLeafPageId[id]];
        assertTrue(rank >= previousRank);
      }
    }
  }

  private byte[][] loadDataItems(ExternalDataItemIndex index, int[] ids) throws IOException {
    byte[][] dataItems = new byte[index.positions.length][];
    if (index.isSpilled()) {
      InputStream inputStream = new BufferedInputStream(Files.newInputStream(index.file.toPath()));
      try (DataInputStream din = new DataInputStream(inputStream)) {
        byte[] header = new byte[8];
        for (int i = 0; i < index.positions.length; i++) {
          din.readFully(header, 0, 8);
          int id = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET);
          int length = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET + 4);
          byte[] data = new byte[length];
          din.readFully(data);
          dataItems[id] = data;
          if (ids != null) {
            ids[i] = id;
          }
        }
      }
    } else {
      for (int i = 0; i < index.positions.length; i++) {
        long position = index.positions[i];
        Object page = taskMemoryManager.getPage(position);
        long offset = taskMemoryManager.getOffsetInPage(position);
        int id = Platform.getInt(page, offset);
        int len = Platform.getInt(page, offset + 4);
        byte[] data = new byte[len];
        Platform.copyMemory(page, offset + 8, data, Platform.BYTE_ARRAY_OFFSET, len);
        dataItems[id] = data;
        if (ids != null) {
          ids[i] = id;
        }
      }
    }
    return dataItems;
  }

  private ExternalLeafPageIndex createMockLeafPageIndex(int numItems, int leafPageCapacity) {
    int[] ids = MathArrays.natural(numItems);
    MathArrays.shuffle(ids);

    // Generate fake envelopes using these ids
    long[] envelopeBuffer = new long[ids.length * ExternalLeafPageIndex.LONGS_PER_ENVELOPE];
    LongArray envelopeArray =
        new LongArray(
            new MemoryBlock(
                envelopeBuffer, Platform.LONG_ARRAY_OFFSET, envelopeBuffer.length * 8L));

    // Assign shuffled ids to leaf pages
    List<LeafPageMetadata> leafPageMetadata = new ArrayList<>();
    int[] itemIdToLeafPageId = new int[ids.length];
    int leafPageId = 0;
    int position = 0;

    Random random = new Random();
    while (position < ids.length) {
      int leafPageSize = Math.min(leafPageCapacity, ids.length - position);

      // The bounds of the leaf page does not need to be correct for the purpose of this test
      double minX = random.nextDouble() * 100;
      double maxX = minX + random.nextDouble() * 10;
      double minY = random.nextDouble() * 100;
      double maxY = minY + random.nextDouble() * 10;

      // Associate the ids with the leaf page
      for (int i = 0; i < leafPageSize; i++) {
        itemIdToLeafPageId[ids[position + i]] = leafPageId;
      }

      leafPageMetadata.add(
          new LeafPageMetadata(leafPageId, position, leafPageSize, minX, maxX, minY, maxY));

      position += leafPageSize;
      leafPageId++;
    }

    return new ExternalLeafPageIndex(
        taskContext,
        consumer,
        taskMemoryManager,
        leafPageMetadata,
        envelopeArray,
        itemIdToLeafPageId);
  }

  private static class DummyMemoryConsumer extends SedonaMemoryConsumer {

    ExternalDataItemIndexBuilder builder;

    DummyMemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
      super(taskMemoryManager, pageSize, mode);
    }

    void setBuilder(ExternalDataItemIndexBuilder builder) {
      this.builder = builder;
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) {
      if (trigger == this && builder != null) {
        return builder.spill(false);
      } else {
        return 0L;
      }
    }
  }
}
