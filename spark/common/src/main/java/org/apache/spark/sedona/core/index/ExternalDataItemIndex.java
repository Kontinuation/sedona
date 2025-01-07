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
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.storage.DiskBlockManager;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Index for querying data items by their item ids. This index can be either in-memory or stored on
 * disk.
 */
public class ExternalDataItemIndex implements AutoCloseable {
  static final Logger logger = LoggerFactory.getLogger(ExternalDataItemIndex.class);

  private final TaskContext taskContext;
  private final SedonaMemoryConsumer consumer;
  private final TaskMemoryManager taskMemoryManager;

  /**
   * The positions of the data items in the memory blocks or the file. We can index this array using
   * the data item id.
   *
   * <ul>
   *   <li>In-memory: the positions are the memory addresses of the data items in the memory blocks
   *   <li>External: the positions are the offsets of the data items in the file
   * </ul>
   */
  public long[] positions;

  /** The memory blocks containing the data items when the index is in-memory. */
  public List<MemoryBlock> memoryBlocks;

  /** The sizes of the memory blocks containing the data items when the index is in-memory. */
  public LongArrayList blockSizes;

  /**
   * A lookup table to find the rank of a data item by its item id. This is for sorting the data
   * items to put objects close to each other contiguously on disk. This field only presents when
   * the index is in-memory.
   */
  public int[] itemIdToRank;

  /** The file containing the data items when the index is external. */
  public File file;

  /** A reader for the spill file. */
  private BlockCachedFileReader fileReader;

  /**
   * Construct an index for querying data items by their item ids when the index is in-memory.
   *
   * @param taskContext the task context
   * @param consumer the memory consumer
   * @param taskMemoryManager the task memory manager
   * @param memoryBlocks the memory blocks containing the data items
   * @param blockSizes the sizes of the memory blocks containing the data items
   * @param positions the positions of the data items in the memory blocks
   */
  public ExternalDataItemIndex(
      TaskContext taskContext,
      SedonaMemoryConsumer consumer,
      TaskMemoryManager taskMemoryManager,
      List<MemoryBlock> memoryBlocks,
      LongArrayList blockSizes,
      long[] positions,
      int[] itemIdToRank) {
    this.taskContext = taskContext;
    this.consumer = consumer;
    this.taskMemoryManager = taskMemoryManager;
    this.positions = positions;
    this.memoryBlocks = memoryBlocks;
    this.blockSizes = blockSizes;
    this.itemIdToRank = itemIdToRank;
    this.file = null;
    this.fileReader = null;
  }

  /**
   * Construct an index for querying data items by their item ids when the index is external.
   *
   * @param taskContext the task context
   * @param consumer the memory consumer
   * @param taskMemoryManager the task memory manager
   * @param file the file containing the data items
   * @param positions the positions of the data items in the file
   */
  public ExternalDataItemIndex(
      TaskContext taskContext,
      SedonaMemoryConsumer consumer,
      TaskMemoryManager taskMemoryManager,
      File file,
      long[] positions)
      throws IOException {
    this.taskContext = taskContext;
    this.consumer = consumer;
    this.taskMemoryManager = taskMemoryManager;
    this.file = file;
    this.memoryBlocks = null;
    this.blockSizes = null;
    this.positions = positions;
    this.fileReader = new BlockCachedFileReader(file, taskMemoryManager.pageSizeBytes());
  }

  public boolean isSpilled() {
    return file != null;
  }

  public long spill(DiskBlockManager diskBlockManager) {
    try {
      return doSpill(diskBlockManager);
    } catch (IOException e) {
      // trySpillAndAcquire will catch IOException and rethrow it as a SparkOutOfMemoryError
      // exception, this does not always make the task fail.
      // The data item index will run into an inconsistent state if the spill fails. We should
      // throw a RuntimeException to fail the task.
      throw new RuntimeException("Failed to spill the index to disk", e);
    }
  }

  private long doSpill(DiskBlockManager diskBlockManager) throws IOException {
    List<MemoryBlock> memoryBlocksToFree;
    synchronized (this) {
      if (memoryBlocks == null) {
        // Already spilled, nothing in memory to spill.
        return 0L;
      }

      // Write data items in memory to disk
      Tuple2<TempLocalBlockId, File> tempLocalBlock = diskBlockManager.createTempLocalBlock();
      File dataItemFile = tempLocalBlock._2();

      // The order of data items in the file is determined by the rank of their item ids
      int[] sortedItemIds = new int[positions.length];
      for (int i = 0; i < positions.length; i++) {
        sortedItemIds[i] = i;
      }
      IntArrays.unstableSort(sortedItemIds, IntComparator.comparingInt(i -> itemIdToRank[i]));
      long offset = 0;
      byte[] writeBuffer = new byte[8192];
      try (OutputStream os =
          new BufferedOutputStream(Files.newOutputStream(dataItemFile.toPath()))) {
        for (int itemId : sortedItemIds) {
          long position = positions[itemId];
          Object page = taskMemoryManager.getPage(position);
          long offsetInPage = taskMemoryManager.getOffsetInPage(position);
          int id = Platform.getInt(page, offsetInPage);
          if (id != itemId) {
            throw new IllegalStateException(
                "Inconsistent data item id. expected: " + itemId + ", actual: " + id);
          }
          int dataLength = Platform.getInt(page, offsetInPage + 4);
          if (writeBuffer.length < dataLength + 8) {
            writeBuffer = new byte[(dataLength + 8) * 2];
          }
          Platform.copyMemory(
              page, offsetInPage, writeBuffer, Platform.BYTE_ARRAY_OFFSET, dataLength + 8L);
          os.write(writeBuffer, 0, dataLength + 8);
          positions[itemId] = offset;
          offset += dataLength + 8;
        }
      }
      taskContext.taskMetrics().incDiskBytesSpilled(dataItemFile.length());

      memoryBlocksToFree = memoryBlocks;
      memoryBlocks = null;
      blockSizes = null;
      itemIdToRank = null;
      file = dataItemFile;

      fileReader = new BlockCachedFileReader(file, taskMemoryManager.pageSizeBytes());
    }

    long freedSize = 0;
    for (MemoryBlock memoryBlock : memoryBlocksToFree) {
      freedSize += memoryBlock.size();
      consumer.doFreePage(memoryBlock);
    }

    logger.trace("Spilled {} bytes to disk. Total items: {}", freedSize, positions.length);
    return freedSize;
  }

  /** A data item with its item id. */
  public static class DataItemWithId {
    public final int itemId;
    public final byte[] dataItem;

    public DataItemWithId(int itemId, byte[] dataItem) {
      this.itemId = itemId;
      this.dataItem = dataItem;
    }
  }

  /**
   * Fetch the data items with the specified item ids. Please note that the order of the data items
   * in the iterator is not guaranteed to be the same as the order of the item ids in the input
   * list. This method is free to reorder the data items to improve the performance.
   *
   * @param itemIds the item ids
   * @return the iterator of the data items
   */
  public synchronized Iterator<DataItemWithId> fetch(IntList itemIds) {
    if ((memoryBlocks == null || memoryBlocks.isEmpty()) && file == null) {
      return Collections.emptyIterator();
    }
    return new DataItemIterator(itemIds, this);
  }

  /** An iterator for fetching data items with the specified item ids. */
  private static class DataItemIterator implements Iterator<DataItemWithId> {
    private IntList itemIds;
    private final ExternalDataItemIndex index;
    private boolean sortedByPosition;
    private int currentItemIdIndex;
    private byte[] currentDataItem;

    private DataItemIterator(IntList itemIds, ExternalDataItemIndex index) {
      this.itemIds = itemIds;
      this.index = index;
      this.sortedByPosition = false;
      this.currentItemIdIndex = 0;
      this.currentDataItem = null;
    }

    @Override
    public boolean hasNext() {
      return currentItemIdIndex < itemIds.size();
    }

    @Override
    public DataItemWithId next() {
      if (currentItemIdIndex >= itemIds.size()) {
        throw new NoSuchElementException("No more data items");
      }
      try {
        loadNext();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to load next data item", e);
      }
      int itemId = itemIds.getInt(currentItemIdIndex++);
      return new DataItemWithId(itemId, currentDataItem);
    }

    private void loadNext() throws IOException {
      // It is possible that the index is being spilled while iterating, we need to synchronize
      // on the index to ensure that we have a consistent view of the index.
      synchronized (index) {
        if (index.memoryBlocks != null) {
          loadFromMemory();
        } else if (index.file != null) {
          if (!sortedByPosition) {
            sortRemainingItemIdsByPosition();
            sortedByPosition = true;
          }
          loadFromFile();
        } else {
          throw new IllegalStateException("No data items to load");
        }
      }
    }

    private void loadFromMemory() {
      int itemId = itemIds.getInt(currentItemIdIndex);
      long position = index.positions[itemId];
      Object page = index.taskMemoryManager.getPage(position);
      long offsetInPage = index.taskMemoryManager.getOffsetInPage(position);
      int dataLength = Platform.getInt(page, offsetInPage + 4);
      currentDataItem = new byte[dataLength];
      Platform.copyMemory(
          page, offsetInPage + 8, currentDataItem, Platform.BYTE_ARRAY_OFFSET, dataLength);
    }

    private void loadFromFile() throws IOException {
      int itemId = itemIds.getInt(currentItemIdIndex);
      long position = index.positions[itemId];
      ByteBuffer buffer = index.fileReader.read(position + 4, 4);
      buffer.order(ByteOrder.nativeOrder());
      int dataLength = buffer.getInt();
      currentDataItem = new byte[dataLength];
      buffer = index.fileReader.read(position + 8, dataLength);
      System.arraycopy(
          buffer.array(), buffer.arrayOffset() + buffer.position(), currentDataItem, 0, dataLength);
    }

    private void sortRemainingItemIdsByPosition() {
      if (currentItemIdIndex + 2 >= itemIds.size()) {
        // Too few item ids left, don't bother sorting them.
        return;
      }

      // There are more than two item ids left to load, re-ordering the loading order may improve
      // the block cache hit rate
      if (currentItemIdIndex > 0) {
        // Only sort the remaining item ids. This will truncate the itemIds list.
        IntArrayList subList =
            new IntArrayList(itemIds.subList(currentItemIdIndex, itemIds.size()));
        subList.sort(IntComparator.comparingLong(id -> index.positions[id]));
        itemIds = subList;
        currentItemIdIndex = 0;
      } else {
        // Sort the whole list
        itemIds.sort(IntComparator.comparingLong(id -> index.positions[id]));
      }
    }
  }

  @Override
  public void close() {
    File fileToDelete;
    List<MemoryBlock> memoryBlocksToFree;
    BlockCachedFileReader fileReaderToClose;

    synchronized (this) {
      fileToDelete = file;
      file = null;
      memoryBlocksToFree = memoryBlocks;
      memoryBlocks = null;
      blockSizes = null;
      itemIdToRank = null;
      fileReaderToClose = fileReader;
      fileReader = null;
      positions = null;
    }

    if (memoryBlocksToFree != null) {
      for (MemoryBlock memoryBlock : memoryBlocksToFree) {
        consumer.doFreePage(memoryBlock);
      }
    }
    if (fileReaderToClose != null) {
      try {
        fileReaderToClose.close();
      } catch (IOException e) {
        logger.warn("Failed to close the spilled file reader", e);
      }
    }
    if (fileToDelete != null) {
      if (!fileToDelete.delete()) {
        logger.warn("Failed to delete file {}", fileToDelete);
      }
    }
  }
}
