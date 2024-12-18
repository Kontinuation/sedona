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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class ExternalDataItemIndexBuilder extends SpillableBuilderBase implements AutoCloseable {

  static final Logger logger = LoggerFactory.getLogger(ExternalDataItemIndexBuilder.class);

  private final SedonaMemoryConsumer memoryConsumer;
  private final TaskContext taskContext;
  private final TaskMemoryManager taskMemoryManager;
  private final long pageSizeBytes;
  private final BlockManager blockManager;

  /**
   * The data items are stored in a memory block. Each data item is stored as a 4-byte id, a 4-byte
   * length, and the serialized data item.
   */
  private List<MemoryBlock> memoryBlocks = new ArrayList<>();

  /** The actual end of payloads of each memory block in the `memoryBlocks` list. */
  private LongArrayList blockSizes = new LongArrayList();

  /** The current memory block that is used to store newly added serialized data items */
  private MemoryBlock currentMemoryBlock = null;

  /** The offset of the next serialized data item to be added to the currentMemoryBlock */
  private long offset = 0;

  /** Total number of data items added to this builder */
  private int totalItems = 0;

  public ExternalDataItemIndexBuilder(
      SedonaMemoryConsumer memoryConsumer,
      TaskContext taskContext,
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      long pageSizeBytes,
      ShuffleWriteMetricsReporter shuffleWriteMetricsReporter) {
    super(blockManager, taskContext, shuffleWriteMetricsReporter);
    this.memoryConsumer = memoryConsumer;
    this.taskContext = taskContext;
    this.taskMemoryManager = taskMemoryManager;
    this.pageSizeBytes = pageSizeBytes;
    this.blockManager = blockManager;
  }

  public void add(int id, byte[] data) {
    growIfNecessary(data.length);

    // Finally, we can safely add the serialized data item to the memory block without triggering a
    // spill.
    nonSpillingAdd(id, data);
  }

  private void growIfNecessary(long dataSize) {
    long requiredSize = offset + dataSize + 8;
    if (currentMemoryBlock != null && requiredSize <= currentMemoryBlock.size()) {
      // The space left in the current memory block is enough to store the new
      // serialized data item.
      return;
    }

    // Allocate a new memory block for storing the new serialized data item
    long newBlockSize = Math.max(pageSizeBytes, dataSize + 8);
    MemoryBlock newMemoryBlock = memoryConsumer.doAllocatePage(newBlockSize);
    if (currentMemoryBlock != null) {
      memoryBlocks.add(currentMemoryBlock);
      blockSizes.add(offset);
    }
    currentMemoryBlock = newMemoryBlock;
    offset = 0;
  }

  private void nonSpillingAdd(int id, byte[] data) {
    assert currentMemoryBlock != null;
    assert offset + data.length + 8 <= currentMemoryBlock.size();
    if (id != totalItems) {
      throw new IllegalArgumentException("id of data items should be sequential starting from 0");
    }

    Platform.putInt(
        currentMemoryBlock.getBaseObject(), currentMemoryBlock.getBaseOffset() + offset, id);
    Platform.putInt(
        currentMemoryBlock.getBaseObject(),
        currentMemoryBlock.getBaseOffset() + offset + 4,
        data.length);
    Platform.copyMemory(
        data,
        Platform.BYTE_ARRAY_OFFSET,
        currentMemoryBlock.getBaseObject(),
        currentMemoryBlock.getBaseOffset() + offset + 8,
        data.length);
    offset += data.length + 8;
    totalItems++;
  }

  private void sealCurrentMemoryBlock() {
    if (currentMemoryBlock != null && offset > 0) {
      memoryBlocks.add(currentMemoryBlock);
      blockSizes.add(offset);
    }
    currentMemoryBlock = null;
    offset = 0;
  }

  public long spill(boolean force) {
    List<MemoryBlock> memoryBlocksToFree = null;
    long freedSize = 0;

    try {
      synchronized (this) {
        if (force || (currentMemoryBlock != null && offset * 2 >= currentMemoryBlock.size())) {
          sealCurrentMemoryBlock();
        }

        if (memoryBlocks.isEmpty()) {
          // Nothing to spill
          return 0;
        }

        // Write serialized data items in `memoryBlocks` to a temp file.
        initializeSpillWriter();
        for (int i = 0; i < memoryBlocks.size(); i++) {
          MemoryBlock memoryBlock = memoryBlocks.get(i);
          long blockSize = blockSizes.getLong(i);
          freedSize += memoryBlock.size();

          // Write this memory block to the spill file
          long pos = 0;
          while (pos < blockSize) {
            int bytesToWrite = (int) Math.min(spillWriteBuffer.length, blockSize - pos);
            Platform.copyMemory(
                memoryBlock.getBaseObject(),
                memoryBlock.getBaseOffset() + pos,
                spillWriteBuffer,
                Platform.BYTE_ARRAY_OFFSET,
                bytesToWrite);
            spillWriter.write(spillWriteBuffer, 0, bytesToWrite);
            pos += bytesToWrite;
          }
        }

        taskContext.taskMetrics().incMemoryBytesSpilled(freedSize);

        // Do not free the pages while we are locking `ExternalDataItemIndexBuilder`. The `freePage`
        // method locks the `TaskMemoryManager`, and it's not a good idea to lock 2 objects
        // in sequence. We may hit deadlock if another thread locks `TaskMemoryManager`
        // and `ExternalDataItemIndexBuilder` in sequence, which may happen in
        // `TaskMemoryManager.acquireExecutionMemory` (when ExternalDataItemIndexBuilder is selected
        // for spilling).
        memoryBlocksToFree = memoryBlocks;
        memoryBlocks = new ArrayList<>();
        blockSizes.clear();
      }
    } finally {
      if (memoryBlocksToFree != null) {
        for (MemoryBlock memoryBlock : memoryBlocksToFree) {
          memoryConsumer.doFreePage(memoryBlock);
        }
      }
    }

    logger.trace("Spilled {} bytes to disk. Total items: {}", freedSize, totalItems);
    return freedSize;
  }

  public long getMemoryUsage() {
    long memoryUsage = 0;
    if (memoryBlocks != null) {
      for (MemoryBlock memoryBlock : memoryBlocks) {
        memoryUsage += memoryBlock.size();
      }
    }
    if (currentMemoryBlock != null) {
      memoryUsage += currentMemoryBlock.size();
    }
    return memoryUsage;
  }

  public ExternalDataItemIndex buildDataItemIndex(ExternalLeafPageIndex leafPages)
      throws IOException {
    if (!hasSpilled()) {
      return buildDataItemIndexInMemory(leafPages);
    } else {
      return buildDataItemIndexExternal(leafPages);
    }
  }

  private ExternalDataItemIndex buildDataItemIndexInMemory(ExternalLeafPageIndex leafPages) {
    long[] addresses = new long[totalItems];

    // Derive the rank of each data item for future sorting when spilling happens
    int[] itemIdToRank = new int[totalItems];
    int[] leafIdToRank = leafPages.rankLeafPagesBySpatialProximity();
    int[] itemIdToLeafId = leafPages.consumeItemIdToLeafPageId();
    for (int i = 0; i < totalItems; i++) {
      itemIdToRank[i] = leafIdToRank[itemIdToLeafId[i]];
    }

    // If there is data in the `currentMemoryBlock`, we need to add it to `memoryBlocks` and
    // `blockSizes`.
    sealCurrentMemoryBlock();

    // Now it should be safe to transfer the ownership of `memoryBlocks` and `blockSizes` to the
    // `ExternalDataItemIndex` object. Almost zero on-heap object allocations will happen after this
    // point.
    List<MemoryBlock> memoryBlocks = this.memoryBlocks;
    LongArrayList blockSizes = this.blockSizes;
    this.memoryBlocks = null;
    this.blockSizes = null;

    // Iterate through all memory blocks to get the addresses of the data items
    int addressIndex = 0;
    for (int i = 0; i < memoryBlocks.size(); i++) {
      MemoryBlock memoryBlock = memoryBlocks.get(i);
      long blockSize = blockSizes.getLong(i);
      long pos = 0;
      while (pos < blockSize) {
        // Read the first 8 bytes to get the id and the length of the data item
        int id = Platform.getInt(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset() + pos);
        int dataLength =
            Platform.getInt(memoryBlock.getBaseObject(), memoryBlock.getBaseOffset() + pos + 4);
        assert id == addressIndex;
        addresses[addressIndex++] =
            taskMemoryManager.encodePageNumberAndOffset(
                memoryBlock, memoryBlock.getBaseOffset() + pos);

        // Move to the next data item
        pos += dataLength + 8;
      }
    }

    return new ExternalDataItemIndex(
        taskContext,
        memoryConsumer,
        taskMemoryManager,
        memoryBlocks,
        blockSizes,
        addresses,
        itemIdToRank);
  }

  private ExternalDataItemIndex buildDataItemIndexExternal(ExternalLeafPageIndex leafPages)
      throws IOException {
    // Force spilling everything to disk. This is for ensuring that the external sorter has
    // enough memory to sort data items.
    spill(true);

    int[] itemIdToLeafPageId = leafPages.consumeItemIdToLeafPageId();
    int[] leafPageRanks = leafPages.rankLeafPagesBySpatialProximity();
    DataItemComparator comparator = new DataItemComparator(leafPageRanks, itemIdToLeafPageId);

    // Create an external sorter for sorting the data items by their ranks
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
    try {
      return doBuildDataItemIndexExternal(leafPages, sorter, leafPageRanks, itemIdToLeafPageId);
    } finally {
      sorter.cleanupResources();
      comparator.freeMemory();
    }
  }

  private ExternalDataItemIndex doBuildDataItemIndexExternal(
      ExternalLeafPageIndex leafPages,
      UnsafeExternalSorter sorter,
      int[] leafPageRanks,
      int[] itemIdToLeafPageId)
      throws IOException {
    if (currentMemoryBlock != null || !this.memoryBlocks.isEmpty()) {
      throw new IllegalStateException(
          "The memory blocks should have been spilled to disk before calling this method");
    }
    this.memoryBlocks = null;
    this.blockSizes = null;

    // Write data in the spill file to the sorter
    closeSpillWriter(spillWriter);
    spillWriter = null;
    byte[] readBuffer = spillWriteBuffer;
    try (SpillFileSequentialReader spillReader =
        new SpillFileSequentialReader(spillFile, spillBlockId, blockManager.serializerManager())) {
      while (true) {
        taskContext.killTaskIfInterrupted();
        try {
          spillReader.readFully(readBuffer, 0, 8);
          int id = Platform.getInt(readBuffer, Platform.BYTE_ARRAY_OFFSET);
          int dataLength = Platform.getInt(readBuffer, Platform.BYTE_ARRAY_OFFSET + 4);
          int leafPageId = itemIdToLeafPageId[id];
          long prefix = leafPageRanks[leafPageId];
          if (readBuffer.length < dataLength + 8) {
            readBuffer = new byte[(dataLength + 8) * 2];
          }
          spillReader.readFully(readBuffer, 8, dataLength);
          sorter.insertRecord(
              readBuffer, Platform.BYTE_ARRAY_OFFSET, dataLength + 8, prefix, false);
        } catch (IOException e) {
          break;
        }
      }
    }
    deleteSpillFile(spillFile);
    spillFile = null;
    spillBlockId = null;

    UnsafeSorterIterator sortedIterator = sorter.getSortedIterator();

    // Read the data items from the sorter and write them to a spill file
    Tuple2<TempLocalBlockId, File> tempLocalBlock =
        blockManager.diskBlockManager().createTempLocalBlock();
    File dataItemFile = tempLocalBlock._2();
    long[] offsets = new long[totalItems];
    long offset = 0;
    try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(dataItemFile.toPath()))) {
      byte[] writeBuffer = readBuffer;
      while (sortedIterator.hasNext()) {
        sortedIterator.loadNext();
        int length = sortedIterator.getRecordLength();
        if (length > writeBuffer.length) {
          writeBuffer = new byte[length * 2];
        }
        Platform.copyMemory(
            sortedIterator.getBaseObject(),
            sortedIterator.getBaseOffset(),
            writeBuffer,
            Platform.BYTE_ARRAY_OFFSET,
            length);
        os.write(writeBuffer, 0, length);

        int id = Platform.getInt(writeBuffer, Platform.BYTE_ARRAY_OFFSET);
        offsets[id] = offset;
        offset += length;
      }
    }
    taskContext.taskMetrics().incDiskBytesSpilled(dataItemFile.length());

    return new ExternalDataItemIndex(
        taskContext, memoryConsumer, taskMemoryManager, dataItemFile, offsets);
  }

  @Override
  public void close() {
    List<MemoryBlock> memoryBlocksToFree;

    // Evacuate the members of this object in a synchronized block to avoid race conditions
    // caused by spilling.
    synchronized (this) {
      memoryBlocksToFree = memoryBlocks;
      if (currentMemoryBlock != null) {
        memoryBlocksToFree.add(currentMemoryBlock);
      }

      currentMemoryBlock = null;
      offset = 0;
      memoryBlocks = null;
      blockSizes = null;
      super.close();
    }

    if (memoryBlocksToFree != null) {
      for (MemoryBlock memoryBlock : memoryBlocksToFree) {
        memoryConsumer.doFreePage(memoryBlock);
      }
    }
  }

  private static class DataItemComparator extends RecordComparator {
    private int[] leafPageRanks;
    private int[] itemIdToLeafPageId;

    DataItemComparator(int[] leafPageRanks, int[] itemIdToLeafPageId) {
      this.leafPageRanks = leafPageRanks;
      this.itemIdToLeafPageId = itemIdToLeafPageId;
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
      // the same. We are already using rank as prefix for sorting, so we can directly return
      // 0 here. However, we still perform the following comparisons for safety.
      int leftId = Platform.getInt(leftBaseObject, leftBaseOffset);
      int rightId = Platform.getInt(rightBaseObject, rightBaseOffset);
      int leftRank = leafPageRanks[itemIdToLeafPageId[leftId]];
      int rightRank = leafPageRanks[itemIdToLeafPageId[rightId]];
      return Integer.compare(leftRank, rightRank);
    }

    /**
     * Manually dropping references to the large arrays. The comparator, as well as the unsafe
     * external sorter holding it, may live until the task is completed since the sorter is
     * registered as a consumer in the task memory manager and will only be cleaned up after the
     * task is completed. This may cause a large amount of memory to be held unnecessarily. This
     * method is called after the sorter has been done with the comparator, and it is safe to drop
     * the references to the large arrays.
     */
    public void freeMemory() {
      leafPageRanks = null;
      itemIdToLeafPageId = null;
    }
  }
}
