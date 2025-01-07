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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.storage.DiskBlockManager;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.shape.fractal.HilbertCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * A class to hold in-memory or external leaf pages. As the class name indicates, it can be either
 * in-memory leaf pages or external leaf pages spilled to disk.
 *
 * <ul>
 *   <li>In-memory leaf pages: the envelopes are stored in memory in the `envelopes` field
 *   <li>External leaf pages: the envelopes are spilled to disk in the `file` field, with block id
 *       stored in the `blockId` field
 * </ul>
 */
public class ExternalLeafPageIndex implements AutoCloseable {

  static final Logger logger = LoggerFactory.getLogger(ExternalLeafPageIndex.class);

  public static final long BYTES_PER_ENVELOPE = 40L;
  public static final int BYTES_PER_ENVELOPE_I = (int) BYTES_PER_ENVELOPE;
  public static final int LONGS_PER_ENVELOPE = BYTES_PER_ENVELOPE_I / 8;

  public static class LeafPageMetadata {
    /** The id of the leaf page. */
    public final int id;

    /**
     * Position is an Envelope array index when the envelopes are in memory or spilled to disk. When
     * the envelopes are spilled to disk, the offset of the envelope on the spill file is position *
     * 40.
     */
    public final int position;

    /**
     * Number of envelopes in the leaf page. The envelopes in the same leaf page are adjacent in the
     * envelope array when envelopes are in memory, and also adjacent in the envelope data file when
     * envelopes are spilled to disk.
     */
    public final int size;

    public final double minX;
    public final double maxX;
    public final double minY;
    public final double maxY;

    public LeafPageMetadata(
        int id, int position, int size, double minX, double maxX, double minY, double maxY) {
      this.id = id;
      this.position = position;
      this.size = size;
      this.minX = minX;
      this.maxX = maxX;
      this.minY = minY;
      this.maxY = maxY;
    }

    public LeafPageMetadata(int id, int position, int size, double[] bounds) {
      this(id, position, size, bounds[0], bounds[1], bounds[2], bounds[3]);
    }
  }

  private final TaskContext taskContext;
  private final MemoryConsumer consumer;
  private final TaskMemoryManager taskMemoryManager;

  public List<LeafPageMetadata> metadata;
  public LongArray envelopes;
  public File file;

  /**
   * A lookup table to find the leaf page id of an item. This is for sorting the items by their
   * associated leaf page ids. Some other parts of the external spatial index will need it. NOTE: DO
   * NOT derive number of envelopes using itemIdToLeafPageId.length. There might be multiple
   * envelopes having the same ID (when subdividing happens). Since there may be multiple envelopes
   * with the same ID, itemIdToLeafPageId[id] only gives one of the leaf page IDs. The collection of
   * envelopes with the same ID may be scattered in multiple leaf pages.
   */
  private int[] itemIdToLeafPageId;

  /** A reader for the spill file. */
  private BlockCachedFileReader fileReader;

  /** A cache for most frequently accessed leaf pages. Only used when leaf pages are spilled. */
  private Cache<Integer, LongArray> cachedLeafPages;

  /**
   * Construct an object holding in-memory leaf pages
   *
   * @param taskContext the task context
   * @param consumer the memory consumer
   * @param taskMemoryManager the task memory manager
   * @param metadata metadata of the leaf pages
   * @param envelopes envelopes of the leaf pages
   * @param itemIdToLeafPageId a lookup table to find the leaf page id of an item
   */
  public ExternalLeafPageIndex(
      TaskContext taskContext,
      MemoryConsumer consumer,
      TaskMemoryManager taskMemoryManager,
      List<LeafPageMetadata> metadata,
      LongArray envelopes,
      int[] itemIdToLeafPageId) {
    this.taskContext = taskContext;
    this.consumer = consumer;
    this.taskMemoryManager = taskMemoryManager;
    this.metadata = metadata;
    this.envelopes = envelopes;
    this.file = null;
    this.itemIdToLeafPageId = itemIdToLeafPageId;
    this.fileReader = null;
    this.cachedLeafPages = null;
  }

  /**
   * Construct an object holding external leaf pages
   *
   * @param taskContext the task context
   * @param consumer the memory consumer
   * @param taskMemoryManager the task memory manager
   * @param metadata metadata of the leaf pages
   * @param file file containing the leaf pages
   * @param itemIdToLeafPageId a lookup table to find the leaf page id of an item
   */
  public ExternalLeafPageIndex(
      TaskContext taskContext,
      MemoryConsumer consumer,
      TaskMemoryManager taskMemoryManager,
      List<LeafPageMetadata> metadata,
      File file,
      int[] itemIdToLeafPageId)
      throws IOException {
    this.taskContext = taskContext;
    this.consumer = consumer;
    this.taskMemoryManager = taskMemoryManager;
    this.metadata = metadata;
    this.file = file;
    this.envelopes = null;
    this.itemIdToLeafPageId = itemIdToLeafPageId;
    this.fileReader = new BlockCachedFileReader(file, taskMemoryManager.pageSizeBytes());
    this.cachedLeafPages = Caffeine.newBuilder().softValues().executor(Runnable::run).build();
  }

  /**
   * Construct an object holding no leaf pages
   *
   * @param taskContext the task context
   * @param consumer the memory consumer
   * @param taskMemoryManager the task memory manager
   */
  public ExternalLeafPageIndex(
      TaskContext taskContext, MemoryConsumer consumer, TaskMemoryManager taskMemoryManager) {
    this.taskContext = taskContext;
    this.consumer = consumer;
    this.taskMemoryManager = taskMemoryManager;
    this.metadata = new ArrayList<>();
    this.envelopes = null;
    this.file = null;
    this.itemIdToLeafPageId = new int[0];
    this.fileReader = null;
    this.cachedLeafPages = null;
  }

  public boolean isSpilled() {
    return file != null;
  }

  /**
   * Rank the leaf pages by their spatial proximity. This is for gathering the data items close to
   * each other when writing them to disk, thus improving the disk I/O efficiency.
   *
   * @return an array containing the rank of each leaf page
   */
  public int[] rankLeafPagesBySpatialProximity() {
    int numLeafPages = this.metadata.size();

    // Derive the bounds of all leaf pages
    double minX = Double.MAX_VALUE;
    double maxX = -Double.MAX_VALUE;
    double minY = Double.MAX_VALUE;
    double maxY = -Double.MAX_VALUE;
    for (LeafPageMetadata metadata : this.metadata) {
      if (metadata.minX <= metadata.maxX) {
        minX = Math.min(minX, metadata.minX);
        maxX = Math.max(maxX, metadata.maxX);
        minY = Math.min(minY, metadata.minY);
        maxY = Math.max(maxY, metadata.maxY);
      }
    }

    // If everything collapses to a point, we can't compute the hilbert value.
    // Simply preserve the original order.
    if (minX >= maxX || minY >= maxY) {
      return IntStream.range(0, numLeafPages).toArray();
    }

    double xFrac = (1 << 15) / (maxX - minX);
    double yFrac = (1 << 15) / (maxY - minY);
    // Compute the hilbert index of each leaf page
    int[] hilbertValues = new int[numLeafPages];
    for (int k = 0; k < numLeafPages; k++) {
      LeafPageMetadata metadata = this.metadata.get(k);
      double centroidX = (metadata.minX + metadata.maxX) / 2;
      double centroidY = (metadata.minY + metadata.maxY) / 2;
      int x = (int) ((centroidX - minX) * xFrac);
      int y = (int) ((centroidY - minY) * yFrac);
      int hilbertValue = HilbertCode.encode(16, x, y);
      hilbertValues[k] = hilbertValue;
    }

    // Resolve ranks of the leaf pages according to their hilbert values
    int[] ranks = new int[numLeafPages];
    for (int i = 0; i < numLeafPages; i++) {
      ranks[i] = i;
    }
    IntArrays.unstableSort(ranks, IntComparator.comparingInt(a -> hilbertValues[a]));
    return ranks;
  }

  public int[] borrowItemIdToLeafPageId() {
    if (itemIdToLeafPageId == null) {
      throw new IllegalStateException("itemIdToLeafPageId has already been consumed");
    }
    return itemIdToLeafPageId;
  }

  public int[] consumeItemIdToLeafPageId() {
    if (itemIdToLeafPageId == null) {
      throw new IllegalStateException("itemIdToLeafPageId has already been consumed");
    }
    int[] result = itemIdToLeafPageId;
    itemIdToLeafPageId = null;
    return result;
  }

  public long spill(DiskBlockManager diskBlockManager) {
    try {
      return doSpill(diskBlockManager);
    } catch (IOException e) {
      // trySpillAndAcquire will catch IOException and rethrow it as a SparkOutOfMemoryError
      // exception, this does not always make the task fail.
      // The leaf page index will run into an inconsistent state if the spill fails. We should
      // throw a RuntimeException to fail the task.
      throw new RuntimeException("Failed to spill leaf pages to disk", e);
    }
  }

  private long doSpill(DiskBlockManager diskBlockManager) throws IOException {
    LongArray envelopesToFree;
    long freedSize;
    int numEnvelopes = 0;
    synchronized (this) {
      if (envelopes == null) {
        // Already spilled, nothing in memory to spill.
        return 0L;
      }

      // Write leaf pages in memory to disk
      Tuple2<TempLocalBlockId, File> tempLocalBlock = diskBlockManager.createTempLocalBlock();
      File leafPageFile = tempLocalBlock._2();

      for (LeafPageMetadata metadata : this.metadata) {
        numEnvelopes += metadata.size;
      }
      try (OutputStream os =
          new BufferedOutputStream(Files.newOutputStream(leafPageFile.toPath()))) {
        byte[] buffer = new byte[8192];
        writeEnvelopesToStream(envelopes, numEnvelopes, os, buffer);
      }
      taskContext.taskMetrics().incDiskBytesSpilled(leafPageFile.length());

      envelopesToFree = envelopes;
      envelopes = null;
      file = leafPageFile;
      fileReader = new BlockCachedFileReader(leafPageFile, taskMemoryManager.pageSizeBytes());
      this.cachedLeafPages = Caffeine.newBuilder().softValues().executor(Runnable::run).build();
    }

    freedSize = envelopesToFree.memoryBlock().size();
    consumer.freeArray(envelopesToFree);
    logger.trace("Spilled {} envelopes to disk, freed bytes: {}", numEnvelopes, freedSize);
    return freedSize;
  }

  public static void writeEnvelopesToStream(
      LongArray envelopeArray, int numEnvelopes, OutputStream stream, byte[] buffer)
      throws IOException {
    int pos = 0;
    int endPos = numEnvelopes * BYTES_PER_ENVELOPE_I;
    while (pos < endPos) {
      int bytesToWrite = Math.min(buffer.length, endPos - pos);
      Platform.copyMemory(
          envelopeArray.getBaseObject(),
          envelopeArray.getBaseOffset() + pos,
          buffer,
          Platform.BYTE_ARRAY_OFFSET,
          bytesToWrite);
      stream.write(buffer, 0, bytesToWrite);
      pos += bytesToWrite;
    }
  }

  public static int get(LongArray envelopeArray, int index, double[] result) {
    int id = (int) envelopeArray.get(index * LONGS_PER_ENVELOPE);
    result[0] = Double.longBitsToDouble(envelopeArray.get(index * LONGS_PER_ENVELOPE + 1));
    result[1] = Double.longBitsToDouble(envelopeArray.get(index * LONGS_PER_ENVELOPE + 2));
    result[2] = Double.longBitsToDouble(envelopeArray.get(index * LONGS_PER_ENVELOPE + 3));
    result[3] = Double.longBitsToDouble(envelopeArray.get(index * LONGS_PER_ENVELOPE + 4));
    return id;
  }

  /**
   * Build an STR tree for the leaf pages
   *
   * @param internalNodeCapacity the capacity of the internal nodes of the STR tree
   * @return the STR tree
   */
  public STRtree buildNonLeafTree(int internalNodeCapacity) {
    STRtree nonLeafTree = new STRtree(internalNodeCapacity);
    for (int leafId = 0; leafId < this.metadata.size(); leafId++) {
      LeafPageMetadata leafMetadata = this.metadata.get(leafId);
      Envelope leafEnvelope;
      if (leafMetadata.minX <= leafMetadata.maxX) {
        leafEnvelope =
            new Envelope(
                leafMetadata.minX, leafMetadata.maxX, leafMetadata.minY, leafMetadata.maxY);
      } else {
        leafEnvelope = new Envelope();
      }
      nonLeafTree.insert(leafEnvelope, leafId);
    }
    nonLeafTree.build();
    return nonLeafTree;
  }

  /** A filter for envelopes in the leaf pages */
  public interface EnvelopeFilter {
    boolean filter(int id, double[] envelope);
  }

  /**
   * Query specified leaf pages for envelopes that intersect with the search envelope
   *
   * @param searchEnv the search envelope
   * @param leafIds the leaf page ids
   * @return the list of item ids that intersect with the search envelope
   */
  public synchronized IntList query(Envelope searchEnv, IntList leafIds) throws IOException {
    if (searchEnv.isNull()) {
      return IntArrayList.of();
    }
    if (envelopes != null) {
      return queryInMemory(searchEnv, null, leafIds);
    } else if (file != null) {
      return querySpilled(searchEnv, null, leafIds);
    } else {
      return IntArrayList.of();
    }
  }

  /**
   * Query specified leaf pages for envelopes that match the filter
   *
   * @param filter the filter
   * @param leafIds the leaf page ids
   * @return the list of item ids that match the filter
   */
  public synchronized IntList query(EnvelopeFilter filter, IntList leafIds) throws IOException {
    if (envelopes != null) {
      return queryInMemory(null, filter, leafIds);
    } else if (file != null) {
      return querySpilled(null, filter, leafIds);
    } else {
      return IntArrayList.of();
    }
  }

  private static void queryEnvelopeArray(
      LongArray envelopeArray,
      int pos,
      int size,
      Envelope searchEnv,
      IntList itemIds,
      double[] scratch) {
    double minX = searchEnv.getMinX();
    double maxX = searchEnv.getMaxX();
    double minY = searchEnv.getMinY();
    double maxY = searchEnv.getMaxY();
    int end = pos + size;
    for (; pos < end; pos++) {
      int id = get(envelopeArray, pos, scratch);
      // Note: scratch[0] <= scratch[1] is for checking if the envelope is non-null
      if (scratch[0] <= scratch[1]
          && scratch[0] <= maxX
          && scratch[1] >= minX
          && scratch[2] <= maxY
          && scratch[3] >= minY) {
        itemIds.add(id);
      }
    }
  }

  private static void filterEnvelopeArray(
      LongArray envelopeArray,
      int pos,
      int size,
      EnvelopeFilter filter,
      IntList itemIds,
      double[] scratch) {
    int end = pos + size;
    for (; pos < end; pos++) {
      int id = get(envelopeArray, pos, scratch);
      // Note: scratch[0] <= scratch[1] is for checking if the envelope is non-null
      if (scratch[0] <= scratch[1] && filter.filter(id, scratch)) {
        itemIds.add(id);
      }
    }
  }

  private IntList queryInMemory(Envelope searchEnv, EnvelopeFilter filter, IntList leafIds) {
    IntList itemIds = new IntArrayList(leafIds.size());
    double[] scratch = new double[4];
    for (int leafId : leafIds) {
      LeafPageMetadata leafMetadata = this.metadata.get(leafId);
      if (searchEnv != null) {
        queryEnvelopeArray(
            envelopes, leafMetadata.position, leafMetadata.size, searchEnv, itemIds, scratch);
      } else if (filter != null) {
        filterEnvelopeArray(
            envelopes, leafMetadata.position, leafMetadata.size, filter, itemIds, scratch);
      }
    }
    return itemIds;
  }

  private IntList querySpilled(Envelope searchEnv, EnvelopeFilter filter, IntList leafIds)
      throws IOException {
    double[] scratch = new double[4];
    IntList itemIds = new IntArrayList(leafIds.size());

    // Sort the leaf ids to make access to the spill file sequential, and reuse the same
    // cached block for nearby leaf pages.
    int[] sortedLeafIds = leafIds.toIntArray();
    Arrays.sort(sortedLeafIds);

    // 1. Find the leaf pages from the cache
    for (int i = 0; i < sortedLeafIds.length; i++) {
      int leafId = sortedLeafIds[i];
      LongArray envelopeArray = cachedLeafPages.getIfPresent(leafId);
      if (envelopeArray != null) {
        int size = metadata.get(leafId).size;
        if (searchEnv != null) {
          queryEnvelopeArray(envelopeArray, 0, size, searchEnv, itemIds, scratch);
        } else if (filter != null) {
          filterEnvelopeArray(envelopeArray, 0, size, filter, itemIds, scratch);
        }
        // Mark the leaf page as processed
        sortedLeafIds[i] = -1;
      }
    }

    // 2. Find the leaf pages from the spill file
    for (int leafId : sortedLeafIds) {
      if (leafId == -1) {
        continue;
      }

      LeafPageMetadata leafMetadata = this.metadata.get(leafId);
      long offset = leafMetadata.position * BYTES_PER_ENVELOPE;
      ByteBuffer buffer = fileReader.read(offset, leafMetadata.size * BYTES_PER_ENVELOPE_I);
      byte[] data;
      if (buffer.position() == 0 && buffer.capacity() == buffer.remaining()) {
        data = buffer.array();
      } else {
        data = new byte[buffer.remaining()];
        buffer.get(data);
      }
      LongArray envelopeArray =
          new LongArray(new MemoryBlock(data, Platform.BYTE_ARRAY_OFFSET, data.length));
      if (searchEnv != null) {
        queryEnvelopeArray(envelopeArray, 0, leafMetadata.size, searchEnv, itemIds, scratch);
      } else if (filter != null) {
        filterEnvelopeArray(envelopeArray, 0, leafMetadata.size, filter, itemIds, scratch);
      }
      cachedLeafPages.put(leafId, envelopeArray);
    }

    return itemIds;
  }

  /** For testing purposes */
  public void invalidateCache() {
    if (cachedLeafPages != null) {
      cachedLeafPages.invalidateAll();
    }
  }

  @Override
  public void close() {
    File fileToDelete;
    LongArray envelopesToFree;
    BlockCachedFileReader fileReaderToClose;

    synchronized (this) {
      envelopesToFree = envelopes;
      envelopes = null;
      fileToDelete = file;
      file = null;
      metadata.clear();
      itemIdToLeafPageId = null;
      fileReaderToClose = fileReader;
      fileReader = null;
      cachedLeafPages = null;
    }

    if (envelopesToFree != null) {
      consumer.freeArray(envelopesToFree);
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
        logger.warn("Failed to delete file: {}", fileToDelete.getAbsolutePath());
      }
    }
  }
}
