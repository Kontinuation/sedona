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

import static org.apache.spark.sedona.core.index.ExternalLeafPageIndex.BYTES_PER_ENVELOPE;
import static org.apache.spark.sedona.core.index.ExternalLeafPageIndex.BYTES_PER_ENVELOPE_I;
import static org.apache.spark.sedona.core.index.ExternalLeafPageIndex.LONGS_PER_ENVELOPE;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sedona.core.index.ExternalLeafPageIndex.LeafPageMetadata;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.SortDataFormat;
import org.apache.spark.util.collection.Sorter;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators.DoublePrefixComparator;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * A class that builds leaf pages containing envelopes using on-heap or off-heap memory. The
 * envelopes in leaf pages are stored contiguously in either memory or file. Each envelope is
 * associated with an id. The memory layout of the envelope sequence is:
 *
 * <pre>
 * [id0, minX0, maxX0, minY0, maxY0, id1, minX1, maxX1, minY1, maxY1, ...]
 * </pre>
 *
 * When building the leaf pages, envelopes that belong to the same leaf page will be re-ordered to
 * be stored contiguously in memory. The `ExternalLeafPages` object will only need to reference the
 * starting position and the number of envelopes in the envelope sequence.
 */
public class ExternalLeafPageIndexBuilder extends SpillableBuilderBase implements AutoCloseable {

  static final Logger logger = LoggerFactory.getLogger(ExternalLeafPageIndexBuilder.class);

  private final MemoryConsumer consumer;
  private final TaskContext taskContext;
  private final TaskMemoryManager taskMemoryManager;
  private final BlockManager blockManager;
  private final long initialSizeBytes;

  /**
   * The envelopes are stored contiguously in memory in the `envelopeArray` field. We prefer to keep
   * the envelope array compact since envelopeArray is less likely to be spilled to disk, and it is
   * faster to sort and build the leaf pages when the envelope array is compact.
   */
  private LongArray envelopeArray = null;

  /** The number of envelopes added to the leaf page index builder. */
  private int size = 0;

  /**
   * The capacity of the envelope array. The usable capacity is 2/3 of the capacity when no spilling
   * happens.
   */
  private int capacity = 0;

  /** The maximum id of the items added to the leaf page index builder. */
  private int maxId = -1;

  /**
   * The total number of envelopes added to this builder. This value is valid no matter if we have
   * spilled or not.
   */
  private int numEnvelopes = 0;

  public ExternalLeafPageIndexBuilder(
      MemoryConsumer consumer,
      TaskContext taskContext,
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      long initialSizeBytes,
      ShuffleWriteMetricsReporter shuffleWriteMetricsReporter) {
    super(blockManager, taskContext, shuffleWriteMetricsReporter);
    this.consumer = consumer;
    this.taskContext = taskContext;
    this.taskMemoryManager = taskMemoryManager;
    this.blockManager = blockManager;
    this.initialSizeBytes = initialSizeBytes;
  }

  public int size() {
    return size;
  }

  public int usableCapacity() {
    if (!hasSpilled()) {
      // We'll reserve 1/3 of the capacity as scratch space for running Tim sort
      // when no-spilling is triggered.
      return (int) (capacity * 0.66);
    } else {
      // When spilling is triggered, we'll use the entire capacity. The sorting
      // of in-mem and spilled data will all be handled by an external sorter.
      return capacity;
    }
  }

  public long getMemoryUsage() {
    if (envelopeArray == null) {
      return 0L;
    }
    return envelopeArray.memoryBlock().size();
  }

  public void add(int id, double minX, double maxX, double minY, double maxY) {
    growIfNecessary();
    envelopeArray.set(size * LONGS_PER_ENVELOPE, id);
    envelopeArray.set(size * LONGS_PER_ENVELOPE + 1, Double.doubleToRawLongBits(minX));
    envelopeArray.set(size * LONGS_PER_ENVELOPE + 2, Double.doubleToRawLongBits(maxX));
    envelopeArray.set(size * LONGS_PER_ENVELOPE + 3, Double.doubleToRawLongBits(minY));
    envelopeArray.set(size * LONGS_PER_ENVELOPE + 4, Double.doubleToRawLongBits(maxY));
    size++;
    maxId = Math.max(maxId, id);
    numEnvelopes++;
  }

  public void add(int id, Envelope envelope) {
    add(id, envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
  }

  /**
   * We'd like to keep the envelope array as one single compact array in memory, since envelope
   * array is less likely to be spilled to disk, and we'll spill geometries first when spilling is
   * triggered. Keeping the envelope array compact will make it faster to sort and reorganize as
   * leaf pages.
   */
  public void growIfNecessary() {
    if (size < usableCapacity()) {
      return;
    }

    long requiredSize =
        Math.max((long) (capacity * LONGS_PER_ENVELOPE * 1.5), initialSizeBytes / 8);
    LongArray newArray;
    try {
      newArray = consumer.allocateArray(requiredSize);
      if (envelopeArray != null) {
        Platform.copyMemory(
            envelopeArray.getBaseObject(),
            envelopeArray.getBaseOffset(),
            newArray.getBaseObject(),
            newArray.getBaseOffset(),
            size * BYTES_PER_ENVELOPE);
        consumer.freeArray(envelopeArray);
      }
    } catch (SparkOutOfMemoryError e) {
      if (envelopeArray == null) {
        // Spill happened when growing the envelope array. Shrink the required size to one
        // page and try again.
        requiredSize = initialSizeBytes / 8;
        newArray = consumer.allocateArray(requiredSize);
      } else {
        throw e;
      }
    }

    envelopeArray = newArray;
    capacity = (int) (newArray.size() / LONGS_PER_ENVELOPE);
  }

  public long spill(boolean force) throws IOException {
    LongArray envelopeArrayToFree = null;
    int oldSize = size;
    long freeSize;
    try {
      synchronized (this) {
        if (envelopeArray == null) {
          return 0L;
        }

        if (!force) {
          if (capacity <= initialSizeBytes / BYTES_PER_ENVELOPE && size * 2 < usableCapacity()) {
            // The envelope array is too small to be spilled. That's already the minimum amount
            // of memory required for this object to function.
            return 0L;
          }
        }

        // Write envelopes in memory to the spill file
        initializeSpillWriter();
        ExternalLeafPageIndex.writeEnvelopesToStream(
            envelopeArray, size, spillWriter, spillWriteBuffer);
        taskContext.taskMetrics().incMemoryBytesSpilled(size * BYTES_PER_ENVELOPE);

        // Postpone the free of the envelope array after we release the lock. This is for preventing
        // deadlock when the envelope array is selected for spilling in
        // `TaskMemoryManager.acquireExecutionMemory` invoked by other threads.
        envelopeArrayToFree = envelopeArray;
        freeSize = envelopeArray.memoryBlock().size();
        envelopeArray = null;
        capacity = 0;
        size = 0;
      }
    } catch (Exception e) {
      // trySpillAndAcquire will catch IOException and rethrow it as a SparkOutOfMemoryError
      // exception, this does not always make the task fail.
      // The leaf page index builder will run into an inconsistent state if the spill fails. We
      // should throw a RuntimeException to fail the task.
      if (e instanceof IOException) {
        throw new RuntimeException("I/O error while spilling envelopes to disk", e);
      } else {
        throw e;
      }
    } finally {
      if (envelopeArrayToFree != null) {
        consumer.freeArray(envelopeArrayToFree);
      }
    }

    logger.trace(
        "Spilled {} envelopes to disk, freed bytes: {}. Total envelopes: {}, maxId: {}",
        oldSize,
        freeSize,
        numEnvelopes,
        maxId);
    return freeSize;
  }

  @Override
  public void close() {
    LongArray envelopeArrayToFree;

    // Evacuate the members of this object in a synchronized block to avoid race conditions
    // caused by spilling.
    synchronized (this) {
      envelopeArrayToFree = envelopeArray;
      envelopeArray = null;
      super.close();
    }

    if (envelopeArrayToFree != null) {
      consumer.freeArray(envelopeArrayToFree);
    }
  }

  public int get(int index, double[] result) {
    assert envelopeArray != null;
    return ExternalLeafPageIndex.get(envelopeArray, index, result);
  }

  public ExternalLeafPageIndex buildLeafPages(int leafNodeCapacity) throws IOException {
    // Compute the number of leaf pages and the number of vertical slices
    int minLeafCount = (int) Math.ceil((double) numEnvelopes / leafNodeCapacity);
    int numVerticalSlices = (int) Math.ceil(Math.sqrt(minLeafCount));
    int verticalSliceCount = (int) Math.ceil(numEnvelopes / (double) numVerticalSlices);

    if (!hasSpilled()) {
      return buildLeafPagesInMem(leafNodeCapacity, minLeafCount, verticalSliceCount);
    } else {
      return buildLeafPagesExternal(leafNodeCapacity, minLeafCount, verticalSliceCount);
    }
  }

  private ExternalLeafPageIndex buildLeafPagesInMem(
      int leafNodeCapacity, int minLeafCount, int verticalSliceCount) {

    List<LeafPageMetadata> leafPageMetadata = new ArrayList<>(minLeafCount);
    int[] itemIdToLeafPageId = new int[maxId + 1];

    if (envelopeArray == null) {
      // No envelopes ingested, simply return an empty external leaf pages object
      return new ExternalLeafPageIndex(taskContext, consumer, taskMemoryManager);
    }

    // Create sorter objects for sorting envelopes by their center x and y
    // The envelope array should have some unused space reserved for sorting at the tail.
    // Please refer to the comment of `usableCapacity` for more details.
    MemoryBlock unused =
        new MemoryBlock(
            envelopeArray.getBaseObject(),
            envelopeArray.getBaseOffset() + size * BYTES_PER_ENVELOPE,
            (capacity - size) * BYTES_PER_ENVELOPE);
    LongArray buffer = new LongArray(unused);
    Sorter<DoubleContainer, LongArray> xSorter =
        new Sorter<>(new UnsafeEnvelopeCenterXDataFormat(buffer));
    Sorter<DoubleContainer, LongArray> ySorter =
        new Sorter<>(new UnsafeEnvelopeCenterYDataFormat(buffer));

    // Sort all the envelopes by their center x
    xSorter.sort(envelopeArray, 0, size, Comparator.comparingDouble(a -> a.value));

    // Partition the space vertically
    int leafPageId = 0;
    int start = 0;
    while (start < size) {
      // Determine the start and end positions of the current vertical slice
      int end = Math.min(start + verticalSliceCount, size);

      // Sort the envelopes in the current vertical slice by their center y
      ySorter.sort(envelopeArray, start, end, Comparator.comparingDouble(a -> a.value));

      // Partition the slice horizontally to retrieve the leaf page bounds. Each leaf page has
      // `leafNodeCapacity` envelopes.
      leafPageId =
          splitVerticalSliceToLeafPages(
              envelopeArray,
              start,
              end,
              leafNodeCapacity,
              leafPageMetadata,
              leafPageId,
              0,
              itemIdToLeafPageId);
      start = end;
    }

    // Release ownership of the envelope array. It will be owned by a read-only data structure
    // for running spatial queries.
    LongArray envelopeArray = this.envelopeArray;
    this.envelopeArray = null;
    this.size = 0;
    this.capacity = 0;

    return new ExternalLeafPageIndex(
        taskContext,
        consumer,
        taskMemoryManager,
        leafPageMetadata,
        envelopeArray,
        itemIdToLeafPageId);
  }

  private ExternalLeafPageIndex buildLeafPagesExternal(
      int leafNodeCapacity, int minLeafCount, int verticalSliceCount) throws IOException {
    // Force spilling everything to disk. This is for ensuring that the external sorter has
    // enough memory to sort envelopes.
    spill(true);

    // Create an external sorter to sort envelopes by their center x
    UnsafeExternalSorter xSorter =
        UnsafeExternalSorter.create(
            taskMemoryManager,
            blockManager,
            blockManager.serializerManager(),
            taskContext,
            EnvelopeComparator::new,
            PrefixComparators.DOUBLE,
            (int) initialSizeBytes / 100,
            initialSizeBytes,
            Integer.MAX_VALUE,
            true);
    try {
      return doBuildLeafPagesExternal(leafNodeCapacity, minLeafCount, verticalSliceCount, xSorter);
    } finally {
      xSorter.cleanupResources();
    }
  }

  private ExternalLeafPageIndex doBuildLeafPagesExternal(
      int leafNodeCapacity, int minLeafCount, int verticalSliceCount, UnsafeExternalSorter xSorter)
      throws IOException {
    // Create an external sorter to sort envelopes by their center x
    insertAllEnvelopesToExternalSorter(xSorter);
    UnsafeSorterIterator iterator = xSorter.getSortedIterator();

    List<LeafPageMetadata> leafPageMetadata = new ArrayList<>(minLeafCount);
    int[] itemIdToLeafPageId = new int[maxId + 1];

    // Allocate memory for leaf pages for storing sorted envelopes.
    // This allocation may make us spill, the spill will only spill the geometries array. This
    // envelope array object has nothing to spill (envelopeArray is already null) for now so
    // we'll be safe.
    LongArray leafEnvelopeArray = null;
    File leafEnvelopeFile = null;
    OutputStream leafEnvelopeWriter = null;
    try {
      leafEnvelopeArray = consumer.allocateArray((long) numEnvelopes * LONGS_PER_ENVELOPE);
      logger.trace(
          "Build leaf envelopes in memory: {} bytes allocated",
          leafEnvelopeArray.memoryBlock().size());
    } catch (SparkOutOfMemoryError e) {
      // If we cannot allocate memory for the leaf pages, we have to save leaf pages to disk
      Tuple2<TempLocalBlockId, File> tempLocalBlock =
          blockManager.diskBlockManager().createTempLocalBlock();
      leafEnvelopeFile = tempLocalBlock._2();
      leafEnvelopeWriter =
          new BufferedOutputStream(Files.newOutputStream(leafEnvelopeFile.toPath()));
      logger.trace(
          "Build leaf envelopes on disk due to memory shortage. Envelope file path: {}",
          leafEnvelopeFile.getAbsolutePath());
    }

    // Allocate a buffer to store envelopes in each vertical slice. The size of this buffer won't
    // be too large (usually less than 10MB) so it is safe to allocate it directly on the heap.
    long[] verticalSliceEnvelopes = new long[verticalSliceCount * LONGS_PER_ENVELOPE];
    LongArray envelopesInSlice =
        new LongArray(
            new MemoryBlock(
                verticalSliceEnvelopes,
                Platform.LONG_ARRAY_OFFSET,
                verticalSliceCount * BYTES_PER_ENVELOPE));
    long[] sortBuffer = new long[verticalSliceCount * 3];
    LongArray sortBufferArray =
        new LongArray(
            new MemoryBlock(sortBuffer, Platform.LONG_ARRAY_OFFSET, verticalSliceCount * 24L));
    Sorter<DoubleContainer, LongArray> ySorter =
        new Sorter<>(new UnsafeEnvelopeCenterYDataFormat(sortBufferArray));

    int leafPageId = 0;
    int numEnvelopesWritten = 0;
    while (iterator.hasNext()) {
      // load verticalSliceCount number of records into memory for sorting
      int numEnvelopesInSlice = 0;
      for (;
          numEnvelopesInSlice < verticalSliceCount && iterator.hasNext();
          numEnvelopesInSlice++) {
        iterator.loadNext();
        Platform.copyMemory(
            iterator.getBaseObject(),
            iterator.getBaseOffset(),
            envelopesInSlice.getBaseObject(),
            envelopesInSlice.getBaseOffset() + numEnvelopesInSlice * BYTES_PER_ENVELOPE,
            BYTES_PER_ENVELOPE);
      }

      // Sort the envelopes in the vertical slice by their center y
      ySorter.sort(
          envelopesInSlice, 0, numEnvelopesInSlice, Comparator.comparingDouble(a -> a.value));

      // Save the sorted slice
      if (leafEnvelopeArray != null) {
        Platform.copyMemory(
            envelopesInSlice.getBaseObject(),
            envelopesInSlice.getBaseOffset(),
            leafEnvelopeArray.getBaseObject(),
            leafEnvelopeArray.getBaseOffset() + numEnvelopesWritten * BYTES_PER_ENVELOPE,
            numEnvelopesInSlice * BYTES_PER_ENVELOPE);
      } else {
        ExternalLeafPageIndex.writeEnvelopesToStream(
            envelopesInSlice, numEnvelopesInSlice, leafEnvelopeWriter, spillWriteBuffer);
      }

      // Partition the slice horizontally to retrieve the leaf page bounds. Each leaf page has
      // `leafNodeCapacity` envelopes.
      leafPageId =
          splitVerticalSliceToLeafPages(
              envelopesInSlice,
              0,
              numEnvelopesInSlice,
              leafNodeCapacity,
              leafPageMetadata,
              leafPageId,
              numEnvelopesWritten,
              itemIdToLeafPageId);
      numEnvelopesWritten += numEnvelopesInSlice;
    }

    if (leafEnvelopeWriter != null) {
      taskContext.taskMetrics().incDiskBytesSpilled(leafEnvelopeFile.length());
      leafEnvelopeWriter.close();
    }

    if (leafEnvelopeArray != null) {
      return new ExternalLeafPageIndex(
          taskContext,
          consumer,
          taskMemoryManager,
          leafPageMetadata,
          leafEnvelopeArray,
          itemIdToLeafPageId);
    } else {
      return new ExternalLeafPageIndex(
          taskContext,
          consumer,
          taskMemoryManager,
          leafPageMetadata,
          leafEnvelopeFile,
          itemIdToLeafPageId);
    }
  }

  private void insertAllEnvelopesToExternalSorter(UnsafeExternalSorter xSorter) throws IOException {
    if (envelopeArray != null) {
      throw new IllegalStateException("The in-memory envelopeArray should have been spilled");
    }

    // Seal the spill writer to finalize the spill file
    closeSpillWriter(spillWriter);
    spillWriter = null;

    // Read the spilled envelopes and feed into the external sorter
    byte[] buffer = new byte[BYTES_PER_ENVELOPE_I];
    LongArray envelopeData =
        new LongArray(new MemoryBlock(buffer, Platform.BYTE_ARRAY_OFFSET, BYTES_PER_ENVELOPE));
    try (SpillFileSequentialReader spillReader =
        new SpillFileSequentialReader(spillFile, spillBlockId, blockManager.serializerManager())) {
      while (true) {
        taskContext.killTaskIfInterrupted();
        try {
          spillReader.readFully(buffer, 0, BYTES_PER_ENVELOPE_I);
          double minX = Double.longBitsToDouble(envelopeData.get(1));
          double maxX = Double.longBitsToDouble(envelopeData.get(2));
          double centerX = (minX + maxX) / 2;
          long prefix = DoublePrefixComparator.computePrefix(centerX);
          xSorter.insertRecord(
              envelopeData.getBaseObject(),
              envelopeData.getBaseOffset(),
              BYTES_PER_ENVELOPE_I,
              prefix,
              false);
        } catch (IOException e) {
          // reached the end of the file
          break;
        }
      }
    }

    // We don't need the spill file anymore
    deleteSpillFile(spillFile);
    spillFile = null;
    spillBlockId = null;
  }

  private static int splitVerticalSliceToLeafPages(
      LongArray envelopeArray,
      int start,
      int end,
      int leafPageCapacity,
      List<LeafPageMetadata> leafPageMetadata,
      int leafPageId,
      int positionOffset,
      int[] itemIdToLeafPageId) {
    double[] leafPageBounds = new double[4];
    for (int leafPageStart = start; leafPageStart < end; leafPageStart += leafPageCapacity) {
      // Determine the start and end positions of the current leaf page
      int leafPageEnd = Math.min(leafPageStart + leafPageCapacity, end);
      int leafPageSize = leafPageEnd - leafPageStart;

      // Compute the leaf page bounds and construct a new leaf page
      computeLeafPageBounds(
          envelopeArray,
          leafPageStart,
          leafPageEnd,
          leafPageBounds,
          leafPageId,
          itemIdToLeafPageId);
      leafPageMetadata.add(
          new LeafPageMetadata(
              leafPageId, positionOffset + leafPageStart, leafPageSize, leafPageBounds));

      leafPageId++;
    }
    return leafPageId;
  }

  private static void computeLeafPageBounds(
      LongArray envelopeArray,
      int leafPageStart,
      int leafPageEnd,
      double[] results,
      int leafPageId,
      int[] itemIdToLeafPageId) {
    // Retrieve the leaf page bounds
    double minX = Double.MAX_VALUE;
    double maxX = -Double.MAX_VALUE;
    double minY = Double.MAX_VALUE;
    double maxY = -Double.MAX_VALUE;
    for (int k = leafPageStart; k < leafPageEnd; k++) {
      int id = ExternalLeafPageIndex.get(envelopeArray, k, results);
      itemIdToLeafPageId[id] = leafPageId;
      if (results[0] <= results[1]) {
        // It is a valid envelope
        minX = Math.min(minX, results[0]);
        maxX = Math.max(maxX, results[1]);
        minY = Math.min(minY, results[2]);
        maxY = Math.max(maxY, results[3]);
      }
    }
    results[0] = minX;
    results[1] = maxX;
    results[2] = minY;
    results[3] = maxY;
  }

  /**
   * A wrapper class for double values. This is used to reuse the same object when sorting, thus
   * avoiding extra object allocations caused by Java's autoboxing.
   */
  private static class DoubleContainer {
    double value;
  }

  /** Describe the data format of envelope array for in-memory sorting. */
  private abstract static class UnsafeEnvelopeDataFormat
      extends SortDataFormat<DoubleContainer, LongArray> {
    private final long[] swapBuffer = new long[LONGS_PER_ENVELOPE];
    private final LongArray buffer;

    UnsafeEnvelopeDataFormat(LongArray buffer) {
      this.buffer = buffer;
    }

    @Override
    public DoubleContainer newKey() {
      return new DoubleContainer();
    }

    @Override
    public DoubleContainer getKey(LongArray data, int pos, DoubleContainer reuse) {
      reuse.value = doGetKey(data, pos);
      return reuse;
    }

    @Override
    public DoubleContainer getKey(LongArray data, int pos) {
      // Since we reuse keys, this method shouldn't be called.
      throw new UnsupportedOperationException();
    }

    // Child classes should implement this method to get the sort key of an envelope.
    // There are 2 implementations for sorting envelopes by their center x and y respectively.
    protected abstract double doGetKey(LongArray data, int pos);

    @Override
    public void swap(LongArray data, int pos0, int pos1) {
      Platform.copyMemory(
          data.getBaseObject(),
          data.getBaseOffset() + pos0 * BYTES_PER_ENVELOPE,
          swapBuffer,
          Platform.LONG_ARRAY_OFFSET,
          BYTES_PER_ENVELOPE);
      Platform.copyMemory(
          data.getBaseObject(),
          data.getBaseOffset() + pos1 * BYTES_PER_ENVELOPE,
          data.getBaseObject(),
          data.getBaseOffset() + pos0 * BYTES_PER_ENVELOPE,
          BYTES_PER_ENVELOPE);
      Platform.copyMemory(
          swapBuffer,
          Platform.LONG_ARRAY_OFFSET,
          data.getBaseObject(),
          data.getBaseOffset() + pos1 * BYTES_PER_ENVELOPE,
          BYTES_PER_ENVELOPE);
    }

    @Override
    public void copyElement(LongArray src, int srcPos, LongArray dst, int dstPos) {
      // Unroll the copy to get rid of the cost of checks within Unsafe.copyMemory
      Object srcObj = src.getBaseObject();
      Object dstObj = dst.getBaseObject();
      long srcOffset = src.getBaseOffset() + srcPos * BYTES_PER_ENVELOPE;
      long dstOffset = dst.getBaseOffset() + dstPos * BYTES_PER_ENVELOPE;
      long value0 = Platform.getLong(srcObj, srcOffset);
      Platform.putLong(dstObj, dstOffset, value0);
      long value1 = Platform.getLong(srcObj, srcOffset + 8);
      Platform.putLong(dstObj, dstOffset + 8, value1);
      long value2 = Platform.getLong(srcObj, srcOffset + 16);
      Platform.putLong(dstObj, dstOffset + 16, value2);
      long value3 = Platform.getLong(srcObj, srcOffset + 24);
      Platform.putLong(dstObj, dstOffset + 24, value3);
      long value4 = Platform.getLong(srcObj, srcOffset + 32);
      Platform.putLong(dstObj, dstOffset + 32, value4);
    }

    @Override
    public void copyRange(LongArray src, int srcPos, LongArray dst, int dstPos, int length) {
      Platform.copyMemory(
          src.getBaseObject(),
          src.getBaseOffset() + srcPos * BYTES_PER_ENVELOPE,
          dst.getBaseObject(),
          dst.getBaseOffset() + dstPos * BYTES_PER_ENVELOPE,
          length * BYTES_PER_ENVELOPE);
    }

    @Override
    public LongArray allocate(int length) {
      assert ((long) length * LONGS_PER_ENVELOPE <= buffer.size())
          : "the buffer is smaller than required: "
              + buffer.size()
              + " < "
              + (length * LONGS_PER_ENVELOPE);
      return buffer;
    }
  }

  private static class UnsafeEnvelopeCenterXDataFormat extends UnsafeEnvelopeDataFormat {
    UnsafeEnvelopeCenterXDataFormat(LongArray buffer) {
      super(buffer);
    }

    @Override
    public double doGetKey(LongArray data, int pos) {
      double minX = Double.longBitsToDouble(data.get(pos * LONGS_PER_ENVELOPE + 1));
      double maxX = Double.longBitsToDouble(data.get(pos * LONGS_PER_ENVELOPE + 2));
      return (minX + maxX) / 2;
    }
  }

  private static class UnsafeEnvelopeCenterYDataFormat extends UnsafeEnvelopeDataFormat {
    UnsafeEnvelopeCenterYDataFormat(LongArray buffer) {
      super(buffer);
    }

    @Override
    public double doGetKey(LongArray data, int pos) {
      double minY = Double.longBitsToDouble(data.get(pos * LONGS_PER_ENVELOPE + 3));
      double maxY = Double.longBitsToDouble(data.get(pos * LONGS_PER_ENVELOPE + 4));
      return (minY + maxY) / 2;
    }
  }

  private static class EnvelopeComparator extends RecordComparator {
    @Override
    public int compare(
        Object leftBaseObject,
        long leftBaseOffset,
        int leftBaseLength,
        Object rightBaseObject,
        long rightBaseOffset,
        int rightBaseLength) {
      double leftMinX =
          Double.longBitsToDouble(Platform.getLong(leftBaseObject, leftBaseOffset + 8));
      double leftMaxX =
          Double.longBitsToDouble(Platform.getLong(leftBaseObject, leftBaseOffset + 16));
      double rightMinX =
          Double.longBitsToDouble(Platform.getLong(rightBaseObject, rightBaseOffset + 8));
      double rightMaxX =
          Double.longBitsToDouble(Platform.getLong(rightBaseObject, rightBaseOffset + 16));
      double leftCenterX = (leftMinX + leftMaxX) / 2;
      double rightCenterX = (rightMinX + rightMaxX) / 2;
      return Double.compare(leftCenterX, rightCenterX);
    }
  }
}
