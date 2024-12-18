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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sedona.core.index.ExternalLeafPageIndex;
import org.apache.spark.sedona.core.index.ExternalLeafPageIndex.EnvelopeFilter;
import org.apache.spark.sedona.core.index.ExternalLeafPageIndex.LeafPageMetadata;
import org.apache.spark.sedona.core.index.ExternalLeafPageIndexBuilder;
import org.apache.spark.sedona.core.index.SedonaMemoryConsumer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;

public class ExternalLeafPageIndexTest extends ExternalIndexTestBase {

  @Test
  public void buildEmptyIndex() throws IOException {
    ExternalLeafPageIndexBuilder builder = create();
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();
    assertEquals(0, leafPages.metadata.size());
    assertEquals(0, leafPages.borrowItemIdToLeafPageId().length);
    assertFalse(leafPages.isSpilled());
    leafPages.spill(diskBlockManager);
    assertFalse(leafPages.isSpilled());
    assertEquals(0, leafPages.metadata.size());
    assertEquals(0, leafPages.borrowItemIdToLeafPageId().length);
    leafPages.close();
  }

  @Test
  public void buildOnePage() throws IOException {
    ExternalLeafPageIndexBuilder builder = create();
    Map<Integer, Envelope> expected = new HashMap<>();
    for (int k = 0; k < 8; k++) {
      builder.add(10 + k, k, k + 1, k + 2, k + 3);
      expected.put(10 + k, new Envelope(k, k + 1, k + 2, k + 3));
    }
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();
    assertFalse(leafPages.isSpilled());
    assertEquals(1, leafPages.metadata.size());
    verifyLeafPages(leafPages, expected);
    leafPages.spill(diskBlockManager);
    assertTrue(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.close();
  }

  @Test
  public void buildMultiplePages() throws IOException {
    ExternalLeafPageIndexBuilder builder = create();
    Map<Integer, Envelope> expected = new HashMap<>();
    for (int k = 0; k < 10; k++) {
      builder.add(k, k, k + 1, k + 2, k + 3);
      builder.add(100 + k, k + 100, k + 101, k + 2, k + 3);
      builder.add(200 + k, k, k + 1, k + 102, k + 103);
      builder.add(300 + k, k + 100, k + 101, k + 102, k + 103);
      expected.put(k, new Envelope(k, k + 1, k + 2, k + 3));
      expected.put(100 + k, new Envelope(k + 100, k + 101, k + 2, k + 3));
      expected.put(200 + k, new Envelope(k, k + 1, k + 102, k + 103));
      expected.put(300 + k, new Envelope(k + 100, k + 101, k + 102, k + 103));
    }
    assertTrue(builder.getMemoryUsage() > 0);
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    assertEquals(0, builder.getMemoryUsage());
    builder.close();

    assertEquals(4, leafPages.metadata.size());
    assertFalse(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.spill(diskBlockManager);
    assertTrue(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.close();
  }

  @Test
  public void consecutiveSpills() throws IOException {
    ExternalLeafPageIndexBuilder builder = create();
    Map<Integer, Envelope> expected = new HashMap<>();
    Random random = new Random();
    int id = 0;
    for (int spills = 0; spills < 10; spills++) {
      for (int k = 0; k < 100; k++) {
        double minX = random.nextDouble();
        double minY = random.nextDouble();
        double maxX = minX + 0.001;
        double maxY = minY + 0.001;
        builder.add(id, minX, maxX, minY, maxY);
        expected.put(id, new Envelope(minX, maxX, minY, maxY));
        id += 1;
      }
      builder.spill(true);
      builder.spill(false);
    }

    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();

    assertFalse(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.spill(diskBlockManager);
    leafPages.spill(diskBlockManager);
    assertTrue(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);

    // repeated close should also be fine
    leafPages.close();
    leafPages.close();
  }

  @Test
  public void buildLotsOfPages() throws IOException {
    ExternalLeafPageIndexBuilder builder = create();
    int initialCapacity = (int) (pageSizeBytes / ExternalLeafPageIndex.BYTES_PER_ENVELOPE);
    int numEnvelopes = (int) (initialCapacity * 3.5);
    Map<Integer, Envelope> expected = new HashMap<>();
    Random random = new Random();
    for (int k = 0; k < numEnvelopes; k++) {
      double minX = random.nextDouble();
      double minY = random.nextDouble();
      double maxX = minX + 0.001;
      double maxY = minY + 0.001;
      builder.add(k, minX, maxX, minY, maxY);
      expected.put(k, new Envelope(minX, maxX, minY, maxY));
    }
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();

    assertFalse(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.spill(diskBlockManager);
    assertTrue(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.close();
  }

  @Test
  public void buildWithAllocationFailure() {
    // If the memory manager keeps throwing OOM, the builder will eventually fail.
    memoryManager.markConsequentOOM(10);
    assertThrows(
        OutOfMemoryError.class,
        () -> {
          try (ExternalLeafPageIndexBuilder builder = create()) {
            builder.add(0, 0, 1, 0, 1);
          }
        });
  }

  @Test
  public void buildWithSpilling() throws IOException {
    // There's only memory for 1 page.
    memoryManager.limit(pageSizeBytes);

    // But we'll write 10x times the number of envelopes that fit in one page.
    int pageCapacity = (int) (pageSizeBytes / ExternalLeafPageIndex.BYTES_PER_ENVELOPE);
    int numEnvelopes = pageCapacity * 10;
    ExternalLeafPageIndexBuilder builder = create();
    Map<Integer, Envelope> expected = new HashMap<>();
    Random random = new Random();

    // Spill happens during the following loop.
    for (int k = 0; k < numEnvelopes; k++) {
      double minX = random.nextDouble();
      double minY = random.nextDouble();
      double maxX = minX + 0.001;
      double maxY = minY + 0.001;
      builder.add(k, minX, maxX, minY, maxY);
      expected.put(k, new Envelope(minX, maxX, minY, maxY));
    }
    assertTrue(builder.hasSpilled());

    // buildLeafPages should handle the spill correctly.
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();
    assertTrue(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.close();
  }

  @Test
  public void buildWithSpillingButResultInMem() throws IOException {
    // There's only memory for 1 page.
    memoryManager.limit(pageSizeBytes);

    // But we'll write 10x times the number of envelopes that fit in one page.
    int pageCapacity = (int) (pageSizeBytes / ExternalLeafPageIndex.BYTES_PER_ENVELOPE);
    int numEnvelopes = pageCapacity * 10;
    ExternalLeafPageIndexBuilder builder = create();
    Map<Integer, Envelope> expected = new HashMap<>();
    Random random = new Random();

    // Spill happens during the following loop.
    for (int k = 0; k < numEnvelopes; k++) {
      double minX = random.nextDouble();
      double minY = random.nextDouble();
      double maxX = minX + 0.001;
      double maxY = minY + 0.001;
      builder.add(k, minX, maxX, minY, maxY);
      expected.put(k, new Envelope(minX, maxX, minY, maxY));
    }
    assertTrue(builder.hasSpilled());

    // When buildLeafPages has enough memory, it will allocate the envelopes in memory.
    memoryManager.limit(pageSizeBytes * 20);
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();
    assertFalse(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.spill(diskBlockManager);
    assertTrue(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.close();
  }

  @Test
  public void buildWithSpillingRepeatedEnvelopes() throws IOException {
    // There's only memory for 1 page.
    memoryManager.limit(pageSizeBytes);

    // But we'll write 10x times the number of envelopes that fit in one page.
    int pageCapacity = (int) (pageSizeBytes / ExternalLeafPageIndex.BYTES_PER_ENVELOPE);
    int numEnvelopes = pageCapacity * 10;
    Map<Integer, Envelope> expected = new HashMap<>();
    ExternalLeafPageIndexBuilder builder = create();

    // Spill happens during the following loop.
    for (int k = 0; k < numEnvelopes; k++) {
      int value = k % 10000;
      int minX = value / 100;
      int minY = value % 100;
      double maxX = minX + 0.001;
      double maxY = minY + 0.001;
      builder.add(k, minX, maxX, minY, maxY);
      expected.put(k, new Envelope(minX, maxX, minY, maxY));
    }
    assertTrue(builder.hasSpilled());

    // This buildLeafPages will definitely trigger the valuation of the recordComparator
    memoryManager.limit(pageSizeBytes * 20);
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();

    assertFalse(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.spill(diskBlockManager);
    assertTrue(leafPages.isSpilled());
    verifyLeafPages(leafPages, expected);
    leafPages.close();
  }

  @Test
  public void buildWithRepeatedEnvelopeIds() throws IOException {
    ExternalLeafPageIndexBuilder builder = create();
    for (int k = 0; k < 100; k++) {
      builder.add(k, k, k + 1, k + 2, k + 3);
    }
    for (int k = 0; k < 100; k++) {
      builder.add(k, k, k + 10, k + 20, k + 30);
    }
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();

    assertFalse(leafPages.isSpilled());
    assertEquals(20, leafPages.metadata.size());

    for (int tests = 0; tests < 1; tests++) {
      int numEnvelopes = 0;
      double[] result = new double[4];
      Map<Integer, Integer> idCount = new HashMap<>();
      Map<Integer, Envelope> lastSeenEnvelope = new HashMap<>();
      for (LeafPageMetadata meta : leafPages.metadata) {
        numEnvelopes += meta.size;
        for (int k = meta.position; k < meta.position + meta.size; k++) {
          int id = ExternalLeafPageIndex.get(leafPages.envelopes, k, result);
          idCount.put(id, idCount.getOrDefault(id, 0) + 1);
          Envelope last = lastSeenEnvelope.get(id);
          if (last != null) {
            assertNotEquals(last, new Envelope(result[0], result[1], result[2], result[3]));
          }
          lastSeenEnvelope.put(id, new Envelope(result[0], result[1], result[2], result[3]));
        }
      }
      assertEquals(200, numEnvelopes);
      idCount.forEach((id, count) -> assertEquals(2, count.intValue()));

      // spill the leaf pages and start over
      leafPages.spill(diskBlockManager);
      assertTrue(leafPages.isSpilled());
    }
    leafPages.close();
  }

  @Test
  public void buildWithRepeatedEnvelopeIdsSpilling() throws IOException {
    // There's only memory for 1 page.
    memoryManager.limit(pageSizeBytes);

    // But we'll write 4x times the number of envelopes that fit in one page.
    int pageCapacity = (int) (pageSizeBytes / ExternalLeafPageIndex.BYTES_PER_ENVELOPE);
    int numEnvelopes = pageCapacity * 2;

    ExternalLeafPageIndexBuilder builder = create();
    for (int k = 0; k < numEnvelopes; k++) {
      builder.add(k, k, k + 1, k + 2, k + 3);
    }
    for (int k = 0; k < numEnvelopes; k++) {
      builder.add(k, k, k + 10, k + 20, k + 30);
    }
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();

    LongArray envelopeArray = leafPages.envelopes;
    if (envelopeArray == null) {
      // Spilled to disk. For now, we load everything to memory for simplicity
      byte[] bytes = Files.readAllBytes(leafPages.file.toPath());
      envelopeArray =
          new LongArray(new MemoryBlock(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length));
    }

    int actualNum = 0;
    double[] result = new double[4];
    Map<Integer, Integer> idCount = new HashMap<>();
    Map<Integer, Envelope> lastSeenEnvelope = new HashMap<>();
    for (LeafPageMetadata meta : leafPages.metadata) {
      actualNum += meta.size;
      for (int k = meta.position; k < meta.position + meta.size; k++) {
        int id = ExternalLeafPageIndex.get(envelopeArray, k, result);
        idCount.put(id, idCount.getOrDefault(id, 0) + 1);
        Envelope last = lastSeenEnvelope.get(id);
        if (last != null) {
          assertNotEquals(last, new Envelope(result[0], result[1], result[2], result[3]));
        }
        lastSeenEnvelope.put(id, new Envelope(result[0], result[1], result[2], result[3]));
      }
    }
    assertEquals(numEnvelopes * 2, actualNum);
    idCount.forEach((id, count) -> assertEquals(2, count.intValue()));
    leafPages.close();
  }

  @Test
  public void rankLeafPages() throws IOException {
    // Create a leaf page index containing 10000 random envelopes
    ExternalLeafPageIndexBuilder builder = create();
    Random random = new Random(0);
    int numEnvelopes = 10000;
    for (int k = 0; k < numEnvelopes; k++) {
      double minX = (random.nextDouble() - 0.5) * 2;
      double minY = (random.nextDouble() - 0.5) * 2;
      double maxX = minX + 0.01;
      double maxY = minY + 0.01;
      builder.add(k, minX, maxX, minY, maxY);
    }
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();

    // Rank the leaf pages by their spatial proximity
    int[] ranks = leafPages.rankLeafPagesBySpatialProximity();

    // Verify the ranks by checking the distances between adjacent leaf pages
    for (int i = 0; i < ranks.length - 1; i++) {
      LeafPageMetadata meta1 = leafPages.metadata.get(ranks[i]);
      LeafPageMetadata meta2 = leafPages.metadata.get(ranks[i + 1]);
      double center1X = (meta1.minX + meta1.maxX) / 2;
      double center1Y = (meta1.minY + meta1.maxY) / 2;
      double center2X = (meta2.minX + meta2.maxX) / 2;
      double center2Y = (meta2.minY + meta2.maxY) / 2;
      double distance =
          Math.sqrt(Math.pow(center1X - center2X, 2) + Math.pow(center1Y - center2Y, 2));
      assertTrue(distance < 0.2);
    }

    leafPages.close();
  }

  @Test
  public void queryEmptyLeafPageIndex() throws IOException {
    ExternalLeafPageIndexBuilder builder = create();
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();
    IntList leafIds = new IntArrayList();
    Envelope queryWindow = new Envelope(0, 1, 0, 1);
    assertTrue(leafPages.query(queryWindow, leafIds).isEmpty());
    EnvelopeFilter filter =
        (id, envelope) ->
            queryWindow.intersects(
                new Envelope(envelope[0], envelope[1], envelope[2], envelope[3]));
    assertTrue(leafPages.query(filter, leafIds).isEmpty());
    leafPages.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void queryEnvelopes() throws IOException {
    // Create a leaf page index containing 1000 random envelopes
    ExternalLeafPageIndexBuilder builder = create();
    STRtree refIndex = new STRtree();
    Random random = new Random();
    for (int k = 0; k < 1000; k++) {
      double minX = (random.nextDouble() - 0.5) * 2;
      double minY = (random.nextDouble() - 0.5) * 2;
      double maxX = minX + 0.01;
      double maxY = minY + 0.01;
      builder.add(k, minX, maxX, minY, maxY);
      refIndex.insert(new Envelope(minX, maxX, minY, maxY), k);
    }
    ExternalLeafPageIndex leafPages = builder.buildLeafPages(10);
    builder.close();
    STRtree nonLeafTree = leafPages.buildNonLeafTree(10);
    refIndex.build();

    for (int trial = 0; trial < 2; trial += 1) {
      // Query the index using random query windows and compare the results
      for (int k = 0; k < 1000; k++) {
        double minX = (random.nextDouble() - 0.5) * 2;
        double minY = (random.nextDouble() - 0.5) * 2;
        double maxX = minX + 0.1;
        double maxY = minY + 0.1;
        Envelope queryWindow = new Envelope(minX, maxX, minY, maxY);
        List<Integer> expected = refIndex.query(queryWindow);
        List<Integer> leafIds = nonLeafTree.query(queryWindow);
        List<Integer> actual = leafPages.query(queryWindow, new IntArrayList(leafIds));
        expected.sort(null);
        actual.sort(null);
        assertEquals(expected, actual);
      }

      leafPages.invalidateCache();

      // Use a customized filter this time, the result should be the same
      for (int k = 0; k < 1000; k++) {
        double minX = (random.nextDouble() - 0.5) * 2;
        double minY = (random.nextDouble() - 0.5) * 2;
        double maxX = minX + 0.1;
        double maxY = minY + 0.1;
        Envelope queryWindow = new Envelope(minX, maxX, minY, maxY);
        List<Integer> expected = refIndex.query(queryWindow);
        EnvelopeFilter filter =
            (id, envelope) ->
                queryWindow.intersects(
                    new Envelope(envelope[0], envelope[1], envelope[2], envelope[3]));
        List<Integer> leafIds = nonLeafTree.query(queryWindow);
        List<Integer> actual = leafPages.query(filter, new IntArrayList(leafIds));
        expected.sort(null);
        actual.sort(null);
        assertEquals(expected, actual);
      }

      // Using out-of-bounds query windows
      Envelope queryWindow = new Envelope(10, 20, 10, 20);
      List<Integer> leafIds = Arrays.asList(0, 1, 3, 4, 7, 8, 9);
      List<Integer> actual = leafPages.query(queryWindow, new IntArrayList(leafIds));
      assertTrue(actual.isEmpty());

      // Using an empty envelope
      actual = leafPages.query(new Envelope(), new IntArrayList(leafIds));
      assertTrue(actual.isEmpty());

      // Using empty leafIds list
      assertTrue(leafPages.query(queryWindow, IntArrayList.of()).isEmpty());

      leafPages.spill(diskBlockManager);
    }

    leafPages.close();
  }

  private ExternalLeafPageIndexBuilder create() {
    DummyMemoryConsumer consumer =
        new DummyMemoryConsumer(taskMemoryManager, pageSizeBytes, MemoryMode.ON_HEAP);
    ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();
    ExternalLeafPageIndexBuilder builder =
        new ExternalLeafPageIndexBuilder(
            consumer, taskContext, taskMemoryManager, blockManager, pageSizeBytes, writeMetrics);
    consumer.setBuilder(builder);
    return builder;
  }

  private static void verifyLeafPages(
      ExternalLeafPageIndex leafPages, Map<Integer, Envelope> expected) throws IOException {
    int totalEnvelopes = 0;
    Set<Integer> seenIds = new HashSet<>();
    int lastEndPosition = 0;

    LongArray envelopeArray = leafPages.envelopes;
    if (envelopeArray == null) {
      // Spilled to disk. For now, we load everything to memory for simplicity
      byte[] bytes = Files.readAllBytes(leafPages.file.toPath());
      envelopeArray =
          new LongArray(new MemoryBlock(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length));
    }

    for (int i = 0; i < leafPages.metadata.size(); i++) {
      LeafPageMetadata leafMeta = leafPages.metadata.get(i);
      int position = leafMeta.position;
      long size = leafMeta.size;
      totalEnvelopes += (int) size;
      assertEquals(lastEndPosition, position);
      lastEndPosition = (int) (position + size);
      double minX = Double.MAX_VALUE;
      double maxX = -Double.MAX_VALUE;
      double minY = Double.MAX_VALUE;
      double maxY = -Double.MAX_VALUE;
      int[] itemIdToLeafPageId = leafPages.borrowItemIdToLeafPageId();
      for (int k = 0; k < size; k++) {
        double[] result = new double[4];
        int id = ExternalLeafPageIndex.get(envelopeArray, position + k, result);
        assertTrue(seenIds.add(id));
        assertEquals(expected.get(id), new Envelope(result[0], result[1], result[2], result[3]));
        minX = Math.min(minX, result[0]);
        maxX = Math.max(maxX, result[1]);
        minY = Math.min(minY, result[2]);
        maxY = Math.max(maxY, result[3]);
        assertEquals(itemIdToLeafPageId[id], i);
      }
      assertEquals(leafMeta.minX, minX, 0.0);
      assertEquals(leafMeta.maxX, maxX, 0.0);
      assertEquals(leafMeta.minY, minY, 0.0);
      assertEquals(leafMeta.maxY, maxY, 0.0);
    }
    assertEquals(expected.size(), totalEnvelopes);
  }

  private static class DummyMemoryConsumer extends SedonaMemoryConsumer {

    ExternalLeafPageIndexBuilder builder;

    DummyMemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
      super(taskMemoryManager, pageSize, mode);
    }

    void setBuilder(ExternalLeafPageIndexBuilder builder) {
      this.builder = builder;
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      if (trigger == this && builder != null) {
        return builder.spill(false);
      } else {
        return 0L;
      }
    }
  }
}
