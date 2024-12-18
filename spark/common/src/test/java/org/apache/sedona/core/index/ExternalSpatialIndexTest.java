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
import static org.junit.Assert.assertTrue;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.sedona.core.index.ExternalDataItemIndex.DataItemWithId;
import org.apache.spark.sedona.core.index.ExternalSpatialIndex;
import org.apache.spark.unsafe.Platform;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;

public class ExternalSpatialIndexTest extends ExternalIndexTestBase {
  private static class ByteArrayComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] a, byte[] b) {
      for (int i = 0; i < Math.min(a.length, b.length); i++) {
        if (a[i] != b[i]) {
          return Byte.compare(a[i], b[i]);
        }
      }
      return Integer.compare(a.length, b.length);
    }
  }

  @Test
  public void testEmptyIndex() throws IOException {
    ExternalSpatialIndex index = create();
    index.build();
    Envelope queryWindow = new Envelope(0, 1, 0, 1);
    assertFalse(index.query(queryWindow).hasNext());
    index.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSpillBeforeBuild() throws IOException {
    // Limit the memory
    memoryManager.limit(pageSizeBytes * 3);

    // Build an index containing 5 pages of data
    int itemSize = 1000;
    int numItems = (int) pageSizeBytes / itemSize * 5;

    ExternalSpatialIndex index = create();
    STRtree refTree = new STRtree();
    Random random = new Random();
    for (int i = 0; i < numItems; i++) {
      double x = random.nextDouble();
      double y = random.nextDouble();
      Envelope envelope = new Envelope(x, x + 0.01, y, y + 0.01);
      byte[] data = new byte[itemSize];
      random.nextBytes(data);
      index.insert(envelope, data);
      refTree.insert(envelope, data);
    }
    refTree.build();
    index.build();
    assertTrue(index.getDataItemIndex().isSpilled());

    // Query 1000 random items
    for (int i = 0; i < 1000; i++) {
      double x = random.nextDouble();
      double y = random.nextDouble();
      Envelope queryWindow = new Envelope(x, x + 0.05, y, y + 0.05);
      Iterator<DataItemWithId> iter = index.query(queryWindow);
      List<byte[]> expected = refTree.query(queryWindow);
      List<byte[]> actual = new ArrayList<>();
      while (iter.hasNext()) {
        actual.add(iter.next().dataItem);
      }
      ByteArrayComparator comparator = new ByteArrayComparator();
      expected.sort(comparator);
      actual.sort(comparator);
      assertEquals(expected.size(), actual.size());
      for (int j = 0; j < expected.size(); j++) {
        assertEquals(expected.get(j).length, actual.get(j).length);
        assertArrayEquals(expected.get(j), actual.get(j));
      }
    }

    index.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testQuery() throws IOException {
    ExternalSpatialIndex index = create();
    STRtree refTree = new STRtree();

    // Insert 10000 random items
    Random random = new Random();
    for (int i = 0; i < 10000; i++) {
      double x = random.nextDouble();
      double y = random.nextDouble();
      Envelope envelope = new Envelope(x, x + 0.01, y, y + 0.01);
      byte[] data = new byte[10];
      random.nextBytes(data);
      index.insert(envelope, data);
      refTree.insert(envelope, data);
    }
    index.build();
    refTree.build();

    for (int trial = 0; trial < 2; trial++) {
      // Query 1000 random items
      for (int i = 0; i < 1000; i++) {
        double x = random.nextDouble();
        double y = random.nextDouble();
        Envelope queryWindow = new Envelope(x, x + 0.05, y, y + 0.05);
        Iterator<DataItemWithId> iter = index.query(queryWindow);
        List<byte[]> expected = refTree.query(queryWindow);
        List<byte[]> actual = new ArrayList<>();
        while (iter.hasNext()) {
          actual.add(iter.next().dataItem);
        }
        ByteArrayComparator comparator = new ByteArrayComparator();
        expected.sort(comparator);
        actual.sort(comparator);
        assertEquals(expected.size(), actual.size());
        for (int j = 0; j < expected.size(); j++) {
          assertEquals(expected.get(j).length, actual.get(j).length);
          assertArrayEquals(expected.get(j), actual.get(j));
        }
      }

      // Query using the empty window
      Envelope queryWindow = new Envelope();
      Iterator<DataItemWithId> iter = index.query(queryWindow);
      assertFalse(iter.hasNext());

      // Query using an out-of-bound window
      queryWindow = new Envelope(2, 3, 2, 3);
      iter = index.query(queryWindow);
      assertFalse(iter.hasNext());

      index.spill();
    }

    index.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testQueryUsingMultiEnvelopes() throws IOException {
    ExternalSpatialIndex index = create();
    STRtree refTree = new STRtree();

    // Insert 10000 random items
    Random random = new Random();
    for (int i = 0; i < 10000; i++) {
      double x = random.nextDouble();
      double y = random.nextDouble();
      Envelope envelope = new Envelope(x, x + 0.01, y, y + 0.01);
      byte[] data = new byte[4];
      Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET, i);
      index.insert(envelope, data);
      refTree.insert(envelope, data);
    }
    index.build();
    refTree.build();

    for (int trial = 0; trial < 2; trial++) {
      // Query 1000 random multi-envelopes coverages
      for (int i = 0; i < 1000; i++) {
        double x = random.nextDouble();
        double y = random.nextDouble();
        List<Envelope> queryWindows = new ArrayList<>();
        List<Integer> expectedIds = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
          Envelope queryWindow = new Envelope(x, x + 0.01, y, y + 0.01);
          queryWindows.add(queryWindow);
          x += 0.01;
          y += 0.01;

          List<byte[]> expectedBytes = refTree.query(queryWindow);
          for (byte[] bytes : expectedBytes) {
            expectedIds.add(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET));
          }
        }

        Iterator<DataItemWithId> iter = index.query(queryWindows);
        List<Integer> actualIds = new ArrayList<>();
        while (iter.hasNext()) {
          int value = Platform.getInt(iter.next().dataItem, Platform.BYTE_ARRAY_OFFSET);
          actualIds.add(value);
        }
        expectedIds = expectedIds.stream().sorted().distinct().collect(Collectors.toList());
        actualIds = actualIds.stream().sorted().distinct().collect(Collectors.toList());
        assertEquals(expectedIds, actualIds);
      }

      index.spill();
    }

    index.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testIndexingMultiEnvelopes() throws IOException {
    ExternalSpatialIndex index = create();
    STRtree refTree = new STRtree();

    // Insert 10000 random data items, each of them has 5 covering envelopes
    Random random = new Random();
    for (int i = 0; i < 10000; i++) {
      double x = random.nextDouble();
      double y = random.nextDouble();
      List<Envelope> envelopes = new ArrayList<>();
      for (int j = 0; j < 5; j++) {
        Envelope envelope = new Envelope(x, x + 0.01, y, y + 0.01);
        envelopes.add(envelope);
        x += 0.01;
        y += 0.01;
      }

      byte[] data = new byte[4];
      Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET, i);
      index.insert(envelopes, data);
      for (Envelope envelope : envelopes) {
        refTree.insert(envelope, data);
      }
    }
    index.build();
    refTree.build();

    for (int trial = 0; trial < 2; trial++) {
      // Query 1000 random items
      for (int i = 0; i < 1000; i++) {
        // Query using one envelope
        double x = random.nextDouble();
        double y = random.nextDouble();
        Envelope queryWindow = new Envelope(x, x + 0.05, y, y + 0.05);
        Iterator<DataItemWithId> iter = index.query(queryWindow);
        List<byte[]> expectedBytes = refTree.query(queryWindow);
        List<Integer> expectedIds = new ArrayList<>();
        for (byte[] bytes : expectedBytes) {
          expectedIds.add(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET));
        }
        List<Integer> actualIds = new ArrayList<>();
        while (iter.hasNext()) {
          int value = Platform.getInt(iter.next().dataItem, Platform.BYTE_ARRAY_OFFSET);
          actualIds.add(value);
        }
        expectedIds = expectedIds.stream().sorted().distinct().collect(Collectors.toList());
        actualIds = actualIds.stream().sorted().distinct().collect(Collectors.toList());
        assertEquals(expectedIds, actualIds);

        // Query using multiple envelopes
        List<Envelope> queryWindows = new ArrayList<>();
        expectedIds.clear();
        for (int j = 0; j < 5; j++) {
          queryWindow = new Envelope(x, x + 0.01, y, y + 0.01);
          queryWindows.add(queryWindow);
          x += 0.01;
          y += 0.01;
          expectedBytes = refTree.query(queryWindow);
          for (byte[] bytes : expectedBytes) {
            expectedIds.add(Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET));
          }
        }
        iter = index.query(queryWindows);
        actualIds.clear();
        while (iter.hasNext()) {
          int value = Platform.getInt(iter.next().dataItem, Platform.BYTE_ARRAY_OFFSET);
          actualIds.add(value);
        }
        expectedIds = expectedIds.stream().sorted().distinct().collect(Collectors.toList());
        actualIds = actualIds.stream().sorted().distinct().collect(Collectors.toList());
        assertEquals(expectedIds, actualIds);
      }

      index.spill();
    }

    index.close();
  }

  @Test
  public void testDeduplicateIntList() {
    IntList intList = new IntArrayList();
    intList.add(1);
    intList.add(2);
    intList.add(1);
    intList.add(3);
    intList.add(2);
    ExternalSpatialIndex.deduplicateInPlace(intList);
    assertEquals(intList.size(), 3);
    assertEquals(intList.getInt(0), 1);
    assertEquals(intList.getInt(1), 2);
    assertEquals(intList.getInt(2), 3);

    // Deduplicating a list with no duplicates
    Collections.shuffle(intList);
    ExternalSpatialIndex.deduplicateInPlace(intList);
    assertEquals(intList.size(), 3);
    assertEquals(intList.getInt(0), 1);
    assertEquals(intList.getInt(1), 2);
    assertEquals(intList.getInt(2), 3);
  }

  private ExternalSpatialIndex create() {
    ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();
    return new ExternalSpatialIndex(
        taskContext, taskMemoryManager, blockManager, pageSizeBytes, 20, 10, writeMetrics);
  }
}
