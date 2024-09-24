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
package org.apache.sedona.common.raster.inputstream.util;

import org.junit.Assert;
import org.junit.Test;

public class ByteRangeTests {
  @Test
  public void testConstructByteRange() {
    ByteRange byteRange = new ByteRange(0, 10);
    Assert.assertEquals(0, byteRange.inclusiveStart);
    Assert.assertEquals(10, byteRange.exclusiveEnd);
    Assert.assertThrows(IllegalArgumentException.class, () -> new ByteRange(10, 10));
  }

  @Test
  public void testSize() {
    ByteRange byteRange = new ByteRange(0, 10);
    Assert.assertEquals(10, byteRange.size());
    byteRange = new ByteRange(0, 1);
    Assert.assertEquals(1, byteRange.size());
  }

  @Test
  public void testContains() {
    ByteRange byteRange = new ByteRange(0, 10);
    Assert.assertTrue(byteRange.contains(0));
    Assert.assertTrue(byteRange.contains(9));
    Assert.assertFalse(byteRange.contains(10));
    Assert.assertFalse(byteRange.contains(11));
    byteRange = new ByteRange(9, 10);
    Assert.assertFalse(byteRange.contains(8));
    Assert.assertTrue(byteRange.contains(9));
    Assert.assertFalse(byteRange.contains(10));
    Assert.assertFalse(byteRange.contains(11));
  }

  @Test
  public void testContainsOrAdjacent() {
    ByteRange byteRange = new ByteRange(0, 10);
    Assert.assertTrue(byteRange.containsOrAdjacent(0));
    Assert.assertTrue(byteRange.containsOrAdjacent(9));
    Assert.assertTrue(byteRange.containsOrAdjacent(10));
    Assert.assertFalse(byteRange.containsOrAdjacent(11));
    byteRange = new ByteRange(9, 10);
    Assert.assertFalse(byteRange.containsOrAdjacent(8));
    Assert.assertTrue(byteRange.containsOrAdjacent(9));
    Assert.assertTrue(byteRange.containsOrAdjacent(10));
    Assert.assertFalse(byteRange.containsOrAdjacent(11));
  }

  @Test
  public void testContainsRange() {
    ByteRange range = new ByteRange(1, 4);
    Assert.assertTrue(range.contains(new ByteRange(1, 4)));
    Assert.assertTrue(range.contains(new ByteRange(1, 2)));
    Assert.assertTrue(range.contains(new ByteRange(2, 3)));
    Assert.assertTrue(range.contains(new ByteRange(3, 4)));
    Assert.assertFalse(range.contains(new ByteRange(4, 5)));
    Assert.assertFalse(range.contains(new ByteRange(5, 6)));
    Assert.assertFalse(range.contains(new ByteRange(0, 1)));
    Assert.assertFalse(range.contains(new ByteRange(-1, 0)));
    Assert.assertFalse(range.contains(new ByteRange(0, 2)));
    Assert.assertFalse(range.contains(new ByteRange(3, 5)));
    Assert.assertFalse(range.contains(new ByteRange(-1, 5)));
  }

  @Test
  public void testOverlaps() {
    ByteRange range = new ByteRange(1, 4);
    Assert.assertTrue(range.overlaps(new ByteRange(1, 4)));
    Assert.assertTrue(range.overlaps(new ByteRange(1, 2)));
    Assert.assertTrue(range.overlaps(new ByteRange(2, 3)));
    Assert.assertTrue(range.overlaps(new ByteRange(3, 4)));
    Assert.assertFalse(range.overlaps(new ByteRange(4, 5)));
    Assert.assertFalse(range.overlaps(new ByteRange(5, 6)));
    Assert.assertFalse(range.overlaps(new ByteRange(0, 1)));
    Assert.assertFalse(range.overlaps(new ByteRange(-1, 0)));
    Assert.assertTrue(range.overlaps(new ByteRange(0, 2)));
    Assert.assertTrue(range.overlaps(new ByteRange(3, 5)));
    Assert.assertTrue(range.overlaps(new ByteRange(-1, 5)));
  }

  @Test
  public void testOverlapsOrAdjacentRange() {
    ByteRange range = new ByteRange(1, 4);
    Assert.assertTrue(range.overlapsOrAdjacent(new ByteRange(1, 4)));
    Assert.assertTrue(range.overlapsOrAdjacent(new ByteRange(1, 2)));
    Assert.assertTrue(range.overlapsOrAdjacent(new ByteRange(2, 3)));
    Assert.assertTrue(range.overlapsOrAdjacent(new ByteRange(3, 4)));
    Assert.assertTrue(range.overlapsOrAdjacent(new ByteRange(4, 5)));
    Assert.assertFalse(range.overlapsOrAdjacent(new ByteRange(5, 6)));
    Assert.assertTrue(range.overlapsOrAdjacent(new ByteRange(0, 1)));
    Assert.assertFalse(range.overlapsOrAdjacent(new ByteRange(-1, 0)));
    Assert.assertTrue(range.overlapsOrAdjacent(new ByteRange(0, 2)));
    Assert.assertTrue(range.overlapsOrAdjacent(new ByteRange(3, 5)));
    Assert.assertTrue(range.overlapsOrAdjacent(new ByteRange(-1, 5)));
  }

  @Test
  public void testEquals() {
    ByteRange range = new ByteRange(10, 20);
    Assert.assertEquals(range, range);
    Assert.assertEquals(range, new ByteRange(10, 20));
    Assert.assertNotEquals(range, null);
    Assert.assertNotEquals(range, new ByteRange(1, 20));
    Assert.assertNotEquals(range, new ByteRange(10, 200));
  }
}
