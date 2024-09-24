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

import java.util.Objects;

/**
 * A range of bytes in a file. The disk cache file can have holes, and we use a tree set of
 * ByteRange to track the ranges that have been cached. Please refer to the {@link ByteRangeSet}
 * class for more details.
 */
public class ByteRange {

  public long inclusiveStart;
  public long exclusiveEnd;

  public ByteRange(long inclusiveStart, long exclusiveEnd) {
    if (inclusiveStart >= exclusiveEnd) {
      throw new IllegalArgumentException("ByteRange inclusiveStart >= exclusiveEnd");
    }
    this.inclusiveStart = inclusiveStart;
    this.exclusiveEnd = exclusiveEnd;
  }

  public boolean contains(long pos) {
    return pos >= inclusiveStart && pos < exclusiveEnd;
  }

  public boolean containsOrAdjacent(long pos) {
    return pos >= inclusiveStart && pos <= exclusiveEnd;
  }

  public boolean contains(ByteRange other) {
    return other.inclusiveStart >= inclusiveStart && other.exclusiveEnd <= exclusiveEnd;
  }

  public boolean overlaps(ByteRange other) {
    return contains(other.inclusiveStart)
        || contains(other.exclusiveEnd - 1)
        || other.contains(inclusiveStart)
        || other.contains(exclusiveEnd - 1);
  }

  public boolean overlapsOrAdjacent(ByteRange other) {
    return containsOrAdjacent(other.inclusiveStart)
        || containsOrAdjacent(other.exclusiveEnd)
        || other.containsOrAdjacent(inclusiveStart)
        || other.containsOrAdjacent(exclusiveEnd);
  }

  public long size() {
    return exclusiveEnd - inclusiveStart;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ByteRange byteRange = (ByteRange) o;
    return inclusiveStart == byteRange.inclusiveStart && exclusiveEnd == byteRange.exclusiveEnd;
  }

  @Override
  public int hashCode() {
    return Objects.hash(inclusiveStart, exclusiveEnd);
  }

  @Override
  public String toString() {
    return String.format("[%d, %d)", inclusiveStart, exclusiveEnd);
  }
}
