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
package org.apache.sedona.core.joinJudgement;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.jts.geom.Geometry;

/**
 * Iterators for producing outer-join results for out-of-bounds geometries. These geometries do not
 * have a corresponding intersecting geometry in the other dataset, so they are paired with a null
 * geometry.
 */
public class OuterJoinIterators {
  public static class LeftOuterJoinIterator<U extends Geometry, T extends Geometry>
      implements Iterator<Pair<U, T>> {
    private final Iterator<U> leftIterator;

    public LeftOuterJoinIterator(Iterator<U> leftIterator) {
      this.leftIterator = leftIterator;
    }

    @Override
    public boolean hasNext() {
      return leftIterator.hasNext();
    }

    @Override
    public Pair<U, T> next() {
      U leftValue = leftIterator.next();
      return Pair.of(leftValue, null);
    }
  }

  public static class RightOuterJoinIterator<U extends Geometry, T extends Geometry>
      implements Iterator<Pair<U, T>> {
    private final Iterator<T> rightIterator;

    public RightOuterJoinIterator(Iterator<T> rightIterator) {
      this.rightIterator = rightIterator;
    }

    @Override
    public boolean hasNext() {
      return rightIterator.hasNext();
    }

    @Override
    public Pair<U, T> next() {
      T rightValue = rightIterator.next();
      return Pair.of(null, rightValue);
    }
  }

  public static class FullOuterJoinIterator<U extends Geometry, T extends Geometry>
      implements Iterator<Pair<U, T>> {
    private final Iterator<U> leftIterator;
    private final Iterator<T> rightIterator;

    public FullOuterJoinIterator(Iterator<U> leftIterator, Iterator<T> rightIterator) {
      this.leftIterator = leftIterator;
      this.rightIterator = rightIterator;
    }

    @Override
    public boolean hasNext() {
      return leftIterator.hasNext() || rightIterator.hasNext();
    }

    @Override
    public Pair<U, T> next() {
      if (leftIterator.hasNext()) {
        U leftValue = leftIterator.next();
        return Pair.of(leftValue, null);
      } else if (rightIterator.hasNext()) {
        T rightValue = rightIterator.next();
        return Pair.of(null, rightValue);
      } else {
        throw new NoSuchElementException("No more outer join results");
      }
    }
  }
}
