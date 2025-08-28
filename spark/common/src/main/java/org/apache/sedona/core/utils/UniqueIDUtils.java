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
package org.apache.sedona.core.utils;

import java.util.Iterator;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class UniqueIDUtils {

  /**
   * Attach unique IDs to each record in the RDD, so that it could be joined back with the
   * subdivided RDD later. The algorithm for assigning IDs is identical to the one in
   * SubdivideIterator, so that the IDs attached to original records are consistent with subdivided
   * records. This function is useful for retrieving information from the original RDD from a
   * subdivided RDD that does not carry the original geometries or user data.
   *
   * @param rdd The RDD to attach IDs to.
   * @return An RDD of (ID, T) pairs.
   * @param <T> The type of the records in the RDD.
   */
  public static <T> JavaRDD<Tuple2<Long, T>> attachId(JavaRDD<T> rdd) {
    int numPartitions = rdd.getNumPartitions();
    return rdd.mapPartitionsWithIndex(
        (index, iterator) -> new RecordWithIdIterator<>(index, numPartitions, iterator), false);
  }

  /**
   * Attach unique IDs to each record in the RDD, so that we can do deduplication on subdivided
   * RDDs, as well as joining subdivided RDD with original RDD.
   *
   * @param <T> The type of geometry in the spatial RDD.
   */
  public static class RecordWithIdIterator<T> implements Iterator<Tuple2<Long, T>> {
    long index;
    final int step;
    final Iterator<T> iterator;

    public RecordWithIdIterator(int index, int step, Iterator<T> iterator) {
      this.index = index;
      this.step = step;
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Tuple2<Long, T> next() {
      index += step;
      return new Tuple2<>(index, iterator.next());
    }
  }
}
