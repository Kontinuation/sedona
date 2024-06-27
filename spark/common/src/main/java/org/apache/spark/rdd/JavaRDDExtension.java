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
package org.apache.spark.rdd;

import java.util.Iterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function3;

/** This class provides a Java-friendly API for the Scala class RDDExtension. */
public class JavaRDDExtension {

  /**
   * A variant of JavaRDDLike.zipPartitions that allows the function to take the index of the
   * partition as a parameter. This is for supporting adaptive index lookup in advanced spatial
   * join.
   */
  public static <T, U, V> JavaRDD<V> javaZipPartitionsWithIndex(
      JavaRDDLike<T, ?> rdd,
      JavaRDDLike<U, ?> other,
      Function3<Integer, Iterator<T>, Iterator<U>, Iterator<V>> f) {

    return RDDExtension.javaZipPartitionsWithIndex(
        rdd,
        other,
        new org.apache.spark.api.java.function.Function3<
            Integer, java.util.Iterator<T>, java.util.Iterator<U>, java.util.Iterator<V>>() {
          @Override
          public java.util.Iterator<V> call(
              Integer index, java.util.Iterator<T> x, java.util.Iterator<U> y) throws Exception {
            return f.call(index, x, y);
          }
        });
  }
}
