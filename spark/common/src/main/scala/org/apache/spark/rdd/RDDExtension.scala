/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.rdd

import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{Function3 => JFunction3}
import org.apache.spark.api.java.{JavaRDD, JavaRDDLike}

import java.util.{Iterator => JIterator}
import java.{lang => jl}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object RDDExtension {

  /**
   * A variant of RDD.zipPartitions that allows the function to take the index of the partition as parameter. This is
   * for supporting adaptive index lookup in advanced spatial join.
   */
  def zipPartitionsWithIndex[T: ClassTag, B: ClassTag, V: ClassTag]
    (rdd1: RDD[T], rdd2: RDD[B], preservesPartitioning: Boolean)
    (f: (Int, Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = rdd1.withScope {
    val sc = rdd1.sparkContext
    new ZippedPartitionsWithIndexRDD2(sc, sc.clean(f), rdd1, rdd2, preservesPartitioning)
  }

  /**
   * A variant of JavaRDDLike.zipPartitions that allows the function to take the index of the partition as parameter.
   * This is for supporting adaptive index lookup in advanced spatial join.
   */
  def javaZipPartitionsWithIndex[T, U, V]
    (rdd: JavaRDDLike[T, _], other: JavaRDDLike[U, _],
     f: JFunction3[jl.Integer, JIterator[T], JIterator[U], JIterator[V]]): JavaRDD[V] = {
    def fn: (Int, Iterator[T], Iterator[U]) => Iterator[V] = {
      (index: Int, x: Iterator[T], y: Iterator[U]) => f.call(index, x.asJava, y.asJava).asScala
    }
    val resultRdd = zipPartitionsWithIndex(rdd.rdd, other.rdd, preservesPartitioning = false)(fn)(rdd.classTag, other.classTag, fakeClassTag[V])
    JavaRDD.fromRDD(resultRdd)(fakeClassTag[V])
  }
}
