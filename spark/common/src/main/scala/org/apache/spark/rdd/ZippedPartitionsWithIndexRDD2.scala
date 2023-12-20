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

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * A variant of ZippedPartitionsRDD that allows the function to take the partition index as parameter.
 */
private[spark] class ZippedPartitionsWithIndexRDD2[A: ClassTag, B: ClassTag, V: ClassTag](
  sc: SparkContext,
  var f: (Int, Iterator[A], Iterator[B]) => Iterator[V],
  var rdd1: RDD[A],
  var rdd2: RDD[B],
  preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(s.index, rdd1.iterator(partitions.head, context), rdd2.iterator(partitions(1), context))
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    f = null
  }
}
