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
package org.apache.spark.sql.sedona_sql.utils

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.UnsafeExternalRowSorter
import org.apache.spark.sql.execution.UnsafeExternalRowSorter.PrefixComputer
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators.UnsignedPrefixComparator
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparator, RecordComparator}

import java.util
import java.util.function.Supplier

object UnsafeRowRDDSorter {
  def sortUnsafeRowRDD(unsafeRowRdd: RDD[UnsafeRow], schema: StructType): RDD[UnsafeRow] = {
    // Most of the code is taken from org.apache.spark.sql.execution.SortExec.
    unsafeRowRdd.mapPartitions { iter =>
      val numFields = schema.size
      val recordComparatorSupplier: Supplier[RecordComparator] =
        () => new UnsafeRowComparator(numFields)
      val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
      val canUseRadixSort = false
      val prefixComputer: UnsafeExternalRowSorter.PrefixComputer = new UnsafeRowPrefixComputer
      val prefixComparator: PrefixComparator = new UnsignedPrefixComparator
      val sorter = UnsafeExternalRowSorter.createWithRecordComparator(
        schema,
        recordComparatorSupplier,
        prefixComparator,
        prefixComputer,
        pageSize,
        canUseRadixSort)

      val metrics = TaskContext.get().taskMetrics()
      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val sortedIterator = sorter.sort(iter)
      metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)

      sortedIterator.map(_.asInstanceOf[UnsafeRow])
    }
  }

  class UnsafeRowComparator(numFields: Int) extends RecordComparator {
    override def compare(
        leftBaseObject: Any,
        leftBaseOffset: Long,
        leftBaseLength: Int,
        rightBaseObject: Any,
        rightBaseOffset: Long,
        rightBaseLength: Int): Int = {
      val leftRow = new UnsafeRow(numFields)
      leftRow.pointTo(leftBaseObject, leftBaseOffset, leftBaseLength)
      val rightRow = new UnsafeRow(numFields)
      rightRow.pointTo(rightBaseObject, rightBaseOffset, rightBaseLength)
      val leftBytes = leftRow.getBytes
      val rightBytes = rightRow.getBytes
      val cmp = util.Arrays.hashCode(leftBytes) - util.Arrays.hashCode(rightBytes)
      if (cmp != 0) cmp else compareByteArray(leftBytes, rightBytes)
    }
  }

  class UnsafeRowPrefixComputer extends UnsafeExternalRowSorter.PrefixComputer {
    override def computePrefix(row: InternalRow): PrefixComputer.Prefix = {
      val prefix = new PrefixComputer.Prefix()
      row match {
        case unsafeRow: UnsafeRow =>
          val bytes = unsafeRow.getBytes
          prefix.value = util.Arrays.hashCode(bytes)
          prefix.isNull = false
        case _ =>
          val prefix = new PrefixComputer.Prefix()
          prefix.value = 0
          prefix.isNull = true
      }
      prefix
    }
  }

  def compareByteArray(a: Array[Byte], b: Array[Byte]): Int = {
    // FIXME: This is a implementation of `java.util.Arrays.compare`, which is only available in Java 9.
    //  This is only for compatibility with Java 8, and it could be slower than `java.util.Arrays.compare`.
    //  We should remove this after we drop Java 8 support.
    if (a eq b) {
      0
    } else {
      val len = math.min(a.length, b.length)
      var i = 0
      var result = 0

      while (i < len && result == 0) {
        val diff = java.lang.Byte.toUnsignedInt(a(i)) - java.lang.Byte.toUnsignedInt(b(i))
        if (diff != 0) {
          result = diff
        }
        i += 1
      }

      if (result == 0) {
        a.length - b.length
      } else {
        result
      }
    }
  }
}
