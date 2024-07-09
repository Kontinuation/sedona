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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.UnsafeExternalRowSorter
import org.apache.spark.sql.sedona_sql.utils.UnsafeRowRDDSorter.UnsafeRowPrefixComputer
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators.UnsignedPrefixComparator
import org.apache.spark.util.collection.unsafe.sort.RecordComparator
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.UnsafeExternalRowSorter.PrefixComputer

import java.util
import java.util.function.Supplier

object JoinedUnsafeRowRDDSorter {
  def sortJoinedUnsafeRowRDD(
      unsafeRowRdd: RDD[UnsafeRow],
      joinType: JoinType,
      schema: StructType,
      joinedAttributes: Seq[Attribute],
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute]): RDD[UnsafeRow] = {
    // Most of the code is taken from org.apache.spark.sql.execution.SortExec.
    if (joinType != LeftOuter && joinType != RightOuter) {
      throw new IllegalArgumentException("Only support LeftOuter and RightOuter join")
    }
    unsafeRowRdd.mapPartitions { iter =>
      val (outerProjection, otherProjection) =
        createProjections(joinType, joinedAttributes, leftAttributes, rightAttributes)
      val numFields = schema.size
      val recordComparatorSupplier: Supplier[RecordComparator] =
        () => new JoinedUnsafeRowComparator(numFields, outerProjection, otherProjection)
      val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
      val canUseRadixSort = false
      val prefixComputer: UnsafeExternalRowSorter.PrefixComputer =
        new JoinedUnsafeRowPrefixComputer(outerProjection)
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

  private class JoinedUnsafeRowComparator(
      numFields: Int,
      outerProjection: UnsafeProjection,
      otherProjection: UnsafeProjection)
      extends RecordComparator {
    override def compare(
        leftBaseObject: Any,
        leftBaseOffset: Long,
        leftBaseLength: Int,
        rightBaseObject: Any,
        rightBaseOffset: Long,
        rightBaseLength: Int): Int = {
      val joinedRow1 = new UnsafeRow(numFields)
      joinedRow1.pointTo(leftBaseObject, leftBaseOffset, leftBaseLength)
      val joinedRow2 = new UnsafeRow(numFields)
      joinedRow2.pointTo(rightBaseObject, rightBaseOffset, rightBaseLength)
      val leftBytes = outerProjection(joinedRow1).getBytes
      val rightBytes = outerProjection(joinedRow2).getBytes
      val cmp = util.Arrays.hashCode(leftBytes) - util.Arrays.hashCode(rightBytes)
      if (cmp != 0) cmp
      else {
        val cmp = UnsafeRowRDDSorter.compareByteArray(leftBytes, rightBytes)
        if (cmp != 0) cmp
        else {
          // Null-last order if there's a tie. This is for easier dropping rows containing nulls
          // when non-null rows are present.
          val v1 = if (isAllNull(otherProjection(joinedRow1))) 1 else 0
          val v2 = if (isAllNull(otherProjection(joinedRow2))) 1 else 0
          v1 - v2
        }
      }
    }
  }

  private class JoinedUnsafeRowPrefixComputer(outerProjection: UnsafeProjection)
      extends UnsafeExternalRowSorter.PrefixComputer {
    private val computer = new UnsafeRowPrefixComputer
    override def computePrefix(joinedRow: InternalRow): PrefixComputer.Prefix = {
      val row = outerProjection(joinedRow)
      computer.computePrefix(row)
    }
  }

  def isAllNull(unsafeRow: UnsafeRow): Boolean = {
    var i = 0
    while (i < unsafeRow.numFields) {
      if (!unsafeRow.isNullAt(i)) {
        return false
      }
      i += 1
    }
    true
  }

  def createProjections(
      joinType: JoinType,
      joinedAttributes: Seq[Attribute],
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute]): (UnsafeProjection, UnsafeProjection) = {
    joinType match {
      case LeftOuter =>
        (
          UnsafeProjection.create(leftAttributes, joinedAttributes),
          UnsafeProjection.create(rightAttributes, joinedAttributes))
      case RightOuter =>
        (
          UnsafeProjection.create(rightAttributes, joinedAttributes),
          UnsafeProjection.create(leftAttributes, joinedAttributes))
      case _ => throw new UnsupportedOperationException(s"Unsupported join type: $joinType")
    }
  }
}
