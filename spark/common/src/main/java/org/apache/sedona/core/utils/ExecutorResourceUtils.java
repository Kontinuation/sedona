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

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class ExecutorResourceUtils {
  public static long inferExecutionMemory(SparkContext context) {
    long executorMemory = (long) context.executorMemory() * 1024 * 1024;
    double memoryFraction =
        Double.parseDouble(context.getConf().get("spark.memory.fraction", "0.6"));
    double storageFraction =
        Double.parseDouble(context.getConf().get("spark.memory.storageFraction", "0.5"));
    int executorCores = Integer.parseInt(context.getConf().get("spark.executor.cores", "1"));
    return (long) (executorMemory * memoryFraction * (1 - storageFraction) / executorCores);
  }

  public static int inferParallelism(SparkContext context) {
    SparkConf conf = context.getConf();
    int executorInstances = conf.getInt("spark.executor.instances", 0);
    int executorCores = conf.getInt("spark.executor.cores", 1);
    boolean isDynamicAllocationEnabled = conf.getBoolean("spark.dynamicAllocation.enabled", false);
    if (isDynamicAllocationEnabled) {
      // Take the maximum of minExecutors, initialExecutors and spark.executor.instances as
      // the number of executor instances. This is the same as
      // Utils.getDynamicAllocationInitialExecutors in Spark source.
      int initialExecutors = conf.getInt("spark.dynamicAllocation.initialExecutors", 0);
      int minExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0);
      executorInstances = Math.max(executorInstances, initialExecutors);
      executorInstances = Math.max(executorInstances, minExecutors);
    }
    return Math.max(context.defaultParallelism(), executorInstances * executorCores);
  }

  public static int getTargetPartitionCount(SparkContext context) {
    return 4 * ExecutorResourceUtils.inferParallelism(context);
  }

  public static int getTargetPartitionCount(
      SparkContext context,
      long idealRecordsPerPartition,
      long numberRows,
      int originalNumPartitions) {
    int targetParallelism =
        (int)
            Math.min(
                Math.max(1, numberRows / idealRecordsPerPartition),
                4L * ExecutorResourceUtils.inferParallelism(context));
    // We'd better get rid of shrinking the number of partitions. The size of each record could be
    // super large (especially when processing raster data), shrinking the number of partitions
    // may result in poor performance or even OOM.
    return Math.max(targetParallelism, originalNumPartitions);
  }
}
