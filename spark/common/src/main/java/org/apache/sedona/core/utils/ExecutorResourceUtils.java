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
}
