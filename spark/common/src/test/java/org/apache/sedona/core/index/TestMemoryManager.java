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
package org.apache.sedona.core.index;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;
import org.apache.spark.SparkConf;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.storage.BlockId;

/**
 * This class is taken from org.apache.spark.memory.TestMemoryManager. It is a mock implementation
 * of MemoryManager that allows for manual control of memory allocation and OOM conditions.
 */
public class TestMemoryManager extends MemoryManager {
  @GuardedBy("this")
  private int consequentOOM = 0;

  @GuardedBy("this")
  private long available = Long.MAX_VALUE;

  @GuardedBy("this")
  private final Map<Long, Long> memoryForTask = new HashMap<>();

  public TestMemoryManager(SparkConf conf) {
    super(conf, 1, Long.MAX_VALUE, Long.MAX_VALUE);
  }

  public void reset() {
    resetConsequentOOM();
    limit(Long.MAX_VALUE);
  }

  @Override
  public synchronized long acquireExecutionMemory(
      long numBytes, long taskAttemptId, MemoryMode memoryMode) {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be non-negative");
    }

    long acquired;
    if (consequentOOM > 0) {
      consequentOOM -= 1;
      acquired = 0;
    } else if (available >= numBytes) {
      available -= numBytes;
      acquired = numBytes;
    } else {
      acquired = available;
      available = 0;
    }

    memoryForTask.put(taskAttemptId, memoryForTask.getOrDefault(taskAttemptId, 0L) + acquired);
    return acquired;
  }

  @Override
  public synchronized void releaseExecutionMemory(
      long numBytes, long taskAttemptId, MemoryMode memoryMode) {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be non-negative");
    }

    available += numBytes;
    long existingMemoryUsage = memoryForTask.getOrDefault(taskAttemptId, 0L);
    long newMemoryUsage = existingMemoryUsage - numBytes;

    if (newMemoryUsage < 0) {
      throw new IllegalArgumentException(
          "Attempting to free "
              + numBytes
              + " of memory for task attempt "
              + taskAttemptId
              + ", but it only allocated "
              + existingMemoryUsage
              + " bytes of memory");
    }

    memoryForTask.put(taskAttemptId, newMemoryUsage);
  }

  @Override
  public long releaseAllExecutionMemoryForTask(long taskAttemptId) {
    Long value = memoryForTask.remove(taskAttemptId);
    return value != null ? value : 0L;
  }

  @Override
  public long getExecutionMemoryUsageForTask(long taskAttemptId) {
    return memoryForTask.getOrDefault(taskAttemptId, 0L);
  }

  @Override
  public boolean acquireStorageMemory(BlockId blockId, long numBytes, MemoryMode memoryMode) {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be non-negative");
    }
    return true;
  }

  @Override
  public boolean acquireUnrollMemory(BlockId blockId, long numBytes, MemoryMode memoryMode) {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be non-negative");
    }
    return true;
  }

  @Override
  public void releaseStorageMemory(long numBytes, MemoryMode memoryMode) {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be non-negative");
    }
  }

  @Override
  public long maxOnHeapStorageMemory() {
    return Long.MAX_VALUE;
  }

  @Override
  public long maxOffHeapStorageMemory() {
    return 0L;
  }

  /**
   * Causes the next call to acquireExecutionMemory() to fail to allocate memory (returning 0),
   * simulating low-on-memory / out-of-memory conditions.
   */
  public void markExecutionAsOutOfMemoryOnce() {
    markConsequentOOM(1);
  }

  /**
   * Causes the next n calls to acquireExecutionMemory() to fail to allocate memory (returning 0),
   * simulating low-on-memory / out-of-memory conditions.
   */
  public synchronized void markConsequentOOM(int n) {
    consequentOOM += n;
  }

  /**
   * Undo the effects of markExecutionAsOutOfMemoryOnce and markConsequentOOM and lets calls to
   * acquireExecutionMemory() (if there is enough memory available).
   */
  public synchronized void resetConsequentOOM() {
    consequentOOM = 0;
  }

  public synchronized void limit(long avail) {
    if (avail < 0) {
      throw new IllegalArgumentException("avail must be non-negative");
    }
    available = avail;
  }
}
