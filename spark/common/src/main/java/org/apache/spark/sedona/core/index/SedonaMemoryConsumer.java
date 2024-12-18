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
package org.apache.spark.sedona.core.index;

import java.util.IdentityHashMap;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * A memory consumer that exposes protected methods {@link #doAllocatePage(long)} and {@link
 * #doFreePage(MemoryBlock)}, so that it can be used by other related components. For example,
 * {@link ExternalSpatialIndex} is a subclass of this class, and the same memory consumer is shared
 * by {@link ExternalLeafPageIndexBuilder} and {@link ExternalDataItemIndexBuilder}. Now we can
 * handle spilling of the external spatial index universally in {@link ExternalSpatialIndex}, and
 * determine which component to spill by ourselves.
 */
public abstract class SedonaMemoryConsumer extends MemoryConsumer {

  /**
   * A set of consumers that are cooperative with this consumer. These consumers are temporary
   * objects for performing one-time operations and will only allocate memory in the same thread as
   * this consumer. We can handle spilling caused by these consumers in the same thread safely
   * without locking.
   */
  protected final IdentityHashMap<MemoryConsumer, Object> cooperativeConsumers =
      new IdentityHashMap<>();

  public SedonaMemoryConsumer(
      TaskMemoryManager taskMemoryManager, long size, MemoryMode memoryMode) {
    super(taskMemoryManager, size, memoryMode);
  }

  public MemoryBlock doAllocatePage(long required) {
    return this.allocatePage(required);
  }

  public void doFreePage(MemoryBlock page) {
    this.freePage(page);
  }

  public void addCooperativeConsumer(MemoryConsumer consumer) {
    this.cooperativeConsumers.put(consumer, null);
  }

  public void removeCooperativeConsumer(MemoryConsumer consumer) {
    this.cooperativeConsumers.remove(consumer);
  }

  public boolean isCooperativeConsumer(MemoryConsumer consumer) {
    return this.cooperativeConsumers.containsKey(consumer);
  }
}
