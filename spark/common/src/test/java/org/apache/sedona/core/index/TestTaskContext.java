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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.metrics.source.Source;
import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.spark.util.TaskFailureListener;
import scala.Function0;
import scala.Option;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;

/**
 * Mocking TaskContext for testing purposes. We are not using Mockito since the cheap and repeatedly
 * called method `killTaskIfInterrupted` is too slow with Mockito.
 */
public class TestTaskContext extends TaskContext {

  private final TaskMetrics taskMetrics = new TaskMetrics();
  private final TaskMemoryManager taskMemoryManager;
  private final List<TaskCompletionListener> taskCompletionListeners = new ArrayList<>();

  TestTaskContext(TaskMemoryManager taskMemoryManager) {
    this.taskMemoryManager = taskMemoryManager;
  }

  @Override
  public boolean isCompleted() {
    return false;
  }

  @Override
  public boolean isInterrupted() {
    return false;
  }

  public boolean isFailed() {
    // This method is not present in Spark 3.5.0, but it is in Spark 3.5.1. We don't annotate it
    // with @Override since it's not present in Spark 3.5.0.
    return false;
  }

  @Override
  public TaskContext addTaskCompletionListener(TaskCompletionListener listener) {
    taskCompletionListeners.add(listener);
    return this;
  }

  @Override
  public TaskContext addTaskFailureListener(TaskFailureListener listener) {
    return null;
  }

  public void runTaskCompletionListeners() {
    // Run the listeners in reverse order to ensure the latest added listener is executed first.
    for (int i = taskCompletionListeners.size() - 1; i >= 0; i--) {
      taskCompletionListeners.get(i).onTaskCompletion(this);
    }
    taskCompletionListeners.clear();
  }

  @Override
  public int stageId() {
    return 0;
  }

  @Override
  public int stageAttemptNumber() {
    return 0;
  }

  @Override
  public int partitionId() {
    return 0;
  }

  // This overrides a method since Spark 3.4. We don't annotate it with @Override since it's not
  // present in Spark 3.3.
  public int numPartitions() {
    return 0;
  }

  @Override
  public int attemptNumber() {
    return 0;
  }

  @Override
  public long taskAttemptId() {
    return 0;
  }

  @Override
  public String getLocalProperty(String key) {
    return "";
  }

  @Override
  public int cpus() {
    return 0;
  }

  @Override
  public Map<String, ResourceInformation> resources() {
    return null;
  }

  @Override
  public java.util.Map<String, ResourceInformation> resourcesJMap() {
    return Collections.emptyMap();
  }

  @Override
  public TaskMetrics taskMetrics() {
    return taskMetrics;
  }

  @Override
  public Seq<Source> getMetricsSources(String sourceName) {
    return null;
  }

  @Override
  public void killTaskIfInterrupted() {}

  @Override
  public Option<String> getKillReason() {
    return null;
  }

  @Override
  public TaskMemoryManager taskMemoryManager() {
    return taskMemoryManager;
  }

  @Override
  public void registerAccumulator(AccumulatorV2<?, ?> a) {}

  @Override
  public void setFetchFailed(FetchFailedException fetchFailed) {}

  @Override
  public void markInterrupted(String reason) {}

  @Override
  public void markTaskFailed(Throwable error) {}

  @Override
  public void markTaskCompleted(Option<Throwable> error) {}

  @Override
  public Option<FetchFailedException> fetchFailed() {
    return null;
  }

  @Override
  public Properties getLocalProperties() {
    return null;
  }

  // Override method in Spark 4
  // Deliberately not annotating it with @Override to be compatible with Spark 3
  public boolean interruptible() {
    return false;
  }

  // Override method in Spark 4
  // Deliberately not annotating it with @Override to be compatible with Spark 3
  public void pendingInterrupt(Option<Thread> threadToInterrupt, String reason) {}

  // Override method in Spark 4
  // Deliberately not annotating it with @Override to be compatible with Spark 3
  public <T extends Closeable> T createResourceUninterruptibly(Function0<T> resourceBuilder) {
    return null;
  }
}
