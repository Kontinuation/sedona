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

import java.io.File;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempLocalBlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/** Base class for builders that spill data to disk when there is not enough memory to build. */
class SpillableBuilderBase {

  static final Logger logger = LoggerFactory.getLogger(SpillableBuilderBase.class);

  protected final BlockManager blockManager;
  protected final TaskContext taskContext;
  protected final ShuffleWriteMetricsReporter shuffleWriteMetricsReporter;

  protected boolean hasSpilled = false;
  protected DiskBlockObjectWriter spillWriter = null;
  protected File spillFile = null;
  protected TempLocalBlockId spillBlockId = null;
  protected final byte[] spillWriteBuffer = new byte[1024 * 32]; // 32KB buffer

  public SpillableBuilderBase(
      BlockManager blockManager,
      TaskContext taskContext,
      ShuffleWriteMetricsReporter shuffleWriteMetricsReporter) {
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.shuffleWriteMetricsReporter = shuffleWriteMetricsReporter;
  }

  public boolean hasSpilled() {
    return hasSpilled;
  }

  protected void close() {
    if (spillWriter != null) {
      closeSpillWriter(spillWriter);
      spillWriter = null;
    }
    if (spillFile != null) {
      deleteSpillFile(spillFile);
      spillFile = null;
    }
    spillBlockId = null;
  }

  protected void closeSpillWriter(DiskBlockObjectWriter spillWriter) {
    FileSegment spilledSegment = spillWriter.commitAndGet();
    taskContext.taskMetrics().incDiskBytesSpilled(spilledSegment.length());
    spillWriter.close();
  }

  protected void deleteSpillFile(File spillFile) {
    if (!spillFile.delete()) {
      logger.warn("Failed to delete file: {}", spillFile.getAbsolutePath());
    }
  }

  protected void initializeSpillWriter() {
    if (spillWriter != null) {
      return;
    }

    // Create a spill writer to write envelopes to the spill file
    Tuple2<TempLocalBlockId, File> tempLocalBlock =
        blockManager.diskBlockManager().createTempLocalBlock();
    spillBlockId = tempLocalBlock._1();
    spillFile = tempLocalBlock._2();
    spillWriter =
        blockManager.getDiskWriter(
            spillBlockId,
            spillFile,
            DummySerializerInstance.INSTANCE,
            spillWriteBuffer.length,
            shuffleWriteMetricsReporter);

    hasSpilled = true;
  }
}
