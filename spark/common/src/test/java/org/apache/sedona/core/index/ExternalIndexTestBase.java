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

import static org.junit.Assert.assertEquals;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.LinkedList;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import scala.Tuple2$;

public class ExternalIndexTestBase {
  private final SparkConf conf = new SparkConf();

  final long pageSizeBytes = conf.getSizeAsBytes(package$.MODULE$.BUFFER_PAGESIZE().key(), "4m");

  protected final TestMemoryManager memoryManager =
      new TestMemoryManager(
          conf.clone()
              .set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false)
              .set(package$.MODULE$.BUFFER_PAGESIZE(), pageSizeBytes));
  protected final TaskMemoryManager taskMemoryManager = new TaskMemoryManager(memoryManager, 0);
  final SerializerManager serializerManager =
      new SerializerManager(
          new JavaSerializer(conf),
          conf.clone().set(package$.MODULE$.SHUFFLE_SPILL_COMPRESS(), true));

  File tempDir;
  final LinkedList<File> spillFilesCreated = new LinkedList<>();

  @Mock(answer = RETURNS_SMART_NULLS)
  protected BlockManager blockManager;

  @Mock(answer = RETURNS_SMART_NULLS)
  protected DiskBlockManager diskBlockManager;

  protected TestTaskContext taskContext;

  @Before
  public void setUp() throws Exception {
    memoryManager.reset();
    MockitoAnnotations.openMocks(this).close();
    tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "unsafe-test");
    spillFilesCreated.clear();
    taskContext = new TestTaskContext(taskMemoryManager);
    when(blockManager.diskBlockManager()).thenReturn(diskBlockManager);
    when(blockManager.serializerManager()).thenReturn(serializerManager);
    when(diskBlockManager.createTempLocalBlock())
        .thenAnswer(
            invocationOnMock -> {
              TempLocalBlockId blockId = new TempLocalBlockId(UUID.randomUUID());
              File file = File.createTempFile("spillFile", ".spill", tempDir);
              spillFilesCreated.add(file);
              return Tuple2$.MODULE$.apply(blockId, file);
            });
    when(blockManager.getDiskWriter(
            any(BlockId.class),
            any(File.class),
            any(SerializerInstance.class),
            anyInt(),
            any(ShuffleWriteMetrics.class)))
        .thenAnswer(
            invocationOnMock -> {
              Object[] args = invocationOnMock.getArguments();

              return new DiskBlockObjectWriter(
                  (File) args[1],
                  serializerManager,
                  (SerializerInstance) args[2],
                  (Integer) args[3],
                  false,
                  (ShuffleWriteMetrics) args[4],
                  (BlockId) args[0]);
            });
  }

  @After
  public void tearDown() {
    try {
      assertEquals(0L, taskMemoryManager.cleanUpAllAllocatedMemory());
    } finally {
      Utils.deleteRecursively(tempDir);
      tempDir = null;
    }
  }
}
