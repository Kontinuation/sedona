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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import org.apache.spark.SparkEnv;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.io.ReadAheadInputStream;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockId;

/**
 * A very simple sequential reader for spill files. It has the same read-ahead capability as the
 * spill reader for sorting.
 */
class SpillFileSequentialReader implements AutoCloseable {

  private InputStream in;
  private DataInputStream din;

  public SpillFileSequentialReader(File file, BlockId blockId, SerializerManager serializerManager)
      throws IOException {
    final ConfigEntry<Object> bufferSizeConfigEntry =
        package$.MODULE$.UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE();
    final int DEFAULT_BUFFER_SIZE_BYTES =
        ((Long) bufferSizeConfigEntry.defaultValue().get()).intValue();
    int bufferSizeBytes =
        SparkEnv.get() == null
            ? DEFAULT_BUFFER_SIZE_BYTES
            : ((Long) SparkEnv.get().conf().get(bufferSizeConfigEntry)).intValue();
    final InputStream bs = new NioBufferedFileInputStream(file, bufferSizeBytes);
    this.in = new ReadAheadInputStream(serializerManager.wrapStream(blockId, bs), bufferSizeBytes);
    this.din = new DataInputStream(this.in);
  }

  public void readFully(byte[] b, int off, int len) throws IOException {
    din.readFully(b, off, len);
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      try {
        in.close();
      } finally {
        in = null;
        din = null;
      }
    }
  }
}
