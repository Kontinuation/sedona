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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * Random access file reader with page cache. This class is not thread-safe, and care should be
 * taken when dealing with the ByteBuffer returned by the read method.
 */
public class BlockCachedFileReader implements AutoCloseable {
  public static final int BUFFER_BLOCK_SIZE_BYTES = 8192;

  private final Cache<Long, ByteBuffer> cache;

  private final FileChannel fileChannel;
  private final long fileSize;

  public BlockCachedFileReader(File file, long maxCachedBytes) throws IOException {
    this.fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    this.fileSize = fileChannel.size();
    int maxCachedBlocks = (int) Math.max(1, (maxCachedBytes / BUFFER_BLOCK_SIZE_BYTES));
    this.cache =
        Caffeine.newBuilder()
            .softValues()
            .maximumSize(maxCachedBlocks)
            .executor(Runnable::run)
            .build();
  }

  /**
   * Read a block of data from the file.
   *
   * <p>CAUTION: The returned ByteBuffer may be cached by the reader, so the caller should not
   * modify the buffer. Also, the returned ByteBuffer is only valid before the next read operation.
   *
   * @param offset The offset of the first byte to read.
   * @param length The number of bytes to read.
   * @return A ByteBuffer containing the read data.
   * @throws IOException If an I/O error occurs.
   */
  public ByteBuffer read(long offset, int length) throws IOException {
    if (offset + length > fileSize) {
      throw new EOFException(
          "Read beyond EOF: requested " + (offset + length) + ", file size " + fileSize);
    }
    long fromBlockId = offset / BUFFER_BLOCK_SIZE_BYTES;
    long toBlockId = (offset + length - 1) / BUFFER_BLOCK_SIZE_BYTES;

    ByteBuffer firstBlock = cache.getIfPresent(fromBlockId);
    if (fromBlockId == toBlockId && firstBlock != null) {
      // Fast path: if the requested single block is in the cache, we can directly return the
      // cached buffer.
      firstBlock.clear();
      int blockOffset = (int) (offset % BUFFER_BLOCK_SIZE_BYTES);
      firstBlock.position(blockOffset);
      firstBlock.limit(blockOffset + length);
      return firstBlock;
    }

    // First, try to find the blocks in the cache
    ByteBuffer[] buffers = new ByteBuffer[(int) (toBlockId - fromBlockId + 1)];
    int remaining = length;
    for (long blockId = fromBlockId; blockId <= toBlockId; blockId++) {
      int offsetInBlock = blockId == fromBlockId ? (int) (offset % BUFFER_BLOCK_SIZE_BYTES) : 0;
      int lengthInBlock = Math.min(BUFFER_BLOCK_SIZE_BYTES - offsetInBlock, remaining);
      remaining -= lengthInBlock;

      // Fetch the block from the cache. The first block is already fetched before, so we don't need
      // to fetch it again.
      ByteBuffer cached = (blockId == fromBlockId ? firstBlock : cache.getIfPresent(blockId));
      if (cached != null) {
        // Use the cached buffer directly
        cached.clear();
        cached.position(offsetInBlock);
        cached.limit(offsetInBlock + lengthInBlock);
        buffers[(int) (blockId - fromBlockId)] = cached;
      }
    }

    // For other blocks, read from the file, and load them to the cache
    remaining = length;
    for (int i = 0; i < buffers.length; i++) {
      long blockId = fromBlockId + i;
      int offsetInBlock = blockId == fromBlockId ? (int) (offset % BUFFER_BLOCK_SIZE_BYTES) : 0;
      int lengthInBlock = Math.min(BUFFER_BLOCK_SIZE_BYTES - offsetInBlock, remaining);
      remaining -= lengthInBlock;

      if (buffers[i] == null) {
        int blockSize =
            (int) Math.min(BUFFER_BLOCK_SIZE_BYTES, fileSize - blockId * BUFFER_BLOCK_SIZE_BYTES);
        ByteBuffer buffer = ByteBuffer.allocate(blockSize);
        readFully(buffer, blockId * BUFFER_BLOCK_SIZE_BYTES, blockSize);

        buffer.flip(); // Prepare for reading
        buffer.position(offsetInBlock);
        buffer.limit(offsetInBlock + lengthInBlock);
        buffers[i] = buffer;
        cache.put(blockId, buffer);
      }
    }

    // Concatenate the buffers
    if (buffers.length == 1) {
      return buffers[0];
    } else {
      ByteBuffer result = ByteBuffer.allocate(length);
      for (ByteBuffer buffer : buffers) {
        result.put(buffer);
      }
      result.flip();
      return result;
    }
  }

  private void readFully(ByteBuffer buffer, long offset, int length) throws IOException {
    int bytesRead = 0;
    while (bytesRead < length) {
      int bytes = fileChannel.read(buffer, offset + bytesRead);
      if (bytes == -1) {
        throw new EOFException("Unexpected EOF while reading " + length + " bytes");
      }
      bytesRead += bytes;
    }
  }

  @Override
  public void close() throws IOException {
    cache.invalidateAll();
    fileChannel.close();
  }
}
