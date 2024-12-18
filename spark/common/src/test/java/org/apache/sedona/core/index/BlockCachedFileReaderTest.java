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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.spark.sedona.core.index.BlockCachedFileReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlockCachedFileReaderTest {
  private static File testFile;
  private BlockCachedFileReader reader;
  private static final int BLOCK_SIZE = BlockCachedFileReader.BUFFER_BLOCK_SIZE_BYTES;

  @BeforeClass
  public static void prepareTestFile() throws IOException {
    // Create a test file with 10 blocks of data (81920 bytes)
    testFile = File.createTempFile("block_cached", "test");
    try (FileOutputStream fos = new FileOutputStream(testFile)) {
      byte[] data = new byte[BLOCK_SIZE * 10];
      // Fill with incrementing values for easy verification
      for (int i = 0; i < data.length; i++) {
        data[i] = (byte) (i & 0xFF);
      }
      fos.write(data);
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    testFile.delete();
  }

  @Before
  public void setUp() throws IOException {
    reader = new BlockCachedFileReader(testFile, BLOCK_SIZE * 5); // Cache up to 5 blocks
  }

  @After
  public void tearDown() throws IOException {
    reader.close();
  }

  @Test
  public void testReadZeroBytes() throws IOException {
    ByteBuffer result = reader.read(0, 0);
    assertEquals(0, result.remaining());
  }

  @Test
  public void testReadWithinSingleBlock() throws IOException {
    // Read 100 bytes from offset 50
    ByteBuffer result = reader.read(50, 100);
    assertEquals(100, result.remaining());
    for (int i = 0; i < 100; i++) {
      assertEquals((byte) ((i + 50) & 0xFF), result.get());
    }

    // Read again, should be cached
    result = reader.read(50, 100);
    assertEquals(100, result.remaining());
    for (int i = 0; i < 100; i++) {
      assertEquals((byte) ((i + 50) & 0xFF), result.get());
    }

    // Read another block, should not be cached
    result = reader.read(BLOCK_SIZE, BLOCK_SIZE);
    assertEquals(BLOCK_SIZE, result.remaining());
    for (int i = 0; i < BLOCK_SIZE; i++) {
      assertEquals((byte) ((i + BLOCK_SIZE) & 0xFF), result.get());
    }
  }

  @Test
  public void testReadCrossingBlockBoundary() throws IOException {
    // Read 200 bytes across first and second block boundary
    int offset = BLOCK_SIZE - 100;
    ByteBuffer result = reader.read(offset, 200);
    assertEquals(200, result.remaining());
    for (int i = 0; i < 200; i++) {
      assertEquals((byte) ((i + offset) & 0xFF), result.get());
    }

    // Read again, should be cached
    result = reader.read(offset, 200);
    assertEquals(200, result.remaining());
    for (int i = 0; i < 200; i++) {
      assertEquals((byte) ((i + offset) & 0xFF), result.get());
    }
  }

  @Test
  public void testReadMultipleBlocks() throws IOException {
    // Read entire second block plus portions of first and third blocks
    int offset = BLOCK_SIZE - 100;
    int length = BLOCK_SIZE + 200;
    ByteBuffer result = reader.read(offset, length);
    assertEquals(length, result.remaining());
    for (int i = 0; i < length; i++) {
      assertEquals((byte) ((i + offset) & 0xFF), result.get());
    }

    // Read again, should be cached
    result = reader.read(offset, length);
    assertEquals(length, result.remaining());
    for (int i = 0; i < length; i++) {
      assertEquals((byte) ((i + offset) & 0xFF), result.get());
    }

    // Read some other blocks, some of them should be cached
    result = reader.read(BLOCK_SIZE * 2, BLOCK_SIZE * 3);
    assertEquals(BLOCK_SIZE * 3, result.remaining());
    for (int i = 0; i < BLOCK_SIZE * 3; i++) {
      assertEquals((byte) ((i + BLOCK_SIZE * 2) & 0xFF), result.get());
    }
  }

  @Test
  public void testReadExactBlockBoundaries() throws IOException {
    // Read exactly one block
    ByteBuffer result = reader.read(BLOCK_SIZE, BLOCK_SIZE);
    assertEquals(BLOCK_SIZE, result.remaining());
    for (int i = 0; i < BLOCK_SIZE; i++) {
      assertEquals((byte) ((i + BLOCK_SIZE) & 0xFF), result.get());
    }

    // Read again, should be cached
    result = reader.read(BLOCK_SIZE, BLOCK_SIZE);
    assertEquals(BLOCK_SIZE, result.remaining());
    for (int i = 0; i < BLOCK_SIZE; i++) {
      assertEquals((byte) ((i + BLOCK_SIZE) & 0xFF), result.get());
    }
  }

  @Test
  public void testReadExactMultipleBlocks() throws IOException {
    // Read exactly multiple blocks
    ByteBuffer result = reader.read(BLOCK_SIZE * 2, BLOCK_SIZE * 3);
    assertEquals(BLOCK_SIZE * 3, result.remaining());
    for (int i = 0; i < BLOCK_SIZE * 3; i++) {
      assertEquals((byte) ((i + BLOCK_SIZE * 2) & 0xFF), result.get());
    }

    // Read again, should be cached
    result = reader.read(BLOCK_SIZE * 2, BLOCK_SIZE * 3);
    assertEquals(BLOCK_SIZE * 3, result.remaining());
    for (int i = 0; i < BLOCK_SIZE * 3; i++) {
      assertEquals((byte) ((i + BLOCK_SIZE * 2) & 0xFF), result.get());
    }
  }

  @Test
  public void testReadLargerThanCacheSize() throws IOException {
    // Read larger than cache size
    ByteBuffer result = reader.read(0, BLOCK_SIZE * 6);
    assertEquals(BLOCK_SIZE * 6, result.remaining());
    for (int i = 0; i < BLOCK_SIZE * 6; i++) {
      assertEquals((byte) ((i) & 0xFF), result.get());
    }

    // Read again, should be partially cached
    result = reader.read(0, BLOCK_SIZE * 6);
    assertEquals(BLOCK_SIZE * 6, result.remaining());
    for (int i = 0; i < BLOCK_SIZE * 6; i++) {
      assertEquals((byte) ((i) & 0xFF), result.get());
    }
  }

  @Test(expected = IOException.class)
  public void testReadAtFileEnd() throws IOException {
    ByteBuffer buffer = reader.read(BLOCK_SIZE * 10, 100);
    assertEquals(0, buffer.remaining());
  }

  @Test(expected = IOException.class)
  public void testReadBeyondFileEnd() throws IOException {
    reader.read(BLOCK_SIZE * 10 + 10, 100);
  }

  @Test(expected = IOException.class)
  public void testReadBeyondFileEnd2() throws IOException {
    reader.read(BLOCK_SIZE * 11, 100);
  }
}
