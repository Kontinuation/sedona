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
package org.apache.sedona.common.raster.inputstream.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BufferedRandomAccessFileTest {
  @Rule public TemporaryFolder temp = new TemporaryFolder();
  private final Random random = new Random();

  @Test
  public void testSmallFile() throws IOException {
    try (BufferedRandomAccessFile braf = new BufferedRandomAccessFile(temp.newFile(), "rw")) {
      byte[] randomBytes = new byte[100];
      random.nextBytes(randomBytes);
      braf.write(randomBytes, 0, randomBytes.length);
      braf.seek(0);

      byte[] buf = new byte[200];
      int bytesRead = braf.read(buf, 0, 0);

      Assert.assertEquals(0, bytesRead);
      Assert.assertThrows(IllegalArgumentException.class, () -> braf.read(buf, 0, -1));

      bytesRead = braf.read(buf, 0, buf.length);
      Assert.assertEquals(randomBytes.length, bytesRead);
      for (int i = 0; i < randomBytes.length; i++) {
        Assert.assertEquals(randomBytes[i], buf[i]);
      }

      bytesRead = braf.read(buf, 0, buf.length);
      Assert.assertEquals(-1, bytesRead);
    }
  }

  @Test
  public void testSequentialRead() throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(temp.newFile(), "rw");
        BufferedRandomAccessFile braf = new BufferedRandomAccessFile(temp.newFile(), "rw")) {
      int testFileSize = 200000;
      byte[] randomBytes = new byte[testFileSize];
      random.nextBytes(randomBytes);
      raf.write(randomBytes, 0, randomBytes.length);
      braf.write(randomBytes, 0, randomBytes.length);
      int[] bufferSizes = {10, 1000, 10000};
      for (int bufferSize : bufferSizes) {
        raf.seek(0);
        braf.seek(0);
        while (true) {
          byte[] buf = new byte[bufferSize];
          int bytesRead = raf.read(buf, 0, buf.length);
          int bytesRead2 = braf.read(buf, 0, buf.length);
          Assert.assertEquals(bytesRead, bytesRead2);
          if (bytesRead < 0) {
            break;
          }
        }
      }
    }
  }

  @Test
  public void testRandomReadWrite() throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(temp.newFile(), "rw");
        BufferedRandomAccessFile braf = new BufferedRandomAccessFile(temp.newFile(), "rw")) {
      int testFileSize = 200000;
      byte[] randomBytes = new byte[testFileSize];
      random.nextBytes(randomBytes);
      raf.write(randomBytes, 0, randomBytes.length);
      braf.write(randomBytes, 0, randomBytes.length);
      for (int k = 0; k < 1000; k++) {
        randomReadWrite(raf, braf, testFileSize * 2);
      }
    }
  }

  private void randomReadWrite(
      RandomAccessFile raf, BufferedRandomAccessFile braf, int maxSeekOffset) throws IOException {
    int offset = random.nextInt(maxSeekOffset);
    raf.seek(offset);
    braf.seek(offset);
    int repeats = random.nextInt(10);
    for (int k = 0; k < repeats; k++) {
      int len = random.nextInt(1000);
      byte[] buf = new byte[len];
      int readLen = raf.read(buf, 0, len);
      int readLen2 = braf.read(buf, 0, len);
      Assert.assertEquals(readLen, readLen2);
      if (readLen < 0) {
        break;
      }
    }
    offset = random.nextInt(maxSeekOffset);
    raf.seek(offset);
    braf.seek(offset);
    repeats = random.nextInt(10);
    for (int k = 0; k < repeats; k++) {
      int len = random.nextInt(1000);
      byte[] buf = new byte[len];
      random.nextBytes(buf);
      raf.write(buf, 0, len);
      braf.write(buf, 0, len);
    }
  }
}
