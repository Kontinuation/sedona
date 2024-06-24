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
package org.apache.sedona.common.raster.inputstream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Random;
import javax.imageio.stream.ImageInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DiskCachedImageInputStreamTest {
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters(name = "disable cache for local file: {0}")
  public static Object[] testConfig() {
    return new Object[] {new Object[] {true}, new Object[] {false}};
  }

  private static final int TEST_FILE_SIZE = 1000;
  private final Configuration conf = new Configuration();
  private final Random random = new Random();
  private File testFile;

  public DiskCachedImageInputStreamTest(boolean disableCacheForLocalFile) {
    conf.set(HadoopImageInputStreamFactory.READ_AHEAD_SIZE_CONF_KEY, "10B");
    conf.setBoolean(
        HadoopImageInputStreamFactory.DONT_CACHE_LOCAL_FILE_CONF_KEY, disableCacheForLocalFile);
  }

  @Before
  public void setup() throws IOException {
    testFile = temp.newFile();
    prepareTestData(testFile);
  }

  @Test
  public void testReadSequentially() throws IOException {
    Path path = new Path(testFile.getPath());
    try (ImageInputStream stream = HadoopImageInputStreamFactory.create(path, conf);
        InputStream in = new BufferedInputStream(Files.newInputStream(testFile.toPath()))) {
      byte[] bActual = new byte[8];
      byte[] bExpected = new byte[bActual.length];
      while (true) {
        int len = random.nextInt(bActual.length + 1);
        int lenActual = stream.read(bActual, 0, len);
        int lenExpected = in.read(bExpected, 0, len);
        Assert.assertEquals(lenExpected, lenActual);
        if (lenActual < 0) {
          break;
        }
        Assert.assertArrayEquals(bExpected, bActual);
      }
    }
  }

  @Test
  public void testReadRandomly() throws IOException {
    Path path = new Path(testFile.getPath());
    try (ImageInputStream stream = HadoopImageInputStreamFactory.create(path, conf);
        RandomAccessFile raf = new RandomAccessFile(testFile, "r")) {
      byte[] bActual = new byte[8];
      byte[] bExpected = new byte[bActual.length];
      for (int k = 0; k < 1000; k++) {
        int offset = random.nextInt(TEST_FILE_SIZE + 1);
        int len = random.nextInt(bActual.length + 1);
        stream.seek(offset);
        raf.seek(offset);
        int lenActual = stream.read(bActual, 0, len);
        int lenExpected = raf.read(bExpected, 0, len);
        Assert.assertEquals(lenExpected, lenActual);
        if (lenActual < 0) {
          continue;
        }
        Assert.assertArrayEquals(bExpected, bActual);
      }

      // Test seek to EOF.
      stream.seek(TEST_FILE_SIZE);
      int len = stream.read(bActual, 0, bActual.length);
      Assert.assertEquals(-1, len);
    }
  }

  private void prepareTestData(File testFile) throws IOException {
    try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(testFile.toPath()))) {
      for (int k = 0; k < TEST_FILE_SIZE; k++) {
        out.write(random.nextInt());
      }
    }
  }
}
