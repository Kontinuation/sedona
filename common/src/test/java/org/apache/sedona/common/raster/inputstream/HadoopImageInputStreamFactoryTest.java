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

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class HadoopImageInputStreamFactoryTest {
  @Test
  public void cachePartitionFreeSpace() {
    Configuration conf = new Configuration();
    conf.set(HadoopImageInputStreamFactory.CACHE_DIR_CONF_KEY, "/tmp");
    conf.set(HadoopImageInputStreamFactory.CACHE_MAX_DISK_SPACE_PERCENT_CONF_KEY, "10");

    long freeSpace = HadoopImageInputStreamFactory.cachePartitionFreeSpace(conf);
    Assert.assertTrue(freeSpace > 0);

    int percent = HadoopImageInputStreamFactory.getCacheMaxDiskSpacePercent(conf);
    Assert.assertEquals(10, percent);
  }

  @Test
  public void testDefaultConfig() {
    Configuration conf = new Configuration();

    long freeSpace = HadoopImageInputStreamFactory.cachePartitionFreeSpace(conf);
    Assert.assertEquals(0, freeSpace);

    int percent = HadoopImageInputStreamFactory.getCacheMaxDiskSpacePercent(conf);
    Assert.assertEquals(0, percent);
  }

  @Test
  public void testDefaultConfigWithCacheDir() {
    Configuration conf = new Configuration();
    conf.set(HadoopImageInputStreamFactory.CACHE_DIR_CONF_KEY, "/tmp");

    long freeSpace = HadoopImageInputStreamFactory.cachePartitionFreeSpace(conf);
    Assert.assertTrue(freeSpace > 0);

    int percent = HadoopImageInputStreamFactory.getCacheMaxDiskSpacePercent(conf);
    Assert.assertEquals(
        HadoopImageInputStreamFactory.DEFAULT_CACHE_MAX_DISK_SPACE_PERCENT, percent);
  }
}
