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
package org.apache.sedona.common.raster.outdb;

import java.io.IOException;
import javax.imageio.stream.ImageInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sedona.common.raster.RasterTestBase;
import org.apache.sedona.common.raster.inputstream.HadoopImageInputStreamFactory;
import org.apache.sedona.common.raster.outdb.OutDbResourcePool.OutDbResource;
import org.apache.sedona.common.raster.outdb.OutDbResourcePool.ResourceKey;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.junit.Assert;
import org.junit.Test;

public class OutDbResourcePoolTest extends RasterTestBase {
  @Test
  public void testAcquireAndRelease() {
    OutDbResourcePool pool = new OutDbResourcePool(3);
    pool.verifyIntegrity();
    OutDbResourcePool.OutDbResource resource = pool.acquire(resourceKey("/path/1", "val1"));
    Assert.assertNull(resource);

    // Add resource to the resource pool
    pool.release(newResource("/path/1", "val1"));
    verifyPool(pool, 1, 1);

    // Acquire resource from the resource pool
    resource = pool.acquire(resourceKey("/path/1", "val1"));
    Assert.assertNotNull(resource);
    Assert.assertEquals(1, resource.refCount);
    verifyPool(pool, 1, 0);

    // Add the same resource to the resource pool won't replace the existing one
    pool.release(newResource("/path/1", "val1"));
    verifyPool(pool, 1, 0);

    // Acquire resource again from the resource pool, will obtain the same resource and increase the
    // ref count
    OutDbResourcePool.OutDbResource resource2 = pool.acquire(resourceKey("/path/1", "val1"));
    Assert.assertEquals(resource, resource2);
    Assert.assertEquals(2, resource.refCount);
    verifyPool(pool, 1, 0);

    // Release the resource. The resource is being acquired as resource2, so it is not free
    // resource.
    pool.release(resource);
    verifyPool(pool, 1, 0);

    // Release the resource again. Now the resource is free
    pool.release(resource2);
    verifyPool(pool, 1, 1);

    // Clean up the pool
    pool.cleanUp();
    verifyPool(pool, 0, 0);
  }

  @Test
  public void testAddingAndReleasingResource() {
    OutDbResourcePool pool = new OutDbResourcePool(3);
    pool.verifyIntegrity();
    OutDbResourcePool.OutDbResource resource = pool.acquire(resourceKey("/path/1", "val1"));
    Assert.assertNull(resource);

    OutDbResourcePool.OutDbResource res1 = newResource("/path/1", "val1");
    pool.add(res1);
    verifyPool(pool, 1, 0);

    pool.release(res1);
    verifyPool(pool, 1, 1);

    // Clean up the pool
    pool.cleanUp();
    verifyPool(pool, 0, 0);
  }

  @Test
  public void testAddingAndReusingNonFreeResource() {
    OutDbResourcePool pool = new OutDbResourcePool(3);
    pool.verifyIntegrity();
    OutDbResourcePool.OutDbResource resource = pool.acquire(resourceKey("/path/1", "val1"));
    Assert.assertNull(resource);

    OutDbResourcePool.OutDbResource res1 = newResource("/path/1", "val1");
    pool.add(res1);
    verifyPool(pool, 1, 0);

    OutDbResourcePool.OutDbResource res2 = pool.acquire(resourceKey("/path/1", "val1"));
    Assert.assertEquals(res1, res2);
    Assert.assertEquals(2, res1.refCount);

    pool.release(res1);
    verifyPool(pool, 1, 0);
    Assert.assertEquals(1, res2.refCount);

    pool.release(res2);
    verifyPool(pool, 1, 1);

    // Clean up the pool
    pool.cleanUp();
    verifyPool(pool, 0, 0);
  }

  @Test
  public void testWeakReference() throws InterruptedException {
    OutDbResourcePool pool = new OutDbResourcePool(3);
    pool.release(newResource("/path/1", "val1"));
    OutDbResourcePool.OutDbResource resource = pool.acquire(resourceKey("/path/1", "val1"));
    Assert.assertNotNull(resource);
    verifyPool(pool, 1, 0);

    // Don't release the resource. Drop the reference to the resource and run GC.
    resource = null;
    for (int k = 0; k < 30; k++) {
      System.gc();
      Thread.sleep(1000);
      if (pool.getResourceCount() == 0) {
        break;
      }
    }

    // The resource is released automatically
    verifyPool(pool, 0, 0);
  }

  @Test
  public void testEvictResource() {
    OutDbResourcePool pool = new OutDbResourcePool(2);
    OutDbResourcePool.OutDbResource resource1 = newResource("/path/1", "val1");
    OutDbResourcePool.OutDbResource resource2 = newResource("/path/2", "val2");
    OutDbResourcePool.OutDbResource resource3 = newResource("/path/3", "val3");
    pool.release(resource1);
    pool.release(resource2);
    verifyPool(pool, 2, 2);

    // Add resource3, resource1 will be evicted
    pool.release(resource3);
    verifyPool(pool, 2, 2);
    Assert.assertNull(pool.acquire(resourceKey("/path/1", "val1")));
    OutDbResourcePool.OutDbResource res2 = pool.acquire(resourceKey("/path/2", "val2"));
    Assert.assertEquals(resource2, res2);
    OutDbResourcePool.OutDbResource res3 = pool.acquire(resourceKey("/path/3", "val3"));
    Assert.assertEquals(resource3, res3);
    verifyPool(pool, 2, 0);

    // Add resource4 and resource5, no resources will be evicted
    OutDbResourcePool.OutDbResource resource4 = newResource("/path/4", "val4");
    OutDbResourcePool.OutDbResource resource5 = newResource("/path/5", "val5");
    pool.release(resource4);
    pool.release(resource5);
    verifyPool(pool, 4, 2);

    // Release resource2 and resource3, resource4 and resource5 will be evicted
    pool.release(res2);
    Assert.assertNull(pool.acquire(resourceKey("/path/4", "val4")));
    verifyPool(pool, 3, 2);
    pool.release(res3);
    Assert.assertNull(pool.acquire(resourceKey("/path/5", "val5")));
    verifyPool(pool, 2, 2);
  }

  @Test
  public void testResourcePoolWithZeroCapacity() {
    OutDbResourcePool pool = new OutDbResourcePool(0);
    OutDbResourcePool.OutDbResource resource = pool.acquire(resourceKey("/path/1", "val1"));
    Assert.assertNull(resource);

    OutDbResourcePool.OutDbResource res1 = newResource("/path/1", "val1");
    pool.add(res1);
    verifyPool(pool, 1, 0);

    // Can share resource being used
    OutDbResourcePool.OutDbResource res2 = pool.acquire(resourceKey("/path/1", "val1"));
    Assert.assertEquals(res1, res2);
    Assert.assertEquals(2, res1.refCount);

    pool.release(res1);
    verifyPool(pool, 1, 0);
    Assert.assertEquals(1, res2.refCount);

    // Don't put resource back to free list since capacity is 0
    pool.release(res2);
    verifyPool(pool, 0, 0);
  }

  @Test
  public void testDiskCacheCapacity() throws IOException {
    OutDbResourcePool pool = new OutDbResourcePool(10);
    Configuration conf = new Configuration();
    conf.set(HadoopImageInputStreamFactory.DONT_CACHE_LOCAL_FILE_CONF_KEY, "false");
    conf.set(HadoopImageInputStreamFactory.CACHE_DIR_CONF_KEY, "/tmp");
    conf.set(HadoopImageInputStreamFactory.CACHE_MAX_DISK_SPACE_PERCENT_CONF_KEY, "10");

    // We are using test4.tiff, test5.tiff and test6.tiff in spark/common/src/test/resources/raster.
    // Each file is 467 bytes, so the capacity of 1000 is capable of holding 2 rasters, but not
    // enough for 3 rasters.
    pool.setFreeResourcesDiskSpaceLimit(10, 1000);

    // Load 3 rasters
    OutDbResource resource1 = newResource(resourceFolder + "/raster/test4.tiff", conf);
    resource1.gridCoverage2D.getRenderedImage().getData();
    OutDbResource resource2 = newResource(resourceFolder + "/raster/test5.tiff", conf);
    resource2.gridCoverage2D.getRenderedImage().getData();
    OutDbResource resource3 = newResource(resourceFolder + "/raster/test6.tiff", conf);
    resource3.gridCoverage2D.getRenderedImage().getData();

    // Add them into the pool as free resources
    pool.release(resource1);
    Assert.assertEquals(1, pool.getFreeResourceCount());
    pool.release(resource2);
    Assert.assertEquals(2, pool.getFreeResourceCount());
    pool.release(resource3);
    Assert.assertEquals(2, pool.getFreeResourceCount());

    verifyPool(pool, 2, 2);

    // test5.tiff and test6.tiff are still present in the pool, while test4.tiff is evicted
    OutDbResource resource =
        pool.acquire(new ResourceKey(new Path(resourceFolder + "/raster/test5.tiff"), conf));
    Assert.assertNotNull(resource);
    pool.release(resource);
    resource = pool.acquire(new ResourceKey(new Path(resourceFolder + "/raster/test6.tiff"), conf));
    Assert.assertNotNull(resource);
    pool.release(resource);
    resource = pool.acquire(new ResourceKey(new Path(resourceFolder + "/raster/test4.tiff"), conf));
    Assert.assertNull(resource);

    pool.cleanUp();
  }

  private OutDbResourcePool.ResourceKey resourceKey(String path, String value) {
    Configuration conf = new Configuration(false);
    conf.set("test_key", value);
    return new OutDbResourcePool.ResourceKey(new Path(path), conf);
  }

  private OutDbResourcePool.OutDbResource newResource(String path, String value) {
    return new OutDbResourcePool.OutDbResource(resourceKey(path, value), null, null);
  }

  private OutDbResourcePool.OutDbResource newResource(String path, Configuration conf)
      throws IOException {
    ImageInputStream stream = HadoopImageInputStreamFactory.create(new Path(path), conf);
    GeoTiffReader reader = new GeoTiffReader(stream);
    GridCoverage2D gridCoverage2D = reader.read(null);
    ResourceKey resourceKey = new ResourceKey(new Path(path), conf);
    return new OutDbResourcePool.OutDbResource(resourceKey, gridCoverage2D, stream);
  }

  private void verifyPool(OutDbResourcePool pool, int resourceCount, int freeResourceCount) {
    Assert.assertEquals(resourceCount, pool.getResourceCount());
    Assert.assertEquals(freeResourceCount, pool.getFreeResourceCount());
    pool.verifyIntegrity();
  }
}
