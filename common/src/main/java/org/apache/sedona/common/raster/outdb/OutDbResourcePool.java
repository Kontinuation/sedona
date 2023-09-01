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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.geotools.coverage.grid.GridCoverage2D;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A resource pool for reusing out-db raster resources. The byte ranges read by other OutDbGridCoverage2D objects
 * could be reused by newly created OutDbGridCoverage2D objects. What's more, this also avoids repeated opening
 * and closing of the same file, which is quite slow for GeoTiff files.
 * Please notice that this is a non-thread-safe class and should be used as a thread-local variable.
 */
public class OutDbResourcePool {
    final static Logger logger = LoggerFactory.getLogger(OutDbResourcePool.class);

    public static class OutDbResource {
        public final ResourceKey resourceKey;
        public final GridCoverage2D gridCoverage2D;
        public final ImageInputStream stream;

        // Tracks reference count to know if the resource is still in use.
        int refCount;

        // The following fields are for managing free resources in a doubly linked list.
        OutDbResource prev;
        OutDbResource next;

        OutDbResource(ResourceKey key, GridCoverage2D gridCoverage2D, ImageInputStream stream) {
            if (gridCoverage2D instanceof OutDbGridCoverage2D) {
                // We are pooling the inner GridCoverage2D objects possessed by OutDbGridCoverage2D, not
                // OutDbGridCoverage2D themselves.
                throw new IllegalArgumentException("gridCoverage2D must not be an instance of OutDbGridCoverage2D");
            }
            this.resourceKey = key;
            this.gridCoverage2D = gridCoverage2D;
            this.stream = stream;
            this.refCount = 1;
            this.prev = this;
            this.next = this;
        }

        OutDbResource() {
            this(null, null, null);
        }

        public void dispose() {
            if (gridCoverage2D != null) {
                try {
                    gridCoverage2D.dispose(true);
                } catch (Exception e) {
                    logger.error("Failed to dispose gridCoverage2D when disposing OutDbResource", e);
                }
            }
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception e) {
                    logger.error("Failed to close stream when disposing OutDbResource", e);
                }
            }
        }
    }

    public static class ResourceKey {
        public final Path path;
        public final byte[] serializedConf;
        private Configuration conf;
        public Map<String, String> params;

        public ResourceKey(Path path, Configuration conf) {
            this(path, conf, null);
        }

        public ResourceKey(Path path, Configuration conf, Map<String, String> params) {
            try {
                serializedConf = HadoopConfigSerializer.serialize(conf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (params != null && !params.isEmpty()) {
                conf = new Configuration(conf);
                params.forEach(conf::set);
            }
            this.path = path;
            this.conf = conf;
            this.params = params;
        }

        public ResourceKey(Path path, byte[] serializedConf, Map<String, String> params) {
            this.path = path;
            this.serializedConf = serializedConf;
            this.conf = null;
            this.params = params;
        }

        public Configuration getConfWithParams() {
            if (conf == null) {
                try {
                    conf = HadoopConfigSerializer.deserialize(serializedConf);
                    if (params != null && !params.isEmpty()) {
                        // The deserialized conf may be cached, so we need to create a new conf object
                        conf = new Configuration(conf);
                        params.forEach(conf::set);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return conf;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ResourceKey) {
                ResourceKey other = (ResourceKey) obj;
                return path.equals(other.path) &&
                        Arrays.equals(serializedConf, other.serializedConf) &&
                        Objects.equals(params, other.params);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(path.hashCode(), Arrays.hashCode(serializedConf), params);
        }
    }

    private static class WeakOutDbResource extends WeakReference<OutDbResource> {
        public final ResourceKey resourceKey;

        public WeakOutDbResource(OutDbResource referent, ReferenceQueue<? super OutDbResource> q) {
            super(referent, q);
            this.resourceKey = referent.resourceKey;
        }
    }

    private final long threadId;

    // We track all resources in a map to avoid creating duplicated GridCoverage2D object for the same file.
    // The key is the file path and the configuration used to open the file. The value is a weak reference
    // to the OutDbResource object. We use weak reference to avoid preventing the resource from being
    // garbage collected if users acquire the resource without releasing it. InferredExpression contains code
    // for automatic releasing of OutDbGridCoverage2D objects, so for most of the time the resources will be given
    // back to the pool in a timely manner.
    //
    // OutDbResource objects were reference counted and will be added to the freeResources list when the refCount
    // drops to 0. The freeResources list has a capacity and will evict the oldest free resource when the capacity
    // is reached, so that we'll keep only a limited number of free resources in memory.
    //
    // There are cases where the OutDbResource object is not released at all, for example, when calling
    // show() or collect() on a dataframe containing out-db rasters. We'll rely on the garbage collector to clean
    // up the resources in such cases. That's when the weak reference comes into play.
    private final Map<ResourceKey, WeakOutDbResource> allResources;
    private final ReferenceQueue<OutDbResource> referenceQueue;
    private final OutDbResource freeResources;
    private int freeResourceCount;
    private final int freeResourcesCapacity;

    public OutDbResourcePool(int freeResourcesCapacity) {
        if (freeResourcesCapacity <= 0) {
            throw new IllegalArgumentException("freeResourcesCapacity must be positive");
        }
        threadId = Thread.currentThread().getId();
        logger.debug("Creating OutDbResourcePool for thread {}, capacity: {}", threadId, freeResourcesCapacity);
        allResources = new HashMap<>();
        referenceQueue = new ReferenceQueue<>();
        freeResources = new OutDbResource();
        freeResourceCount = 0;
        this.freeResourcesCapacity = freeResourcesCapacity;
    }

    public OutDbResource acquire(ResourceKey key) {
        drainReferenceQueue();
        WeakOutDbResource ref = allResources.get(key);
        OutDbResource resource = (ref == null ? null : ref.get());
        if (resource != null) {
            tryRemoveFreeResource(resource);
            resource.refCount += 1;
            logger.debug("Acquired OutDbResource object(ref={}, path={}) for thread {}. Pool stats: {}/{}",
                    resource.refCount, key.path, threadId, freeResourceCount, allResources.size());
        } else {
            logger.debug("No OutDbResource object found for thread {}, path={}. Pool stats: {}/{}",
                    threadId, key.path, freeResourceCount, allResources.size());
        }
        return resource;
    }

    public void add(OutDbResource resource) {
        if (resource.refCount <= 0) {
            throw new IllegalStateException("refCount of OutDbResource is not positive");
        }
        ResourceKey key = resource.resourceKey;
        WeakOutDbResource ref = allResources.get(key);
        OutDbResource existing = (ref == null ? null : ref.get());
        if (existing == null) {
            // Add this resource to the pool, so that it can be shared by other out-db grid coverage objects
            if (resource.next != resource) {
                throw new IllegalStateException("resource is in the free resource list of another pool");
            }
            allResources.put(key, new WeakOutDbResource(resource, referenceQueue));
            logger.debug("Added new OutDbResource object for thread {}, path={}. Pool stats: {}/{}",
                    threadId, resource.resourceKey.path, freeResourceCount, allResources.size());
        } else {
            logger.debug("Ignored adding duplicated OutDbResource object(ref={}, path={}) for thread {}. Pool stats: {}/{}",
                    resource.refCount, resource.resourceKey.path, threadId, freeResourceCount, allResources.size());
        }
    }

    public void release(OutDbResource resource) {
        if (resource.refCount > 0) {
            resource.refCount -= 1;
        }

        drainReferenceQueue();
        ResourceKey key = resource.resourceKey;
        WeakOutDbResource ref = allResources.get(key);
        OutDbResource existing = (ref == null ? null : ref.get());
        if (existing == null) {
            // Add newly allocated resource to the pool.
            if (resource.refCount != 0) {
                throw new IllegalStateException("refCount of newly created resource is not 0");
            }
            if (resource.next != resource) {
                throw new IllegalStateException("resource is in the free resource list of another pool");
            }
            allResources.put(key, new WeakOutDbResource(resource, referenceQueue));
            addFreeResource(resource);
            logger.debug("Releasing new OutDbResource object for thread {}, path={}. Pool stats: {}/{}",
                    threadId, resource.resourceKey.path, freeResourceCount, allResources.size());
            return;
        }

        if (existing != resource) {
            resource.dispose();
            logger.debug("Ignored releasing duplicated OutDbResource object(ref={}, path={}) for thread {}. Pool stats: {}/{}",
                    resource.refCount, resource.resourceKey.path, threadId, freeResourceCount, allResources.size());
            return;
        }

        if (resource.refCount == 0) {
            addFreeResource(resource);
        }
        logger.debug("Released OutDbResource object(ref={}, path={}) for thread {}. Pool stats: {}/{}",
                resource.refCount, resource.resourceKey.path, threadId, freeResourceCount, allResources.size());
    }

    public int getResourceCount() {
        drainReferenceQueue();
        return allResources.size();
    }

    public int getFreeResourceCount() {
        return freeResourceCount;
    }

    public void verifyIntegrity() {
        drainReferenceQueue();
        if (freeResourceCount > freeResourcesCapacity) {
            throw new IllegalStateException("freeResourceCount is greater than freeResourcesCapacity");
        }
        int count = 0;
        for (WeakOutDbResource ref : allResources.values()) {
            OutDbResource resource = ref.get();
            if (resource == null) {
                continue;
            }
            if (resource.refCount == 0) {
                count += 1;
            } else if (resource.refCount < 0) {
                throw new IllegalStateException("refCount of OutDbResource object is negative");
            }
        }
        if (count != freeResourceCount) {
            throw new IllegalStateException("freeResourceCount does not match with the actual number of free resources");
        }
        count = 0;
        OutDbResource cur = freeResources.next;
        while (cur != freeResources) {
            count += 1;
            if (cur.next.prev != cur || cur.prev.next != cur) {
                throw new IllegalStateException("freeResources list is broken");
            }
            cur = cur.next;
        }
        if (count != freeResourceCount) {
            throw new IllegalStateException("freeResourceCount does not match with the length of freeResources list");
        }
    }

    public void cleanUp() {
        while (freeResources.prev != freeResources) {
            OutDbResource resource = freeResources.prev;
            tryRemoveFreeResource(resource);
            allResources.remove(resource.resourceKey);
            resource.dispose();
        }
    }

    @SuppressWarnings("deprecation")
    protected void finalize() {
        logger.debug("Finalizing OutDbResourcePool for thread {}: containing {}/{} objects", threadId, freeResourceCount, allResources.size());
    }

    private void drainReferenceQueue() {
        WeakOutDbResource ref;
        while ((ref = (WeakOutDbResource) referenceQueue.poll()) != null) {
            allResources.remove(ref.resourceKey);
        }
    }

    private void addFreeResource(OutDbResource resource) {
        resource.prev = freeResources;
        resource.next = freeResources.next;
        freeResources.next.prev = resource;
        freeResources.next = resource;
        freeResourceCount += 1;
        evictOldFreeResources();
    }

    private void evictOldFreeResources() {
        while (freeResourceCount > freeResourcesCapacity) {
            // Evict the oldest free resource.
            OutDbResource evicted = freeResources.prev;
            tryRemoveFreeResource(evicted);
            allResources.remove(evicted.resourceKey);
            logger.debug("Evicted OutDbResource object for thread {}, path={}. Pool stats: {}/{}",
                    threadId, evicted.resourceKey.path, freeResourceCount, allResources.size());
            evicted.dispose();
        }
    }

    private void tryRemoveFreeResource(OutDbResource resource) {
        if (resource.next != resource) {
            resource.prev.next = resource.next;
            resource.next.prev = resource.prev;
            resource.next = resource;
            resource.prev = resource;
            freeResourceCount -= 1;
        }
    }
}
