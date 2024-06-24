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

/** A factory class for instantiating thread local {@link OutDbResourcePool} objects. */
public class ThreadLocalOutDbResourcePool {
  private ThreadLocalOutDbResourcePool() {}

  public static final String FREE_RESOURCES_POOL_SIZE_CONF_KEY = "wherobots.raster.outdb.pool.size";
  public static final int DEFAULT_FREE_RESOURCES_POOL_SIZE = 100;

  private static int freeResourcesCapacity =
      Integer.parseInt(
          System.getProperty(
              FREE_RESOURCES_POOL_SIZE_CONF_KEY,
              Integer.toString(DEFAULT_FREE_RESOURCES_POOL_SIZE)));

  public static void setFreeResourcesCapacity(int freeResourcesCapacity) {
    ThreadLocalOutDbResourcePool.freeResourcesCapacity = freeResourcesCapacity;
  }

  private static final ThreadLocal<OutDbResourcePool> threadLocal =
      ThreadLocal.withInitial(() -> new OutDbResourcePool(freeResourcesCapacity));

  public static OutDbResourcePool get() {
    return threadLocal.get();
  }
}
