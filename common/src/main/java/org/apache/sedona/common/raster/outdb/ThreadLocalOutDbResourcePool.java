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
import java.util.Locale;
import javax.imageio.stream.ImageInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sedona.common.raster.inputstream.HadoopImageInputStreamFactory;
import org.apache.sedona.common.raster.workarounds.RuntimePatches;
import org.geotools.api.data.DataSourceException;
import org.geotools.api.parameter.GeneralParameterValue;
import org.geotools.api.parameter.ParameterValue;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.gce.arcgrid.ArcGridFormat;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.util.factory.Hints;

/** A factory class for instantiating thread local {@link OutDbResourcePool} objects. */
public class ThreadLocalOutDbResourcePool {
  private ThreadLocalOutDbResourcePool() {}

  public static final String FREE_RESOURCES_POOL_SIZE_CONF_KEY = "wherobots.raster.outdb.pool.size";
  public static final int DEFAULT_FREE_RESOURCES_POOL_SIZE = 100;

  /**
   * Automatically rescale pixel values to the range of the data type. This is useful when the
   * GeoTiff has scale and offset values in the metadata. Default is true.
   */
  public static final String READER_AUTO_RESCALE_CONF_KEY = "raster.reader.auto-rescale";

  private static final int freeResourcesCapacity =
      Integer.parseInt(
          System.getProperty(
              FREE_RESOURCES_POOL_SIZE_CONF_KEY,
              Integer.toString(DEFAULT_FREE_RESOURCES_POOL_SIZE)));

  private static final ThreadLocal<OutDbResourcePool> threadLocal =
      ThreadLocal.withInitial(() -> new OutDbResourcePool(freeResourcesCapacity));

  private static OutDbResourcePool get() {
    return threadLocal.get();
  }

  /**
   * Get an out-db resource from the pool of current thread, or create a new one if it does not
   * exist.
   *
   * @param key the key to use to get the resource
   * @return the out-db resource
   * @throws IOException if the resource cannot be created
   */
  public static OutDbResourcePool.OutDbResource getOrCreateOutDbResource(
      OutDbResourcePool.ResourceKey key) throws IOException {
    OutDbResourcePool pool = get();
    OutDbResourcePool.OutDbResource resource = pool.acquire(key);
    if (resource == null) {
      AbstractGridFormat format = getFileFormat(key.path);
      Configuration conf = key.getConfWithParams();
      ImageInputStream stream = HadoopImageInputStreamFactory.create(key.path, conf);
      try {
        GridCoverage2D sourceGrid = readGridCoverage(format, stream, conf);
        resource = new OutDbResourcePool.OutDbResource(key, sourceGrid, stream);
        pool.add(resource);
      } catch (Exception e) {
        stream.close();
        throw new DataSourceException("Failed to create out-db grid coverage", e);
      }
    }
    return resource;
  }

  /**
   * Release an out-db resource back to the pool. The resource must be allocated in current thread
   *
   * @param resource the resource to release
   */
  public static void releaseOutDbResource(OutDbResourcePool.OutDbResource resource) {
    OutDbResourcePool pool = get();
    pool.release(resource);
  }

  private static AbstractGridFormat getFileFormat(Path path) {
    String fileName = path.getName().toUpperCase(Locale.ROOT);
    AbstractGridFormat format;
    if (fileName.endsWith(".TIFF") || fileName.endsWith(".TIF")) {
      format = new GeoTiffFormat();
    } else if (fileName.endsWith(".ASC")) {
      format = new ArcGridFormat();
    } else {
      // If we cannot infer the file type, we assume that it is GeoTIFF.
      format = new GeoTiffFormat();
    }
    return format;
  }

  private static GridCoverage2D readGridCoverage(
      AbstractGridFormat format, ImageInputStream stream, Configuration conf) throws IOException {
    Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
    boolean rescale = conf.getBoolean(READER_AUTO_RESCALE_CONF_KEY, true);
    ParameterValue<Boolean> rescalePixels = AbstractGridFormat.RESCALE_PIXELS.createValue();
    rescalePixels.setValue(rescale);
    GeneralParameterValue[] parameters = {rescalePixels};
    if (format instanceof GeoTiffFormat) {
      return RuntimePatches.createGeoTiffReader(stream, hints).read(parameters);
    } else {
      return format.getReader(stream, hints).read(parameters);
    }
  }
}
