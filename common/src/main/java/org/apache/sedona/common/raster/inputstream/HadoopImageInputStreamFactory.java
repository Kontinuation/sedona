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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.imageio.stream.ImageInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory class for creating HadoopImageInputStream. It also tunes the Hadoop configuration for
 * better performance.
 */
public class HadoopImageInputStreamFactory {

  private static final Logger logger = LoggerFactory.getLogger(HadoopImageInputStreamFactory.class);

  private HadoopImageInputStreamFactory() {}

  public static final String READ_AHEAD_SIZE_CONF_KEY = "wherobots.raster.outdb.readahead";
  public static final int DEFAULT_READ_AHEAD_SIZE = 64 * 1024;

  public static final String ENABLE_CACHE_CONF_KEY = "wherobots.raster.outdb.enablecache";
  public static final String CACHE_DIR_CONF_KEY = "wherobots.raster.outdb.cache.dir";
  public static final String DONT_CACHE_LOCAL_FILE_CONF_KEY =
      "wherobots.raster.outdb.dont.cache.local.file";

  public static final String CACHE_MAX_DISK_SPACE_PERCENT_CONF_KEY =
      "wherobots.raster.outdb.cache.maxDiskSpacePercent";
  public static final int DEFAULT_CACHE_MAX_DISK_SPACE_PERCENT = 8;

  /**
   * Create a HadoopImageInputStream for the given path.
   *
   * @param path the path to read from
   * @param conf the Hadoop configuration
   * @return a HadoopImageInputStream
   * @throws IOException if failed to create the input stream
   */
  public static ImageInputStream create(Path path, Configuration conf) throws IOException {
    Configuration tunedConf = new Configuration(conf);
    String scheme = path.toUri().getScheme();
    if (scheme != null && (scheme.equals("http") || scheme.equals("https"))) {
      Path s3Path = convertHttpToS3Path(path);
      if (s3Path != null) {
        path = s3Path;
      } else {
        throw new UnsupportedOperationException(
            "Only http or https path of S3 objects is supported: " + path);
      }
    }

    scheme = path.toUri().getScheme();

    if ("s3a".equals(scheme)) {
      tunedConf.set("fs.s3a.experimental.input.fadvise", "random");
    }

    HadoopImageInputStream stream = new HadoopImageInputStream(path, tunedConf);
    boolean isCached = tunedConf.getBoolean(ENABLE_CACHE_CONF_KEY, true);
    if (!isCached) {
      return stream;
    }

    boolean dontCacheLocalFile = tunedConf.getBoolean(DONT_CACHE_LOCAL_FILE_CONF_KEY, true);
    FileSystem fs = path.getFileSystem(tunedConf);
    if (dontCacheLocalFile && fs.getScheme().equals("file")) {
      return stream;
    }

    String cacheDirString = tunedConf.get(CACHE_DIR_CONF_KEY, null);
    File cacheDir = cacheDirString != null ? new File(cacheDirString) : null;

    try {
      int readAhead =
          (int) tunedConf.getStorageSize(READ_AHEAD_SIZE_CONF_KEY, -1, StorageUnit.BYTES);
      if (readAhead < 0) {
        if ("s3a".equals(scheme)) {
          readAhead =
              (int) tunedConf.getStorageSize("fs.s3a.readahead.range", -1, StorageUnit.BYTES);
        }
      }
      if (readAhead < 0) {
        readAhead = DEFAULT_READ_AHEAD_SIZE;
      }
      return new DiskCachedImageInputStream(stream, readAhead, cacheDir);
    } catch (Exception e) {
      stream.close();
      throw e;
    }
  }

  /**
   * Detect if the path is the http or https path of an S3 object, and convert it to the s3a path.
   *
   * @param path the path to convert
   * @return the converted path, or null if the path is not an http or https path of an S3 object
   */
  public static Path convertHttpToS3Path(Path path) {
    URI uri = path.toUri();
    String scheme = uri.getScheme();
    if (scheme != null && (scheme.equals("http") || scheme.equals("https"))) {
      String host = uri.getHost();
      String pathStr = uri.getPath();

      // Handle path-style requests: https://s3.region-code.amazonaws.com/bucket-name/key-name
      if (host.startsWith("s3.") && host.endsWith(".amazonaws.com")) {
        if (pathStr.startsWith("/")) {
          pathStr = pathStr.substring(1); // Remove leading slash
        }
        int firstSlash = pathStr.indexOf('/');
        if (firstSlash > 0) {
          String bucket = pathStr.substring(0, firstSlash);
          String key = pathStr.substring(firstSlash + 1);
          return new Path("s3a://" + bucket + "/" + key);
        }
      }

      // Handle virtual-hosted-style: https://bucket-name.s3.region-code.amazonaws.com/key-name
      if (host.contains(".s3.") && host.endsWith(".amazonaws.com")) {
        int s3Index = host.indexOf(".s3.");
        String bucket = host.substring(0, s3Index);
        return new Path("s3a://" + bucket + pathStr);
      }
    }
    return null;
  }

  /**
   * Get the free space of the cache partition in bytes.
   *
   * @param conf the Hadoop configuration
   * @return the free space of the cache partition in bytes
   */
  public static long cachePartitionFreeSpace(Configuration conf) {
    boolean isCached = conf.getBoolean(ENABLE_CACHE_CONF_KEY, true);
    if (!isCached) {
      return 0;
    }
    String cacheDirString = conf.get(CACHE_DIR_CONF_KEY, null);
    if (cacheDirString == null) {
      return 0;
    }
    try {
      return Files.getFileStore(Paths.get(cacheDirString)).getUsableSpace();
    } catch (IOException e) {
      logger.error(
          String.format("Cannot get usable space of out-db cache directory %s", cacheDirString), e);
      return 0;
    }
  }

  public static int getCacheMaxDiskSpacePercent(Configuration conf) {
    String cacheDirString = conf.get(CACHE_DIR_CONF_KEY, null);
    if (cacheDirString == null) {
      return 0;
    }
    return conf.getInt(CACHE_MAX_DISK_SPACE_PERCENT_CONF_KEY, DEFAULT_CACHE_MAX_DISK_SPACE_PERCENT);
  }
}
