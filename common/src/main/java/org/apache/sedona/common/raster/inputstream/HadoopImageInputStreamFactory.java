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

import java.io.IOException;
import javax.imageio.stream.ImageInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.Path;

/**
 * A factory class for creating HadoopImageInputStream. It also tunes the Hadoop configuration for
 * better performance.
 */
public class HadoopImageInputStreamFactory {

    private HadoopImageInputStreamFactory() {
    }

    public static final String READ_AHEAD_SIZE_CONF_KEY = "wherobots.raster.outdb.readahead";
    public static final String ENABLE_CACHE_CONF_KEY = "wherobots.raster.outdb.enablecache";
    public static final int DEFAULT_READ_AHEAD_SIZE = 64 * 1024;

    /**
     * Create a HadoopImageInputStream for the given path.
     *
     * @param path     the path to read from
     * @param conf     the Hadoop configuration
     * @return a HadoopImageInputStream
     * @throws IOException if failed to create the input stream
     */
    public static ImageInputStream create(Path path, Configuration conf) throws IOException {
        Configuration tunedConf = new Configuration(conf);
        String scheme = path.toUri().getScheme();

        if ("s3a".equals(scheme)) {
            tunedConf.set("fs.s3a.experimental.input.fadvise", "random");
        }

        HadoopImageInputStream stream = new HadoopImageInputStream(path, tunedConf);
        boolean isCached = tunedConf.getBoolean(ENABLE_CACHE_CONF_KEY, true);
        if (!isCached) {
            return stream;
        }

        try {
            int readAhead = (int) tunedConf.getStorageSize(READ_AHEAD_SIZE_CONF_KEY, -1,
                    StorageUnit.BYTES);
            if (readAhead < 0) {
                if ("s3a".equals(scheme)) {
                    readAhead = (int) tunedConf.getStorageSize("fs.s3a.readahead.range", -1,
                            StorageUnit.BYTES);
                }
            }
            if (readAhead < 0) {
                readAhead = DEFAULT_READ_AHEAD_SIZE;
            }
            return new DiskCachedImageInputStream(stream, readAhead);
        } catch (Exception e) {
            stream.close();
            throw e;
        }
    }
}
