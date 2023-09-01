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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

/**
 * A utility class for serializing Hadoop {@link Configuration} objects.
 */
public class HadoopConfigSerializer {
    private HadoopConfigSerializer() {}

    // Serializing a Hadoop Configuration object is expensive (usually takes 10+ ms), so we cache the
    // serialized bytes.
    private static final LoadingCache<Configuration, byte[]> serializeCache = Caffeine.newBuilder()
            .maximumSize(100)
            .build(HadoopConfigSerializer::doSerialize);
    private static final LoadingCache<ByteBuffer, Configuration> deserializeCache = Caffeine.newBuilder()
            .maximumSize(100)
            .build(HadoopConfigSerializer::doDeserialize);

    /**
     * Serialize a Hadoop {@link Configuration} object to a byte array.
     *
     * @param conf the Hadoop configuration
     * @return a byte array
     */
    public static byte[] serialize(Configuration conf) throws IOException {
        return serializeCache.get(conf);
    }

    /**
     * Deserialize a Hadoop {@link Configuration} object from a byte array.
     *
     * @param serializedConf the byte array
     * @return a Hadoop configuration
     */
    public static Configuration deserialize(byte[] serializedConf) throws IOException {
        return deserializeCache.get(ByteBuffer.wrap(serializedConf));
    }

    private static byte[] doSerialize(Configuration conf) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream os = new ObjectOutputStream(bos)) {
            conf.write(os);
            os.flush();
            return bos.toByteArray();
        }
    }

    public static Configuration doDeserialize(ByteBuffer buf) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(buf.array());
             ObjectInputStream is = new ObjectInputStream(bis)) {
            Configuration conf = new Configuration(false);
            conf.readFields(is);
            return conf;
        }
    }
}
