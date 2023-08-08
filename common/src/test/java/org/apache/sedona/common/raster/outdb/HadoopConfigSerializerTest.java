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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class HadoopConfigSerializerTest {
    @Test
    public void testSerializeDefaultHadoopConfig() throws IOException {
        Configuration conf = new Configuration();
        Configuration conf2 = HadoopConfigSerializer.deserialize(HadoopConfigSerializer.serialize(conf));
        Assert.assertNotNull(conf.get("io.file.buffer.size"));
        Assert.assertEquals(conf.get("io.file.buffer.size"), conf2.get("io.file.buffer.size"));
    }

    @Test
    public void testSerializeEmptyHadoopConfig() throws IOException {
        Configuration conf = new Configuration(false);
        Configuration conf2 = HadoopConfigSerializer.deserialize(HadoopConfigSerializer.serialize(conf));
        Assert.assertNull(conf.get("io.file.buffer.size"));
        Assert.assertNull(conf2.get("io.file.buffer.size"));
    }

    @Test
    public void testSerializeHadoopConfig() throws IOException {
        Configuration conf = new Configuration();
        conf.set("test_key1", "test_value1");
        conf.set("test_key2", "test_value2");
        Configuration conf2 = HadoopConfigSerializer.deserialize(HadoopConfigSerializer.serialize(conf));
        Assert.assertEquals(conf.get("io.file.buffer.size"), conf2.get("io.file.buffer.size"));
        Assert.assertEquals("test_value1", conf2.get("test_key1"));
        Assert.assertEquals("test_value2", conf2.get("test_key2"));
    }
}
