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
package org.apache.spark.sql.sedona_sql.utils

import org.apache.hadoop.conf.Configuration
import org.apache.sedona.common.raster.inputstream.HadoopImageInputStreamFactory
import org.apache.sedona.common.raster.outdb.ThreadLocalOutDbResourcePool
import org.apache.spark.SparkConf

/**
 * We need to call some package-private functions in SparkHadoopUtil to retrieve Hadoop
 * Configuration from SparkConf, so we need this util as an indirection.
 */
object SparkHadoopUtil {

  /**
   * Create a new Hadoop Configuration based on the given SparkConf. This will also add the
   * wherobots specific configurations to the Hadoop configuration.
   * @param sparkConf
   *   SparkConf
   * @return
   *   Hadoop Configuration
   */
  def newConfiguration(sparkConf: SparkConf): Configuration = {
    val hadoopConf = org.apache.spark.deploy.SparkHadoopUtil.get.newConfiguration(sparkConf)

    // Add wherobots specific configurations to the Hadoop configuration
    wherobotsConfigKeys.foreach { key =>
      sparkConf.getOption("spark." + key).foreach(hadoopConf.set(key, _))
    }

    hadoopConf
  }

  private val wherobotsConfigKeys = Seq(
    ThreadLocalOutDbResourcePool.FREE_RESOURCES_POOL_SIZE_CONF_KEY,
    HadoopImageInputStreamFactory.READ_AHEAD_SIZE_CONF_KEY,
    HadoopImageInputStreamFactory.ENABLE_CACHE_CONF_KEY,
    HadoopImageInputStreamFactory.CACHE_DIR_CONF_KEY,
    HadoopImageInputStreamFactory.DONT_CACHE_LOCAL_FILE_CONF_KEY)
}
