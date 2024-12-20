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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Row}

/**
 * Work with internal data types of Spark SQL in a way compatible with multiple Spark versions.
 */
object SparkCompatUtil {

  /**
   * Creates a [[Row]] encoder for schema `schema`.
   */
  def rowEncoderFor(schema: StructType): Encoder[Row] = {
    try {
      // Try to use RowEncoder for Spark 3.3 and 3.4
      val rowEncoderClass = Class.forName("org.apache.spark.sql.catalyst.encoders.RowEncoder")
      val applyMethod = rowEncoderClass.getMethod("apply", classOf[StructType])
      applyMethod.invoke(null, schema).asInstanceOf[Encoder[Row]]
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException =>
        // Use Encoders.row for Spark 3.5
        val encodersClass = Class.forName("org.apache.spark.sql.Encoders")
        val rowMethod = encodersClass.getMethod("row", classOf[StructType])
        rowMethod.invoke(null, schema).asInstanceOf[Encoder[Row]]
    }
  }
}
