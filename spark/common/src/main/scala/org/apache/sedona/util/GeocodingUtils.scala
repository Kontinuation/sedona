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
package org.apache.sedona.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object GeocodingUtils {
  def createAddressIndex(df: DataFrame): DataFrame = {
    val normalizedAddressDf =
      df.select(col("id"), explode(expr("ExpandAddress(location)")).alias("address"))

    val extractTokenPhrases = udf((address: String) => {
      if (address == null) {
        Seq.empty[String]
      } else {
        val normalized = address.toLowerCase.replaceAll("[^a-zA-Z0-9 ]", " ")
        val tokens = normalized.split("\\s+").filter(_.nonEmpty)
        tokens.sliding(2).map(_.mkString(" ")).toSeq
      }
    })
    val AddressDbPhrase = normalizedAddressDf.select(
      col("id"),
      explode(extractTokenPhrases(col("address"))).alias("token_phrase"))

    AddressDbPhrase
      .groupBy("token_phrase")
      .agg(collect_list(col("id")).alias("address_ids"), count("*").alias("frequency"))
      .repartition(col("token_phrase"))
  }

}
