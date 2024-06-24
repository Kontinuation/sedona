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
package org.apache.sedona.sql

import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

class pythonUdfTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen {
  // Override sparkConfig to provide additional configurations
  override def sparkConfig: Map[String, String] = defaultSparkConfig ++ Map(
    "spark.wherobots.inference.entrance" -> (resourceFolder + "python/udfEntrance.py"),
    "spark.wherobots.inference.files" -> (resourceFolder + "python/udfDefinition.py"),
    "spark.wherobots.inference.args" -> "3")

  describe("Python UDF test") {
    it("should find the standalone Python UDF in the correct location and register it") {
      sparkSession.sql("SELECT py_concat_of2('a', 'b')").take(1)(0)(0) should be("ab")
    }

    it("should find the custom Python Pandas UDF from a separate module and register it") {
      sparkSession.sql("SELECT custom_pandas_udf('abc')").take(1)(0)(0) should be("ABC")
      sparkSession.sql("SELECT custom_pandas_udf('abcd')").take(1)(0)(0) should be("abcd")
    }
  }
}
