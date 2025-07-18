#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import argparse
import sys

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from udfDefinition import register_pandas_UDF

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Run a PySpark job with a custom Pandas UDF"
    )
    parser.add_argument(
        "batch_size", type=int, help="The batch size to be used in the Pandas UDF"
    )
    args = parser.parse_args()

    # Create a Spark session
    gateway = pyspark.java_gateway.launch_gateway()
    jsc = gateway.jvm.org.apache.sedona.spark.SedonaContext.jsc()
    jconf = gateway.jvm.org.apache.sedona.spark.SedonaContext.jconf()
    conf = pyspark.conf.SparkConf(True, gateway.jvm, jconf)
    sc = pyspark.SparkContext(gateway=gateway, jsc=jsc, conf=conf)
    spark = SparkSession(sc)

    # A standalone Python UDF
    py_concat_of2_udf = udf(lambda x, y: str(x) + str(y), StringType())
    # Register the UDF
    spark.udf.register("py_concat_of2", py_concat_of2_udf)

    # Register a UDF imported from a Python module
    register_pandas_UDF(spark, args.batch_size)

    sys.exit(0)
