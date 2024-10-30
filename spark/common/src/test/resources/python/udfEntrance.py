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
