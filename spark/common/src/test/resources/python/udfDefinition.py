import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType


# A Python Pandas UDF
def create_custom_pandas_udf(batch_size):
    @pandas_udf(StringType())
    def custom_pandas_udf(s: pd.Series) -> pd.Series:
        # Example custom logic using batch_size
        # (Note: In a real scenario, the logic might be more complex)
        return s.apply(lambda x: x.upper() if len(x) <= batch_size else x.lower())

    return custom_pandas_udf


def register_pandas_UDF(spark, batch_size):
    # Register a UDF imported from a Python module
    spark.udf.register("custom_pandas_udf", create_custom_pandas_udf(batch_size))
