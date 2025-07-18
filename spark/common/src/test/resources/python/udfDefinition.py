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
