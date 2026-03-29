# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Iterable

import pandas as pd
import pyarrow as pa
from pyspark.serializers import SpecialLengths, write_int
from pyspark.sql.pandas.serializers import ArrowStreamPandasSerializer


class DirectBatchSerializer(ArrowStreamPandasSerializer):
    def load_stream(self, stream):
        batches = super(ArrowStreamPandasSerializer, self).load_stream(stream)
        for batch in batches:
            table = pa.Table.from_batches([batch])
            yield [self.arrow_to_pandas(column) for column in table.itercolumns()]

    def _create_array(self, series, arrow_type, spark_type=None, arrow_cast=False):
        if isinstance(series, pa.Array):
            return series
        return super()._create_array(series, arrow_type, spark_type, arrow_cast)

    def dump_stream(self, iterator, stream):
        def init_stream_yield_batches():
            should_write_start_length = True
            for batch in iterator:
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False
                yield self._create_batch(batch)

        return super(ArrowStreamPandasSerializer, self).dump_stream(
            init_stream_yield_batches(), stream
        )


def normalize_result(result) -> list:
    if isinstance(result, tuple):
        return list(result)
    if isinstance(result, list):
        return result
    return [result]


def apply_direct_batch(iterator: Iterable[list], udf_info):
    for columns in iterator:
        if udf_info.arg_offsets is None:
            args = columns
        else:
            args = [columns[offset] for offset in udf_info.arg_offsets]
        result = udf_info.function(*args)
        normalized = normalize_result(result)
        output = []
        for value in normalized:
            if isinstance(value, pa.Array):
                output.append(pd.Series(value.to_pylist()))
            else:
                output.append(value)
        yield output
