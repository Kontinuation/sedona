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

"""Spatially stratified sampling partitions the data into a grid and randomly samples each partition independently."""
from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def spatially_stratified_sample(
    dataframe: DataFrame,
    fraction: float,
    partition_count: int,
    geometry: Optional[str] = None,
    seed: int = 42,
):
    """Spatially stratified sampling of a DataFrame containing spatial data.

    Args:
        dataframe: DataFrame containing spatial data to be sampled. Must contain a geometry column.
        fraction: Sampling rate between 0 and 1
        partition_count: Number of partitions to divide the data into. If not a perfect square, the number of partitions
         in each dimension will be rounded to the nearest integer.
        geometry: Column containing the geometry data. Default is "geometry"
        seed: Seed for sampling the data

    Returns:
        the input DataFrame sampled down to the specified rate
    """
    sedona = SparkSession.getActiveSession()

    result_df = sedona._jvm.org.apache.sedona.stats.sampling.SpatiallyStratifiedSampling.spatiallyStratifiedSample(
        dataframe._jdf, fraction, partition_count, geometry, seed
    )

    return DataFrame(result_df, sedona)
