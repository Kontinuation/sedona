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

import pytest
from pyspark.sql import DataFrame, SparkSession
from sedona.sql.st_constructors import ST_GeomFromWKT
from sedona.stats.sampling.spatially_stratified_sampling import \
    spatially_stratified_sample
from tests.test_base import TestBase


def create_sample_dataframe():
    spark = SparkSession.getActiveSession()
    return spark.createDataFrame(
        [(1, "POINT(1 1)"), (2, "POINT(2 2)")], ["id", "geometry"]
    ).withColumn("geometry", ST_GeomFromWKT("geometry"))


class TestSpatialSampling(TestBase):
    def test_spatially_stratified_sample_valid_parameters(self):
        self.spark
        df = create_sample_dataframe()

        result = spatially_stratified_sample(df, 0.5, 2, "geometry", 42)

        assert isinstance(result, DataFrame)
        assert result.count() <= 1

    def test_spatially_stratified_sample_invalid_fraction(self):
        self.spark
        df = create_sample_dataframe()

        with pytest.raises(Exception):
            spatially_stratified_sample(df, 1.5, 2, "geometry", 42)

    def test_spatially_stratified_sample_invalid_partition_count(self):
        self.spark
        df = create_sample_dataframe()

        with pytest.raises(Exception):
            spatially_stratified_sample(df, 0.5, 0, "geometry", 42)

    def test_spatially_stratified_sample_invalid_geometry_column(self):
        self.spark
        df = create_sample_dataframe()

        with pytest.raises(Exception):
            spatially_stratified_sample(df, 0.5, 2, "nonexistent", 42)
