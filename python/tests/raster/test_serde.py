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

from tests.test_base import TestBase
from pyspark.sql.functions import expr
from sedona.sql.types import RasterType

from tests import world_map_raster_input_location

class TestRasterSerde(TestBase):
    def test_banded_sample_model(self):
        df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(3, 'I', 'BandedSampleModel', 10, 8, 100, 100, 10, -10, 0, 0, 3857) as raster")
        raster = df.first()[0]
        assert raster.width == 10 and raster.height == 8 and len(raster.bands_meta) == 3
        self.validate_test_raster(raster)

    def test_pixel_interleaved_sample_model(self):
        df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(3, 'I', 'PixelInterleavedSampleModel', 10, 10, 100, 100, 10, -10, 0, 0, 3857) as raster")
        raster = df.first()[0]
        assert raster.width == 10 and raster.height == 10 and len(raster.bands_meta) == 3
        self.validate_test_raster(raster)
        df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(4, 'I', 'PixelInterleavedSampleModelComplex', 8, 10, 100, 100, 10, -10, 0, 0, 3857) as raster")
        raster = df.first()[0]
        assert raster.width == 8 and raster.height == 10 and len(raster.bands_meta) == 4
        self.validate_test_raster(raster)

    def test_component_sample_model(self):
        for pixel_type in ['B', 'S', 'US', 'I', 'F', 'D']:
            df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(4, '{}', 'ComponentSampleModel', 10, 10, 100, 100, 10, -10, 0, 0, 3857) as raster".format(pixel_type))
            raster = df.first()[0]
            assert raster.width == 10 and raster.height == 10 and len(raster.bands_meta) == 4
            self.validate_test_raster(raster)

    def test_multi_pixel_packed_sample_model(self):
        df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(1, 'B', 'MultiPixelPackedSampleModel', 10, 10, 100, 100, 10, -10, 0, 0, 3857) as raster")
        raster = df.first()[0]
        assert raster.width == 10 and raster.height == 10 and len(raster.bands_meta) == 1
        self.validate_test_raster(raster, packed=True)

    def test_single_pixel_packed_sample_model(self):
        df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(4, 'I', 'SinglePixelPackedSampleModel', 10, 10, 100, 100, 10, -10, 0, 0, 3857) as raster")
        raster = df.first()[0]
        assert raster.width == 10 and raster.height == 10 and len(raster.bands_meta) == 4
        self.validate_test_raster(raster, packed=True)

    def test_raster_read_from_geotiff(self):
        raster_path = world_map_raster_input_location
        df = TestRasterSerde.spark.read.format("binaryFile").load(raster_path).selectExpr("RS_FromGeoTiff(content) as raster")
        raster = df.first()[0]
        assert raster.width == 1440
        assert raster.height == 720
        ds = raster.as_rasterio()
        row, col = ds.index(114.737, 38.215)
        band = ds.read(1)
        assert row == 207
        assert col == 1178
        assert band[row, col] == 121
        raster.close()

    def test_outdb_raster(self):
        raster_path = world_map_raster_input_location
        df = TestRasterSerde.spark.sql("SELECT RS_FromPath('{}') as raster".format(raster_path))
        raster = df.first()[0]
        assert raster.width == 1440
        assert raster.height == 720
        ds = raster.as_rasterio()
        row, col = ds.index(114.737, 38.215)
        band = ds.read(1)
        assert row == 207
        assert col == 1178
        assert band[row, col] == 121
        raster.close()

    def test_to_pandas(self):
        spark = TestRasterSerde.spark
        df = spark.sql("SELECT RS_MakeRasterForTesting(3, 'I', 'BandedSampleModel', 10, 8, 100, 100, 10, -10, 0, 0, 3857) as raster")
        pandas_df = df.toPandas()
        raster = pandas_df.iloc[0]['raster']
        self.validate_test_raster(raster)

    def validate_test_raster(self, raster, packed = False):
        arr = raster.as_numpy()
        bands, height, width = arr.shape
        assert bands > 0 and width > 0 and height > 0
        for b in range(bands):
            for y in range(height):
                for x in range(width):
                    expected = b + y * width + x
                    if packed:
                        expected = expected % 16
                    assert arr[b, y, x] == expected
