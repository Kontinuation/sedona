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
import rasterio

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
        r_orig = rasterio.open(raster_path)
        band = r_orig.read(1)
        df = TestRasterSerde.spark.read.format("binaryFile").load(raster_path).selectExpr("RS_FromGeoTiff(content) as raster")
        raster = df.first()[0]
        assert raster.width == r_orig.width
        assert raster.height == r_orig.height
        assert (band == raster.as_numpy()[0, :, :]).all()
        ds = raster.as_rasterio()
        band_actual = ds.read(1)
        assert (band == band_actual).all()
        raster.close()
        r_orig.close()

    def test_outdb_raster(self):
        raster_path = world_map_raster_input_location
        r_orig = rasterio.open(raster_path)
        band = r_orig.read(1)
        for eager_loading in ['true', 'false']:
            df = TestRasterSerde.spark.sql("SELECT RS_FromPath('{}', '', {}) as raster".format(raster_path, eager_loading))
            raster = df.first()[0]
            assert raster.width == r_orig.width
            assert raster.height == r_orig.height
            ds = raster.as_rasterio()
            band_actual = ds.read(1)
            assert (band == band_actual).all()
            raster.close()
        r_orig.close()

    def test_outdb_tiled_raster(self):
        raster_path = world_map_raster_input_location
        r_orig = rasterio.open(raster_path)
        band = r_orig.read(1)
        r_orig.close()
        df = TestRasterSerde.spark.sql("SELECT RS_TileExplode(RS_FromPath('{}'), 1, 256, 256) AS (x, y, rast)".format(raster_path))
        df = df.withColumn("meta", expr("RS_Metadata(rast)"))
        rows = df.collect()
        for row in rows:
            ip_x, ip_y, width, height, scale_x, scale_y, skew_x, skew_y, srid, num_bands = row['meta']
            r_tile = row['rast']
            assert width == r_tile.width
            assert height == r_tile.height
            assert ip_x == r_tile.affine_trans.ip_x
            assert ip_y == r_tile.affine_trans.ip_y
            assert scale_x == r_tile.affine_trans.scale_x
            assert scale_y == r_tile.affine_trans.scale_y
            assert skew_x == r_tile.affine_trans.skew_x
            assert skew_y == r_tile.affine_trans.skew_y
            start_x = row['x'] * 256
            end_x = (row['x'] + 1) * 256
            start_y = row['y'] * 256
            end_y = (row['y'] + 1) * 256
            assert (band[start_y:end_y, start_x:end_x] == r_tile.as_numpy()).all()
            r_tile.close()

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
