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

import os

from shapely.geometry.base import BaseGeometry
import geopandas

from pyspark.sql.functions import expr

from tests.test_base import TestBase


class TestGeoJSON(TestBase):
    def test_interoperability_with_geopandas(self, tmp_path):
        df = (
            self.spark.range(0, 10)
            .toDF("id")
            .withColumn("geom", expr("ST_Point(id, id)"))
            .withColumn("text", expr("concat('test', id)"))
        )
        geojson_save_path = os.path.join(tmp_path, "test.geojson")
        df.write.format("geojson").option("geometry.column", "geom").mode(
            "overwrite"
        ).save(geojson_save_path)

        # Load GeoJSON file written by sedona using geopandas
        geojson_file_paths = [
            os.path.join(geojson_save_path, fname)
            for fname in os.listdir(geojson_save_path)
            if fname.endswith("json")
        ]
        for geojson_file_path in geojson_file_paths:
            gdf = geopandas.read_file(geojson_file_path)
            assert gdf.dtypes["geometry"].name == "geometry"

        # Load GeoJSON file written by geopandas using sedona
        geojson_save_path2 = os.path.join(tmp_path, "test_2.geojson")
        gdf.to_file(geojson_save_path2, driver="GeoJSON")

        df = (
            self.spark.read.format("geojson")
            .option("multiLine", "true")
            .load(geojson_save_path2)
        )
        df = df.selectExpr("explode(features) as feature").select("feature.*")
        row = df.first()
        assert isinstance(row["geometry"], BaseGeometry)
        assert row["properties"]["text"].startswith("test")
