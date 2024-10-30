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
from sedona.raster import gdal_conf


class TestGdalConfig:

    def test_no_spark_config(self):
        gdal_conf._set_spark_conf_in_test(None)
        session = gdal_conf.get_rasterio_aws_session("test-bucket")
        assert session is None

    def test_empty_spark_config(self):
        gdal_conf._set_spark_conf_in_test({})
        session = gdal_conf.get_rasterio_aws_session("test-bucket")
        assert session is None

    def test_global_only(self):
        gdal_conf._set_spark_conf_in_test(
            {
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
            }
        )
        session = gdal_conf.get_rasterio_aws_session("s3://test-bucket/test/path")
        assert session.unsigned
        session = gdal_conf.get_rasterio_aws_session("s3a://bucket_2/test/path")
        assert session.unsigned
        session = gdal_conf.get_rasterio_aws_session("/vsis3/bucket3/test/path")
        assert session.unsigned

    def test_per_bucket(self):
        gdal_conf._set_spark_conf_in_test(
            {
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.connection.maximum": "2000",
                "spark.hadoop.fs.s3a.bucket.priv-bucket.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.bucket.priv-bucket.access.key": "test_access_key",
                "spark.hadoop.fs.s3a.bucket.priv-bucket.secret.key": "test_secret_key",
                "spark.hadoop.fs.s3a.bucket.priv-bucket.session.token": "test_token",
            }
        )
        session = gdal_conf.get_rasterio_aws_session("s3://test-bucket/test/path")
        assert session.unsigned
        assert "aws_access_key_id" not in session.credentials
        assert "aws_secret_access_key" not in session.credentials
        session = gdal_conf.get_rasterio_aws_session("s3://priv-bucket/test")
        assert not session.unsigned
        assert session.credentials["aws_access_key_id"] == "test_access_key"
        assert session.credentials["aws_secret_access_key"] == "test_secret_key"
        assert session.credentials["aws_session_token"] == "test_token"

    def test_per_bucket_2(self):
        gdal_conf._set_spark_conf_in_test(
            {
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.access.key": "test_access_key",
                "spark.hadoop.fs.s3a.secret.key": "test_secret_key",
                "spark.hadoop.fs.s3a.bucket.pub-bucket.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
            }
        )
        session = gdal_conf.get_rasterio_aws_session("s3://test-bucket/test/path")
        assert not session.unsigned
        assert session.credentials["aws_access_key_id"] == "test_access_key"
        assert session.credentials["aws_secret_access_key"] == "test_secret_key"
        session = gdal_conf.get_rasterio_aws_session("s3://pub-bucket/test")
        assert session.unsigned
        assert "aws_access_key_id" not in session.credentials
        assert "aws_secret_access_key" not in session.credentials

    def test_export_to_env(self):
        gdal_conf._set_spark_conf_in_test(
            {
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
                "spark.hadoop.fs.s3a.bucket.pub-bucket.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
            }
        )
        gdal_conf.export_gdal_conf_to_env()
        try:
            # clear spark conf and reload config from environment variables
            gdal_conf._set_spark_conf_in_test({})
            session = gdal_conf.get_rasterio_aws_session("s3://test-bucket/test/path")
            assert not session.unsigned
            session = gdal_conf.get_rasterio_aws_session("s3://pub-bucket/test/path")
            assert session.unsigned
        finally:
            gdal_conf.clear_gdal_conf_from_env()
