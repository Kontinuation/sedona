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

import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from rasterio.session import AWSSession  # type: ignore

try:
    import boto3  # type: ignore
except ImportError:
    boto3 = None

# The spark config inferred from SparkContext on driver or TaskContext on executor
_spark_conf: Optional[Dict[str, str]] = None

_base_s3a_conf: Optional[Dict[str, str]] = None
_per_bucket_s3a_conf: Optional[Dict[str, Dict[str, str]]] = None

_base_gdal_conf: Optional[Dict[str, str]] = None
_per_bucket_gdal_conf: Optional[Dict[str, Dict[str, str]]] = None


BASE_GDAL_CONF_ENV_KEY = "__SEDONA_BASE_GDAL_CONF__"
PER_BUCKET_GDAL_CONF_ENV_KEY = "__SEDONA_PER_BUCKET_GDAL_CONF__"


S3A_CONFIG_TO_GDAL_CONFIG_MAP = {
    "access.key": "AWS_ACCESS_KEY_ID",
    "secret.key": "AWS_SECRET_ACCESS_KEY",
    "session.token": "AWS_SESSION_TOKEN",
}


def get_gdal_conf_for_s3_bucket(bucket_name: str) -> Dict[str, str]:
    _load_gdal_conf()
    if _base_gdal_conf is None or _per_bucket_gdal_conf is None:
        return {}
    if bucket_name in _per_bucket_gdal_conf:
        return _per_bucket_gdal_conf[bucket_name]
    else:
        return _base_gdal_conf


def get_gdal_conf(path: str) -> Dict[str, str]:
    bucket_name = None
    if path.startswith("s3a://"):
        bucket_name = path.split("/")[2]
    elif path.startswith("s3://"):
        bucket_name = path.split("/")[2]
    elif path.startswith("/vsis3/"):
        bucket_name = path.split("/")[2]
    if bucket_name is None:
        return {}
    return get_gdal_conf_for_s3_bucket(bucket_name)


def export_gdal_conf_to_env():
    _load_gdal_conf()
    if _base_gdal_conf is None and _per_bucket_gdal_conf is None:
        return
    if BASE_GDAL_CONF_ENV_KEY not in os.environ:
        os.environ[BASE_GDAL_CONF_ENV_KEY] = json.dumps(_base_gdal_conf)
    if PER_BUCKET_GDAL_CONF_ENV_KEY not in os.environ:
        os.environ[PER_BUCKET_GDAL_CONF_ENV_KEY] = json.dumps(_per_bucket_gdal_conf)


def clear_gdal_conf_from_env():
    del os.environ[BASE_GDAL_CONF_ENV_KEY]
    del os.environ[PER_BUCKET_GDAL_CONF_ENV_KEY]


def get_rasterio_aws_session(path: str) -> Optional[AWSSession]:
    conf = get_gdal_conf(path)
    if not conf:
        return None
    if boto3 is None:
        # AWSSession requires boto3 to work properly
        return None
    args: Dict[str, Any] = {}
    if "AWS_ACCESS_KEY_ID" in conf:
        args["aws_access_key_id"] = conf["AWS_ACCESS_KEY_ID"]
    if "AWS_SECRET_ACCESS_KEY" in conf:
        args["aws_secret_access_key"] = conf["AWS_SECRET_ACCESS_KEY"]
    if "AWS_SESSION_TOKEN" in conf:
        args["aws_session_token"] = conf["AWS_SESSION_TOKEN"]
    if "AWS_NO_SIGN_REQUEST" in conf:
        args["aws_unsigned"] = conf["AWS_NO_SIGN_REQUEST"] == "YES"
    if "AWS_REQUEST_PAYER" in conf:
        args["requester_pays"] = conf["AWS_REQUEST_PAYER"] == "requester"
    return AWSSession(**args)


def _set_spark_conf_in_test(spark_conf: Optional[Dict[str, str]]):
    global _spark_conf, _base_gdal_conf, _per_bucket_gdal_conf, _base_s3a_conf, _per_bucket_s3a_conf
    _spark_conf = spark_conf
    _base_gdal_conf = None
    _per_bucket_gdal_conf = None
    _base_s3a_conf = None
    _per_bucket_s3a_conf = None


def _load_spark_conf() -> Optional[Dict[str, str]]:
    global _spark_conf
    if _spark_conf is not None:
        return _spark_conf
    try:
        from pyspark import TaskContext
        from pyspark.sql import SparkSession

        task_context = TaskContext.get()
        if task_context is not None:
            # running on spark executor
            _spark_conf = task_context._localProperties
        else:
            # running on spark driver
            session = SparkSession.getActiveSession()
            if session is not None:
                conf_list = session.sparkContext.getConf().getAll()
                _spark_conf = {k: v for k, v in conf_list}
    except ImportError:
        # Not running with spark, there's no spark conf to load
        _spark_conf = {}
    return _spark_conf


def _parse_s3a_conf(
    spark_conf: Dict[str, str],
) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    regex = re.compile(r"spark\.hadoop\.fs\.s3a(\.bucket\.([^.]+]*))?\.(.*)")
    global_s3_configs = {}
    per_bucket_s3_configs: Dict[str, Dict[str, str]] = {}
    for key, value in spark_conf.items():
        m = regex.match(key)
        if m is None:
            continue
        bucket_name = m.group(2)
        conf_key = m.group(3)
        if bucket_name is None:
            global_s3_configs[conf_key] = value
        else:
            if bucket_name not in per_bucket_s3_configs:
                per_bucket_s3_configs[bucket_name] = {}
            per_bucket_s3_configs[bucket_name][conf_key] = value
    return (global_s3_configs, per_bucket_s3_configs)


def _convert_s3a_configs_to_gdal_configs(s3a_config: Dict[str, str]) -> Dict[str, str]:
    gdal_configs = {}
    for key, value in s3a_config.items():
        if key in S3A_CONFIG_TO_GDAL_CONFIG_MAP:
            gdal_configs[S3A_CONFIG_TO_GDAL_CONFIG_MAP[key]] = value
        elif key == "aws.credentials.provider":
            if "AnonymousAWSCredentialsProvider" in value:
                gdal_configs["AWS_NO_SIGN_REQUEST"] = "YES"
            else:
                gdal_configs["AWS_NO_SIGN_REQUEST"] = "NO"
                if "RequestPayer" in value:
                    gdal_configs["AWS_REQUEST_PAYER"] = "requester"
        elif key == "requester.pays.enabled":
            if value == "true":
                gdal_configs["AWS_REQUEST_PAYER"] = "requester"
    return gdal_configs


def _load_gdal_conf():
    global _base_gdal_conf, _per_bucket_gdal_conf
    if _base_gdal_conf is not None and _per_bucket_gdal_conf is not None:
        return

    # try loading gdal config from env
    if (
        BASE_GDAL_CONF_ENV_KEY in os.environ
        or PER_BUCKET_GDAL_CONF_ENV_KEY in os.environ
    ):
        _base_gdal_conf = (
            json.loads(os.environ[BASE_GDAL_CONF_ENV_KEY])
            if BASE_GDAL_CONF_ENV_KEY in os.environ
            else {}
        )
        _per_bucket_gdal_conf = (
            json.loads(os.environ[PER_BUCKET_GDAL_CONF_ENV_KEY])
            if PER_BUCKET_GDAL_CONF_ENV_KEY in os.environ
            else {}
        )
        return

    # try loading gdal config from spark config
    spark_conf = _load_spark_conf()
    if spark_conf is None:
        return

    global _base_s3a_conf, _per_bucket_s3a_conf
    _base_s3a_conf, _per_bucket_s3a_conf = _parse_s3a_conf(spark_conf)
    _base_gdal_conf = _convert_s3a_configs_to_gdal_configs(_base_s3a_conf)
    _per_bucket_gdal_conf = {}
    for bucket, confs in _per_bucket_s3a_conf.items():
        gdal_confs = _convert_s3a_configs_to_gdal_configs(confs)
        _per_bucket_gdal_conf[bucket] = gdal_confs
