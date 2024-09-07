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

"""DBSCAN is a popular clustering algorithm for spatial data.

It identifies groups of data where enough records are close enough to each other. This implementation leverages spark,
sedona and graphframes to support large scale datasets and various, heterogeneous geometric feature types.
"""
from typing import Optional

import pyspark.sql.functions as f
from kneed import KneeLocator
from pyspark.sql import DataFrame, SparkSession
from sedona.sql.st_functions import ST_Distance

from sedona.stats.utils import get_geometry_column_name, reduce_dataset_size

ID_COLUMN_NAME = "__id"
DEFAULT_MAX_SAMPLE_SIZE = 1000000  # 1 million


def dbscan(
    dataframe: DataFrame,
    epsilon: float,
    min_pts: int,
    geometry: Optional[str] = None,
    include_outliers: bool = True,
    use_spheroid=False,
):
    """Annotates a dataframe with a cluster label for each data record using the DBSCAN algorithm.

    The dataframe should contain at least one GeometryType column. Rows must be unique. If one geometry column is
    present it will be used automatically. If two are present, the one named 'geometry' will be used. If more than one
    are present and neither is named 'geometry', the column name must be provided.

    Args:
        dataframe: spark dataframe containing the geometries
        epsilon: minimum distance parameter of DBSCAN algorithm
        min_pts: minimum number of points parameter of DBSCAN algorithm
        geometry: name of the geometry column
        include_outliers: whether to return outlier points. If True, outliers are returned with a cluster value of -1.
            Default is False
        use_spheroid: whether to use a cartesian or spheroidal distance calculation. Default is false

    Returns:
        A PySpark DataFrame containing the cluster label for each row
    """
    sedona = SparkSession.getActiveSession()

    result_df = sedona._jvm.org.apache.sedona.stats.clustering.DBSCAN.dbscan(
        dataframe._jdf,
        float(epsilon),
        min_pts,
        geometry,
        include_outliers,
        use_spheroid,
    )

    return DataFrame(result_df, sedona)


def get_knee_locator(
    dataframe: DataFrame,
    min_points: int,
    geometry: Optional[str] = None,
    approximate_knn: bool = False,
    use_spheroid: bool = False,
    max_sample_size: Optional[int] = DEFAULT_MAX_SAMPLE_SIZE,
    **kwargs,
) -> KneeLocator:
    """Create a KneeLocator for the purposes of selecting an epsilon value for passing into a DBSCAN execution.

    Finding the knee of the plot of (index, distance to kth nearest neighbor), where k = min_points is a common
    heuristic for selecting the epsilon parameter for DBSCAN. This function calculates the kth nearest neighbor distance
    for a random sample of max_sample_size records and feeds them into a KneeLocator object provided by the kneed lib.
    While often a good start, this method is not fool proof. It is recommended to visualize the knee plot to sanity
    check before moving forward with the provided epsilon value.

    See https://kneed.readthedocs.io/en/stable/parameters.html

    See https://medium.com/@tarammullin/dbscan-parameter-estimation-ff8330e3a3bd

    Args:
        dataframe: apache sedona dataframe containing the geometries. This should be the same dataframe you intend to
            pass to the dbscan function.
        min_points:  the min points parameter you intend to pass to the dbscan function. This will impact the epsilon
            value that is calculated.
        geometry: name of the geometry column
        approximate_knn: whether to use approximate KNN. When false will use exact KNN join. Default is False
        use_spheroid: whether to use a cartesian or spheroidal distance calculation. False will use Cartesian. Default
            is false
        max_sample_size:  the maximum number of records from dataframe to use when calculating the knee. If the
            dataframe has more records than this, it will be downsampled to approximately this size. Default is 1
            million. Records are collected to the driver to visualize/calculate the knee, so this is important for
            stability.
        kwargs: additional keyword arguments to pass to the KneeLocator constructor

    Returns:
        A KneeLocator object derived from the input DataFrame, downsampled to approximately max_sample_size records.
        Retrieve the recommended epsilon value with the return value's knee_y property.
    """
    if geometry is None:
        geometry = get_geometry_column_name(dataframe)

    # Default to convex curve for knee locator
    if "curve" not in kwargs:
        kwargs["curve"] = "convex"

    dataframe = dataframe.withColumn(ID_COLUMN_NAME, f.sha2(f.to_json(f.struct("*")), 256))

    if max_sample_size is not None:
        l_dataframe = reduce_dataset_size(dataframe, max_sample_size)
    else:
        l_dataframe = dataframe

    knn_function = "ST_AKNN" if approximate_knn else "ST_KNN"
    use_spheroid_string = "TRUE" if use_spheroid else "FALSE"

    # min_points +1 because we are not counting the row matching to itself
    kth_distance_df = (
        l_dataframe.alias("l")
        .join(
            dataframe.alias("r"),
            f.expr(f"{knn_function}(l.{geometry}, r.{geometry}, {min_points} + 1, {use_spheroid_string})"),
        )
        .groupBy(f"l.{ID_COLUMN_NAME}")
        .agg(f.max(ST_Distance(f.col(f"l.{geometry}"), f.col(f"r.{geometry}"))).alias("kth_distance"))
        .select("kth_distance")
        .orderBy("kth_distance")
    )
    y = [row.kth_distance for row in kth_distance_df.collect()]
    x = list(range(len(y)))
    # TODO: remember why S=0.0 rather than the recommended 1.0 from the paper
    return KneeLocator(x, y, S=0.0, direction="increasing", **kwargs)
