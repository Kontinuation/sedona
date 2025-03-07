
<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

Sedona supports nearest-neighbour searching on geospatial data by providing a geospatial k-Nearest Neighbors (kNN) join method. This method involves identifying the k-nearest neighbors for a given spatial point or region based on geographic proximity, typically using spatial coordinates and a suitable distance metric like Euclidean or great-circle distance.

## ST_KNN

Introduction: join operation to find the k-nearest neighbors of a point or region in a spatial dataset.

Format: `ST_KNN(R: Table, S: Table, k: Integer, use_sphere: Boolean, search_radius: Double)`

* `R` represents the queries side table.
* `S` represents the objects side table.
* `K` denotes the number of nearest neighbors to retrieve.
* `use_sphere` is a boolean value that specifies whether to calculate distances using the sphere model.
* `search_radius` is an optional parameter that defines the maximum distance within which neighbors will be searched, without imposing any constraints on its value.

Queries side table contains geometries that are used to find the k-nearest neighbors in the object side table.

When either queries or objects data contain non-point data (geometries), we take the centroid of each geometry.

In case there are ties in the distance, the result will include all the tied geometries only when the following sedona config is set to true:

```
spark.sedona.join.knn.includeTieBreakers=true
```

Filter Pushdown Considerations:

When using ST_KNN with filters applied to the resulting DataFrame, some of these filters may be pushed down to the object side of the kNN join. This means the filters will be applied to the object side reader before the kNN join is executed. If you want the filters to be applied after the kNN join, ensure that you first materialize the kNN join results and then apply the filters.

For example, you can use the following approach:

Scala Example:

```
val knnResult = knnJoinDF.cache()
val filteredResult = knnResult.filter(condition)
```

SQL Example:

```
CREATE OR REPLACE TEMP VIEW knnResult AS
SELECT * FROM (
  -- Your KNN join SQL here
) AS knnView;
CACHE TABLE knnResult;
SELECT * FROM knnResult WHERE condition;
```

SQL Example

Suppose we have two tables `QUERIES` and `OBJECTS` with the following data:

QUERIES table:

```
ID  GEOMETRY            NAME
1   POINT(1 1)	        station1
2   POINT(10 10)	    station2
3   POINT(-0.5 -0.5)	station3
```

OBJECTS table:

```
ID  GEOMETRY            NAME
1	POINT(11 5)         bank1
2	POINT(12 1)         bank2
3	POINT(-1 -1)        bank3
4	POINT(-3 5)         bank4
5	POINT(9 8)          bank5
6	POINT(4 3)          bank6
7	POINT(-4 -5)        bank7
8	POINT(4 -2)         bank8
9	POINT(-3 1)         bank9
10	POINT(-7 3)         bank10
11	POINT(11 5)         bank11
12	POINT(12 1)         bank12
13	POINT(-1 -1)        bank13
14	POINT(-3 5)         bank14
15	POINT(9 8)          bank15
16	POINT(4 3)          bank16
17	POINT(-4 -5)        bank17
18	POINT(4 -2)         bank18
19	POINT(-3 1)         bank19
20	POINT(-7 3)         bank20
```

Example 1: Query Without search_radius Parameter

```sql
SELECT
    QUERIES.ID AS QUERY_ID,
    QUERIES.GEOMETRY AS QUERIES_GEOM,
    OBJECTS.GEOMETRY AS OBJECTS_GEOM
FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOMETRY, OBJECTS.GEOMETRY, 4, FALSE)
```

Explanation:

This query performs a k-Nearest Neighbor (kNN) join between the QUERIES table and the OBJECTS table. For each geometry in the QUERIES table, the function ST_KNN identifies the 4 nearest geometries (based on spatial distance) from the OBJECTS table.

`FALSE` indicates that the spheroid distance model is not being used. Instead a planar distance model is being applied.

Since the `search_radius` parameter is not provided, there are no constraints on the maximum distance between the query and the objects.

Output:

```
+--------+-----------------+-------------+
|QUERY_ID|QUERIES_GEOM     |OBJECTS_GEOM |
+--------+-----------------+-------------+
|3       |POINT (-0.5 -0.5)|POINT (-1 -1)|
|3       |POINT (-0.5 -0.5)|POINT (-1 -1)|
|3       |POINT (-0.5 -0.5)|POINT (-3 1) |
|3       |POINT (-0.5 -0.5)|POINT (-3 1) |
|1       |POINT (1 1)      |POINT (-1 -1)|
|1       |POINT (1 1)      |POINT (-1 -1)|
|1       |POINT (1 1)      |POINT (4 3)  |
|1       |POINT (1 1)      |POINT (4 3)  |
|2       |POINT (10 10)    |POINT (9 8)  |
|2       |POINT (10 10)    |POINT (9 8)  |
|2       |POINT (10 10)    |POINT (11 5) |
|2       |POINT (10 10)    |POINT (11 5) |
+--------+-----------------+-------------+
```

Example 2: Query With `search_radius` Parameter

```sql
SELECT
    QUERIES.ID AS QUERY_ID,
    QUERIES.GEOMETRY AS QUERIES_GEOM,
    OBJECTS.GEOMETRY AS OBJECTS_GEOM
FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOMETRY, OBJECTS.GEOMETRY, 4, FALSE, 3.0)
```

Explanation:

This query is similar to  Example 1 but this example includes an additional parameter, `search_radius = 3.0`. With this parameter, only neighbors within 3.0 units of each query geometry are considered.

The `search_radius` acts as a filter, removing any object geometries that are farther than the specified radius.

When `search_radius` is applied, the query may return fewer than `K` neighbors if there aren’t enough points within the radius.

Output:

```
+--------+-----------------+-------------+
|QUERY_ID|QUERIES_GEOM     |OBJECTS_GEOM |
+--------+-----------------+-------------+
|2       |POINT (10 10)    |POINT (9 8)  |
|2       |POINT (10 10)    |POINT (9 8)  |
|3       |POINT (-0.5 -0.5)|POINT (-1 -1)|
|3       |POINT (-0.5 -0.5)|POINT (-1 -1)|
|3       |POINT (-0.5 -0.5)|POINT (-3 1) |
|3       |POINT (-0.5 -0.5)|POINT (-3 1) |
|1       |POINT (1 1)      |POINT (-1 -1)|
|1       |POINT (1 1)      |POINT (-1 -1)|
+--------+-----------------+-------------+
```

In this output:

Some object points from the previous result set are missing because they fall outside the 3.0-unit search radius.

For some queries (e.g., `QUERY_ID = 2`), fewer than 4 neighbors are returned since not enough points are within the specified radius.

The `search_radius` ensures more focused results by excluding distant neighbors, which can be useful for scenarios requiring spatial proximity.

## ST_AKNN

Introduction: join operation to find the k-nearest neighbors of a point or region in a spatial dataset.

Format: `ST_AKNN(R: Table, S: Table, k: Integer, use_sphere: Boolean, search_radius: Double)`

The `ST_AKNN` function is similar to `ST_KNN`, but it uses approximate algorithms to find the k-nearest neighbors. This can be useful for large datasets where exact kNN search is computationally expensive. The trade-off is that approximate algorithms may not always return the exact k-nearest neighbors, but they provide a good approximation in a reasonable amount of time.

Filter Pushdown Considerations:

Filter pushdown behavior in ST_AKNN is similar to that in ST_KNN. If filters are applied to the resulting DataFrame, they might be pushed down to the object side reader before the kNN join is executed. To apply filters after the join, ensure you materialize the kNN join results before applying any filters.

SQL Example

```sql
SELECT
    QUERIES.ID AS QUERY_ID,
    QUERIES.GEOMETRY AS QUERIES_GEOM,
    OBJECTS.GEOMETRY AS OBJECTS_GEOM
FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOMETRY, OBJECTS.GEOMETRY, 4, FALSE)
```
