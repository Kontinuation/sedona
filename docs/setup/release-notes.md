!!!warning
	Support of Spark 2.X and Scala 2.11 was removed in Sedona 1.3.0+ although some parts of the source code might still be compatible. Sedona 1.3.0+ releases binary for both Scala 2.12 and 2.13.

!!!danger
	Sedona Python currently only works with Shapely 1.x. If you use GeoPandas, please use <= GeoPandas `0.11.1`. GeoPandas > 0.11.1 will automatically install Shapely 2.0. If you use Shapely, please use <= `1.8.4`.

## Sedona 1.4.1

Sedona 1.4.1 is compiled against, Spark 3.3 / Spark 3.4 / Flink 1.12, Java 8.

### Highlights

* [X] **Sedona Spark** More raster functions and bridge RasterUDT and Map Algebra operators. See [Raster based operators](../../api/sql/Raster-operators/#raster-based-operators) and [Raster to Map Algebra operators](../../api/sql/Raster-operators/#raster-to-map-algebra-operators).
* [X] **Sedona Spark & Flink** Added geodesic / geography functions:
    * ST_DistanceSphere
    * ST_DistanceSpheroid
    * ST_AreaSpheroid
    * ST_LengthSpheroid
* [X] **Sedona Spark & Flink** Introduced `SedonaContext` to unify Sedona entry points.
* [X] **Sedona Spark** Support Spark 3.4.
* [X] **Sedona Spark** Added a number of new ST functions.
* [X] **Zeppelin** Zeppelin helium plugin supports ploting geometries like linestring, polygon.

### API change

* **Sedona Spark & Flink** Introduced a new entry point called SedonaContext to unify all Sedona entry points in different compute engines and deprecate old Sedona register entry points. Users no longer have to register Sedona kryo serializer and import many tedious Python classes.
    * **Sedona Spark**:
        * Scala:
        ```scala
        import org.apache.sedona.spark.SedonaContext
        val sedona = SedonaContext.create(SedonaContext.builder().master("local[*]").getOrCreate())
        sedona.sql("SELECT ST_GeomFromWKT(XXX) FROM")
        ```
        * Python:
        ```python
        from sedona.spark import *

        config = SedonaContext.builder().\
           config('spark.jars.packages',
               'org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.4.1,'
               'org.datasyslab:geotools-wrapper:1.4.0-28.2'). \
           getOrCreate()
        sedona = SedonaContext.create(config)
        sedona.sql("SELECT ST_GeomFromWKT(XXX) FROM")
        ```
    * **Sedona Flink**:
    ```java
    import org.apache.sedona.flink.SedonaContext
    StreamTableEnvironment sedona = SedonaContext.create(env, tableEnv);
    sedona.sqlQuery("SELECT ST_GeomFromWKT(XXX) FROM")
    ```

### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-266'>SEDONA-266</a>] -         RS_Values throws UnsupportedOperationException for shuffled point arrays
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-267'>SEDONA-267</a>] -         Cannot pip install apache-sedona 1.4.0 from source distribution
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-273'>SEDONA-273</a>] -         Set a upper bound for Shapely, Pandas and GeoPandas
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-277'>SEDONA-277</a>] -         Sedona spark artifacts for scala 2.13 do not have proper POMs
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-283'>SEDONA-283</a>] -         Artifacts were deployed twice when running mvn clean deploy
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-284'>SEDONA-284</a>] -         Property values in dependency deduced POMs for shaded modules were not substituted
</li>
</ul>

### New Feature

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-196'>SEDONA-196</a>] -         Add ST_Force3D to Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-239'>SEDONA-239</a>] -         Implement ST_NumPoints
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-264'>SEDONA-264</a>] -         zeppelin helium plugin supports ploting geometry like linestring, polygon
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-280'>SEDONA-280</a>] -         Add ST_GeometricMedian
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-281'>SEDONA-281</a>] -         Support geodesic / geography functions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-286'>SEDONA-286</a>] -         Support optimized distance join on ST_DistanceSpheroid and ST_DistanceSphere
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-287'>SEDONA-287</a>] -         Use SedonaContext to unify Sedona entry points
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-292'>SEDONA-292</a>] -         Bridge Sedona Raster and Map Algebra operators
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-297'>SEDONA-297</a>] -         Implement ST_NRings
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-302'>SEDONA-302</a>] -         Implement ST_Translate
</li>
</ul>

### Improvement

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-167'>SEDONA-167</a>] -         Add __pycache__ to Python .gitignore
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-265'>SEDONA-265</a>] -         Migrate all ST functions to Sedona Inferred Expressions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-269'>SEDONA-269</a>] -         Add data source for writing binary files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-270'>SEDONA-270</a>] -         Remove redundant serialization for rasters
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-271'>SEDONA-271</a>] -         Add raster function RS_SRID
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-274'>SEDONA-274</a>] -         Move all ST function logics to Sedona common
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-275'>SEDONA-275</a>] -         Add raster function RS_SetSRID
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-276'>SEDONA-276</a>] -         Add support for Spark 3.4
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-279'>SEDONA-279</a>] -         Sedona-Flink should not depend on Sedona-Spark modules
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-282'>SEDONA-282</a>] -         R – Add raster write function
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-290'>SEDONA-290</a>] -         RDD Spatial Joins should follow the iterator model
</li>
</ul>

## Sedona 1.4.0

Sedona 1.4.0 is compiled against, Spark 3.3 / Flink 1.12, Java 8.

### Highlights

* [X] **Sedona Spark** Pushdown spatial predicate on GeoParquet to reduce memory consumption by 10X: see [explanation](../../api/sql/Optimizer/#geoparquet)
* [X] **Sedona Spark & Flink** Serialize and deserialize geometries 3 - 7X faster
* [X] **Sedona Spark** Automatically use broadcast index spatial join for small datasets
* [X] **Sedona Spark** New RasterUDT added to Sedona GeoTiff reader.
* [X] **Sedona Spark** A number of bug fixes and improvement to the Sedona R module.

### API change

* **Sedona Spark & Flink** Packaging strategy changed. See [Maven Coordinate](../maven-coordinates). Please change your Sedona dependencies if needed. We recommend `sedona-spark-shaded-3.0_2.12-1.4.0` and `sedona-flink-shaded-3.0_2.12-1.4.0`
* **Sedona Spark & Flink** GeoTools-wrapper version upgraded. Please use `geotools-wrapper-1.4.0-28.2`.

### Behavior change

* **Sedona Flink** Sedona Flink no longer outputs any LinearRing type geometry. All LinearRing are changed to LineString.
* **Sedona Spark** Join optimization strategy changed. Sedona no longer optimizes spatial join when use a spatial predicate together with a equijoin predicate. By default, it prefers equijoin whenever possible. SedonaConf adds a config option called `sedona.join.optimizationmode`, it can be configured as one of the following values:
	* `all`: optimize all joins having spatial predicate in join conditions. This was the behavior of Apache Sedona prior to 1.4.0.
	* `none`: disable spatial join optimization.
	* `nonequi`: only enable spatial join optimization on non-equi joins. This is the default mode.

When `sedona.join.optimizationmode` is configured as `nonequi`, it won't optimize join queries such as `SELECT * FROM A, B WHERE A.x = B.x AND ST_Contains(A.geom, B.geom)`, since it is an equi-join with equi-condition `A.x = B.x`. Sedona will optimize for `SELECT * FROM A, B WHERE A.x = B.x AND ST_Contains(A.geom, B.geom)`


### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-218'>SEDONA-218</a>] -         Flaky test caused by improper handling of null struct values in Adapter.toDf
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-221'>SEDONA-221</a>] -         Outer join throws NPE for null geometries
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-222'>SEDONA-222</a>] -         GeoParquet reader does not work in non-local mode
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-224'>SEDONA-224</a>] -         java.lang.NoSuchMethodError when loading GeoParquet files using Spark 3.0.x ~ 3.2.x
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-225'>SEDONA-225</a>] -         Cannot count dataframes loaded from GeoParquet files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-227'>SEDONA-227</a>] -         Python SerDe Performance Degradation
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-230'>SEDONA-230</a>] -         rdd.saveAsGeoJSON should generate feature properties with field names
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-233'>SEDONA-233</a>] -         Incorrect results for several joins in a single stage
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-236'>SEDONA-236</a>] -         Flakey python tests in tests.serialization.test_[de]serializers
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-242'>SEDONA-242</a>] -         Update jars dependencies in Sedona R to Sedona 1.4.0 version
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-250'>SEDONA-250</a>] -         R Deprecate use of Spark 2.4
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-252'>SEDONA-252</a>] -         Fix disabled RS_Base64 test
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-255'>SEDONA-255</a>] -         R – Translation issue for ST_Point and ST_PolygonFromEnvelope
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-258'>SEDONA-258</a>] -         Cannot directly assign raw spatial RDD to CircleRDD using Python binding
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-259'>SEDONA-259</a>] -         Adapter.toSpatialRdd in Python binding does not have valid implementation for specifying custom field names for user data
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-261'>SEDONA-261</a>] -         Cannot run distance join using broadcast index join when the distance expression references to attributes from the right-side relation
</li>
</ul>
        
### New Feature

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-156'>SEDONA-156</a>] -         predicate pushdown support for GeoParquet
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-215'>SEDONA-215</a>] -         Add ST_ConcaveHull
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-216'>SEDONA-216</a>] -         Upgrade jts version to 1.19.0
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-235'>SEDONA-235</a>] -         Create ST_S2CellIds in Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-246'>SEDONA-246</a>] -         R GeoTiff read/write
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-254'>SEDONA-254</a>] -         R – Add raster type
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-262'>SEDONA-262</a>] -         Don&#39;t optimize equi-join by default, add an option to configure when to optimize spatial joins
</li>
</ul>
        
<h2>        Improvement
</h2>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-205'>SEDONA-205</a>] -         Use BinaryType in GeometryUDT in Sedona Spark
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-207'>SEDONA-207</a>] -         Faster serialization/deserialization of geometry objects
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-212'>SEDONA-212</a>] -         Move shading to separate maven modules
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-217'>SEDONA-217</a>] -         Automatically broadcast small datasets
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-220'>SEDONA-220</a>] -         Upgrade Ubuntu build image from 18.04 to 20.04
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-226'>SEDONA-226</a>] -         Support reading and writing GeoParquet file metadata
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-228'>SEDONA-228</a>] -         Standardize logging dependencies
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-231'>SEDONA-231</a>] -         Redundant Serde Removal
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-234'>SEDONA-234</a>] -         ST_Point inconsistencies
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-243'>SEDONA-243</a>] -         Improve Sedona R file readers: GeoParquet and Shapefile
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-244'>SEDONA-244</a>] -         Align R read/write functions with the Sparklyr framework
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-249'>SEDONA-249</a>] -         Add jvm flags for running tests on Java 17 
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-251'>SEDONA-251</a>] -         Add raster type to Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-253'>SEDONA-253</a>] -         Upgrade geotools to version 28.2
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-260'>SEDONA-260</a>] -         More intuitive configuration of partition and index-build side of spatial joins in Sedona SQL
</li>
</ul>

## Sedona 1.3.1

This version is a minor release on Sedoma 1.3.0 line. It fixes a few critical bugs in 1.3.0. We suggest all 1.3.0 users to migrate to this version.

### Bug fixes

* [SEDONA-204](https://issues.apache.org/jira/browse/SEDONA-204) - Init value in X/Y/Z max should be -Double.MAX
* [SEDONA-206](https://issues.apache.org/jira/browse/SEDONA-206) - Performance regression of ST_Transform in 1.3.0-incubating
* [SEDONA-210](https://issues.apache.org/jira/browse/SEDONA-210) - 1.3.0-incubating doesn't work with Scala 2.12 sbt projects
* [SEDONA-211](https://issues.apache.org/jira/browse/SEDONA-211) - Enforce release managers to use JDK 8
* [SEDONA-201](https://issues.apache.org/jira/browse/SEDONA-201) - Implement ST_MLineFromText and ST_MPolyFromText methods


### New Feature

* [SEDONA-196](https://issues.apache.org/jira/browse/SEDONA-196) - Add ST_Force3D to Sedona
* [SEDONA-197](https://issues.apache.org/jira/browse/SEDONA-197) - Add ST_ZMin, ST_ZMax to Sedona
* [SEDONA-199](https://issues.apache.org/jira/browse/SEDONA-199) - Add ST_NDims to Sedona


### Improvement

* [SEDONA-194](https://issues.apache.org/jira/browse/SEDONA-194) - Merge org.datasyslab.sernetcdf into Sedona
* [SEDONA-208](https://issues.apache.org/jira/browse/SEDONA-208) - Use Spark RuntimeConfig in SedonaConf


## Sedona 1.3.0

This version is a major release on Sedona 1.3.0 line and consists of 50 PRs. It includes many new functions, optimization and bug fixes.

### Highlights

- [X] Sedona on Spark in this release is compiled against Spark 3.3.
- [X] Sedona on Flink in this release is compiled against Flink 1.14.
- [X] Scala 2.11 support is removed.
- [X] Spark 2.X support is removed.
- [X] Python 3.10 support is added.
- [X] Aggregators in Flink are added
- [X] Correctness fixes for corner cases in range join and distance join.
- [X] Native GeoParquet read and write (https://sedona.apache.org/tutorial/sql/#load-geoparquet).
    * `df = spark.read.format("geoparquet").option("fieldGeometry", "myGeometryColumn").load("PATH/TO/MYFILE.parquet")`
    * `df.write.format("geoparquet").save("PATH/TO/MYFILE.parquet")`
- [X] DataFrame style API (https://sedona.apache.org/tutorial/sql/#dataframe-style-api)
    * `df.select(ST_Point(min_value, max_value).as("point"))`
- [X] Allow WKT format CRS in ST_Transform
    * `ST_Transform(geom, "srcWktString", "tgtWktString")`

```yaml

GEOGCS["WGS 84",
  DATUM["WGS_1984",
  SPHEROID["WGS 84",6378137,298.257223563,
  AUTHORITY["EPSG","7030"]],
  AUTHORITY["EPSG","6326"]],
  PRIMEM["Greenwich",0,
  AUTHORITY["EPSG","8901"]],
  UNIT["degree",0.0174532925199433,
  AUTHORITY["EPSG","9122"]],
  AUTHORITY["EPSG","4326"]]

```

### Bug fixes

  * [SEDONA-119](https://issues.apache.org/jira/browse/SEDONA-119) - ST_Touches join query returns true for polygons whose interiors intersect
  * [SEDONA-136](https://issues.apache.org/jira/browse/SEDONA-136) - Enable testAsEWKT for Flink
  * [SEDONA-137](https://issues.apache.org/jira/browse/SEDONA-137) - Fix ST_Buffer for Flink to work
  * [SEDONA-138](https://issues.apache.org/jira/browse/SEDONA-138) - Fix ST_GeoHash for Flink to work
  * [SEDONA-153](https://issues.apache.org/jira/browse/SEDONA-153) - Python Serialization Fails with Nulls
  * [SEDONA-158](https://issues.apache.org/jira/browse/SEDONA-158) - Fix wrong description about ST_GeometryN in the API docs
  * [SEDONA-169](https://issues.apache.org/jira/browse/SEDONA-169) - Fix ST_RemovePoint in accordance with the API document
  * [SEDONA-178](https://issues.apache.org/jira/browse/SEDONA-178) - Correctness issue in distance join queries
  * [SEDONA-182](https://issues.apache.org/jira/browse/SEDONA-182) - ST_AsText should not return SRID
  * [SEDONA-186](https://issues.apache.org/jira/browse/SEDONA-186) - collecting result rows of a spatial join query with SELECT * fails with serde error
  * [SEDONA-188](https://issues.apache.org/jira/browse/SEDONA-188) - Python warns about missing `jars` even when some are found
  * [SEDONA-193](https://issues.apache.org/jira/browse/SEDONA-193) - ST_AsBinary produces EWKB by mistake

### New Features

  * [SEDONA-94](https://issues.apache.org/jira/browse/SEDONA-94) - GeoParquet  Support For Sedona
  * [SEDONA-125](https://issues.apache.org/jira/browse/SEDONA-125) - Allows customized CRS in ST_Transform
  * [SEDONA-166](https://issues.apache.org/jira/browse/SEDONA-166) - Provide Type-safe DataFrame Style API
  * [SEDONA-168](https://issues.apache.org/jira/browse/SEDONA-168) - Add ST_Normalize to Apache Sedona
  * [SEDONA-171](https://issues.apache.org/jira/browse/SEDONA-171) - Add ST_SetPoint to Apache Sedona


### Improvement

  * [SEDONA-121](https://issues.apache.org/jira/browse/SEDONA-121) - Add equivalent constructors left over from Spark to Flink
  * [SEDONA-132](https://issues.apache.org/jira/browse/SEDONA-132) - Create common module for SQL functions
  * [SEDONA-133](https://issues.apache.org/jira/browse/SEDONA-133) - Allow user-defined schemas in Adapter.toDf()
  * [SEDONA-139](https://issues.apache.org/jira/browse/SEDONA-139) - Fix wrong argument order in Flink unit tests
  * [SEDONA-140](https://issues.apache.org/jira/browse/SEDONA-140) - Update Sedona Dependencies in R Package
  * [SEDONA-143](https://issues.apache.org/jira/browse/SEDONA-143) - Add missing unit tests for the Flink predicates
  * [SEDONA-144](https://issues.apache.org/jira/browse/SEDONA-144) - Add ST_AsGeoJSON to the Flink API
  * [SEDONA-145](https://issues.apache.org/jira/browse/SEDONA-145) - Fix ST_AsEWKT to reserve the Z coordinate
  * [SEDONA-146](https://issues.apache.org/jira/browse/SEDONA-146) - Add missing output funtions to the Flink API
  * [SEDONA-147](https://issues.apache.org/jira/browse/SEDONA-147) - Add SRID functions to the Flink API
  * [SEDONA-148](https://issues.apache.org/jira/browse/SEDONA-148) - Add boolean functions to the Flink API
  * [SEDONA-149](https://issues.apache.org/jira/browse/SEDONA-149) - Add Python 3.10 support
  * [SEDONA-151](https://issues.apache.org/jira/browse/SEDONA-151) - Add ST aggregators to Sedona Flink
  * [SEDONA-152](https://issues.apache.org/jira/browse/SEDONA-152) - Add reader/writer functions for GML and KML
  * [SEDONA-154](https://issues.apache.org/jira/browse/SEDONA-154) - Add measurement functions to the Flink API
  * [SEDONA-157](https://issues.apache.org/jira/browse/SEDONA-157) - Add coordinate accessors to the Flink API
  * [SEDONA-159](https://issues.apache.org/jira/browse/SEDONA-159) - Add Nth accessor functions to the Flink API
  * [SEDONA-160](https://issues.apache.org/jira/browse/SEDONA-160) - Fix geoparquetIOTests.scala to cleanup after test
  * [SEDONA-161](https://issues.apache.org/jira/browse/SEDONA-161) - Add ST_Boundary to the Flink API
  * [SEDONA-162](https://issues.apache.org/jira/browse/SEDONA-162) - Add ST_Envelope to the Flink API
  * [SEDONA-163](https://issues.apache.org/jira/browse/SEDONA-163) - Better handle of unsupported types in shapefile reader
  * [SEDONA-164](https://issues.apache.org/jira/browse/SEDONA-164) - Add geometry count functions to the Flink API
  * [SEDONA-165](https://issues.apache.org/jira/browse/SEDONA-165) - Upgrade Apache Rat to 0.14
  * [SEDONA-170](https://issues.apache.org/jira/browse/SEDONA-170) - Add ST_AddPoint and ST_RemovePoint to the Flink API
  * [SEDONA-172](https://issues.apache.org/jira/browse/SEDONA-172) - Add ST_LineFromMultiPoint to Apache Sedona
  * [SEDONA-176](https://issues.apache.org/jira/browse/SEDONA-176) - Make ST_Contains conform with OGC standard, and add ST_Covers and ST_CoveredBy functions.
  * [SEDONA-177](https://issues.apache.org/jira/browse/SEDONA-177) - Support spatial predicates other than INTERSECTS and COVERS/COVERED_BY in RangeQuery.SpatialRangeQuery and JoinQuery.SpatialJoinQuery
  * [SEDONA-181](https://issues.apache.org/jira/browse/SEDONA-181) - Build fails with java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$
  * [SEDONA-189](https://issues.apache.org/jira/browse/SEDONA-189) - Prepare geometries in broadcast join
  * [SEDONA-192](https://issues.apache.org/jira/browse/SEDONA-192) - Null handling in predicates
  * [SEDONA-195](https://issues.apache.org/jira/browse/SEDONA-195) - Add wkt validation and an optional srid to ST_GeomFromWKT/ST_GeomFromText

### Task

* [SEDONA-150](https://issues.apache.org/jira/browse/SEDONA-150) - Drop Spark 2.4 and Scala 2.11 support


## Sedona 1.2.1

This version is a maintenance release on Sedona 1.2.0 line. It includes bug fixes.

Sedona on Spark is now compiled against Spark 3.3, instead of Spark 3.2.


### SQL (for Spark)

Bug fixes:

* [SEDONA-104](https://issues.apache.org/jira/browse/SEDONA-104): Bug in reading band values of GeoTiff images
* [SEDONA-118](https://issues.apache.org/jira/browse/SEDONA-118): Fix the wrong result in ST_Within
* [SEDONA-123](https://issues.apache.org/jira/browse/SEDONA-123): Fix the check for invalid lat/lon in ST_GeoHash

Improvement:

* [SEDONA-96](https://issues.apache.org/jira/browse/SEDONA-96): Refactor ST_MakeValid to use GeometryFixer
* [SEDONA-108](https://issues.apache.org/jira/browse/SEDONA-108): Write support for GeoTiff images
* [SEDONA-122](https://issues.apache.org/jira/browse/SEDONA-122): Overload ST_GeomFromWKB for BYTES column
* [SEDONA-127](https://issues.apache.org/jira/browse/SEDONA-127): Add null safety to ST_GeomFromWKT/WKB/Text
* [SEDONA-129](https://issues.apache.org/jira/browse/SEDONA-129): Support Spark 3.3
* [SEDONA-135](https://issues.apache.org/jira/browse/SEDONA-135): Consolidate and upgrade hadoop dependency

New features:

* [SEDONA-107](https://issues.apache.org/jira/projects/SEDONA/issues/SEDONA-107): Add St_Reverse function
* [SEDONA-105](https://issues.apache.org/jira/browse/SEDONA-105): Add ST_PointOnSurface function
* [SEDONA-95](https://issues.apache.org/jira/browse/SEDONA-95): Add ST_Disjoint predicate
* [SEDONA-112](https://issues.apache.org/jira/browse/SEDONA-112): Add ST_AsEWKT
* [SEDONA-106](https://issues.apache.org/jira/browse/SEDONA-106): Add ST_LineFromText
* [SEDONA-117](https://issues.apache.org/jira/browse/SEDONA-117): Add RS_AppendNormalizedDifference
* [SEDONA-97](https://issues.apache.org/jira/browse/SEDONA-97): Add ST_Force_2D
* [SEDONA-98](https://issues.apache.org/jira/browse/SEDONA-98): Add ST_IsEmpty
* [SEDONA-116](https://issues.apache.org/jira/browse/SEDONA-116): Add ST_YMax and ST_Ymin
* [SEDONA-115](https://issues.apache.org/jira/browse/SEDONA-15): Add ST_XMax and ST_Min
* [SEDONA-120](https://issues.apache.org/jira/browse/SEDONA-120): Add ST_BuildArea
* [SEDONA-113](https://issues.apache.org/jira/browse/SEDONA-113): Add ST_PointN
* [SEDONA-124](https://issues.apache.org/jira/browse/SEDONA-124): Add ST_CollectionExtract
* [SEDONA-109](https://issues.apache.org/jira/browse/SEDONA-109): Add ST_OrderingEquals

### Flink

New features:

* [SEDONA-107](https://issues.apache.org/jira/projects/SEDONA/issues/SEDONA-107): Add St_Reverse function
* [SEDONA-105](https://issues.apache.org/jira/browse/SEDONA-105): Add ST_PointOnSurface function
* [SEDONA-95](https://issues.apache.org/jira/browse/SEDONA-95): Add ST_Disjoint predicate
* [SEDONA-112](https://issues.apache.org/jira/browse/SEDONA-112): Add ST_AsEWKT
* [SEDONA-97](https://issues.apache.org/jira/browse/SEDONA-97): Add ST_Force_2D
* [SEDONA-98](https://issues.apache.org/jira/browse/SEDONA-98): Add ST_IsEmpty
* [SEDONA-116](https://issues.apache.org/jira/browse/SEDONA-116): Add ST_YMax and ST_Ymin
* [SEDONA-115](https://issues.apache.org/jira/browse/SEDONA-15): Add ST_XMax and ST_Min
* [SEDONA-120](https://issues.apache.org/jira/browse/SEDONA-120): Add ST_BuildArea
* [SEDONA-113](https://issues.apache.org/jira/browse/SEDONA-113): Add ST_PointN
* [SEDONA-110](https://issues.apache.org/jira/browse/SEDONA-110): Add ST_GeomFromGeoHash
* [SEDONA-121](https://issues.apache.org/jira/browse/SEDONA-124): More ST constructors to Flink
* [SEDONA-122](https://issues.apache.org/jira/browse/SEDONA-122): Overload ST_GeomFromWKB for BYTES column


## Sedona 1.2.0

This version is a major release on Sedona 1.2.0 line. It includes bug fixes and new features: Sedona with Apache Flink.

### RDD

Bug fix:

* [SEDONA-18](https://issues.apache.org/jira/browse/SEDONA-18): Fix an error reading Shapefile
* [SEDONA-73](https://issues.apache.org/jira/browse/SEDONA-73): Exclude scala-library from scala-collection-compat

Improvement:

* [SEDONA-77](https://issues.apache.org/jira/browse/SEDONA-77): Refactor Format readers and spatial partitioning functions to be standalone libraries. So they can be used by Flink and others.

### SQL

New features:

* [SEDONA-4](https://issues.apache.org/jira/browse/SEDONA-4): Handle nulls in SQL functions
* [SEDONA-65](https://issues.apache.org/jira/browse/SEDONA-65): Create ST_Difference function
* [SEDONA-68](https://issues.apache.org/jira/browse/SEDONA-68) Add St_Collect function.
* [SEDONA-82](https://issues.apache.org/jira/browse/SEDONA-82): Create ST_SymmDifference function
* [SEDONA-75](https://issues.apache.org/jira/browse/SEDONA-75): Add support for "3D" geometries: Preserve Z coordinates on geometries when serializing, ST_AsText, ST_Z, ST_3DDistance
* [SEDONA-86](https://issues.apache.org/jira/browse/SEDONA-86): Support empty geometries in ST_AsBinary and ST_AsEWKB
* [SEDONA-90](https://issues.apache.org/jira/browse/SEDONA-90): Add ST_Union
* [SEDONA-100](https://issues.apache.org/jira/browse/SEDONA-100): Add st_multi function

Bug fix:

* [SEDONA-89](https://issues.apache.org/jira/browse/SEDONA-89): GeometryUDT equals should test equivalence of the other object

### Flink

Major update:

* [SEDONA-80](https://issues.apache.org/jira/browse/SEDONA-80): Geospatial stream processing support in Flink Table API
* [SEDONA-85](https://issues.apache.org/jira/browse/SEDONA-85): ST_Geohash function in Flink
* [SEDONA-87](https://issues.apache.org/jira/browse/SEDONA-87): Support Flink Table and DataStream conversion
* [SEDONA-93](https://issues.apache.org/jira/browse/SEDONA-93): Add ST_GeomFromGeoJSON


## Sedona 1.1.1

This version is a maintenance release on Sedona 1.1.X line. It includes bug fixes and a few new functions.

### Global

New feature:

* [SEDONA-73](https://issues.apache.org/jira/browse/SEDONA-73): Scala source code supports Scala 2.13

### SQL

Bug fix:

* [SEDONA-67](https://issues.apache.org/jira/browse/SEDONA-67): Support Spark 3.2

New features:

* [SEDONA-43](https://issues.apache.org/jira/browse/SEDONA-43): Add ST_GeoHash and ST_GeomFromGeoHash
* [SEDONA-45](https://issues.apache.org/jira/browse/SEDONA-45): Add ST_MakePolygon
* [SEDONA-71](https://issues.apache.org/jira/browse/SEDONA-71): Add ST_AsBinary, ST_AsEWKB, ST_SRID, ST_SetSRID



## Sedona 1.1.0

This version is a major release on Sedona 1.1.0 line. It includes bug fixes and new features: R language API, Raster data and Map algebra support

### Global

Dependency upgrade: 

* [SEDONA-30](https://issues.apache.org/jira/browse/SEDONA-30): Use Geotools-wrapper 1.1.0-24.1 to include geotools GeoTiff libraries.

Improvement on join queries in core and SQL:

* [SEDONA-63](https://issues.apache.org/jira/browse/SEDONA-63): Skip empty partitions in NestedLoopJudgement
* [SEDONA-64](https://issues.apache.org/jira/browse/SEDONA-64): Broadcast dedupParams to improve performance

Behavior change:

* [SEDONA-62](https://issues.apache.org/jira/browse/SEDONA-62): Ignore HDF test in order to avoid NASA copyright issue

### Core

Bug fix:

* [SEDONA-41](https://issues.apache.org/jira/browse/SEDONA-41): Fix rangeFilter bug when the leftCoveredByRight para is false
* [SEDONA-53](https://issues.apache.org/jira/browse/SEDONA-53): Fix SpatialKnnQuery NullPointerException

### SQL

Major update:

* [SEDONA-30](https://issues.apache.org/jira/browse/SEDONA-30): Add GeoTiff raster I/O and Map Algebra function

New function:

* [SEDONA-27](https://issues.apache.org/jira/browse/SEDONA-27): Add ST_Subdivide and ST_SubdivideExplode functions

Bug fix:

* [SEDONA-56](https://issues.apache.org/jira/browse/SEDONA-56): Fix broadcast join with Adapter Query Engine enabled
* [SEDONA-22](https://issues.apache.org/jira/browse/SEDONA-22), [SEDONA-60](https://issues.apache.org/jira/browse/SEDONA-60): Fix join queries in SparkSQL when one side has no rows or only one row

### Viz

N/A

### Python

Improvement:

* [SEDONA-59](https://issues.apache.org/jira/browse/SEDONA-59): Make pyspark dependency of Sedona Python optional

Bug fix:

* [SEDONA-50](https://issues.apache.org/jira/browse/SEDONA-50): Remove problematic logging conf that leads to errors on Databricks
* Fix the issue: Spark dependency in setup.py was configured to be < v3.1.0 by mistake.

### R

Major update:

* [SEDONA-31](https://issues.apache.org/jira/browse/SEDONA-31): Add R interface for Sedona
 
## Sedona 1.0.1

This version is a maintenance release on Sedona 1.0.0 line. It includes bug fixes, some new features, one ==API change==

### Known issue

In Sedona v1.0.1 and eariler versions, the Spark dependency in setup.py was configured to be ==< v3.1.0== [by mistake](https://github.com/apache/incubator-sedona/blob/8235924ac80939cbf2ce562b0209b71833ed9429/python/setup.py#L39). When you install  Sedona Python (apache-sedona v1.0.1) from Pypi, pip might unstall PySpark 3.1.1 and install PySpark 3.0.2 on your machine.

Three ways to fix this:

1. After install apache-sedona v1.0.1, unstall PySpark 3.0.2 and reinstall PySpark 3.1.1

2. Ask pip not to install Sedona dependencies: `pip install --no-deps apache-sedona`

3. Install Sedona from the latest setup.py (on GitHub) manually.

### Global

Dependency upgrade:

* [SEDONA-16](https://issues.apache.org/jira/browse/SEDONA-16): Use a GeoTools Maven Central wrapper to fix failed Jupyter notebook examples
* [SEDONA-29](https://issues.apache.org/jira/browse/SEDONA-29): upgrade to Spark 3.1.1
* [SEDONA-33](https://issues.apache.org/jira/browse/SEDONA-33): jts2geojson version from 0.14.3 to 0.16.1

### Core

Bug fix:

* [SEDONA-35](https://issues.apache.org/jira/browse/SEDONA-35): Address user-data mutability issue with Adapter.toDF()

### SQL

Bug fix:

* [SEDONA-14](https://issues.apache.org/jira/browse/SEDONA-14): Saving dataframe to CSV or Parquet fails due to unknown type
* [SEDONA-15](https://issues.apache.org/jira/browse/SEDONA-15): Add ST_MinimumBoundingRadius and ST_MinimumBoundingCircle functions
* [SEDONA-19](https://issues.apache.org/jira/browse/SEDONA-19): Global indexing does not work with SQL joins
* [SEDONA-20](https://issues.apache.org/jira/browse/SEDONA-20): Case object GeometryUDT and GeometryUDT instance not equal in Spark 3.0.2

New function:

* [SEDONA-21](https://issues.apache.org/jira/browse/SEDONA-21): allows Sedona to be used in pure SQL environment
* [SEDONA-24](https://issues.apache.org/jira/browse/SEDONA-24): Add ST_LineSubString and ST_LineInterpolatePoint
* [SEDONA-26](https://issues.apache.org/jira/browse/SEDONA-26): Add broadcast join support


### Viz

Improvement:

* [SEDONA-32](https://issues.apache.org/jira/browse/SEDONA-32): Speed up ST_Render

API change:

* [SEDONA-29](https://issues.apache.org/jira/browse/SEDONA-29): Upgrade to Spark 3.1.1 and fix ST_Pixelize

### Python

Bug fix:

* [SEDONA-19](https://issues.apache.org/jira/browse/SEDONA-19): Global indexing does not work with SQL joins

## Sedona 1.0.0

This version is the first Sedona release since it joins the Apache Incubator. It includes new functions, bug fixes, and ==API changes==.

### Global

Key dependency upgrade:

* [SEDONA-1](https://issues.apache.org/jira/browse/SEDONA-1): upgrade to JTS 1.18
* upgrade to GeoTools 24.0
* upgrade to jts2geojson 0.14.3

Key dependency packaging strategy change:

* JTS, GeoTools, jts2geojson are no longer packaged in Sedona jars. End users need to add them manually. See [here](../maven-coordinates).

Key compilation target change:

* [SEDONA-3](https://issues.apache.org/jira/browse/SEDONA-3): Paths and class names have been changed to Apache Sedona
* [SEDONA-7](https://issues.apache.org/jira/browse/SEDONA-7): build the source code for Spark 2.4, 3.0, Scala 2.11, 2.12, Python 3.7, 3.8, 3.9. See [here](../compile).


### Sedona-core

Bug fix:

* PR [443](https://github.com/apache/incubator-sedona/pull/443): read multiple Shape Files by multiPartitions
* PR [451](https://github.com/apache/incubator-sedona/pull/451) (==API change==): modify CRSTransform to ignore datum shift

New function:

* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8): spatialRDD.flipCoordinates()

API / behavior change:

* PR [488](https://github.com/apache/incubator-sedona/pull/488): JoinQuery.SpatialJoinQuery/DistanceJoinQuery now returns `<Geometry, List>` instead of `<Geometry, HashSet>` because we can no longer use HashSet in Sedona for duplicates removal. All original duplicates in both input RDDs will be preserved in the output.

### Sedona-sql

Bug fix:

* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8) (==API change==): ST_Transform slow due to lock contention. See [here](/api/sql/GeoSparkSQL-Function/#st_transform)
* PR [427](https://github.com/apache/incubator-sedona/pull/427): ST_Point and ST_PolygonFromEnvelope now allows Double type

New function:

* PR [499](https://github.com/apache/incubator-sedona/pull/449): ST_Azimuth, ST_X, ST_Y, ST_StartPoint, ST_Boundary, ST_EndPoint, ST_ExteriorRing, ST_GeometryN, ST_InteriorRingN, ST_Dump, ST_DumpPoints, ST_IsClosed, ST_NumInteriorRings, ST_AddPoint, ST_RemovePoint, ST_IsRing
* PR [459](https://github.com/apache/incubator-sedona/pull/459): ST_LineMerge
* PR [460](https://github.com/apache/incubator-sedona/pull/460): ST_NumGeometries
* PR [469](https://github.com/apache/incubator-sedona/pull/469): ST_AsGeoJSON
* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8): ST_FlipCoordinates

Behavior change:

* PR [480](https://github.com/apache/incubator-sedona/pull/480): Aggregate Functions rewrite for new Aggregator API. The functions can be used as typed functions in code and enable compilation-time type check.

API change:

* [SEDONA-11](https://issues.apache.org/jira/browse/SEDONA-11): Adapter.toDf() will directly generate a geometry type column. ST_GeomFromWKT is no longer needed.

### Sedona-viz

API change: Drop the function which can generate SVG vector images because the required library has an incompatible license and the SVG image is not good at plotting big data

### Sedona Python

API/Behavior change:

* Python-to-Sedona adapter is moved to a separate module. To use Sedona Python, see [here](../overview/#prepare-python-adapter-jar)

New function:

* PR [448](https://github.com/apache/incubator-sedona/pull/448): Add support for partition number in spatialPartitioning function `spatial_rdd.spatialPartitioning(grid_type, NUM_PARTITION)`


## GeoSpark legacy release notes

### v1.3.1

This version includes the official release of GeoSpark Python wrapper. It also contains a number of bug fixes and new functions. The tutorial section provides some articles to explain the usage of GeoSpark Python wrapper.

**GeoSpark Core**

Bug fix:

* Issue #[344](https://github.com/DataSystemsLab/GeoSpark/issues/344) and PR #[365](https://github.com/DataSystemsLab/GeoSpark/pull/365): GeoJSON reader cannot handle "id"
* Issue #[420](https://github.com/DataSystemsLab/GeoSpark/issues/420) and PR #[421](https://github.com/DataSystemsLab/GeoSpark/pull/421): Cannot handle null value in geojson properties
* PR #[422](https://github.com/DataSystemsLab/GeoSpark/pull/422): Use HTTPS to resolve dependencies in Maven Build

New functions:

* Issue #[399](https://github.com/DataSystemsLab/GeoSpark/issues/399) and PR #[401](https://github.com/DataSystemsLab/GeoSpark/pull/401): saveAsWKB
* PR #[402](https://github.com/DataSystemsLab/GeoSpark/pull/402): saveAsWKT

**GeoSpark SQL**

New functions:

* PR #[359](https://github.com/DataSystemsLab/GeoSpark/pull/359): ST_NPoints
* PR #[373](https://github.com/DataSystemsLab/GeoSpark/pull/373): ST_GeometryType
* PR #[398](https://github.com/DataSystemsLab/GeoSpark/pull/398): ST_SimplifyPreserveTopology
* PR #[406](https://github.com/DataSystemsLab/GeoSpark/pull/406): ST_MakeValid
* PR #[416](https://github.com/DataSystemsLab/GeoSpark/pull/416): ST\_Intersection\_aggr

Performance:

* Issue #[345](https://github.com/DataSystemsLab/GeoSpark/issues/345) and PR #[346](https://github.com/DataSystemsLab/GeoSpark/pull/346): the performance issue of Adapter.toDF() function

Bug fix:

* Issue #[395](https://github.com/DataSystemsLab/GeoSpark/issues/395) and PR #[396](https://github.com/DataSystemsLab/GeoSpark/pull/396): Fix the geometry col bug in Adapter

**GeoSpark Viz**

Bug fix:

* Issue #[378](https://github.com/DataSystemsLab/GeoSpark/issues/378) and PR #[379](https://github.com/DataSystemsLab/GeoSpark/pull/379): Classpath issue when integrating GeoSparkViz with s3

**GeoSpark Python**

Add new GeoSpark python wrapper for RDD and SQL APIs

**Contributors (12)**

* Mariano Gonzalez
* Paweł Kociński
* Semen Komissarov
* Jonathan Leitschuh
* Netanel Malka
* Keivan Shahida
* Sachio Wakai
* Hui Wang
* Wrussia
* Jia Yu
* Harry Zhu
* Ilya Zverev


### v1.3.0

This release has been skipped due to a bug in GeoSpark Python wrapper.

### v1.2.0

This version contains numerous bug fixes, new functions, and new GeoSpark module.

**License change**

From MIT to Apache License 2.0

**GeoSpark Core**

Bug fix:

* Issue #[224](https://github.com/DataSystemsLab/GeoSpark/issues/224) load GeoJSON non-spatial attributes.
* Issue #[228](https://github.com/DataSystemsLab/GeoSpark/issues/228) Shapefiel Reader fails to handle UNDEFINED type.
* Issue #[320](https://github.com/DataSystemsLab/GeoSpark/issues/320) Read CSV ArrayIndexOutOfBoundsException

New functions:

* PR #[270](https://github.com/DataSystemsLab/GeoSpark/pull/270) #[298](https://github.com/DataSystemsLab/GeoSpark/pull/298) Add GeoJSON Reader to load GeoJSON with all attributes. See [GeoSpark doc](../tutorial/rdd/#from-geojson) for an example.
* PR #[314](https://github.com/DataSystemsLab/GeoSpark/pull/314) Add WktReader and WkbReader. Their usage is simialr to GeoJSON reader.

**GeoSpark SQL**

Bug fix:

* Issue #[244](https://github.com/DataSystemsLab/GeoSpark/issues/244) JTS side location conflict
* Issue #[245](https://github.com/DataSystemsLab/GeoSpark/issues/245) Drop ST_Circle in 1.2.0
* Issue #[288](https://github.com/DataSystemsLab/GeoSpark/issues/288) ST_isValid fails
* Issue #[321](https://github.com/DataSystemsLab/GeoSpark/issues/321) ST_Point doesn't accept null user data
* PR #[284](https://github.com/DataSystemsLab/GeoSpark/pull/284) ST_Union_Aggr bug
* PR #[331](https://github.com/DataSystemsLab/GeoSpark/pull/331) Adapter doesn't handle null values

New SQL functions:

* ST_IsValid
* ST_PrecisionReduce
* ST_Touches
* ST_Overlaps
* ST_Equals
* ST_Crosses
* ST_IsSimple
* ST_AsText

Behavior / API change:

* GeoSpark Adapter will automatically carry all attributes between DataFrame and RDD. No need to use UUID in SQL ST functions to pass values. Please read [GeoSpark doc](../tutorial/sql/#dataframe-to-spatialrdd).

**GeoSpark Viz**

Bug fix:

* Issue #[231](https://github.com/DataSystemsLab/GeoSpark/issues/231) Pixel NullPointException
* Issue #[234](https://github.com/DataSystemsLab/GeoSpark/issues/234) OutOfMemory for large images

New functions

* Add the DataFrame support. Please read [GeoSpark doc](../tutorial/viz)
* ST_Pixelize
* ST_TileName
* ST_Colorize
* ST_EncodeImage
* ST_Render

Behavior / API change

* GeoSparkViz Maven coordinate changed. You need to specify Spark version. Please read [GeoSpark Maven coordinate](GeoSpark-All-Modules-Maven-Central-Coordinates/#geospark-viz-120-and-later)

**GeoSpark-Zeppelin**

New functions

* Add the support of connecting GeoSpark and Zeppelin
* Add the support of connecting GeoSparkViz and Zeppelin

**Contributors (13)**

Anton Peniaziev, Avshalom Orenstein, Jia Yu, Jordan Perr-Sauer, JulienPeloton, Sergii Mikhtoniuk, Netanel Malka, Rishabh Mishra, sagar1993, Shi-Hao Liu, Serhuela, tociek, Wrussia

### v1.1.3

This version contains a critical bug fix for GeoSpark-core RDD API.

**GeoSpark Core**

* Fixed Issue #[222](https://github.com/DataSystemsLab/GeoSpark/issues/222): geometry toString() method has cumulative non-spatial attributes. See PR #[223](https://github.com/DataSystemsLab/GeoSpark/pull/223)

**GeoSpark SQL**

None

**GeoSpark Viz**

None

### v1.1.2

This version contains several bug fixes and several small improvements.

**GeoSpark Core**

* Added WKB input format support (Issue #[2](https://github.com/DataSystemsLab/GeoSpark/issues/2), [213](https://github.com/DataSystemsLab/GeoSpark/issues/213)): See PR #[203](https://github.com/DataSystemsLab/GeoSpark/pull/203), [216](https://github.com/DataSystemsLab/GeoSpark/pull/216). Thanks for the patch from Lucas C.!
* Added empty constructors for typed SpatialRDDs. This is especially useful when the users want to load a persisted RDD from disk and assemble a typed SpatialRDD by themselves. See PR #[211](https://github.com/DataSystemsLab/GeoSpark/pull/211)
* Fixed Issue #[214](https://github.com/DataSystemsLab/GeoSpark/issues/214): duplicated geometry parts when print each Geometry in a SpatialRDD to a String using toString() method. See PR #[216](https://github.com/DataSystemsLab/GeoSpark/pull/216)

**GeoSpark SQL**

* Added ST_GeomFromWKB expression (Issue #[2](https://github.com/DataSystemsLab/GeoSpark/issues/2)): See PR #[203](https://github.com/DataSystemsLab/GeoSpark/pull/203). Thanks for the patch from Lucas C.!
* Fixed Issue #[193](https://github.com/DataSystemsLab/GeoSpark/issues/193): IllegalArgumentException in RangeJoin: Number of partitions must be >= 0. See PR #[207](https://github.com/DataSystemsLab/GeoSpark/pull/207)
* Fixed Issue #[204](https://github.com/DataSystemsLab/GeoSpark/issues/204): Wrong ST_Intersection result. See PR #[205](https://github.com/DataSystemsLab/GeoSpark/pull/205)
* [For Developer] Separate the expression catalog and the udf registrator to simplify the steps of merging patches among different Spark versions. See PR #[209](https://github.com/DataSystemsLab/GeoSpark/pull/209)

**GeoSpark Viz**

None

### v1.1.1

This release has been skipped due to wrong Maven Central configuration.

### v1.1.0

This version adds very efficient R-Tree and Quad-Tree index serializers and supports Apache Spark and  SparkSQL 2.3. See [Maven Central coordinate](./GeoSpark-All-Modules-Maven-Central-Coordinates) to locate the particular version.

**GeoSpark Core**

* Fixed Issue #[185](https://github.com/DataSystemsLab/GeoSpark/issues/185): CRStransform throws Exception for Bursa wolf parameters. See PR #[189](https://github.com/DataSystemsLab/GeoSpark/pull/189).
* Fixed Issue #[190](https://github.com/DataSystemsLab/GeoSpark/issues/190): Shapefile reader doesn't support Chinese characters (中文字符). See PR #[192](https://github.com/DataSystemsLab/GeoSpark/pull/192).
* Add R-Tree and Quad-Tree index serializer. GeoSpark custom index serializer has around 2 times smaller index size and faster serialization than Apache Spark kryo serializer. See PR #[177](https://github.com/DataSystemsLab/GeoSpark/pull/177).

**GeoSpark SQL**

* Fixed Issue #[194](https://github.com/DataSystemsLab/GeoSpark/issues/194): doesn't support Spark 2.3.
* Fixed Issue #[188](https://github.com/DataSystemsLab/GeoSpark/issues/188):ST_ConvexHull should accept any type of geometry as an input. See PR #[189](https://github.com/DataSystemsLab/GeoSpark/pull/189).
* Add ST_Intersection function. See Issue #[110](https://github.com/DataSystemsLab/GeoSpark/issues/110) and PR #[189](https://github.com/DataSystemsLab/GeoSpark/pull/189).

**GeoSpark Viz**

* Fixed Issue #[154](https://github.com/DataSystemsLab/GeoSpark/issues/154): GeoSpark kryp serializer and GeoSparkViz conflict. See PR #[178](https://github.com/DataSystemsLab/GeoSpark/pull/178)

### v1.0.1
**GeoSpark Core**

* Fixed Issue #[170](https://github.com/DataSystemsLab/GeoSpark/issues/170)

**GeoSpark SQL**

* Fixed Issue #[171](https://github.com/DataSystemsLab/GeoSpark/issues/171)
* Added the support of SparkSQL 2.2. GeoSpark-SQL for Spark 2.1 is published separately ([Maven Coordinates](./GeoSpark-All-Modules-Maven-Central-Coordinates)).

**GeoSpark Viz**
None

---
### v1.0.0
**GeoSpark Core**

* Add GeoSparkConf class to read GeoSparkConf from SparkConf

**GeoSpark SQL**

* Initial release: fully supports SQL/MM-Part3 Spatial SQL standard

**GeoSpark Viz**

* Republish GeoSpark Viz under "GeoSparkViz" folder. All "Babylon" strings have been replaced to "GeoSparkViz"

---

---

---
### v0.9.1 (GeoSpark-core)
* **Bug fixes**: Fixed "Missing values when reading Shapefile": [Issue #141](https://github.com/DataSystemsLab/GeoSpark/issues/141)
* **Performance improvement**: Solved Issue [#91](https://github.com/DataSystemsLab/GeoSpark/issues/91), [#103](https://github.com/DataSystemsLab/GeoSpark/issues/103), [#104](https://github.com/DataSystemsLab/GeoSpark/issues/104), [#125](https://github.com/DataSystemsLab/GeoSpark/issues/125), [#150](https://github.com/DataSystemsLab/GeoSpark/issues/150).
    * Add GeoSpark customized Kryo Serializer to significantly reduce memory footprint. This serializer which follows Shapefile compression rule takes less memory than the default Kryo. See [PR 139](https://github.com/DataSystemsLab/GeoSpark/pull/139).
    * Delete the duplicate removal by using Reference Point concept. This eliminates one data shuffle but still guarantees the accuracy. See [PR 131](https://github.com/DataSystemsLab/GeoSpark/pull/131).
* **New Functionalities added**:
    * **SpatialJoinQueryFlat/DistanceJoinQueryFlat** returns the join query in a flat way following database iteration model: Each row has fixed two members [Polygon, Point]. This API is more efficient for unbalanced length of join results. 
    * The left and right shapes in Range query, Distance query, Range join query, Distance join query can be switched.
    * The index side in Range query, Distance query, Range join query, Distance join query can be switched.
    * The generic SpatialRdd supports heterogenous geometries
    * Add KDB-Tree spatial partitioning method which is more balanced than Quad-Tree
    * Range query, Distance query, Range join query, Distance join query, KNN query supports heterogenous inputs.
### v0.8.2 (GeoSpark-core)
* **Bug fixes**: Fix the shapefile RDD null pointer bug when running in cluster mode. See Issue https://github.com/DataSystemsLab/GeoSpark/issues/115
* **New function added**: Provide granular control to SpatialRDD sampling utils. SpatialRDD has a setter and getter for a parameter called "sampleNumber". The user can manually specify the sample size for spatial partitioning.
### v0.8.1 (GeoSpark-core)
* **Bug fixes**: (1) Fix the blank DBF attribute error when load DBF along with SHX file. (2) Allow user to call CRS transformation function at any time. Previously, it was only allowed in GeoSpark constructors
### v0.8.0 (GeoSpark-core)
* **New input format added**: GeoSpark is able to load and query ESRI ShapeFile (.shp, .shx, .dbf) from local disk and HDFS! Users first need to build a Shapefile RDD by giving Spark Context and an input path then call ShapefileRDD.getSpatialRDD to retrieve Spatial RDD. ([Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/scala/org/datasyslab/geospark/showcase), [Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/java/org/datasyslab/geospark/showcase))
* **Join Query Performance enhancement 1**: GeoSpark provides a new Quad-Tree Spatial Partitioning method to speed up Join Query. Users need to pass GridType.QUADTREE parameter to RDD1.spatialPartitioning() function. Then users need to use RDD1.partitionTree in RDD2.spatialPartitioning() function. This Quad-Tree partitioning method (1) avoids overflowed spatial objects when partitioning spatial objects. (2) checking a spatial object against the Quad-Tree grids is completed in a log complexity tree search. ([Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/scala/org/datasyslab/geospark/showcase), [Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/java/org/datasyslab/geospark/showcase))
* **Join Query Performance enhancement 2**: Internally, GeoSpark uses zipPartitions instead of CoGroup to join two Spatial RDD so that the incurred shuffle overhead decreases.
* **SpatialRDD Initialization Performance enhancement**: GeoSpark uses mapPartition instead of flatMapToPair to generate Spatial Objects. This will speed up the calculation.
* **API changed**: Since it chooses mapPartition API in mappers, GeoSpark no longer supports the old user supplified format mapper. However, if you are using your own format mapper for old GeoSpark version, you just need to add one more loop to fit in GeoSpark 0.8.0. Please see [GeoSpark user supplied format mapper examples](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/java/org/datasyslab/geospark/showcase)
* **Alternative SpatialRDD constructor added**: GeoSpark no longer forces users to provide StorageLevel parameter in their SpatialRDD constructors. This will siginicantly accelerate all Spatial RDD initialization.
 1. If he only needs Spatial Range Query and KNN query, the user can totally remove this parameter from their constructors.
 2. If he needs Spatial Join Query or Distance Join Query but he knows his dataset boundary and approximate total count, the user can also remove StorageLevel parameter and append a Envelope type dataset boundary and an approxmiate total count as additional parameters.
 3. **If he needs Spatial Join Query or Distance Join Query but knows nothing about his dataset**, the user still has to pass StorageLevel parameter.
* **Bug fix**: Fix bug [Issue #97](https://github.com/DataSystemsLab/GeoSpark/issues/97) and [Issue #100](https://github.com/DataSystemsLab/GeoSpark/issues/100).

### v0.1 - v0.7
|      Version     	| Summary                                                                                                                                                                                                               	|
|:----------------:	|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
|0.7.0| **Coordinate Reference System (CRS) Transformation (aka. Coordinate projection) added:** GeoSpark allows users to transform the original CRS (e.g., degree based coordinates such as EPSG:4326 and WGS84) to any other CRS (e.g., meter based coordinates such as EPSG:3857) so that it can accurately process both geographic data and geometrical data. Please specify your desired CRS in GeoSpark Spatial RDD constructor ([Example](https://github.com/DataSystemsLab/GeoSpark/blob/master/core/src/main/scala/org/datasyslab/geospark/showcase/ScalaExample.scala#L221)); **Unnecessary dependencies removed**: NetCDF/HDF support depends on [SerNetCDF](https://github.com/jiayuasu/SerNetCDF). SetNetCDF becomes optional dependency to reduce fat jar size; **Default JDK/JRE change to JDK/JRE 1.8**: To satisfy CRS transformation requirement, GeoSpark is compiled by JDK 1.8 by default; **Bug fix**: fix a small format bug when output spatial RDD to disk.|
|0.6.2| **New input format added:** Add a new input format mapper called EarthdataHDFPointMapper so that GeoSpark can load, query and save NASA Petabytes NetCDF/HDF Earth Data ([Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/scala/org/datasyslab/geospark/showcase),[Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/core/src/main/java/org/datasyslab/geospark/showcase)); **Bug fix:** Print UserData attribute when output Spatial RDDs as GeoJSON or regular text file|
|0.6.1| **Bug fixes:** Fix typos LineString DistanceJoin API|
|0.6.0| **Major updates:** (1) DistanceJoin is merged into JoinQuery. GeoSpark now supports complete DistanceJoin between Points, Polygons, and LineStrings. (2) Add Refine Phase to Spatial Range and Join Query. Use real polygon coordinates instead of its MBR to filter the final results. **API changes:** All spatial range and join  queries now take a parameter called *ConsiderBoundaryIntersection*. This will tell GeoSpark whether returns the objects intersect with windows.|
|0.5.3| **Bug fix:** Fix [Issue #69](https://github.com/DataSystemsLab/GeoSpark/issues/69): Now, if two objects have the same coordinates but different non-spatial attributes (UserData), GeoSpark treats them as different objects.|
|0.5.2| **Bug fix:** Fix [Issue #58](https://github.com/DataSystemsLab/GeoSpark/issues/58) and [Issue #60](https://github.com/DataSystemsLab/GeoSpark/issues/60); **Performance enhancement:** (1) Deprecate all old Spatial RDD constructors. See the JavaDoc [here](http://www.public.asu.edu/~jiayu2/geospark/javadoc/0.5.2/). (2) Recommend the new SRDD constructors which take an additional RDD storage level and automatically cache rawSpatialRDD to accelerate internal SRDD analyze step|
|0.5.1| **Bug fix:** (1) GeoSpark: Fix inaccurate KNN result when K is large (2) GeoSpark: Replace incompatible Spark API call [Issue #55](https://github.com/DataSystemsLab/GeoSpark/issues/55); (3) Babylon: Remove JPG output format temporarily due to the lack of OpenJDK support|
| 0.5.0| **Major updates:** We are pleased to announce the initial version of [Babylon](https://github.com/DataSystemsLab/GeoSpark/tree/master/src/main/java/org/datasyslab/babylon) a large-scale in-memory geospatial visualization system extending GeoSpark. Babylon and GeoSpark are integrated together. You can just import GeoSpark and enjoy! More details are available here: [Babylon GeoSpatial Visualization](https://github.com/DataSystemsLab/GeoSpark/tree/master/src/main/java/org/datasyslab/babylon)|
| 0.4.0| **Major updates:** ([Example](https://github.com/DataSystemsLab/GeoSpark/blob/master/src/main/java/org/datasyslab/geospark/showcase/Example.java)) 1. Refactor constrcutor API usage. 2. Simplify Spatial Join Query API. 3. Add native support for LineStringRDD; **Functionality enhancement:** 1. Release the persist function back to users. 2. Add more exception explanations.
|       0.3.2      	| Functionality enhancement: 1. [JTSplus Spatial Objects](https://github.com/jiayuasu/JTSplus) now carry the original input data. Each object stores "UserData" and provides getter and setter. 2. Add a new SpatialRDD constructor to transform a regular data RDD to a spatial partitioned SpatialRDD.                                                                             	|
|       0.3.1      	| Bug fix: Support Apache Spark 2.X version, fix a bug which results in inaccurate results when doing join query, add more unit test cases                                                                              	|
|        0.3       	| Major updates: Significantly shorten query time on spatial join for skewed data; Support load balanced spatial partitioning methods (also serve as the global index); Optimize code for iterative spatial data mining 	|
|        0.2       	| Improve code structure and refactor API                                                    																																|
|        0.1       	| Support spatial range, join and Knn               |

### GeoSpark-Viz (old)
|      Version     	| Summary                                                                                                                                                                                                               	|
|:----------------:	|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
|0.2.2| **Add the support of new output storage**: Now the user is able to output gigapixel or megapixel resolution images (image tiles or stitched single image) to HDFS and Amazon S3. Please use the new ImageGenerator not the BabylonImageGenerator class.|
|0.2.1| **Performance enhancement**: significantly accelerate single image generation pipeline. **Bug fix**:fix a bug in scatter plot parallel rendering.|
|0.2.0| **API updates for [Issue #80](https://github.com/DataSystemsLab/GeoSpark/issues/80):** 1. Babylon now has two different OverlayOperators for raster image and vector image: RasterOverlayOperator and VectorOverlayOperator; 2. Babylon merged old SparkImageGenerator and NativeJavaGenerator into a new BabylonImageGenerator which has neat APIs; **New feature:** Babylon can use Scatter Plot to visualize NASA Petabytes NetCDF/HDF format Earth Data. ([Scala Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/babylon/src/main/scala/org/datasyslab/geospark/showcase),[Java Example](https://github.com/DataSystemsLab/GeoSpark/tree/master/babylon/src/main/java/org/datasyslab/babylon/showcase))
|0.1.1| **Major updates:** Babylon supports vector image and outputs SVG image format|
|0.1.0| **Major updates:** Babylon initial version supports raster images|
