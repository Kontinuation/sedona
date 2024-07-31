## Usage

SedonaSQL supports many parameters. To change their values,

1. Set it through SparkConf:

```scala
sparkSession = SparkSession.builder().
      config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator").
      config("sedona.global.index","true")
      master("local[*]").appName("mySedonaSQLdemo").getOrCreate()
```

2. Check your current SedonaSQL configuration:

```scala
val sedonaConf = new SedonaConf(sparkSession.conf)
println(sedonaConf)
```

3. Sedona parameters can be changed at runtime:

```scala
sparkSession.conf.set("sedona.global.index","false")
```

## Tuning for Spatial Join

SedonaDB features an advanced spatial join algorithm since v1.2.1, which does not require tuning to achieve good performance. Advanced spatial join would analyze both joined datasets and tune spatial join parameters automatically. The following parameters for tuning spatial join won't work when using advanced spatial join:

* sedona.global.index
* sedona.global.indextype
* sedona.join.indexbuildside
* sedona.join.spatitionside

The advanced spatial join algorithm is enabled by default, users can disable advanced spatial join by setting `sedona.join.advanced` to false and tune spatial join parameters manually.

## Explanation

* sedona.join.advanced
    * Using advanced spatial join algorithm
    * Default: true
    * Possible values: true, false
* sedona.global.index
	* Use spatial index (currently, only supports in SQL range join and SQL distance join), only valid when "sedona.join.advanced" is false
	* Default: true
	* Possible values: true, false
* sedona.global.indextype
	* Spatial index type, only valid when "sedona.global.index" is true and "sedona.join.advanced" is false
	* Default: rtree
	* Possible values: rtree, quadtree
* sedona.join.autoBroadcastJoinThreshold
	* Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join.
      By setting this value to -1 automatic broadcasting can be disabled.
	* Default: The default value is the same as spark.sql.autoBroadcastJoinThreshold
	* Possible values: any integer with a byte suffix i.e. 10MB or 512KB
* sedona.join.gridtype
	* Spatial partitioning grid type for join query
	* Default: kdbtree
	* Possible values: quadtree, kdbtree
* spark.sedona.join.knn.includeTieBreakers
	* KNN join will include all ties in the result, possibly returning more than k results
	* Default: false
	* Possible values: true, false
* sedona.join.indexbuildside **(Advanced users only!)**
	* The side which Sedona builds spatial indices on, only valid when "sedona.join.advanced" is false
	* Default: left
	* Possible values: left, right
* sedona.join.numpartition **(Advanced users only!)**
	* Number of partitions for both sides in a join query
	* Default: -1, in this case it will be automatically tuned according to the size of both datasets when using advanced spatial join algorithm; when not using advanced spatial join it means use the existing partitions of the dominant side.
	* Possible values: any integers
* sedona.join.spatitionside **(Advanced users only!)**
	* The dominant side in spatial partitioning stage, only valid when "sedona.join.advanced" is false
	* Default: left
	* Possible values: left, right
* sedona.join.optimizationmode **(Advanced users only!)**
	* When should Sedona optimize spatial join SQL queries
	* Default: nonequi
	* Possible values:
		* all: Always optimize spatial join queries, even for equi-joins.
		* none: Disable optimization for spatial joins.
		* nonequi: Optimize spatial join queries that are not equi-joins.
* spark.sedona.join.maxSamplesForAdaptiveBroadcastJoinExecutionMode **(Advanced users only!)**
	* Decide the execution mode of local join after observing specified number of geometries on the stream side when running broadcast index join. The more samples observed, the more likely that the optimal execution mode will be selected and it run the rest of the join more efficiently. The downside is that observed samples may be processed by a sub-optimal execution mode, observing too many samples will degrade the spatial join performance.
	* Default: 10
	* Possible values: any non-negative integer
