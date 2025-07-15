/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.core.utils;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.enums.JoinSpartitionDominantSide;
import org.apache.sedona.core.enums.JoinSubdivideMode;
import org.apache.sedona.core.enums.SpatialJoinOptimizationMode;
import org.apache.sedona.core.spatialOperator.Subdivide;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitionerBuilder.SpatialPartitionBuildingStrategy;
import org.apache.sedona.core.spatialRddTool.AdvancedStatCollector;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.Utils;
import org.locationtech.jts.geom.Envelope;

public class SedonaConf implements Serializable {
  private final String REVERSE_GEOCODE_DISTANCE_PREFIX = "spark.sedona.reverse.geocode.distance.";

  // Global parameters of Sedona. All these parameters can be initialized through SparkConf.

  private boolean useIndex;

  private IndexType indexType;

  // Parameters for JoinQuery including RangeJoin and DistanceJoin

  private JoinSpartitionDominantSide joinSparitionDominantSide;

  private JoinBuildSide joinBuildSide;

  private long joinApproximateTotalCount;

  private Envelope datasetBoundary;

  private int fallbackPartitionNum;

  private GridType joinGridType;

  private long autoBroadcastJoinThreshold;

  private long adaptiveAutoBroadcastJoinThreshold;

  private SpatialJoinOptimizationMode spatialJoinOptimizationMode;

  private boolean useAdvancedSpatialJoin;

  // Internal parameters for self-driving optimized spatial join
  private long maxSamplesForSpatialPartitioning;
  private long minSamplesForSpatialPartitioning;
  private double minSamplingRate;
  private double sizeEstimationSampleGrowthRate;
  private int considerTopKLargestGeometries;
  private long expectedPerPartitionCount;
  private int maxGuessedPartitionNumber;
  private SpatialPartitionBuildingStrategy spatialPartitionBuildingStrategy;
  private int maxSamplesForAdaptiveBroadcastJoinExecutionMode;

  // Parameters for setting external (spill-able) spatial index
  private boolean useExternalSpatialIndex;
  private int externalSpatialIndexLeafPageCapacity;
  private int externalSpatialIndexInternalNodeCapacity;
  private boolean forceSpillExternalSpatialIndex;

  // Parameters for enabling auto-subdividing when running spatial joins
  private JoinSubdivideMode spatialJoinSubdivideLeft;
  private JoinSubdivideMode spatialJoinSubdivideRight;
  private Subdivide.SubdivideRDDOptions leftSubdivideRDDOptions;
  private Subdivide.SubdivideRDDOptions rightSubdivideRDDOptions;
  private JoinSubdivideMode localJoinSubdivideLeft;
  private JoinSubdivideMode localJoinSubdivideRight;
  private SubdivideOptions leftLocalJoinSubdivideOptions;
  private SubdivideOptions rightLocalJoinSubdivideOptions;

  // Internal Parameters for automatic subdivide parameter tuning
  private int subdivideDuplicationFactorThreshold;
  private long perPartitionShuffleWriteSizeThreshold;
  private int subdivideNumPointsThreshold;
  private double subdivideCollisionFactorThreshold;
  private double subdivideNonPolygonalCollisionFactorThreshold;
  private double subdivideExtentSizeRatioThreshold;

  // Parameters for debugging spatial partitioning
  private boolean enableMetricsForSpatialPartitioning;
  private String spatialPartitionerSavePath;

  // Parameters for knn joins
  private boolean includeTieBreakersInKNNJoins = false;
  private double skewnessCutoffRatioInKNNJoins = 1.0;
  private int skewnessMinimumMBRCountInKNNJoins = 100;
  private int skewnessMaximumMBRDividesInKNNJoins = 100;
  private boolean enableParallelPartitioningInKNNJoins = true;
  private int maxRowsPerPartitionInKNNJoins = 524288;

  // Parameters for geocoding
  private String reverseGeocodingTableName;
  private Map<String, Double> reverseGeocodingDistanceThresholds;
  private Boolean reverseGeocodingAssertLayerExists;

  // Parameters for geostats
  private Boolean DBSCANIncludeOutliers = true;
  private Boolean LOFApproximateKNN = false;

  // Parameter for adaptive broadcast join
  private boolean allowPlanBroadcastJoin;
  private boolean autoReBalanceStreamSide;
  private double streamSideSkewScoreThreshold;
  private double streamSideUnderPartitioningThreshold;
  private long streamSideIdealPartitionSize;

  // Parameters for raster loading
  private boolean enableRasterLoadAutoRepartition;
  private int rasterLoadNumPartitions;
  private long rasterLoadPerPartitionSize;
  private int rasterLoadingParallelism;

  // Parameters for libpostal integration
  private String libPostalDataDir;
  private Boolean libPostalUseSenzing = false;

  public static SedonaConf fromActiveSession() {
    return new SedonaConf(SparkSession.active().conf());
  }

  public static SedonaConf fromSparkEnv() {
    return new SedonaConf(SparkEnv.get().conf());
  }

  private interface ConfGetter {
    String get(String key, String defaultValue);

    String get(String key);

    boolean contains(String key);

    java.util.Map<String, String> getAll();
  }

  public SedonaConf(SparkConf sparkConf) {
    this(
        new ConfGetter() {
          @Override
          public String get(String key, String defaultValue) {
            return sparkConf.get(key, defaultValue);
          }

          @Override
          public String get(String key) {
            return sparkConf.get(key, null);
          }

          public boolean contains(String key) {
            return sparkConf.contains(key);
          }

          @Override
          public java.util.Map<String, String> getAll() {
            java.util.Map<String, String> map = new java.util.HashMap<>();
            for (scala.Tuple2<String, String> t : sparkConf.getAll()) {
              map.put(t._1(), t._2());
            }
            return map;
          }
        });
  }

  public SedonaConf(RuntimeConfig runtimeConfig) {
    this(
        new ConfGetter() {
          @Override
          public String get(String key, String defaultValue) {
            return runtimeConfig.get(key, defaultValue);
          }

          @Override
          public String get(String key) {
            return runtimeConfig.get(key, null);
          }

          @Override
          public boolean contains(String key) {
            return runtimeConfig.contains(key);
          }

          @Override
          public java.util.Map<String, String> getAll() {
            return scala.collection.JavaConverters.mapAsJavaMap(runtimeConfig.getAll());
          }
        });
  }

  private SedonaConf(ConfGetter confGetter) {
    this.useIndex = Boolean.parseBoolean(getConfigValue(confGetter, "global.index", "true"));
    this.indexType =
        IndexType.getIndexType(getConfigValue(confGetter, "global.indextype", "rtree"));
    this.joinApproximateTotalCount =
        Long.parseLong(getConfigValue(confGetter, "join.approxcount", "-1"));
    String[] boundaryString = getConfigValue(confGetter, "join.boundary", "0,0,0,0").split(",");
    this.datasetBoundary =
        new Envelope(
            Double.parseDouble(boundaryString[0]),
            Double.parseDouble(boundaryString[1]),
            Double.parseDouble(boundaryString[2]),
            Double.parseDouble(boundaryString[3]));
    this.joinGridType =
        GridType.getGridType(getConfigValue(confGetter, "join.gridtype", "kdbtree"));
    this.joinBuildSide =
        JoinBuildSide.getBuildSide(getConfigValue(confGetter, "join.indexbuildside", "left"));
    this.joinSparitionDominantSide =
        JoinSpartitionDominantSide.getJoinSparitionDominantSide(
            getConfigValue(confGetter, "join.spatitionside", "left"));
    this.fallbackPartitionNum =
        Integer.parseInt(getConfigValue(confGetter, "join.numpartition", "-1"));
    String joinThreshold =
        getConfigValue(
            confGetter,
            "join.autoBroadcastJoinThreshold",
            confGetter.get("spark.sql.autoBroadcastJoinThreshold"));
    this.autoBroadcastJoinThreshold = bytesFromString(joinThreshold);
    this.adaptiveAutoBroadcastJoinThreshold =
        bytesFromString(
            getConfigValue(confGetter, "join.adaptiveAutoBroadcastJoinThreshold", joinThreshold));
    this.spatialJoinOptimizationMode =
        SpatialJoinOptimizationMode.getSpatialJoinOptimizationMode(
            getConfigValue(confGetter, "join.optimizationmode", "nonequi"));

    // Above are Apache Sedona parameters.
    // Everything below are Wherobots-DB parameters

    this.useAdvancedSpatialJoin =
        Boolean.parseBoolean(confGetter.get("spark.sedona.join.advanced", "true"));
    if (this.useAdvancedSpatialJoin) {
      // Always use R-Tree index for advanced spatial join, even for broadcast indexed join.
      this.useIndex = true;
      this.indexType = IndexType.RTREE;
    }

    // Internal parameters for advanced, self-driving optimized spatial join. Users usually do not
    // need to tune these parameters.
    this.maxSamplesForSpatialPartitioning =
        Long.parseLong(
            confGetter.get(
                "spark.sedona.join.maxSamplesForSpatialPartitioning",
                Long.toString(AdvancedStatCollector.DEFAULT_MAX_SAMPLES)));
    this.minSamplesForSpatialPartitioning =
        Long.parseLong(
            confGetter.get("spark.sedona.join.minSamplesForSpatialPartitioning", "10000"));
    this.minSamplingRate =
        Double.parseDouble(
            confGetter.get(
                "spark.sedona.join.minSamplingRate",
                Double.toString(AdvancedStatCollector.DEFAULT_MIN_SAMPLING_RATE)));
    this.sizeEstimationSampleGrowthRate =
        Double.parseDouble(
            confGetter.get(
                "spark.sedona.join.sizeEstimationSampleGrowthRate",
                Double.toString(AdvancedStatCollector.DEFAULT_SIZE_ESTIMATION_SAMPLE_GROWTH_RATE)));
    this.considerTopKLargestGeometries =
        Integer.parseInt(
            confGetter.get(
                "spark.sedona.join.subdivide.considerTopKLargestGeometries",
                Integer.toString(AdvancedStatCollector.DEFAULT_TOP_K_LARGEST_GEOMETRIES)));
    this.expectedPerPartitionCount =
        Long.parseLong(confGetter.get("spark.sedona.join.expectedPerPartitionCount", "10000000"));
    this.maxGuessedPartitionNumber =
        Integer.parseInt(confGetter.get("spark.sedona.join.maxGuessedPartitionNumber", "-1"));
    if (this.maxGuessedPartitionNumber == -1) {
      // If maxGuessedPartitionNumber is not set, we use 10 times the total number of executor cores
      // as the default value.
      int totalExecutorCores =
          Integer.parseInt(confGetter.get("spark.executor.instances", "1"))
              * Integer.parseInt(confGetter.get("spark.executor.cores", "1"));
      this.maxGuessedPartitionNumber = Math.max(10 * totalExecutorCores, 10000);
    }
    this.spatialPartitionBuildingStrategy =
        SpatialPartitionBuildingStrategy.valueOf(
            confGetter
                .get("spark.sedona.join.spatialPartitionBuildingStrategy", "subsampling")
                .toUpperCase(Locale.ROOT));
    this.maxSamplesForAdaptiveBroadcastJoinExecutionMode =
        Integer.parseInt(
            confGetter.get(
                "spark.sedona.join.maxSamplesForAdaptiveBroadcastJoinExecutionMode", "10"));

    // Parameters for setting external (spill-able) spatial index
    this.useExternalSpatialIndex =
        Boolean.parseBoolean(confGetter.get("spark.sedona.join.useExternalSpatialIndex", "true"));
    this.externalSpatialIndexLeafPageCapacity =
        Integer.parseInt(
            confGetter.get("spark.sedona.join.externalSpatialIndexLeafPageCapacity", "100"));
    this.externalSpatialIndexInternalNodeCapacity =
        Integer.parseInt(
            confGetter.get("spark.sedona.join.externalSpatialIndexInternalNodeCapacity", "10"));
    this.forceSpillExternalSpatialIndex =
        Boolean.parseBoolean(
            confGetter.get("spark.sedona.join.forceSpillExternalSpatialIndex", "false"));

    // Parameters for enabling auto-subdividing when running spatial joins
    this.spatialJoinSubdivideLeft =
        JoinSubdivideMode.getJoinSubdivideMode(
            confGetter.get("spark.sedona.join.subdivideLeft", "auto"));
    boolean keepRowData =
        Boolean.parseBoolean(
            confGetter.get("spark.sedona.join.subdivideLeft.keepRowData", "false"));
    // Options for pre-spatial-partitioning subdivide
    SubdivideOptions options = readSubdivideOptions(confGetter, "spark.sedona.join.subdivideLeft");
    this.leftSubdivideRDDOptions = new Subdivide.SubdivideRDDOptions(options, false, keepRowData);
    // Options for local join subdivide
    this.localJoinSubdivideLeft =
        JoinSubdivideMode.getJoinSubdivideMode(
            confGetter.get("spark.sedona.join.subdivideLeftInLocalJoin", "auto"));
    this.leftLocalJoinSubdivideOptions =
        readSubdivideOptions(confGetter, "spark.sedona.join.subdivideLeftInLocalJoin");

    this.spatialJoinSubdivideRight =
        JoinSubdivideMode.getJoinSubdivideMode(
            confGetter.get("spark.sedona.join.subdivideRight", "auto"));
    keepRowData =
        Boolean.parseBoolean(
            confGetter.get("spark.sedona.join.subdivideRight.keepRowData", "false"));
    // Options for pre-spatial-partitioning subdivide
    options = readSubdivideOptions(confGetter, "spark.sedona.join.subdivideRight");
    this.rightSubdivideRDDOptions = new Subdivide.SubdivideRDDOptions(options, false, keepRowData);
    // Options for local join subdivide
    this.localJoinSubdivideRight =
        JoinSubdivideMode.getJoinSubdivideMode(
            confGetter.get("spark.sedona.join.subdivideRightInLocalJoin", "auto"));
    this.rightLocalJoinSubdivideOptions =
        readSubdivideOptions(confGetter, "spark.sedona.join.subdivideRightInLocalJoin");

    // Internal parameters for automatic subdivide parameter tuning
    this.subdivideDuplicationFactorThreshold =
        Integer.parseInt(
            confGetter.get("spark.sedona.join.subdivideDuplicationFactorThreshold", "5"));
    this.perPartitionShuffleWriteSizeThreshold =
        bytesFromString(
            confGetter.get(
                "spark.sedona.join.subdividePerPartitionShuffleWriteSizeThreshold", "20gb"));
    this.subdivideNumPointsThreshold =
        Integer.parseInt(confGetter.get("spark.sedona.join.subdivideNumPointsThreshold", "100"));
    this.subdivideCollisionFactorThreshold =
        Double.parseDouble(
            confGetter.get("spark.sedona.join.localSubdivideCollisionFactorThreshold", "5"));
    this.subdivideNonPolygonalCollisionFactorThreshold =
        Double.parseDouble(
            confGetter.get(
                "spark.sedona.join.localSubdivideNonPolygonalCollisionFactorThreshold", "0.5"));
    this.subdivideExtentSizeRatioThreshold =
        Double.parseDouble(
            confGetter.get("spark.sedona.join.localSubdivideExtentSizeRatioThreshold", "0"));

    // Parameters for debugging
    this.enableMetricsForSpatialPartitioning =
        Boolean.parseBoolean(
            confGetter.get("spark.sedona.join.debug.enableMetricsForSpatialPartitioning", "false"));
    this.spatialPartitionerSavePath =
        confGetter.get("spark.sedona.join.debug.spatialPartitionerSavePath", "");

    // Parameters for knn joins
    this.includeTieBreakersInKNNJoins =
        Boolean.parseBoolean(confGetter.get("spark.sedona.join.knn.includeTieBreakers", "false"));

    this.skewnessCutoffRatioInKNNJoins =
        Double.parseDouble(confGetter.get("spark.sedona.join.knn.skewnessCutoffRatio", "1.0"));

    this.skewnessMinimumMBRCountInKNNJoins =
        Integer.parseInt(confGetter.get("spark.sedona.join.knn.skewnessMinimumMBRCount", "100"));

    this.skewnessMaximumMBRDividesInKNNJoins =
        Integer.parseInt(confGetter.get("spark.sedona.join.knn.skewnessMaximumMBRDivides", "100"));

    this.enableParallelPartitioningInKNNJoins =
        Boolean.parseBoolean(
            confGetter.get("spark.sedona.join.knn.enableParallelPartitioning", "true"));

    this.maxRowsPerPartitionInKNNJoins =
        Integer.parseInt(confGetter.get("spark.sedona.join.knn.maxRowsPerPartition", "524288"));

    // If this is disabled, we will never generate broadcast index join plan for spatial join.
    // This does not completely disable broadcast index join. If the statistics retrieved in the
    // analyze phase show that one of the relation is smaller than
    // spark.sedona.join.adaptiveAutoBroadcastJoinThreshold, we will switch to broadcast index join
    // at query running time. This also allow us to automatically repartition the stream relation
    // if skew is detected by the analyze phase.
    this.allowPlanBroadcastJoin =
        Boolean.parseBoolean(confGetter.get("spark.sedona.join.allowPlanBroadcastJoin", "true"));

    // When the spatial join physical executor determines to use broadcast index join at query
    // running time after analyzing joined datasets, we can choose to repartition the stream side
    // if it is skewed or underpartitioned. This is useful when joining very unbalanced data with
    // a small dataset. This configuration is disabled by default since it may introduce performance
    // regression to existing workload when enabled.
    this.autoReBalanceStreamSide =
        Boolean.parseBoolean(confGetter.get("spark.sedona.join.autoReBalanceStreamSide", "true"));

    // When the spatial join physical executor determines to use broadcast index join at query
    // running time after analyzing joined datasets, we can choose to re-balance the stream side
    // if it is skewed. This threshold determines how skewed the data needs to be to trigger
    // automatic repartitioning.
    this.streamSideSkewScoreThreshold =
        Double.parseDouble(confGetter.get("spark.sedona.join.streamSideSkewScoreThreshold", "2.0"));

    // When the spatial join physical executor determines to use broadcast index join at query
    // running time after analyzing joined datasets, we can choose to repartition the stream side
    // if it is underpartitioned relative to the cluster parallelism. This threshold determines how
    // underpartitioned the data needs to be to trigger automatic repartitioning.
    // The value is a ratio of the number of partitions in the stream side to the value returned by
    // org.apache.sedona.core.utils.ExecutorResourceUtils.inferParallelism
    this.streamSideUnderPartitioningThreshold =
        Double.parseDouble(
            confGetter.get("spark.sedona.join.streamSideUnderPartitioningThreshold", "0.25"));

    // When the spatial join physical executor determines to use broadcast index join at query
    // running time after analyzing joined datasets, we can choose to repartition the stream side
    // if it is underpartitioned relative to the cluster parallelism. This value is used to
    // calculate the ideal (minimum) number of records per partition. This prevents
    // over-partitioning the stream side when the data size is small.
    this.streamSideIdealPartitionSize =
        Long.parseLong(confGetter.get("spark.sedona.join.streamSideIdealPartitionSize", "10000"));

    // Parameters for raster loading

    // Automatically repartition the loaded DataFrame when using
    // spark.read.format("raster").load(...)
    // to load rasters. This is for re-balancing the tiles evenly to multiple executor cores.
    this.enableRasterLoadAutoRepartition =
        Boolean.parseBoolean(confGetter.get("spark.sedona.raster.load.autoRepartition", "true"));

    // The number of partitions to repartition the DataFrame loaded by
    // spark.read.format("raster").load(...). This is only effective when
    // spark.sedona.raster.load.autoRepartition is true. If not set, the default value is 0, which
    // means that the number of partitions will be determined on the fly based on the size of
    // the DataFrame and the number of available cores.
    this.rasterLoadNumPartitions =
        Integer.parseInt(confGetter.get("spark.sedona.raster.load.numPartitions", "0"));

    // The size of each partition when repartitioning the DataFrame loaded by
    // spark.read.format("raster").load(...). This is only effective when
    // spark.sedona.raster.load.autoRepartition is true and Spark dynamic allocation is enabled.
    this.rasterLoadPerPartitionSize =
        bytesFromString(confGetter.get("spark.sedona.raster.load.perPartitionSize", "500mb"));

    // The number of threads used to load the metadata of out-db rasters in parallel when using
    // spark.read.format("raster").load(...). This is only effective when retile is true, or
    // Sedona detected that raster metadata is needed right away after loading. If not set, the
    // default value is 4 * number of available processors.
    int defaultRasterLoadingParallelism = Runtime.getRuntime().availableProcessors() * 4;
    this.rasterLoadingParallelism =
        Integer.parseInt(
            confGetter.get(
                "spark.sedona.raster.load.parallelism",
                Integer.toString(defaultRasterLoadingParallelism)));

    this.reverseGeocodingTableName =
        confGetter.get(
            "spark.sedona.reverse.geocode.table",
            "wherobots_open_data.overture_maps_foundation.geocodes");

    this.reverseGeocodingDistanceThresholds =
        initializeReverseGeocodingDistanceThresholds(confGetter);

    this.reverseGeocodingAssertLayerExists =
        Boolean.parseBoolean(confGetter.get("spark.sedona.reverse.geocode.assert.layers", "true"));

    this.DBSCANIncludeOutliers =
        Boolean.parseBoolean(confGetter.get("spark.sedona.dbscan.includeOutliers", "true"));

    this.LOFApproximateKNN =
        Boolean.parseBoolean(confGetter.get("spark.sedona.lof.approximateKNN", "false"));

    // Parameters for libpostal integration
    String libPostalDataDir =
        confGetter.get(
            "spark.sedona.libpostal.dataDir",
            Paths.get(System.getProperty("java.io.tmpdir"))
                .resolve(Paths.get("libpostal"))
                .toString());
    if (!libPostalDataDir.isEmpty() && !libPostalDataDir.endsWith("/")) {
      libPostalDataDir = libPostalDataDir + "/";
    }
    this.libPostalDataDir = libPostalDataDir;

    this.libPostalUseSenzing =
        Boolean.parseBoolean(confGetter.get("spark.sedona.libpostal.useSenzing", "true"));
  }

  private Map<String, Double> initializeReverseGeocodingDistanceThresholds(ConfGetter confGetter) {
    Map<String, Double> reverseGeocodingDistanceThresholds =
        confGetter.getAll().entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(REVERSE_GEOCODE_DISTANCE_PREFIX))
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().substring(REVERSE_GEOCODE_DISTANCE_PREFIX.length()),
                    entry -> Double.parseDouble(entry.getValue())));

    // These default values are designed for overture
    if (!reverseGeocodingDistanceThresholds.containsKey("default")) {
      reverseGeocodingDistanceThresholds.put("default", 0.0);
    }

    if (reverseGeocodingDistanceThresholds.size() == 1) {
      reverseGeocodingDistanceThresholds.put("places", 0.0006); // ~20 feet
      reverseGeocodingDistanceThresholds.put("addresses", 0.0003);
    }

    return reverseGeocodingDistanceThresholds;
  }

  private SubdivideOptions readSubdivideOptions(ConfGetter confGetter, String prefix) {
    int maxCoordinates = Integer.parseInt(confGetter.get(prefix + ".maxCoordinates", "1000"));
    double maxWidth = Double.parseDouble(confGetter.get(prefix + ".maxWidth", "0.1"));
    double maxHeight = Double.parseDouble(confGetter.get(prefix + ".maxHeight", "0.1"));
    int maxDepth = Integer.parseInt(confGetter.get(prefix + ".maxDepth", "50"));
    SubdivideOptions.MultiPointSubDivider multiPointSubDivider =
        SubdivideOptions.MultiPointSubDivider.valueOf(
            confGetter.get(prefix + ".multiPointSubDivider", "decompose").toUpperCase(Locale.ROOT));
    SubdivideOptions.LineStringSubDivider lineStringSubDivider =
        SubdivideOptions.LineStringSubDivider.valueOf(
            confGetter
                .get(prefix + ".lineStringSubDivider", "cut_segments")
                .toUpperCase(Locale.ROOT));
    SubdivideOptions.PolygonSubDivider polygonSubDivider =
        SubdivideOptions.PolygonSubDivider.valueOf(
            confGetter.get(prefix + ".polygonSubDivider", "box_approx").toUpperCase(Locale.ROOT));
    return new SubdivideOptions(
        maxCoordinates,
        maxWidth,
        maxHeight,
        maxDepth,
        multiPointSubDivider,
        lineStringSubDivider,
        polygonSubDivider);
  }

  // Helper method to prioritize `sedona.*` over `spark.sedona.*`
  private String getConfigValue(ConfGetter confGetter, String keySuffix, String defaultValue) {
    String sedonaKey = "sedona." + keySuffix;
    String sparkSedonaKey = "spark.sedona." + keySuffix;

    if (confGetter.contains(sedonaKey)) {
      return confGetter.get(sedonaKey, defaultValue);
    } else {
      return confGetter.get(sparkSedonaKey, defaultValue);
    }
  }

  public boolean getUseIndex() {
    return useIndex;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public long getJoinApproximateTotalCount() {
    return joinApproximateTotalCount;
  }

  public Envelope getDatasetBoundary() {
    return datasetBoundary;
  }

  public JoinBuildSide getJoinBuildSide() {
    return joinBuildSide;
  }

  public GridType getJoinGridType() {
    return joinGridType;
  }

  public JoinSpartitionDominantSide getJoinSparitionDominantSide() {
    return joinSparitionDominantSide;
  }

  public int getFallbackPartitionNum() {
    return fallbackPartitionNum;
  }

  public long getAutoBroadcastJoinThreshold() {
    return autoBroadcastJoinThreshold;
  }

  public long getAdaptiveAutoBroadcastJoinThreshold() {
    return adaptiveAutoBroadcastJoinThreshold;
  }

  public String toString() {
    try {
      String sb = "";
      Class<?> objClass = this.getClass();
      sb += "Sedona Configuration:\n";
      Field[] fields = objClass.getDeclaredFields();
      for (Field field : fields) {
        String name = field.getName();
        Object value = field.get(this);
        sb += name + ": " + value.toString() + "\n";
      }
      return sb;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  static long bytesFromString(String str) {
    if (str == null || str.isEmpty()) {
      return 0;
    }
    if (str.startsWith("-")) {
      return -1 * Utils.byteStringAsBytes(str.substring(1));
    } else {
      return Utils.byteStringAsBytes(str);
    }
  }

  public SpatialJoinOptimizationMode getSpatialJoinOptimizationMode() {
    return spatialJoinOptimizationMode;
  }

  public boolean useAdvancedSpatialJoin() {
    return useAdvancedSpatialJoin;
  }

  public long getMaxSamplesForSpatialPartitioning() {
    return maxSamplesForSpatialPartitioning;
  }

  public long getMinSamplesForSpatialPartitioning() {
    return minSamplesForSpatialPartitioning;
  }

  public double getMinSamplingRate() {
    return minSamplingRate;
  }

  public double getSizeEstimationSampleGrowthRate() {
    return sizeEstimationSampleGrowthRate;
  }

  public long getExpectedPerPartitionCount() {
    return expectedPerPartitionCount;
  }

  public int getMaxGuessedPartitionNumber() {
    return maxGuessedPartitionNumber;
  }

  public SpatialPartitionBuildingStrategy getSpatialPartitionBuildingStrategy() {
    return spatialPartitionBuildingStrategy;
  }

  public int getMaxSamplesForAdaptiveBroadcastJoinExecutionMode() {
    return maxSamplesForAdaptiveBroadcastJoinExecutionMode;
  }

  public JoinSubdivideMode getSpatialJoinSubdivideLeft() {
    return spatialJoinSubdivideLeft;
  }

  public JoinSubdivideMode getSpatialJoinSubdivideRight() {
    return spatialJoinSubdivideRight;
  }

  public Subdivide.SubdivideRDDOptions getLeftSubdivideRDDOptions() {
    return leftSubdivideRDDOptions;
  }

  public Subdivide.SubdivideRDDOptions getRightSubdivideRDDOptions() {
    return rightSubdivideRDDOptions;
  }

  public JoinSubdivideMode getLocalJoinSubdivideLeft() {
    return localJoinSubdivideLeft;
  }

  public JoinSubdivideMode getLocalJoinSubdivideRight() {
    return localJoinSubdivideRight;
  }

  public SubdivideOptions getLeftLocalJoinSubdivideOptions() {
    return leftLocalJoinSubdivideOptions;
  }

  public SubdivideOptions getRightLocalJoinSubdivideOptions() {
    return rightLocalJoinSubdivideOptions;
  }

  public int getSubdivideConsiderTopKLargestGeometries() {
    return considerTopKLargestGeometries;
  }

  public int getSubdivideDuplicationFactorThreshold() {
    return subdivideDuplicationFactorThreshold;
  }

  public long perPartitionShuffleWriteSizeThreshold() {
    return perPartitionShuffleWriteSizeThreshold;
  }

  public int getSubdivideNumPointsThreshold() {
    return subdivideNumPointsThreshold;
  }

  public double getSubdivideCollisionFactorThreshold() {
    return subdivideCollisionFactorThreshold;
  }

  public double getSubdivideNonPolygonalCollisionFactorThreshold() {
    return subdivideNonPolygonalCollisionFactorThreshold;
  }

  public double getSubdivideExtentSizeRatioThreshold() {
    return subdivideExtentSizeRatioThreshold;
  }

  public boolean metricsForSpatialPartitioningEnabled() {
    return enableMetricsForSpatialPartitioning;
  }

  public String getSpatialPartitionerSavePath() {
    return spatialPartitionerSavePath;
  }

  public boolean isIncludeTieBreakersInKNNJoins() {
    return includeTieBreakersInKNNJoins;
  }

  public double getSkewnessCutoffRatioInKNNJoins() {
    return skewnessCutoffRatioInKNNJoins;
  }

  public int getSkewnessMinimumMBRCountInKNNJoins() {
    return skewnessMinimumMBRCountInKNNJoins;
  }

  public int getSkewnessMaximumMBRDividesInKNNJoins() {
    return skewnessMaximumMBRDividesInKNNJoins;
  }

  public boolean isEnableParallelPartitioningInKNNJoins() {
    return enableParallelPartitioningInKNNJoins;
  }

  public int getMaxRowsPerPartitionInKNNJoins() {
    return maxRowsPerPartitionInKNNJoins;
  }

  public boolean allowPlanBroadcastJoin() {
    return allowPlanBroadcastJoin;
  }

  public boolean autoReBalanceStreamSide() {
    return autoReBalanceStreamSide;
  }

  public String getReverseGeocodingTableName() {
    return reverseGeocodingTableName;
  }

  public Map<String, Double> getReverseGeocodingDistanceThresholds() {
    return reverseGeocodingDistanceThresholds;
  }

  public Boolean getReverseGeocodingAssertLayerExists() {
    return reverseGeocodingAssertLayerExists;
  }

  public Boolean getDBSCANIncludeOutliers() {
    return DBSCANIncludeOutliers;
  }

  public Boolean getLOFApproximateKNN() {
    return LOFApproximateKNN;
  }

  public boolean useExternalSpatialIndex() {
    return useExternalSpatialIndex;
  }

  public int getExternalSpatialIndexLeafPageCapacity() {
    return externalSpatialIndexLeafPageCapacity;
  }

  public int getExternalSpatialIndexInternalNodeCapacity() {
    return externalSpatialIndexInternalNodeCapacity;
  }

  public boolean forceSpillExternalSpatialIndex() {
    return forceSpillExternalSpatialIndex;
  }

  public double getStreamSideSkewScoreThreshold() {
    return streamSideSkewScoreThreshold;
  }

  public double getStreamSideUnderPartitioningThreshold() {
    return streamSideUnderPartitioningThreshold;
  }

  public long getStreamSideIdealPartitionSize() {
    return streamSideIdealPartitionSize;
  }

  public int getRasterLoadingParallelism() {
    return rasterLoadingParallelism;
  }

  public int getRasterLoadNumPartitions() {
    return rasterLoadNumPartitions;
  }

  public long getRasterLoadPerPartitionSize() {
    return rasterLoadPerPartitionSize;
  }

  public boolean isEnableRasterLoadAutoRepartition() {
    return enableRasterLoadAutoRepartition;
  }

  public String getLibPostalDataDir() {
    return libPostalDataDir;
  }

  public Boolean getLibPostalUseSenzing() {
    return libPostalUseSenzing;
  }
}
