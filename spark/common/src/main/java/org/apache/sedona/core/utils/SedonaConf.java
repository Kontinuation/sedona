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

import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.enums.JoinSparitionDominantSide;
import org.apache.sedona.core.enums.JoinSubdivideMode;
import org.apache.sedona.core.enums.SpatialJoinOptimizationMode;
import org.apache.sedona.core.spatialOperator.Subdivide;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitionerBuilder.SpatialPartitionBuildingStrategy;
import org.apache.sedona.core.spatialRddTool.AdvancedStatCollector;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.Utils;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Locale;

public class SedonaConf
        implements Serializable
{

    // Global parameters of Sedona. All these parameters can be initialized through SparkConf.

    private boolean useIndex;

    private IndexType indexType;

    // Parameters for JoinQuery including RangeJoin and DistanceJoin

    private JoinSparitionDominantSide joinSparitionDominantSide;

    private JoinBuildSide joinBuildSide;

    private long joinApproximateTotalCount;

    private Envelope datasetBoundary;

    private int fallbackPartitionNum;

    private GridType joinGridType;

    private long autoBroadcastJoinThreshold;

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
    private long subdivideDupGeomSizeThreshold;
    private int subdivideNumPointsThreshold;
    private double subdivideCollisionFactorThreshold;
    private double subdivideNonPolygonalCollisionFactorThreshold;
    private double subdivideExtentSizeRatioThreshold;

    // Parameters for debugging spatial partitioning
    private boolean enableMetricsForSpatialPartitioning;
    private String spatialPartitionerSavePath;

    public static SedonaConf fromActiveSession() {
        return new SedonaConf(SparkSession.active().conf());
    }

    public SedonaConf(RuntimeConfig runtimeConfig)
    {
        this.useIndex = Boolean.parseBoolean(runtimeConfig.get("sedona.global.index", "true"));
        this.indexType = IndexType.getIndexType(runtimeConfig.get("sedona.global.indextype", "rtree"));
        this.joinApproximateTotalCount = Long.parseLong(runtimeConfig.get("sedona.join.approxcount", "-1"));
        String[] boundaryString = runtimeConfig.get("sedona.join.boundary", "0,0,0,0").split(",");
        this.datasetBoundary = new Envelope(Double.parseDouble(boundaryString[0]), Double.parseDouble(boundaryString[1]),
                Double.parseDouble(boundaryString[2]), Double.parseDouble(boundaryString[3]));
        this.joinGridType = GridType.getGridType(runtimeConfig.get("sedona.join.gridtype", "kdbtree"));
        this.joinBuildSide = JoinBuildSide.getBuildSide(runtimeConfig.get("sedona.join.indexbuildside", "left"));
        this.joinSparitionDominantSide = JoinSparitionDominantSide.getJoinSparitionDominantSide(
                runtimeConfig.get("sedona.join.spatitionside", "left"));
        this.fallbackPartitionNum = Integer.parseInt(runtimeConfig.get("sedona.join.numpartition", "-1"));
        this.autoBroadcastJoinThreshold = bytesFromString(
                runtimeConfig.get("sedona.join.autoBroadcastJoinThreshold",
                        runtimeConfig.get("spark.sql.autoBroadcastJoinThreshold")
                )
        );
        this.spatialJoinOptimizationMode = SpatialJoinOptimizationMode.getSpatialJoinOptimizationMode(
                runtimeConfig.get("sedona.join.optimizationmode", "nonequi"));

        // Above are Apache Sedona parameters.
        // Everything below are Wherobots-DB parameters

        this.useAdvancedSpatialJoin = Boolean.parseBoolean(runtimeConfig.get("spark.sedona.join.advanced", "true"));
        if (this.useAdvancedSpatialJoin) {
            // Always use R-Tree index for advanced spatial join, even for broadcast indexed join.
            this.useIndex = true;
            this.indexType = IndexType.RTREE;
        }

        // Internal parameters for advanced, self-driving optimized spatial join. Users usually do not need to
        // tune these parameters.
        this.maxSamplesForSpatialPartitioning = Long.parseLong(
                runtimeConfig.get("spark.sedona.join.maxSamplesForSpatialPartitioning",
                        Long.toString(AdvancedStatCollector.DEFAULT_MAX_SAMPLES)));
        this.minSamplesForSpatialPartitioning = Long.parseLong(
                runtimeConfig.get("spark.sedona.join.minSamplesForSpatialPartitioning", "10000"));
        this.minSamplingRate = Double.parseDouble(
            runtimeConfig.get("spark.sedona.join.minSamplingRate",
                    Double.toString(AdvancedStatCollector.DEFAULT_MIN_SAMPLING_RATE)));
        this.sizeEstimationSampleGrowthRate = Double.parseDouble(
                runtimeConfig.get("spark.sedona.join.sizeEstimationSampleGrowthRate",
                        Double.toString(AdvancedStatCollector.DEFAULT_SIZE_ESTIMATION_SAMPLE_GROWTH_RATE)));
        this.considerTopKLargestGeometries = Integer.parseInt(
                runtimeConfig.get("spark.sedona.join.subdivide.considerTopKLargestGeometries",
                        Integer.toString(AdvancedStatCollector.DEFAULT_TOP_K_LARGEST_GEOMETRIES)));
        this.expectedPerPartitionCount = Long.parseLong(
                runtimeConfig.get("spark.sedona.join.expectedPerPartitionCount", "10000000"));
        this.maxGuessedPartitionNumber = Integer.parseInt(
                runtimeConfig.get("spark.sedona.join.maxGuessedPartitionNumber", "-1"));
        if (this.maxGuessedPartitionNumber == -1) {
            // If maxGuessedPartitionNumber is not set, we use 10 times the total number of executor cores as the default value.
            int totalExecutorCores = Integer.parseInt(runtimeConfig.get("spark.executor.instances", "1")) *
                    Integer.parseInt(runtimeConfig.get("spark.executor.cores", "1"));
            this.maxGuessedPartitionNumber = Math.max(10 * totalExecutorCores, 10000);
        }
        this.spatialPartitionBuildingStrategy =
                SpatialPartitionBuildingStrategy.valueOf(
                        runtimeConfig.get("spark.sedona.join.spatialPartitionBuildingStrategy", "subsampling")
                                .toUpperCase(Locale.ROOT));

        // Parameters for enabling auto-subdividing when running spatial joins
        this.spatialJoinSubdivideLeft = JoinSubdivideMode.getJoinSubdivideMode(
                runtimeConfig.get("spark.sedona.join.subdivideLeft", "auto"));
        boolean keepRowData = Boolean.parseBoolean(runtimeConfig.get("spark.sedona.join.subdivideLeft.keepRowData", "false"));
        // Options for pre-spatial-partitioning subdivide
        SubdivideOptions options = readSubdivideOptions(runtimeConfig, "spark.sedona.join.subdivideLeft");
        this.leftSubdivideRDDOptions = new Subdivide.SubdivideRDDOptions(options, false, keepRowData);
        // Options for local join subdivide
        this.localJoinSubdivideLeft = JoinSubdivideMode.getJoinSubdivideMode(
                runtimeConfig.get("spark.sedona.join.subdivideLeftInLocalJoin", "auto"));
        this.leftLocalJoinSubdivideOptions = readSubdivideOptions(runtimeConfig, "spark.sedona.join.subdivideLeftInLocalJoin");

        this.spatialJoinSubdivideRight = JoinSubdivideMode.getJoinSubdivideMode(
                runtimeConfig.get("spark.sedona.join.subdivideRight", "auto"));
        keepRowData = Boolean.parseBoolean(runtimeConfig.get("spark.sedona.join.subdivideRight.keepRowData", "false"));
        // Options for pre-spatial-partitioning subdivide
        options = readSubdivideOptions(runtimeConfig, "spark.sedona.join.subdivideRight");
        this.rightSubdivideRDDOptions = new Subdivide.SubdivideRDDOptions(options, false, keepRowData);
        // Options for local join subdivide
        this.localJoinSubdivideRight = JoinSubdivideMode.getJoinSubdivideMode(
                runtimeConfig.get("spark.sedona.join.subdivideRightInLocalJoin", "auto"));
        this.rightLocalJoinSubdivideOptions = readSubdivideOptions(runtimeConfig, "spark.sedona.join.subdivideRightInLocalJoin");

        // Internal parameters for automatic subdivide parameter tuning
        this.subdivideDuplicationFactorThreshold = Integer.parseInt(
                runtimeConfig.get("spark.sedona.join.subdivideDuplicationFactorThreshold", "5"));
        this.subdivideDupGeomSizeThreshold = Long.parseLong(
                runtimeConfig.get("spark.sedona.join.subdivideDupGeomSizeThreshold", Long.toString(1024 * 1024 * 500)));
        this.subdivideNumPointsThreshold = Integer.parseInt(
                runtimeConfig.get("spark.sedona.join.subdivideNumPointsThreshold", "100"));
        this.subdivideCollisionFactorThreshold = Double.parseDouble(
                runtimeConfig.get("spark.sedona.join.localSubdivideCollisionFactorThreshold", "5"));
        this.subdivideNonPolygonalCollisionFactorThreshold = Double.parseDouble(
                runtimeConfig.get("spark.sedona.join.localSubdivideNonPolygonalCollisionFactorThreshold", "0.5"));
        this.subdivideExtentSizeRatioThreshold = Double.parseDouble(
                runtimeConfig.get("spark.sedona.join.localSubdivideExtentSizeRatioThreshold", "0"));

        // Parameters for debugging
        this.enableMetricsForSpatialPartitioning = Boolean.parseBoolean(
                runtimeConfig.get("spark.sedona.join.debug.enableMetricsForSpatialPartitioning", "false"));
        this.spatialPartitionerSavePath = runtimeConfig.get("spark.sedona.join.debug.spatialPartitionerSavePath", "");
    }

    private SubdivideOptions readSubdivideOptions(RuntimeConfig runtimeConfig, String prefix) {
        int maxCoordinates = Integer.parseInt(runtimeConfig.get(prefix + ".maxCoordinates", "1000"));
        double maxWidth = Double.parseDouble(runtimeConfig.get(prefix + ".maxWidth", "0.1"));
        double maxHeight = Double.parseDouble(runtimeConfig.get(prefix + ".maxHeight", "0.1"));
        int maxDepth = Integer.parseInt(runtimeConfig.get(prefix + ".maxDepth", "50"));
        SubdivideOptions.MultiPointSubDivider multiPointSubDivider = SubdivideOptions.MultiPointSubDivider.valueOf(
                runtimeConfig.get(prefix + ".multiPointSubDivider", "decompose").toUpperCase(Locale.ROOT));
        SubdivideOptions.LineStringSubDivider lineStringSubDivider = SubdivideOptions.LineStringSubDivider.valueOf(
                runtimeConfig.get(prefix + ".lineStringSubDivider", "cut_segments").toUpperCase(Locale.ROOT));
        SubdivideOptions.PolygonSubDivider polygonSubDivider = SubdivideOptions.PolygonSubDivider.valueOf(
                runtimeConfig.get(prefix + ".polygonSubDivider", "box_approx").toUpperCase(Locale.ROOT));
        return new SubdivideOptions(maxCoordinates, maxWidth, maxHeight, maxDepth, multiPointSubDivider,
                lineStringSubDivider, polygonSubDivider);
    }

    public boolean getUseIndex()
    {
        return useIndex;
    }

    public IndexType getIndexType()
    {
        return indexType;
    }


    public long getJoinApproximateTotalCount()
    {
        return joinApproximateTotalCount;
    }

    public Envelope getDatasetBoundary()
    {
        return datasetBoundary;
    }

    public JoinBuildSide getJoinBuildSide()
    {
        return joinBuildSide;
    }

    public GridType getJoinGridType()
    {
        return joinGridType;
    }

    public JoinSparitionDominantSide getJoinSparitionDominantSide()
    {
        return joinSparitionDominantSide;
    }

    public int getFallbackPartitionNum()
    {
        return fallbackPartitionNum;
    }

    public long getAutoBroadcastJoinThreshold()
    {
        return autoBroadcastJoinThreshold;
    }

    public String toString()
    {
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
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    static long bytesFromString(String str) {
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

    public long getSubdivideDupGeomSizeThreshold() {
        return subdivideDupGeomSizeThreshold;
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
}
