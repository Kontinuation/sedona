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

package org.apache.sedona.core.showcase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.core.spatialOperator.JoinQuery;
import org.apache.sedona.core.spatialOperator.KNNQuery;
import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialRDD.CircleRDD;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class Example.
 */
public class Example
        implements Serializable
{

    /**
     * The sc.
     */
    public static JavaSparkContext sc;

    /**
     * The geometry factory.
     */
    static GeometryFactory geometryFactory;

    /**
     * The Point RDD input location.
     */
    static String PointRDDInputLocation;

    /**
     * The Point RDD offset.
     */
    static Integer PointRDDOffset;

    /**
     * The Point RDD num partitions.
     */
    static Integer PointRDDNumPartitions;

    /**
     * The Point RDD splitter.
     */
    static FileDataSplitter PointRDDSplitter;

    /**
     * The Point RDD index type.
     */
    static IndexType PointRDDIndexType;

    /**
     * The object RDD.
     */
    static PointRDD objectRDD;

    /**
     * The Polygon RDD input location.
     */
    static String PolygonRDDInputLocation;

    /**
     * The Polygon RDD start offset.
     */
    static Integer PolygonRDDStartOffset;

    /**
     * The Polygon RDD end offset.
     */
    static Integer PolygonRDDEndOffset;

    /**
     * The Polygon RDD num partitions.
     */
    static Integer PolygonRDDNumPartitions;

    /**
     * The Polygon RDD splitter.
     */
    static FileDataSplitter PolygonRDDSplitter;

    /**
     * The query window RDD.
     */
    static PolygonRDD queryWindowRDD;

    /**
     * The join query partitioning type.
     */
    static GridType joinQueryPartitioningType;

    /**
     * The each query loop times.
     */
    static int eachQueryLoopTimes;

    /**
     * The k NN query point.
     */
    static Point kNNQueryPoint;

    /**
     * The range query window.
     */
    static Envelope rangeQueryWindow;

    static String ShapeFileInputLocation;

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("SedonaRunnableExample").setMaster("local[2]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        String resourceFolder = System.getProperty("user.dir") + "/src/test/resources/";

        PointRDDInputLocation = resourceFolder + "arealm-small.csv";
        PointRDDSplitter = FileDataSplitter.CSV;
        PointRDDIndexType = IndexType.RTREE;
        PointRDDNumPartitions = 5;
        PointRDDOffset = 1;

        PolygonRDDInputLocation = resourceFolder + "primaryroads-polygon.csv";
        PolygonRDDSplitter = FileDataSplitter.CSV;
        PolygonRDDNumPartitions = 5;
        PolygonRDDStartOffset = 0;
        PolygonRDDEndOffset = 9;

        geometryFactory = new GeometryFactory();
        kNNQueryPoint = geometryFactory.createPoint(new Coordinate(-84.01, 34.01));
        rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01);
        joinQueryPartitioningType = GridType.QUADTREE;
        eachQueryLoopTimes = 5;

        ShapeFileInputLocation = resourceFolder + "shapefiles/polygon";

        try {
            testSpatialRangeQuery();
            testSpatialRangeQueryUsingIndex();
            testSpatialKnnQuery();
            testSpatialKnnQueryUsingIndex();
            testSpatialJoinQuery();
            testSpatialJoinQueryUsingIndex();
            testDistanceJoinQuery();
            testDistanceJoinQueryUsingIndex();
            testZOrderKnnJoinQuery();
            testLoadShapefileIntoPolygonRDD();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println("DEMOs failed!");
            return;
        }
        sc.stop();
        System.out.println("All DEMOs passed!");
    }

    /**
     * Test spatial range query.
     *
     * @throws Exception the exception
     */
    public static void testSpatialRangeQuery()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true);
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, false).count();
            assert resultSize > -1;
        }
    }

    /**
     * Test spatial range query using index.
     *
     * @throws Exception the exception
     */
    public static void testSpatialRangeQueryUsingIndex()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true);
        objectRDD.buildIndex(PointRDDIndexType, false);
        objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count();
            assert resultSize > -1;
        }
    }

    /**
     * Test spatial knn query.
     *
     * @throws Exception the exception
     */
    public static void testSpatialKnnQuery()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true);
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, false);
            assert result.size() > -1;
        }
    }

    /**
     * Test spatial knn query using index.
     *
     * @throws Exception the exception
     */
    public static void testSpatialKnnQueryUsingIndex()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true);
        objectRDD.buildIndex(PointRDDIndexType, false);
        objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true);
            assert result.size() > -1;
        }
    }

    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    public static void testSpatialJoinQuery()
            throws Exception
    {
        queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true);
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true);

        objectRDD.spatialPartitioning(joinQueryPartitioningType);
        queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

        objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, false, true).count();
            assert resultSize > 0;
        }
    }

    /**
     * Test spatial join query using index.
     *
     * @throws Exception the exception
     */
    public static void testSpatialJoinQueryUsingIndex()
            throws Exception
    {
        queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true);
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true);

        objectRDD.spatialPartitioning(joinQueryPartitioningType);
        queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

        objectRDD.buildIndex(PointRDDIndexType, true);

        objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, true, false).count();
            assert resultSize > 0;
        }
    }

    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    public static void testDistanceJoinQuery()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true);
        CircleRDD queryWindowRDD = new CircleRDD(objectRDD, 0.1);

        objectRDD.spatialPartitioning(GridType.QUADTREE);
        queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

        objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        for (int i = 0; i < eachQueryLoopTimes; i++) {

            long resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, false, true).count();
            assert resultSize > 0;
        }
    }

    /**
     * Test spatial join query using index.
     *
     * @throws Exception the exception
     */
    public static void testDistanceJoinQueryUsingIndex()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true);
        CircleRDD queryWindowRDD = new CircleRDD(objectRDD, 0.1);

        objectRDD.spatialPartitioning(GridType.QUADTREE);
        queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

        objectRDD.buildIndex(IndexType.RTREE, true);

        objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, true, true).count();
            assert resultSize > 0;
        }
    }

    private static void testZOrderKnnJoinQuery()
            throws Exception
    {
        // this test uses the same dataset for both object and query RDD
        // object RDD holds the data (neighbors) to be queried
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, 4);
        // query RDD holds the data to be used as query points
        PointRDD queryRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, 4);

        objectRDD.setNeighborSampleNumber(0);
        objectRDD.spatialPartitioning(GridType.ZORDER);
        queryRDD.setNeighborSampleNumber(0);
        queryRDD.spatialPartitioning(objectRDD.getPartitioner());

        objectRDD.buildIndex(IndexType.RTREE, true);
        queryRDD.buildIndex(IndexType.RTREE, true);

        objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        List<Tuple2<Point, List<Point>>> knnOutputs = JoinQuery.KNNJoinQuery(objectRDD, queryRDD, IndexType.RTREE, 10, DistanceMetric.EUCLIDEAN).collect();
        StringBuilder output = getListContent(knnOutputs);
        assert !output.toString().isEmpty();
        long resultSize = knnOutputs.size();
        assert resultSize > 0;
    }

    private static StringBuilder getListContent(List<Tuple2<Point, List<Point>>> collect) {
        // Use StringBuilder to accumulate the output
        StringBuilder output = new StringBuilder();
        // Iterate over the list and accumulate each tuple into the StringBuilder
        for (Tuple2<Point, List<Point>> tuple : collect) {
            Point queryPoint = tuple._1();
            List<Point> neighbors = tuple._2();
            output.append("Query Point: ").append(queryPoint).append("\n");
            output.append("Neighbors:\n");
            for (Point neighbor : neighbors) {
                output.append("\t").append(neighbor).append("\n");
            }
            output.append("\n"); // Add an empty line for better readability
        }
        return output;
    }

    public static void testLoadShapefileIntoPolygonRDD()
            throws Exception
    {
        SpatialRDD shapefileRDD = ShapefileReader.readToGeometryRDD(sc, ShapeFileInputLocation);
        PolygonRDD spatialRDD = new PolygonRDD(shapefileRDD.rawSpatialRDD);
        try {
            RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), false, false).count();
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
