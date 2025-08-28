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
package org.apache.sedona.core.spatialOperator;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.core.utils.UniqueIDUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

public class SubdivideTest extends TestBase {

  @BeforeClass
  public static void onceExecutedBeforeAll() throws IOException {
    initialize(SubdivideTest.class.getSimpleName());
  }

  @AfterClass
  public static void teardown() {
    sc.stop();
  }

  @Test
  public void subdivideSpatialRDD() {
    GeometryFactory factory = new GeometryFactory();
    List<Geometry> geoms = new ArrayList<>();
    for (int k = 0; k < 1000; k++) {
      Coordinate[] coordinates = new Coordinate[2];
      coordinates[0] = new Coordinate(k, k);
      coordinates[1] = new Coordinate(k + 10, k + 10);
      LineString lineString = factory.createLineString(coordinates);
      lineString.setUserData(String.format("LineString-%d", k));
      geoms.add(lineString);
    }
    JavaRDD<Geometry> rawSpatialRDD = sc.parallelize(geoms, 10);
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.setRawSpatialRDD(rawSpatialRDD);

    SubdivideOptions options = new SubdivideOptions(1, 1);
    Subdivide.SubdivideRDDOptions subdivideRDDOptions =
        new Subdivide.SubdivideRDDOptions(options, true, true);
    SpatialRDD<Geometry> resultRDD = Subdivide.subdivideSpatialRDD(spatialRDD, subdivideRDDOptions);

    // Basic checks
    assertTrue(resultRDD.rawSpatialRDD.count() > geoms.size());
    assertEquals(
        geoms.size(),
        resultRDD
            .rawSpatialRDD
            .map(
                geom -> {
                  Subdivide.SubdividedPart part = (Subdivide.SubdividedPart) geom.getUserData();
                  return part.id;
                })
            .distinct()
            .count());

    // Subdivide correctness check
    List<Iterable<Geometry>> subdividedGeoms =
        resultRDD
            .rawSpatialRDD
            .groupBy(
                geom -> {
                  Subdivide.SubdividedPart part = (Subdivide.SubdividedPart) geom.getUserData();
                  return part.id;
                })
            .values()
            .collect();
    for (Iterable<Geometry> iterable : subdividedGeoms) {
      Iterator<Geometry> iterator = iterable.iterator();
      Geometry origGeom = null;
      double length = 0;
      while (iterator.hasNext()) {
        Geometry geom = iterator.next();
        Subdivide.SubdividedPart part = (Subdivide.SubdividedPart) geom.getUserData();
        if (origGeom == null) {
          origGeom = part.origGeomWithoutUserData;
        } else {
          assertEquals(origGeom, part.origGeomWithoutUserData);
        }
        assertNull(part.origGeomWithoutUserData.getUserData());
        int k = (int) Math.round(origGeom.getCoordinates()[0].x);
        assertEquals(String.format("LineString-%d", k), part.userData);
        assertTrue(origGeom.covers(geom));
        length += geom.getLength();
      }
      assertNotNull(origGeom);
      assertEquals(origGeom.getLength(), length, 1e-10);
    }
  }

  @Test
  public void attachId() {
    List<String> testData = new ArrayList<>();
    for (int k = 0; k < 1000; k++) {
      testData.add(String.format("test-data-%06d", k));
    }
    JavaRDD<String> rdd = sc.parallelize(testData, 10);
    JavaRDD<Tuple2<Long, String>> resultRdd = UniqueIDUtils.attachId(rdd);

    // Assigned unique IDs should be distinct
    assertEquals(1000, resultRdd.map(Tuple2::_1).distinct().count());

    // Original data should be unchanged
    List<String> resultData =
        resultRdd.map(Tuple2::_2).collect().stream().sorted().collect(Collectors.toList());
    assertEquals(testData, resultData);
  }

  @Test
  public void noOpSubdivideSpatialRDD() {
    GeometryFactory factory = new GeometryFactory();
    List<Geometry> geoms = new ArrayList<>();
    for (int k = 0; k < 1000; k++) {
      Point geom = factory.createPoint(new Coordinate(k, k));
      geom.setUserData(String.format("test-data-%06d", k));
      geoms.add(geom);
    }
    JavaRDD<Geometry> rawSpatialRDD = sc.parallelize(geoms, 10);
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.setRawSpatialRDD(rawSpatialRDD);

    SpatialRDD<Geometry> resultRDD = Subdivide.noOpSubdivideSpatialRDD(spatialRDD);
    JavaRDD<Tuple2<Long, Geometry>> rawSpatialRDDWithId = UniqueIDUtils.attachId(rawSpatialRDD);

    // The two RDDs should be able to be joined properly
    JavaPairRDD<Long, Geometry> keyedResultRDD =
        resultRDD
            .getRawSpatialRDD()
            .keyBy(
                (geom) -> {
                  Subdivide.SubdividedPart part = (Subdivide.SubdividedPart) geom.getUserData();
                  return part.id;
                });
    JavaPairRDD<Long, Tuple2<Tuple2<Long, Geometry>, Geometry>> joinedRdd =
        rawSpatialRDDWithId.keyBy(Tuple2::_1).join(keyedResultRDD);
    assertEquals(1000, joinedRdd.count());
    assertEquals(1000, joinedRdd.keys().distinct().count());

    // Geometry and user data should be unchanged
    List<Tuple2<Long, Tuple2<Tuple2<Long, Geometry>, Geometry>>> joined = joinedRdd.collect();
    for (Tuple2<Long, Tuple2<Tuple2<Long, Geometry>, Geometry>> tuple : joined) {
      Geometry original = tuple._2()._1()._2();
      Geometry subdivided = tuple._2()._2();
      assertEquals(original, subdivided);
      Subdivide.SubdividedPart part = (Subdivide.SubdividedPart) subdivided.getUserData();
      assertEquals(original.getUserData(), part.userData);
    }
  }
}
