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

import static org.junit.Assert.*;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.util.SizeEstimator;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

public class GeometrySizeEstimatorTest {
  private static final GeometryFactory factory = new GeometryFactory();

  @Test
  public void estimateSize() {
    Geometry geom = factory.createPoint(new Coordinate(1, 1));
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      sb.append("test");
    }
    String testString = sb.toString();
    UnsafeRow row = new UnsafeRow(100);
    byte[] buf = new byte[1000];
    row.pointTo(buf, buf.length);
    Object[] userDataArray = {
      buf, testString, new int[1000], row,
    };
    for (Object userData : userDataArray) {
      geom.setUserData(userData);
      long size = GeometrySizeEstimator.estimateSize(geom);
      long expectedSize = SizeEstimator.estimate(geom);
      assertTrue(size > expectedSize || Math.abs(expectedSize - size) < expectedSize * 0.1);
    }
  }

  @Test
  public void estimatePointWithoutUserData() {
    Geometry geom = factory.createPoint(new Coordinate(1, 1));
    long size = GeometrySizeEstimator.estimateSizeWithoutUserData(geom);
    long expectedSize = SizeEstimator.estimate(geom);
    assertTrue(size > expectedSize || Math.abs(expectedSize - size) < expectedSize * 0.1);
  }

  @Test
  public void estimateLineStringWithoutUserData() {
    for (int k = 2; k < 100; k++) {
      Coordinate[] coordinates = new Coordinate[k];
      for (int i = 0; i < k; i++) {
        coordinates[i] = new Coordinate(i, i);
      }
      Geometry geom = factory.createLineString(coordinates);
      long size = GeometrySizeEstimator.estimateSizeWithoutUserData(geom);
      long expectedSize = SizeEstimator.estimate(geom);
      assertTrue(size > expectedSize || Math.abs(expectedSize - size) < expectedSize * 0.1);
    }
  }

  @Test
  public void estimatePolygonWithoutUserData() {
    for (int k = 3; k < 100; k++) {
      Coordinate[] coordinates = new Coordinate[k + 1];
      for (int i = 0; i < k; i++) {
        coordinates[i] = new Coordinate(i, i);
      }
      coordinates[k] = coordinates[0];
      Geometry geom = factory.createPolygon(coordinates);
      long size = GeometrySizeEstimator.estimateSizeWithoutUserData(geom);
      long expectedSize = SizeEstimator.estimate(geom);
      assertTrue(size > expectedSize || Math.abs(expectedSize - size) < expectedSize * 0.1);
    }
  }

  @Test
  public void estimatePolygonWithHolesWithoutUserData() {
    Geometry central = factory.createPoint(new Coordinate(1, 1));
    for (int k = 8; k < 100; k++) {
      LinearRing shell = ((Polygon) central.buffer(10, k)).getExteriorRing();
      LinearRing hole = ((Polygon) central.buffer(5, k)).getExteriorRing();
      Geometry geom = factory.createPolygon(shell, new LinearRing[] {hole});
      long size = GeometrySizeEstimator.estimateSizeWithoutUserData(geom);
      long expectedSize = SizeEstimator.estimate(geom);
      assertTrue(Math.abs(expectedSize - size) < expectedSize * 0.1);
    }
  }
}
