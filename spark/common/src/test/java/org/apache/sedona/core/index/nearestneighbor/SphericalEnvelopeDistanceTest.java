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
package org.apache.sedona.core.index.nearestneighbor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.sedona.common.sphere.Haversine;
import org.apache.spark.sedona.core.index.nearestneighbor.SphericalEnvelopeDistance;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.index.strtree.ItemBoundable;

public class SphericalEnvelopeDistanceTest {

  private static final GeometryFactory FACTORY = new GeometryFactory();

  private static class DummySphericalEnvelopeDistance implements SphericalEnvelopeDistance {
    double distanceLowerBound(Envelope a, Coordinate b) {
      Geometry geom = FACTORY.createPoint(b);
      ItemBoundable item = new ItemBoundable(geom.getEnvelopeInternal(), geom);
      return distanceLowerBound(a, item);
    }
  }

  private static final DummySphericalEnvelopeDistance INSTANCE =
      new DummySphericalEnvelopeDistance();

  @Test
  public void testNonSphericalEnvelope() {
    double distance =
        INSTANCE.distanceLowerBound(new Envelope(0, 1, -90.1, 0), new Coordinate(0, 0));
    assertEquals(0, distance, 1e-6);
    distance = INSTANCE.distanceLowerBound(new Envelope(0, 1, 0, 90.1), new Coordinate(0, 0));
    assertEquals(0, distance, 1e-6);
  }

  @Test
  public void testNonSphericalPoint() {
    double distance =
        INSTANCE.distanceLowerBound(new Envelope(0, 1, 0, 1), new Coordinate(0, 90.1));
    assertEquals(0, distance, 1e-6);
    distance = INSTANCE.distanceLowerBound(new Envelope(0, 1, 0, 1), new Coordinate(0, -90.1));
    assertEquals(0, distance, 1e-6);
  }

  @Test
  public void testPointInEnvelope() {
    double distance = INSTANCE.distanceLowerBound(new Envelope(0, 1, 0, 1), new Coordinate(0, 0));
    assertEquals(0, distance, 1e-6);
    distance = INSTANCE.distanceLowerBound(new Envelope(0, 1, 0, 1), new Coordinate(0, 1));
    assertEquals(0, distance, 1e-6);
    distance = INSTANCE.distanceLowerBound(new Envelope(0, 1, 0, 1), new Coordinate(0.5, 0.5));
    assertEquals(0, distance, 1e-6);
  }

  @Test
  public void testPointInEnvelopeCrossAntimeridian() {
    // Test envelope crossing antimeridian from east to west (170 to 190)
    Envelope eastWestEnvelope = new Envelope(170, 190, 0, 1);
    double[] testLongitudesInEnvelope = {550, 190, 180, 179, 170, -170, -179, -180, -190, -550};
    double[] testLongitudesNotInEnvelope = {551, 169, -100, 0, 100, -169, -551};

    for (double longitude : testLongitudesInEnvelope) {
      double distance =
          INSTANCE.distanceLowerBound(eastWestEnvelope, new Coordinate(longitude, 0.5));
      assertEquals(0, distance, 1e-6);
    }
    for (double longitude : testLongitudesNotInEnvelope) {
      double distance =
          INSTANCE.distanceLowerBound(eastWestEnvelope, new Coordinate(longitude, 0.5));
      assertTrue(distance > 0);
    }

    // Test envelope crossing antimeridian from west to east (-190 to -170)
    Envelope westEastEnvelope = new Envelope(-190, -170, 0, 1);

    for (double longitude : testLongitudesInEnvelope) {
      double distance =
          INSTANCE.distanceLowerBound(westEastEnvelope, new Coordinate(longitude, 0.5));
      assertEquals(0, distance, 1e-6);
    }
    for (double longitude : testLongitudesNotInEnvelope) {
      double distance =
          INSTANCE.distanceLowerBound(westEastEnvelope, new Coordinate(longitude, 0.5));
      assertTrue(distance > 0);
    }
  }

  @Test
  public void testPointWithinLatitudeRange() {
    Envelope envelope = new Envelope(-70, -60, -60, -50);
    double[] testLatitudes = {-60, -55, -50};
    double[] testLongitudes = {-75, -55};
    for (double latitude : testLatitudes) {
      for (double longitude : testLongitudes) {
        double distance =
            INSTANCE.distanceLowerBound(envelope, new Coordinate(longitude, latitude));
        assertTrue(distance > 0);
        double upperBound = Haversine.distance(longitude, latitude, -70, latitude);
        assertTrue(distance <= upperBound);
        upperBound = Haversine.distance(longitude, latitude, -60, latitude);
        assertTrue(distance <= upperBound);
      }
    }
  }

  @Test
  public void testPointWithinLongitudeRange() {
    Envelope envelope = new Envelope(-70, -60, -60, -50);
    double[] testLongitudes = {-70, -65, -60};
    double[] testLatitudes = {-70, -40};
    for (double longitude : testLongitudes) {
      for (double latitude : testLatitudes) {
        double distance =
            INSTANCE.distanceLowerBound(envelope, new Coordinate(longitude, latitude));
        assertTrue(distance > 0);
        double upperBound = Haversine.distance(longitude, latitude, longitude, -60);
        assertTrue(distance <= upperBound);
        upperBound = Haversine.distance(longitude, latitude, longitude, -50);
        assertTrue(distance <= upperBound);
      }
    }
  }

  @Test
  public void testPointOutSideLongLatRange() {
    Envelope envelope = new Envelope(-70, -60, -60, -50);
    double[] testLongitudes = {-75, -55};
    double[] testLatitudes = {-65, -45};
    for (double longitude : testLongitudes) {
      for (double latitude : testLatitudes) {
        double distance =
            INSTANCE.distanceLowerBound(envelope, new Coordinate(longitude, latitude));
        assertTrue(distance > 0);
        double upperBound = Haversine.distance(longitude, latitude, -70, -60);
        assertTrue(distance <= upperBound);
        upperBound = Haversine.distance(longitude, latitude, -70, -50);
        assertTrue(distance <= upperBound);
        upperBound = Haversine.distance(longitude, latitude, -60, -50);
        assertTrue(distance <= upperBound);
        upperBound = Haversine.distance(longitude, latitude, -60, -60);
        assertTrue(distance <= upperBound);
      }
    }
  }
}
