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
package org.apache.sedona.common.geometryObjects;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateFilter;
import org.locationtech.jts.geom.CoordinateSequenceComparator;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.GeometryFilter;

/**
 * A geometry object that represents a null geometry. This is mainly for performing outer joins,
 * where we have to handle null geometries in SpatialRDDs, but still need to access the user data.
 */
public class NullGeometry extends Geometry {

  private static final GeometryFactory NULL_FACTORY = new GeometryFactory();

  public NullGeometry() {
    super(NULL_FACTORY);
  }

  @Override
  public String getGeometryType() {
    return "Null";
  }

  @Override
  public Coordinate getCoordinate() {
    return null;
  }

  @Override
  public Coordinate[] getCoordinates() {
    return new Coordinate[0];
  }

  @Override
  public int getNumPoints() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public int getDimension() {
    return 0;
  }

  @Override
  public Geometry getBoundary() {
    return null;
  }

  @Override
  public int getBoundaryDimension() {
    return 0;
  }

  @Override
  protected Geometry reverseInternal() {
    return null;
  }

  @Override
  public boolean equalsExact(Geometry other, double tolerance) {
    return false;
  }

  @Override
  public void apply(CoordinateFilter filter) {}

  @Override
  public void apply(CoordinateSequenceFilter filter) {}

  @Override
  public void apply(GeometryFilter filter) {}

  @Override
  public void apply(GeometryComponentFilter filter) {}

  @Override
  protected Geometry copyInternal() {
    return new NullGeometry();
  }

  @Override
  public void normalize() {}

  @Override
  protected Envelope computeEnvelopeInternal() {
    return new Envelope();
  }

  @Override
  protected int compareToSameClass(Object o) {
    return 0;
  }

  @Override
  protected int compareToSameClass(Object o, CoordinateSequenceComparator comp) {
    return 0;
  }

  @Override
  protected int getTypeCode() {
    return 1000; // A type code other than any geometry type defined by JTS
  }
}
