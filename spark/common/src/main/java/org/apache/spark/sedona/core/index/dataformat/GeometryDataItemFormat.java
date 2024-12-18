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
package org.apache.spark.sedona.core.index.dataformat;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.common.geometryObjects.NullGeometry;
import org.apache.sedona.common.geometrySerde.GeometryBuffer;
import org.apache.sedona.common.geometrySerde.GeometrySerde;
import org.apache.sedona.common.geometrySerde.SerializedCoordinateFilters.StatisticsCollector;
import org.apache.sedona.core.spatialOperator.Subdivide;
import org.apache.sedona.core.spatialPartitioning.OuterJoinSpatialPartitioner.OuterJoinUserData;
import org.apache.sedona.core.wrapper.UniqueGeometry;
import org.apache.spark.sedona.core.index.DataItemFormat;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedGeometry;

/**
 * The data item format for accessing geometry data. This is an adapter for externalizing in-memory
 * STR-tree based spatial indexes. A better approach to support Spark SQL spatial join queries is to
 * use {@code UnsafeRowDataItemFormat}.
 */
public class GeometryDataItemFormat implements DataItemFormat<GeometryDataItem> {

  private final Kryo kryo = new Kryo();
  private final UnsafeOutput out = new UnsafeOutput(1024, -1);
  private final UnsafeInput in = new UnsafeInput(0);
  private final GeometrySerde geometrySerde = new GeometrySerde();
  private final StatisticsCollector statsCollector = new StatisticsCollector();

  public GeometryDataItemFormat() {
    kryo.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy());
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    kryo.setReferences(false);

    kryo.register(Point.class, geometrySerde);
    kryo.register(LineString.class, geometrySerde);
    kryo.register(Polygon.class, geometrySerde);
    kryo.register(MultiPoint.class, geometrySerde);
    kryo.register(MultiLineString.class, geometrySerde);
    kryo.register(MultiPolygon.class, geometrySerde);
    kryo.register(GeometryCollection.class, geometrySerde);
    kryo.register(Circle.class, geometrySerde);
    kryo.register(Envelope.class, geometrySerde);
    kryo.register(NullGeometry.class, geometrySerde);
    kryo.register(UniqueGeometry.class, geometrySerde);
    kryo.register(Subdivide.SubdividedPart.class);
    kryo.register(OuterJoinUserData.class);
    kryo.register(UnsafeRow.class);
    kryo.register(GeometryDataItem.class);
  }

  @Override
  public byte[] serialize(GeometryDataItem dataItem) {
    out.clear();
    geometrySerde.write(kryo, out, dataItem.geometry);
    return out.toBytes();
  }

  public byte[] serialize(Geometry geometry) {
    out.clear();
    geometrySerde.write(kryo, out, geometry);
    return out.toBytes();
  }

  @Override
  public GeometryDataItem deserialize(byte[] bytes) {
    in.setBuffer(bytes);
    Geometry geom = (Geometry) geometrySerde.read(kryo, in, Geometry.class);
    return new GeometryDataItem(geom);
  }

  public Geometry deserializeToGeometry(byte[] bytes) {
    in.setBuffer(bytes);
    return (Geometry) geometrySerde.read(kryo, in, Geometry.class);
  }

  @Override
  public Envelope extractEnvelope(GeometryDataItem dataItem) {
    return dataItem.geometry.getEnvelopeInternal();
  }

  public Envelope extractEnvelope(byte[] serialized) {
    in.setBuffer(serialized);
    GeometryBuffer geometryBuffer = geometrySerde.readGeometryBuffer(new UnsafeInput(serialized));
    statsCollector.reset();
    statsCollector.apply(geometryBuffer);
    return statsCollector.getEnvelope();
  }

  @Override
  public Geometry extractGeometry(GeometryDataItem dataItem) {
    return dataItem.geometry;
  }

  @Override
  public PreparedGeometry extractPreparedGeometry(GeometryDataItem dataItem) {
    return dataItem.getPreparedGeometry();
  }

  /**
   * Handling serialization/deserialization of user data attached to the geometry specially for
   * commonly used data types. This will be faster than using the generic Kryo object reader and
   * writer.
   */
  private enum UserDataType {
    /** No user data */
    NULL(0),
    /**
     * User data is UnsafeRow. This is the most common case when running inner spatial joins using
     * DataFrame/SQL API.
     */
    UNSAFE_ROW(1),
    /** User data when running an outer spatial join. */
    OUTER_JOIN_USER_DATA(2),
    /** User data when running a spatial join with global subdividing. */
    SUBDIVIDED_PART(3),
    /**
     * Any other user data. This won't happen when running spatial joins using DataFrame/SQL API.
     */
    OTHER(4);

    private final int id;

    UserDataType(int id) {
      this.id = id;
    }

    public static UserDataType fromId(int id) {
      switch (id) {
        case 0:
          return NULL;
        case 1:
          return UNSAFE_ROW;
        case 2:
          return OUTER_JOIN_USER_DATA;
        case 3:
          return SUBDIVIDED_PART;
        case 4:
          return OTHER;
        default:
          throw new IllegalArgumentException("Unknown user data type id: " + id);
      }
    }
  }
}
