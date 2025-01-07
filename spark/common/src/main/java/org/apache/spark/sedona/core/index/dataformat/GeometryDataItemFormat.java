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
import org.apache.sedona.common.geometrySerde.GeometryBufferFactory;
import org.apache.sedona.common.geometrySerde.GeometrySerializer;
import org.apache.sedona.common.geometrySerde.SerializedCoordinateFilters.StatisticsCollector;
import org.apache.sedona.core.wrapper.UniqueGeometry;
import org.apache.spark.sedona.core.index.DataItemFormat;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;

/**
 * The data item format for accessing geometry data. This is an adapter for externalizing in-memory
 * STR-tree based spatial indexes. It also serves as a better serializer for geometry objects
 * containing user data, since the most commonly used user data types have special treatments to get
 * rid of unnecessary overhead.
 */
public class GeometryDataItemFormat implements DataItemFormat<GeometryDataItem> {

  private final Kryo kryo = new Kryo();
  private final UnsafeOutput out = new UnsafeOutput(1024, -1);
  private final UnsafeInput in = new UnsafeInput(0);
  private final StatisticsCollector statsCollector = new StatisticsCollector();

  public GeometryDataItemFormat() {
    kryo.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy());
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    UserDataSerializer.registerClasses(kryo);
  }

  @Override
  public byte[] serialize(GeometryDataItem dataItem) {
    Geometry geometry = dataItem.geometry;
    return serialize(geometry);
  }

  public byte[] serialize(Geometry geometry) {
    out.clear();
    Object userData = geometry.getUserData();
    if (geometry instanceof NullGeometry) {
      out.writeByte((byte) Type.NULL_GEOMETRY.id);
    } else if (geometry instanceof UniqueGeometry) {
      out.writeByte((byte) Type.UNIQUE_GEOMETRY.id);
      UniqueGeometry<?> uniqueGeometry = (UniqueGeometry<?>) geometry;
      out.writeString(((UniqueGeometry<?>) geometry).getUniqueId());
      byte[] data = GeometrySerializer.serialize((Geometry) uniqueGeometry.getOriginalGeometry());
      out.writeInt(data.length);
      out.write(data, 0, data.length);
    } else if (geometry instanceof Circle) {
      out.writeByte((byte) Type.CIRCLE.id);
      Circle circle = (Circle) geometry;
      out.writeDouble(circle.getRadius());
      Geometry innerGeometry = circle.getCenterGeometry();
      byte[] data = GeometrySerializer.serialize(innerGeometry);
      out.writeInt(data.length);
      out.write(data, 0, data.length);
      UserDataSerializer.write(kryo, out, innerGeometry.getUserData());
    } else {
      out.writeByte((byte) Type.GEOMETRY.id);
      byte[] data = GeometrySerializer.serialize(geometry);
      out.writeInt(data.length);
      out.write(data, 0, data.length);
    }
    UserDataSerializer.write(kryo, out, userData);
    return out.toBytes();
  }

  @Override
  public GeometryDataItem deserialize(byte[] bytes) {
    Geometry geometry = deserializeToGeometry(bytes);
    return new GeometryDataItem(geometry);
  }

  public Geometry deserializeToGeometry(byte[] bytes) {
    in.setBuffer(bytes);

    Geometry geometry;
    Type type = Type.fromId(in.readByte());
    byte[] data;
    switch (type) {
      case NULL_GEOMETRY:
        geometry = new NullGeometry();
        geometry.setUserData(UserDataSerializer.read(kryo, in));
        break;
      case UNIQUE_GEOMETRY:
        String uniqueId = in.readString();
        data = new byte[in.readInt()];
        in.readBytes(data);
        geometry = new UniqueGeometry<>(uniqueId, GeometrySerializer.deserialize(data));
        geometry.setUserData(UserDataSerializer.read(kryo, in));
        break;
      case GEOMETRY:
        data = new byte[in.readInt()];
        in.readBytes(data);
        geometry = GeometrySerializer.deserialize(data);
        geometry.setUserData(UserDataSerializer.read(kryo, in));
        break;
      case CIRCLE:
        double radius = in.readDouble();
        data = new byte[in.readInt()];
        in.readBytes(data);
        Geometry centerGeometry = GeometrySerializer.deserialize(data);
        centerGeometry.setUserData(UserDataSerializer.read(kryo, in));
        Circle circle = new Circle(centerGeometry, radius);
        circle.setUserData(UserDataSerializer.read(kryo, in));
        geometry = circle;
        break;
      default:
        throw new IllegalArgumentException("Unknown type id: " + type.id);
    }

    return geometry;
  }

  @Override
  public Envelope extractEnvelope(GeometryDataItem dataItem) {
    return dataItem.geometry.getEnvelopeInternal();
  }

  public Envelope extractEnvelope(byte[] serialized) {
    in.setBuffer(serialized);

    Type type = Type.fromId(in.readByte());
    byte[] data;
    switch (type) {
      case NULL_GEOMETRY:
        return new Envelope();
      case UNIQUE_GEOMETRY:
        in.readString();
        data = new byte[in.readInt()];
        in.readBytes(data);
        break;
      case GEOMETRY:
        data = new byte[in.readInt()];
        in.readBytes(data);
        break;
      case CIRCLE:
        return deserializeToGeometry(serialized).getEnvelopeInternal();
      default:
        throw new IllegalArgumentException("Unknown type id: " + type.id);
    }

    GeometryBuffer geometryBuffer = GeometryBufferFactory.wrap(data);
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

  private enum Type {
    GEOMETRY(0),
    CIRCLE(1),
    NULL_GEOMETRY(2),
    UNIQUE_GEOMETRY(3);

    private final int id;

    Type(int id) {
      this.id = id;
    }

    public static Type fromId(int id) {
      switch (id) {
        case 0:
          return GEOMETRY;
        case 1:
          return CIRCLE;
        case 2:
          return NULL_GEOMETRY;
        case 3:
          return UNIQUE_GEOMETRY;
        default:
          throw new IllegalArgumentException("Unknown type id: " + id);
      }
    }
  }
}
