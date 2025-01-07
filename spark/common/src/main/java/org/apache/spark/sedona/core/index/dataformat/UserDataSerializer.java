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
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import org.apache.sedona.core.spatialOperator.Subdivide.SubdividedPart;
import org.apache.sedona.core.spatialPartitioning.OuterJoinSpatialPartitioner.OuterJoinUserData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/**
 * Serializer/Deserializer for user data attached to geometries. This is specialized for commonly
 * used data types to improve performance.
 */
public class UserDataSerializer {

  private UserDataSerializer() {}

  /**
   * Register classes for user data serialization/deserialization.
   *
   * @param kryo Kryo instance
   */
  public static void registerClasses(Kryo kryo) {
    kryo.register(UnsafeRow.class);
    kryo.register(SubdividedPart.class);
    kryo.register(OuterJoinUserData.class);
  }

  /**
   * Serialize user data to the output stream.
   *
   * @param kryo Kryo instance
   * @param out Output stream
   * @param userData User data to serialize
   */
  public static void write(Kryo kryo, UnsafeOutput out, Object userData) {
    if (userData == null) {
      out.writeByte(UserDataType.NULL.id);
    } else if (userData instanceof UnsafeRow) {
      out.writeByte(UserDataType.UNSAFE_ROW.id);
      ((UnsafeRow) userData).write(kryo, out);
    } else if (userData instanceof SubdividedPart) {
      out.writeByte(UserDataType.SUBDIVIDED_PART.id);
      ((SubdividedPart) userData).write(kryo, out);
    } else if (userData instanceof OuterJoinUserData) {
      out.writeByte(UserDataType.OUTER_JOIN_USER_DATA.id);
      ((OuterJoinUserData) userData).write(kryo, out);
    } else {
      out.writeByte(UserDataType.OTHER.id);
      kryo.writeClassAndObject(out, userData);
    }
  }

  /**
   * Deserialize user data from the input stream.
   *
   * @param kryo Kryo instance
   * @param in Input stream
   * @return Deserialized user data
   */
  public static Object read(Kryo kryo, Input in) {
    int typeId = in.readByte();
    UserDataType type = UserDataType.fromId(typeId);
    switch (type) {
      case NULL:
        return null;
      case UNSAFE_ROW:
        UnsafeRow unsafeRow = new UnsafeRow();
        unsafeRow.read(kryo, in);
        return unsafeRow;
      case SUBDIVIDED_PART:
        SubdividedPart subdivided = new SubdividedPart();
        subdivided.read(kryo, in);
        return subdivided;
      case OUTER_JOIN_USER_DATA:
        OuterJoinUserData outerJoinData = new OuterJoinUserData();
        outerJoinData.read(kryo, in);
        return outerJoinData;
      case OTHER:
        return kryo.readClassAndObject(in);
      default:
        throw new IllegalArgumentException("Unknown user data type id: " + typeId);
    }
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
