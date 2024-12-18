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
package org.apache.sedona.common.geometrySerde;

import java.lang.reflect.Field;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import sun.misc.Unsafe;

class UnsafeGeometryBuffer implements GeometryBuffer {
  private static final Unsafe UNSAFE;
  private static final long BYTE_ARRAY_BASE_OFFSET;

  static {
    Unsafe unsafe;
    long byteArrayOffset = 0;
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      unsafe = (Unsafe) theUnsafe.get(null);
      byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);
    } catch (IllegalAccessException | NoSuchFieldException | SecurityException e) {
      // Unsafe is not available
      unsafe = null;
    }
    UNSAFE = unsafe;
    BYTE_ARRAY_BASE_OFFSET = byteArrayOffset;
  }

  public static boolean isUnsafeAvailable() {
    return UNSAFE != null;
  }

  private CoordinateType coordinateType = CoordinateType.XY;
  private final Object baseObject;
  private final long baseOffset;
  private final long length;
  private int markOffset = 0;

  public UnsafeGeometryBuffer(int bufferSize) {
    this(new byte[bufferSize], BYTE_ARRAY_BASE_OFFSET, bufferSize);
  }

  public UnsafeGeometryBuffer(byte[] bytes, int offset) {
    this(bytes, offset + BYTE_ARRAY_BASE_OFFSET, bytes.length - offset);
  }

  public UnsafeGeometryBuffer(byte[] bytes) {
    this(bytes, BYTE_ARRAY_BASE_OFFSET, bytes.length);
  }

  public UnsafeGeometryBuffer(Object baseObject, long baseOffset, long length) {
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.length = length;
  }

  @Override
  public CoordinateType getCoordinateType() {
    return coordinateType;
  }

  @Override
  public void setCoordinateType(CoordinateType coordinateType) {
    this.coordinateType = coordinateType;
  }

  @Override
  public int getLength() {
    return (int) length;
  }

  @Override
  public void mark(int offset) {
    markOffset = offset;
  }

  @Override
  public int getMark() {
    return markOffset;
  }

  @Override
  public void putByte(int offset, byte value) {
    assert offset < length;
    UNSAFE.putByte(baseObject, baseOffset + offset, value);
  }

  @Override
  public byte getByte(int offset) {
    assert offset < length;
    return UNSAFE.getByte(baseObject, baseOffset + offset);
  }

  @Override
  public void putBytes(int offset, byte[] inBytes) {
    assert offset + inBytes.length <= length;
    UNSAFE.copyMemory(
        inBytes, BYTE_ARRAY_BASE_OFFSET, baseObject, baseOffset + offset, inBytes.length);
  }

  @Override
  public void getBytes(byte[] outBytes, int offset, int length) {
    assert offset + length <= this.length;
    UNSAFE.copyMemory(baseObject, baseOffset + offset, outBytes, BYTE_ARRAY_BASE_OFFSET, length);
  }

  @Override
  public void putInt(int offset, int value) {
    assert offset + 4 <= length;
    UNSAFE.putInt(baseObject, baseOffset + offset, value);
  }

  @Override
  public int getInt(int offset) {
    assert offset + 4 <= length;
    return UNSAFE.getInt(baseObject, baseOffset + offset);
  }

  @Override
  public void putCoordinate(int offset, Coordinate coordinate) {
    long coordOffset = baseOffset + offset;
    assert offset + coordinateType.bytes <= length;
    switch (coordinateType) {
      case XY:
        UNSAFE.putDouble(baseObject, coordOffset, coordinate.x);
        UNSAFE.putDouble(baseObject, coordOffset + 8, coordinate.y);
        break;
      case XYZ:
        UNSAFE.putDouble(baseObject, coordOffset, coordinate.x);
        UNSAFE.putDouble(baseObject, coordOffset + 8, coordinate.y);
        UNSAFE.putDouble(baseObject, coordOffset + 16, coordinate.getZ());
        break;
      case XYM:
        UNSAFE.putDouble(baseObject, coordOffset, coordinate.x);
        UNSAFE.putDouble(baseObject, coordOffset + 8, coordinate.y);
        UNSAFE.putDouble(baseObject, coordOffset + 16, coordinate.getM());
        break;
      case XYZM:
        UNSAFE.putDouble(baseObject, coordOffset, coordinate.x);
        UNSAFE.putDouble(baseObject, coordOffset + 8, coordinate.y);
        UNSAFE.putDouble(baseObject, coordOffset + 16, coordinate.getZ());
        UNSAFE.putDouble(baseObject, coordOffset + 24, coordinate.getM());
        break;
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
  }

  @Override
  public CoordinateSequence getCoordinate(int offset) {
    long coordOffset = baseOffset + offset;
    assert coordOffset + coordinateType.bytes <= baseOffset + length;
    double x = UNSAFE.getDouble(baseObject, coordOffset);
    double y = UNSAFE.getDouble(baseObject, coordOffset + 8);
    double z;
    double m;
    Coordinate[] coordinates = new Coordinate[1];
    switch (coordinateType) {
      case XY:
        coordinates[0] = new CoordinateXY(x, y);
        return new CoordinateArraySequence(coordinates, 2, 0);
      case XYZ:
        z = UNSAFE.getDouble(baseObject, coordOffset + 16);
        coordinates[0] = new Coordinate(x, y, z);
        return new CoordinateArraySequence(coordinates, 3, 0);
      case XYM:
        m = UNSAFE.getDouble(baseObject, coordOffset + 16);
        coordinates[0] = new CoordinateXYM(x, y, m);
        return new CoordinateArraySequence(coordinates, 3, 1);
      case XYZM:
        z = UNSAFE.getDouble(baseObject, coordOffset + 16);
        m = UNSAFE.getDouble(baseObject, coordOffset + 24);
        coordinates[0] = new CoordinateXYZM(x, y, z, m);
        return new CoordinateArraySequence(coordinates, 4, 1);
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
  }

  @Override
  public void putCoordinates(int offset, CoordinateSequence coordinates) {
    long coordOffset = baseOffset + offset;
    int numCoordinates = coordinates.size();
    assert coordOffset + (long) coordinateType.bytes * numCoordinates <= baseOffset + length;
    switch (coordinateType) {
      case XY:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          UNSAFE.putDouble(baseObject, coordOffset, coord.x);
          UNSAFE.putDouble(baseObject, coordOffset + 8, coord.y);
          coordOffset += 16;
        }
        break;
      case XYZ:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          UNSAFE.putDouble(baseObject, coordOffset, coord.x);
          UNSAFE.putDouble(baseObject, coordOffset + 8, coord.y);
          UNSAFE.putDouble(baseObject, coordOffset + 16, coord.getZ());
          coordOffset += 24;
        }
        break;
      case XYM:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          UNSAFE.putDouble(baseObject, coordOffset, coord.x);
          UNSAFE.putDouble(baseObject, coordOffset + 8, coord.y);
          UNSAFE.putDouble(baseObject, coordOffset + 16, coord.getM());
          coordOffset += 24;
        }
        break;
      case XYZM:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          UNSAFE.putDouble(baseObject, coordOffset, coord.x);
          UNSAFE.putDouble(baseObject, coordOffset + 8, coord.y);
          UNSAFE.putDouble(baseObject, coordOffset + 16, coord.getZ());
          UNSAFE.putDouble(baseObject, coordOffset + 24, coord.getM());
          coordOffset += 32;
        }
        break;
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
  }

  @Override
  public CoordinateSequence getCoordinates(int offset, int numCoordinates) {
    long coordOffset = baseOffset + offset;
    assert coordOffset + (long) coordinateType.bytes * numCoordinates <= baseOffset + length;
    Coordinate[] coordinates = new Coordinate[numCoordinates];
    int dimension = 2;
    int measures = 0;
    switch (coordinateType) {
      case XY:
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(baseObject, coordOffset);
          double y = UNSAFE.getDouble(baseObject, coordOffset + 8);
          coordinates[k] = new CoordinateXY(x, y);
          coordOffset += 16;
        }
        break;
      case XYZ:
        dimension = 3;
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(baseObject, coordOffset);
          double y = UNSAFE.getDouble(baseObject, coordOffset + 8);
          double z = UNSAFE.getDouble(baseObject, coordOffset + 16);
          coordinates[k] = new Coordinate(x, y, z);
          coordOffset += 24;
        }
        break;
      case XYM:
        dimension = 3;
        measures = 1;
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(baseObject, coordOffset);
          double y = UNSAFE.getDouble(baseObject, coordOffset + 8);
          double m = UNSAFE.getDouble(baseObject, coordOffset + 16);
          coordinates[k] = new CoordinateXYM(x, y, m);
          coordOffset += 24;
        }
        break;
      case XYZM:
        dimension = 4;
        measures = 1;
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(baseObject, coordOffset);
          double y = UNSAFE.getDouble(baseObject, coordOffset + 8);
          double z = UNSAFE.getDouble(baseObject, coordOffset + 16);
          double m = UNSAFE.getDouble(baseObject, coordOffset + 24);
          coordinates[k] = new CoordinateXYZM(x, y, z, m);
          coordOffset += 32;
        }
        break;
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
    return new CoordinateArraySequence(coordinates, dimension, measures);
  }

  @Override
  public void filterCoordinates(int offset, int numCoordinates, SerializedCoordinateFilter filter) {
    long coordOffset = baseOffset + offset;
    assert coordOffset + (long) coordinateType.bytes * numCoordinates <= baseOffset + length;
    switch (coordinateType) {
      case XY:
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(baseObject, coordOffset);
          double y = UNSAFE.getDouble(baseObject, coordOffset + 8);
          filter.coordinate(x, y, Double.NaN, Double.NaN);
          coordOffset += 16;
        }
        break;
      case XYZ:
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(baseObject, coordOffset);
          double y = UNSAFE.getDouble(baseObject, coordOffset + 8);
          double z = UNSAFE.getDouble(baseObject, coordOffset + 16);
          filter.coordinate(x, y, z, Double.NaN);
          coordOffset += 24;
        }
        break;
      case XYM:
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(baseObject, coordOffset);
          double y = UNSAFE.getDouble(baseObject, coordOffset + 8);
          double m = UNSAFE.getDouble(baseObject, coordOffset + 16);
          filter.coordinate(x, y, Double.NaN, m);
          coordOffset += 24;
        }
        break;
      case XYZM:
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(baseObject, coordOffset);
          double y = UNSAFE.getDouble(baseObject, coordOffset + 8);
          double z = UNSAFE.getDouble(baseObject, coordOffset + 16);
          double m = UNSAFE.getDouble(baseObject, coordOffset + 24);
          filter.coordinate(x, y, z, m);
          coordOffset += 32;
        }
        break;
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
  }

  @Override
  public GeometryBuffer slice(int offset) {
    assert offset < length;
    return new UnsafeGeometryBuffer(baseObject, baseOffset + offset, length - offset);
  }

  @Override
  public byte[] toByteArray() {
    if (baseObject instanceof byte[]
        && baseOffset == BYTE_ARRAY_BASE_OFFSET
        && length == ((byte[]) baseObject).length) {
      // The buffer is already a byte array, and we are referencing the entire array
      return (byte[]) baseObject;
    } else {
      byte[] copy = new byte[(int) length];
      UNSAFE.copyMemory(baseObject, baseOffset, copy, BYTE_ARRAY_BASE_OFFSET, length);
      return copy;
    }
  }
}
