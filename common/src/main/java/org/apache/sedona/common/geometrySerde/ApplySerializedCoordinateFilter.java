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

import org.apache.sedona.common.enums.GeometryType;
import org.locationtech.jts.io.WKBConstants;

/** Helper class for applying a serialized coordinate filter to a serialized geometry. */
public class ApplySerializedCoordinateFilter {

  private ApplySerializedCoordinateFilter() {}

  public static void apply(GeometryBuffer buffer, SerializedCoordinateFilter filter) {
    // Check minimum buffer size for header
    if (buffer.getLength() < 8) {
      throw new IllegalArgumentException("Buffer to be filtered is incomplete");
    }

    // Parse header byte
    int preambleByte = buffer.getByte(0) & 0xFF;
    int wkbType = preambleByte >> 4;
    CoordinateType coordType = CoordinateType.valueOf((preambleByte & 0x0F) >> 1);
    buffer.setCoordinateType(coordType);

    // Notify filter of geometry and coordinate types
    filter.geometryType(GeometryType.fromWkbType(wkbType));
    filter.coordinateType(coordType);

    if (wkbType != WKBConstants.wkbGeometryCollection) {
      applyGeometry(buffer, wkbType, coordType, filter);
    } else {
      applyGeometryCollection(buffer, filter);
    }
  }

  private static void applyGeometry(
      GeometryBuffer buffer,
      int wkbType,
      CoordinateType coordType,
      SerializedCoordinateFilter filter) {
    // Get number of coordinates
    int numCoordinates = buffer.getInt(4);
    filter.numCoordinates(numCoordinates);

    // Set initial position after header
    int position = 8;

    // Process each point coordinate
    if (numCoordinates > 0) {
      buffer.filterCoordinates(position, numCoordinates, filter);
      position += coordType.bytes * numCoordinates;
    }

    // Skip structural information
    switch (wkbType) {
      case WKBConstants.wkbMultiLineString:
        int numComponents = buffer.getInt(position);
        position += 4 * (numComponents + 1);
        break;

      case WKBConstants.wkbPolygon:
        if (numCoordinates > 0) {
          int numRings = buffer.getInt(position);
          position += 4 * (numRings + 1);
        }
        break;

      case WKBConstants.wkbMultiPolygon:
        // Process coordinate sequence
        int numPolygons = buffer.getInt(position);
        position += 4;
        for (int i = 0; i < numPolygons; i++) {
          int numRings = buffer.getInt(position);
          position += 4 * (numRings + 1);
        }
        break;

      default:
        break;
    }
    buffer.mark(position);
  }

  private static void applyGeometryCollection(
      GeometryBuffer buffer, SerializedCoordinateFilter filter) {
    int numGeometries = buffer.getInt(4);
    int position = 8;
    int totalLength = position;
    GeometryBuffer currentBuffer = buffer;
    for (int k = 0; k < numGeometries; k++) {
      currentBuffer = currentBuffer.slice(position);
      apply(currentBuffer, filter);
      position = currentBuffer.getMark();
      position = alignedOffset(position);
      totalLength += position;
    }
    buffer.mark(totalLength);
  }

  private static int alignedOffset(int offset) {
    return (offset + 7) & ~7;
  }
}
