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

/**
 * Directly running filter on the serialized geometry without deserializing it. This is useful for
 * computing the bounding box of the serialized geometry or getting some other basic statistics
 * about the serialized geometry efficiently.
 */
public interface SerializedCoordinateFilter {

  /**
   * This method is called once for each geometry, or each sub-geometry in the geometry collection.
   *
   * @param type geometry type
   */
  void geometryType(GeometryType type);

  /**
   * This method is called once for each coordinate type in the serialized geometry.
   *
   * @param type coordinate type
   */
  void coordinateType(CoordinateType type);

  /**
   * This method is called once for each geometry, or each sub-geometry in the geometry collection.
   * Please note that this may not give you the exact number of coordinates when the geometry is a
   * MultiPoint containing empty points. To know the exact number of coordinates, please count the
   * number of coordinates in the coordinate() method and ignore those coordinates that are NaN.
   *
   * @param num number of coordinates
   */
  void numCoordinates(int num);

  /**
   * This method is called once for each coordinate in the serialized geometry. Sometimes this
   * method is called with NaN values for x and y, which indicates that the coordinate is an empty
   * point in a MultiPoint.
   *
   * @param x x coordinate, NaN if not present
   * @param y y coordinate, NaN if not present
   * @param z z coordinate, NaN if not present
   * @param m m coordinate, NaN if not present
   */
  void coordinate(double x, double y, double z, double m);

  /**
   * Apply the filter to the given buffer.
   *
   * @param buffer buffer to be filtered
   */
  default void apply(GeometryBuffer buffer) {
    ApplySerializedCoordinateFilter.apply(buffer, this);
  }
}
