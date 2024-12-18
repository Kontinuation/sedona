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
import org.locationtech.jts.geom.Envelope;

public class SerializedCoordinateFilters {

  /**
   * A serialized coordinate filter for collecting basic statistics of serialized geometries without
   * first deserializing them.
   */
  public static class StatisticsCollector implements SerializedCoordinateFilter {
    public GeometryType geometryType;
    public int numCoordinates;
    public double minX;
    public double minY;
    public double maxX;
    public double maxY;

    public StatisticsCollector() {
      reset();
    }

    @Override
    public void coordinate(double x, double y, double z, double m) {
      if (!Double.isNaN(x)) {
        minX = Math.min(minX, x);
        maxX = Math.max(maxX, x);
        minY = Math.min(minY, y);
        maxY = Math.max(maxY, y);
        numCoordinates++;
      }
    }

    @Override
    public void coordinateType(CoordinateType type) {}

    @Override
    public void geometryType(GeometryType type) {
      if (geometryType == null) {
        geometryType = type;
      }
    }

    @Override
    public void numCoordinates(int num) {}

    public Envelope getEnvelope() {
      if (minX <= maxX) {
        return new Envelope(minX, maxX, minY, maxY);
      } else {
        return new Envelope();
      }
    }

    public void reset() {
      geometryType = null;
      numCoordinates = 0;
      minX = Double.MAX_VALUE;
      minY = Double.MAX_VALUE;
      maxX = -Double.MAX_VALUE;
      maxY = -Double.MAX_VALUE;
    }
  }
}
