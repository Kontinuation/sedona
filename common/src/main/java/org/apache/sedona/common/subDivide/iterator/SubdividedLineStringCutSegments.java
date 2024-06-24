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
package org.apache.sedona.common.subDivide.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

/**
 * Subdivide a line string into segments. The segments are cut at the points where the length of the
 * segment exceeds the maximum length. Cutting segments may introduce some numerical errors, but it
 * should be negligible for most applications. If you really need to avoid numerical errors at all,
 * you can try {@link SubdividedLineStringPreserveSegments}.
 */
public class SubdividedLineStringCutSegments implements Iterator<Geometry> {
  private final GeometryFactory factory;
  private final Coordinate[] coordinates;
  private final double maxLength;
  private double lastX = 0;
  private double lastY = 0;
  private int index = 1;

  public SubdividedLineStringCutSegments(LineString lineString, SubdivideOptions options) {
    factory = lineString.getFactory();
    maxLength = (options.maxWidth * 0.5 + options.maxHeight * 0.5);
    coordinates = lineString.getCoordinates();
    if (coordinates.length > 0) {
      lastX = coordinates[0].getX();
      lastY = coordinates[0].getY();
      index = 1;
    }
  }

  @Override
  public boolean hasNext() {
    return index < coordinates.length;
  }

  @Override
  public Geometry next() {
    double length = 0;
    List<Coordinate> slice = new ArrayList<>();
    slice.add(new Coordinate(lastX, lastY));
    for (; index < coordinates.length; index++) {
      double x = coordinates[index].getX();
      double y = coordinates[index].getY();
      double dx = x - lastX;
      double dy = y - lastY;
      double segmentLength = Math.sqrt(dx * dx + dy * dy);
      if (length + segmentLength < maxLength) {
        slice.add(coordinates[index]);
        length += segmentLength;
        lastX = x;
        lastY = y;
      } else {
        double ratio = (maxLength - length) / segmentLength;
        // Find the point on the line segment that is maxLength away from the start
        double newX = lastX + dx * ratio;
        double newY = lastY + dy * ratio;
        slice.add(new Coordinate(newX, newY));
        lastX = newX;
        lastY = newY;
        break;
      }
    }
    return factory.createLineString(slice.toArray(new Coordinate[0]));
  }
}
