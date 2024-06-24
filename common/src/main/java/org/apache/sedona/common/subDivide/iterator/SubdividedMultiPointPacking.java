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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.shape.fractal.HilbertCode;

/** Pack a MultiPoint into a set of smaller MultiPoints that fit within a given width and height. */
public class SubdividedMultiPointPacking implements Iterator<Geometry> {
  private final Iterator<Geometry> iterator;

  public SubdividedMultiPointPacking(MultiPoint multiPoint, SubdivideOptions options) {
    List<Geometry> packedMultiPoints = pack(multiPoint, options.maxWidth, options.maxHeight);
    iterator = packedMultiPoints.iterator();
  }

  private static class CoordinateWithHilbertID {
    final int hilbertID;
    final Coordinate coordinate;

    CoordinateWithHilbertID(int hilbertID, Coordinate coordinate) {
      this.hilbertID = hilbertID;
      this.coordinate = coordinate;
    }
  }

  private static List<Geometry> pack(MultiPoint multiPoint, double maxWidth, double maxHeight) {
    Envelope envelope = multiPoint.getEnvelopeInternal();
    if (envelope.getWidth() <= maxWidth && envelope.getHeight() <= maxHeight) {
      // No need to pack
      return Collections.singletonList(multiPoint.copy());
    }

    // Sort the coordinates by hilbert ID for packing
    Coordinate[] coordinates = multiPoint.getCoordinates();
    CoordinateWithHilbertID[] coordinatesWithHilbertID =
        new CoordinateWithHilbertID[coordinates.length];
    double xFrac = 255 / Math.max(envelope.getWidth(), maxWidth);
    double yFrac = 255 / Math.max(envelope.getHeight(), maxHeight);
    for (int i = 0; i < coordinates.length; i++) {
      int x = (int) ((coordinates[i].x - envelope.getMinX()) * xFrac);
      int y = (int) ((coordinates[i].y - envelope.getMinY()) * yFrac);
      int hilbertID = HilbertCode.encode(16, x, y);
      coordinatesWithHilbertID[i] = new CoordinateWithHilbertID(hilbertID, coordinates[i]);
    }
    Arrays.sort(coordinatesWithHilbertID, Comparator.comparingInt(a -> a.hilbertID));

    GeometryFactory factory = multiPoint.getFactory();
    List<Geometry> boxes = new ArrayList<>();
    List<Coordinate> currentBox = new ArrayList<>();

    int i = 0;
    while (i < coordinatesWithHilbertID.length) {
      // Start a new box
      Coordinate coord = coordinatesWithHilbertID[i++].coordinate;
      double xMin = coord.x;
      double yMin = coord.y;
      double xMax = xMin;
      double yMax = yMin;
      currentBox.clear();
      currentBox.add(coord);

      // Expand the box
      while (i < coordinatesWithHilbertID.length) {
        coord = coordinatesWithHilbertID[i].coordinate;
        xMax = Math.max(xMax, coord.x);
        yMax = Math.max(yMax, coord.y);
        xMin = Math.min(xMin, coord.x);
        yMin = Math.min(yMin, coord.y);
        if (xMax - xMin > maxWidth || yMax - yMin > maxHeight) {
          break;
        } else {
          coord = coordinatesWithHilbertID[i++].coordinate;
          currentBox.add(coord);
        }
      }

      // Finalize the box
      if (currentBox.size() == 1) {
        // Single point
        boxes.add(factory.createPoint(currentBox.get(0)));
      } else {
        MultiPoint packed =
            factory.createMultiPointFromCoords(currentBox.toArray(new Coordinate[0]));
        boxes.add(packed);
      }
    }

    return boxes;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Geometry next() {
    return iterator.next();
  }
}
