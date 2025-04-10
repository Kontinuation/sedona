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
package org.apache.spark.sedona.core.index.nearestneighbor;

import org.apache.sedona.common.sphere.Haversine;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.ItemBoundable;

public interface SphericalEnvelopeDistance extends EnvelopeDistance {

  /**
   * Computes the lower bound of spherical or spheroidal distance between an envelope and an item.
   * The returned value is conservative. It can be 0 when we are not certain about the distance.
   *
   * @param a the envelope, usually an STR-tree node containing children items.
   * @param b the item, usually a query point
   * @return the lower bound of distance between the envelope and the item.
   */
  default double distanceLowerBound(Envelope a, ItemBoundable b) {
    Geometry g = (Geometry) b.getItem();
    Coordinate pt = g.getCentroid().getCoordinate();
    double x0 = pt.getX();
    double y0 = pt.getY();
    if (y0 < -90 || y0 > 90) {
      // Not a valid latitude. Return a conservative value
      return 0;
    }

    if (a.contains(pt)) {
      return 0;
    }

    double xmin = a.getMinX();
    double xmax = a.getMaxX();
    double ymin = a.getMinY();
    double ymax = a.getMaxY();
    if (ymin < -90 || ymax > 90) {
      // Not an envelope with valid latitude. Return a conservative value
      return 0;
    }

    // Shift the longitudes to [-180, 180]
    x0 = normalizeLongitude(x0);
    xmin = normalizeLongitude(xmin);
    xmax = normalizeLongitude(xmax);

    // Shift the envelope to be within the same semi-sphere as the point pt if the envelope
    // is crossing the anti-meridian
    if (xmax < xmin) {
      // Crossing the anti-meridian
      if (x0 >= 0) {
        xmax += 360;
      } else {
        xmin -= 360;
      }
    }

    if (a.contains(x0, y0)) {
      return 0;
    }

    // Find a point (x1, y1) on the envelope that is closest to the point pt
    double x1;
    double y1;
    if (xmin > x0) {
      x1 = xmin;
    } else {
      x1 = Math.min(xmax, x0);
    }
    if (ymin > y0) {
      y1 = ymin;
    } else {
      y1 = Math.min(ymax, y0);
    }

    // Compute the distance between the point pt and the closest point on the envelope
    double dist = Haversine.distance(x0, y0, x1, y1);

    // Multiply a factor to the distance to get a more conservative value
    return dist * 0.8;
  }

  /**
   * Normalize longitude to [-180, 180]. This is almost equivalent with the following code:
   *
   * <pre>{@code
   * while (lon < -180) {
   *   lon += 360;
   * }
   * while (lon > 180) {
   *   lon -= 360;
   * }
   * return lon;
   * }</pre>
   *
   * @param lon longitude
   * @return normalized longitude
   */
  static double normalizeLongitude(double lon) {
    if (lon >= -180.0 && lon <= 180.0) {
      return lon;
    } else if (lon > 180) {
      return (lon + 180.0) % 360.0 - 180.0;
    } else {
      return 180.0 - (180.0 - lon) % 360.0;
    }
  }
}
