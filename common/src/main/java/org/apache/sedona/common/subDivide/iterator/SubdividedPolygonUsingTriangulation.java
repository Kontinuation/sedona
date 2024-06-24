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

import java.util.Iterator;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.triangulate.polygon.PolygonTriangulator;

public class SubdividedPolygonUsingTriangulation implements Iterator<Geometry> {
  private int index;
  private final int numGeometries;
  private final Geometry triangulatedPolygon;

  public SubdividedPolygonUsingTriangulation(Polygon polygon) {
    if (polygon.getNumPoints() <= 4) {
      triangulatedPolygon = polygon.copy();
      numGeometries = 1;
    } else {
      triangulatedPolygon = PolygonTriangulator.triangulate(polygon);
      numGeometries = triangulatedPolygon.getNumGeometries();
    }
  }

  public boolean hasNext() {
    return index < numGeometries;
  }

  public Geometry next() {
    return triangulatedPolygon.getGeometryN(index++);
  }
}
