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
package org.apache.sedona.common.subDivide;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.sedona.common.subDivide.iterator.SubdividedLineStringCutSegments;
import org.apache.sedona.common.subDivide.iterator.SubdividedLineStringPreserveSegments;
import org.apache.sedona.common.subDivide.iterator.SubdividedMultiPoint;
import org.apache.sedona.common.subDivide.iterator.SubdividedMultiPointPacking;
import org.apache.sedona.common.subDivide.iterator.SubdividedPolygonUsingBoxes;
import org.apache.sedona.common.subDivide.iterator.SubdividedPolygonUsingOverlay;
import org.apache.sedona.common.subDivide.iterator.SubdividedPolygonUsingOverlayNG;
import org.apache.sedona.common.subDivide.iterator.SubdividedPolygonUsingOverlayNG2;
import org.apache.sedona.common.subDivide.iterator.SubdividedPolygonUsingTriangulation;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * This is a more comprehensive class that is capable of subdividing geometries based on their
 * extents. It can handle various kinds of geometries, including points, lines, and polygons.
 */
public class ExtentBasedGeometrySubDivider {
  private final SubdivideOptions options;

  /**
   * Constructor for ExtentBasedGeometrySubDivider.
   *
   * @param options The options for subdividing geometries.
   */
  public ExtentBasedGeometrySubDivider(SubdivideOptions options) {
    this.options = options;
  }

  /**
   * Subdivide a geometry into smaller geometries based on the maximum number of vertices, width,
   * height, and area.
   *
   * @param geometry The geometry to subdivide.
   * @return An iterator of the smaller geometries.
   */
  @SuppressWarnings("unchecked")
  public Iterator<Geometry> subdivide(Geometry geometry) {
    if (geometry.isEmpty() || geometry instanceof Point) {
      return (Iterator<Geometry>) new SingletonIterator(geometry.copy());
    } else if (geometry instanceof MultiPoint) {
      return subdivideMultiPoint((MultiPoint) geometry);
    } else if (geometry instanceof LineString) {
      return subdivideLineString((LineString) geometry);
    } else if (geometry instanceof Polygon) {
      return subdividePolygon((Polygon) geometry);
    } else {
      List<Iterator<Geometry>> iterators = new ArrayList<>();
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        iterators.add(subdivide(geometry.getGeometryN(i)));
      }
      return IteratorUtils.chainedIterator(iterators);
    }
  }

  private Iterator<Geometry> subdivideMultiPoint(MultiPoint geometry) {
    switch (options.multiPointSubDivider) {
      case DECOMPOSE:
        return new SubdividedMultiPoint(geometry);
      case PACKING:
        return new SubdividedMultiPointPacking(geometry, options);
      default:
        throw new IllegalArgumentException(
            "Unsupported multi-point sub-divider: " + options.multiPointSubDivider);
    }
  }

  private Iterator<Geometry> subdivideLineString(LineString geometry) {
    switch (options.lineStringSubDivider) {
      case CUT_SEGMENTS:
        return new SubdividedLineStringCutSegments(geometry, options);
      case PRESERVE_SEGMENTS:
        return new SubdividedLineStringPreserveSegments(geometry, options);
      default:
        throw new IllegalArgumentException(
            "Unsupported line-string sub-divider: " + options.lineStringSubDivider);
    }
  }

  private Iterator<Geometry> subdividePolygon(Polygon geometry) {
    switch (options.polygonSubDivider) {
      case BOX_APPROX:
        return new SubdividedPolygonUsingBoxes(geometry, options);
      case OVERLAY:
        return new SubdividedPolygonUsingOverlay(geometry, options);
      case OVERLAY_NG:
        return new SubdividedPolygonUsingOverlayNG(geometry, options);
      case OVERLAY_NG2:
        return new SubdividedPolygonUsingOverlayNG2(geometry, options);
      case TRIANGULATION:
        return new SubdividedPolygonUsingTriangulation(geometry);
      default:
        throw new IllegalArgumentException(
            "Unsupported polygon sub-divider: " + options.polygonSubDivider);
    }
  }
}
