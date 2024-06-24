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
import java.util.List;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

/**
 * Subdivides a polygon into boxes. The boxes is an approximated coverage of the polygon. This
 * allows us to get rid of indexing a big concave or slim polygon using its extent, where the extent
 * is much larger than the actual polygon. This is also useful for indexing a polygon with large
 * holes.
 */
public class SubdividedPolygonUsingBoxes implements Iterator<Geometry> {
  private final GeometryFactory factory;
  private final Iterator<Envelope> iterator;

  public SubdividedPolygonUsingBoxes(Polygon polygon, SubdivideOptions options) {
    this((new PreparedGeometryFactory()).create(polygon), options);
  }

  public SubdividedPolygonUsingBoxes(PreparedGeometry preparedGeometry, SubdivideOptions options) {
    this.factory = preparedGeometry.getGeometry().getFactory();
    List<Envelope> boxes = splitToBoxes(preparedGeometry, options);
    this.iterator = boxes.iterator();
  }

  public static List<Envelope> splitToBoxes(
      PreparedGeometry preparedGeometry, SubdivideOptions options) {
    Envelope extent = preparedGeometry.getGeometry().getEnvelopeInternal();
    List<Envelope> output = new java.util.ArrayList<>();
    splitToBoxes(preparedGeometry, options, extent, 0, output);
    return output;
  }

  private static void splitToBoxes(
      PreparedGeometry preparedGeometry,
      SubdivideOptions options,
      Envelope extent,
      int depth,
      List<Envelope> output) {
    Geometry extentGeom = preparedGeometry.getGeometry().getFactory().toGeometry(extent);
    if (!preparedGeometry.intersects(extentGeom)) {
      // Not intersecting, simply discard this box
      return;
    }
    if (depth >= options.maxDepth) {
      // Reached maximum depth, no need to subdivide further
      output.add(extent);
      return;
    }
    if (preparedGeometry.covers(extentGeom)) {
      // Fully covered, no need to subdivide
      output.add(extent);
      return;
    }
    if (extent.getWidth() < options.maxWidth && extent.getHeight() < options.maxHeight) {
      // Small enough
      output.add(extent);
      return;
    }

    // Subdivide the box into 2 half planes
    double minX = extent.getMinX();
    double minY = extent.getMinY();
    double maxX = extent.getMaxX();
    double maxY = extent.getMaxY();
    double midX = (minX + maxX) / 2;
    double midY = (minY + maxY) / 2;
    double width = maxX - minX;
    double height = maxY - minY;
    if (width > height) {
      // Split horizontally
      Envelope leftExtent = new Envelope(minX, midX, minY, maxY);
      Envelope rightExtent = new Envelope(midX, maxX, minY, maxY);
      splitToBoxes(preparedGeometry, options, leftExtent, depth + 1, output);
      splitToBoxes(preparedGeometry, options, rightExtent, depth + 1, output);
    } else {
      // Split vertically
      Envelope lowerExtent = new Envelope(minX, maxX, minY, midY);
      Envelope upperExtent = new Envelope(minX, maxX, midY, maxY);
      splitToBoxes(preparedGeometry, options, lowerExtent, depth + 1, output);
      splitToBoxes(preparedGeometry, options, upperExtent, depth + 1, output);
    }
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Geometry next() {
    Envelope envelope = iterator.next();
    return factory.toGeometry(envelope);
  }
}
