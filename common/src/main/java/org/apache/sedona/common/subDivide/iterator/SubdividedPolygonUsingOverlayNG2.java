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

import static org.apache.sedona.common.subDivide.iterator.SubdividedPolygonUsingBoxes.splitToBoxes;

import java.util.Iterator;
import java.util.List;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.operation.overlay.OverlayOp;
import org.locationtech.jts.operation.overlayng.OverlayNGRobust;

public class SubdividedPolygonUsingOverlayNG2 implements Iterator<Geometry> {
  private final GeometryFactory factory;
  private final Geometry geometry;
  private final Iterator<Envelope> iterator;

  public SubdividedPolygonUsingOverlayNG2(Polygon polygon, SubdivideOptions options) {
    this((new PreparedGeometryFactory()).create(polygon), options);
  }

  public SubdividedPolygonUsingOverlayNG2(
      PreparedGeometry preparedGeometry, SubdivideOptions options) {
    geometry = preparedGeometry.getGeometry();
    List<Envelope> boxes = splitToBoxes(preparedGeometry, options);
    factory = geometry.getFactory();
    iterator = boxes.iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Geometry next() {
    Envelope envelope = iterator.next();
    Geometry clipBox = factory.toGeometry(envelope);
    return OverlayNGRobust.overlay(geometry, clipBox, OverlayOp.INTERSECTION);
  }
}
