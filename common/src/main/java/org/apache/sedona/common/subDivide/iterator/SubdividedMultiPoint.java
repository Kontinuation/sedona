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
import org.locationtech.jts.geom.MultiPoint;

public class SubdividedMultiPoint implements Iterator<Geometry> {
  private final MultiPoint multiPoint;
  private int index = 0;
  private int numPoints = 0;

  public SubdividedMultiPoint(MultiPoint multiPoint) {
    this.multiPoint = multiPoint;
    this.numPoints = multiPoint.getNumPoints();
  }

  @Override
  public boolean hasNext() {
    return index < numPoints;
  }

  @Override
  public Geometry next() {
    return multiPoint.getGeometryN(index++).copy();
  }
}
