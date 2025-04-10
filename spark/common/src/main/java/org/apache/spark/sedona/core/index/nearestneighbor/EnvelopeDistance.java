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

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.ItemBoundable;

/**
 * Interface for computing the lower bound of distance between an envelope and an item. This is used
 * by the nearest neighbor search algorithm to prune the search space.
 */
public interface EnvelopeDistance {

  /**
   * Computes the lower bound of distance between an envelope and an item. The returned value can be
   * smaller than the actual minimum distance, but cannot be larger. Any objects inside the envelope
   * a should have a larger distance to b than the returned value.
   *
   * @param a the envelope, usually an STR-tree node containing children items.
   * @param b the item, usually a query point
   * @return the lower bound of distance between the envelope and the item.
   */
  double distanceLowerBound(Envelope a, ItemBoundable b);
}
