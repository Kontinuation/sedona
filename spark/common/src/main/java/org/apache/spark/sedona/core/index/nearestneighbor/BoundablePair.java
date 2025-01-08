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

import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.apache.spark.sedona.core.index.DataItemFormat;
import org.apache.spark.sedona.core.index.ExternalLeafPageIndex.EnvelopeFilter;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement.DataObjectWithId;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.Boundable;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.ItemDistance;

/**
 * A pair of {@link Boundable}s, whose data items support a distance metric between them. Used to
 * compute the distance between the members, and to expand a member relative to the other in order
 * to produce new branches of the Branch-and-Bound evaluation tree. Provides an ordering based on
 * the distance between the members, which allows building a priority queue by minimum distance.
 *
 * @author Martin Davis
 */
public class BoundablePair implements Comparable<BoundablePair> {

  public static class LeafPageOrDataItemBoundable extends ItemBoundable {

    public final boolean isDataItem;

    public LeafPageOrDataItemBoundable(Envelope bounds, Object item, boolean isDataItem) {
      super(bounds, item);
      this.isDataItem = isDataItem;
    }
  }

  private final LeafPageOrDataItemBoundable boundable1;
  private final Boundable boundable2;
  private final double distance;
  private final ItemDistance itemDistance;

  public <T> BoundablePair(
      LeafPageOrDataItemBoundable boundable1,
      Boundable boundable2,
      ItemDistance itemDistance,
      DataItemFormat<T> format) {
    this.boundable1 = boundable1;
    this.boundable2 = boundable2;
    this.itemDistance = itemDistance;
    distance = distance(format);
  }

  /**
   * Gets one of the member {@link Boundable}s in the pair (indexed by [0, 1]).
   *
   * @param i the index of the member to return (0 or 1)
   * @return the chosen member
   */
  public Boundable getBoundable(int i) {
    if (i == 0) return boundable1;
    return boundable2;
  }

  /**
   * Computes the distance between the {@link Boundable}s in this pair. The boundables are either
   * composites or leaves. If either is composite, the distance is computed as the minimum distance
   * between the bounds. If both are leaves, the distance is computed by itemDistance.distance.
   *
   * @return the distance between the {@link Boundable}s in this pair
   */
  @SuppressWarnings("unchecked")
  private <T> double distance(DataItemFormat<T> format) {
    // if items, compute exact distance
    if (containsDataItem()) {
      DataObjectWithId<T> dataObjectWithId = (DataObjectWithId<T>) boundable1.getItem();
      Geometry geometry = format.extractGeometry(dataObjectWithId.dataObject);
      return itemDistance.distance(
          new ItemBoundable(geometry.getEnvelopeInternal(), geometry), (ItemBoundable) boundable2);
    }
    // otherwise compute distance between bounds of boundables
    return ((Envelope) boundable1.getBounds()).distance(((Envelope) boundable2.getBounds()));
  }

  /**
   * Gets the minimum possible distance between the Boundables in this pair. If the members are both
   * items, this will be the exact distance between them. Otherwise, this distance will be a lower
   * bound on the distances between the items in the members.
   *
   * @return the exact or lower bound distance for this pair
   */
  public double getDistance() {
    return distance;
  }

  /** Compares two pairs based on their minimum distances */
  @Override
  public int compareTo(BoundablePair nd) {
    return Double.compare(distance, nd.distance);
  }

  /**
   * Tests if both elements of the pair cannot be further expanded.
   *
   * @return true if both pair elements cannot be further expanded
   */
  public boolean containsDataItem() {
    return boundable1.isDataItem;
  }

  /**
   * For a pair which is not a data item (i.e. has at least one composite boundable) computes a list
   * of new pairs from the expansion of the leaf page boundable with distance less than minDistance
   * and adds them to a priority queue.
   *
   * <p>Note that expanded pairs may contain the same item/node on both sides. This must be allowed
   * to support distance functions which have non-zero distances between the item and itself
   * (non-zero reflexive distance).
   *
   * @param priQ the priority queue to add the new pairs to
   * @param minDistance the limit on the distance between added pairs
   * @param index the spatial index
   * @param format the data item format for interpreting the data items
   * @throws IOException if an error occurs while fetching data items
   */
  public <T> void expandToQueue(
      PriorityQueue<BoundablePair> priQ,
      double minDistance,
      ExternalSpatialIndexWithRefinement<T> index,
      DataItemFormat<T> format)
      throws IOException {
    if (containsDataItem()) {
      throw new IllegalArgumentException("neither boundable is composite");
    }
    expand(boundable1, boundable2, priQ, minDistance, index, format);
  }

  private static final EnvelopeFilter ALWAYS_TRUE = (leafPageId1, env) -> true;

  private <T> void expand(
      LeafPageOrDataItemBoundable bnd,
      Boundable bndOther,
      PriorityQueue<BoundablePair> priQ,
      double minDistance,
      ExternalSpatialIndexWithRefinement<T> index,
      DataItemFormat<T> format)
      throws IOException {
    int leafPageId = (int) bnd.getItem();
    IntList dataItemIds =
        index.getSpatialIndex().getLeafPageIndex().query(ALWAYS_TRUE, IntList.of(leafPageId));
    Iterator<DataObjectWithId<T>> dataIter = index.fetchDataObjects(dataItemIds);
    while (dataIter.hasNext()) {
      DataObjectWithId<T> data = dataIter.next();
      Envelope envData = format.extractEnvelope(data.dataObject);
      LeafPageOrDataItemBoundable bndData = new LeafPageOrDataItemBoundable(envData, data, true);
      BoundablePair bp = new BoundablePair(bndData, bndOther, itemDistance, format);
      // only add to queue if this pair might contain the closest points
      // MD - it's actually faster to construct the object rather than called distance(child,
      // bndOther)!
      if (bp.getDistance() < minDistance) {
        priQ.add(bp);
      }
    }
  }
}
