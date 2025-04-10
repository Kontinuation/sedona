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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.spark.sedona.core.index.DataItemFormat;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement.DataObjectWithId;
import org.apache.spark.sedona.core.index.nearestneighbor.BoundablePair.PageOrDataItemBoundable;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.Boundable;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.ItemDistance;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * The nearest neighbor search algorithm ported from JTS STRtree to work with external spatial
 * index.
 */
public class NearestNeighborSearch {
  private NearestNeighborSearch() {}

  @SuppressWarnings("unchecked")
  public static <T> List<DataObjectWithId<T>> nearestNeighbours(
      ExternalSpatialIndexWithRefinement<T> index,
      Envelope env,
      Object item,
      ItemDistance itemDist,
      int k)
      throws IOException {
    if (index.count() == 0) {
      return new ArrayList<>();
    }

    DataItemFormat<T> format = index.getDataItemFormat();
    STRtree nonLeafTree = index.getNonLeafTree();

    // initialize internal structures
    PriorityQueue<BoundablePair> priQ = new PriorityQueue<>();

    Boundable itemBnd = new ItemBoundable(env, item);
    PageOrDataItemBoundable rootBnd = PageOrDataItemBoundable.internalNode(nonLeafTree.getRoot());
    BoundablePair root = new BoundablePair(rootBnd, itemBnd, itemDist, format);
    priQ.add(root);

    PriorityQueue<BoundablePair> kNearestNeighbors = new PriorityQueue<>();

    double distanceLowerBound = Double.POSITIVE_INFINITY;
    while (!priQ.isEmpty() && distanceLowerBound >= 0.0) {
      // pop head of queue and expand one side of pair
      BoundablePair bndPair = priQ.poll();
      double pairDistance = bndPair.getDistance();

      /*
       * If the distance for the first node in the queue
       * is >= the current maximum distance in the k queue , all other nodes
       * in the queue must also have a greater distance.
       * So the current minDistance must be the true minimum,
       * and we are done.
       */
      if (pairDistance >= distanceLowerBound) {
        break;
      }

      /*
       * If the pair members are leaves
       * then their distance is the exact lower bound.
       * Update the distanceLowerBound to reflect this
       * (which must be smaller, due to the test
       * immediately prior to this).
       */
      if (bndPair.containsDataItem()) {
        // assert: currentDistance < minimumDistanceFound
        if (kNearestNeighbors.size() < k) {
          kNearestNeighbors.add(bndPair);
        } else {
          BoundablePair bp1 = kNearestNeighbors.peek();
          if (bp1.getDistance() > pairDistance) {
            kNearestNeighbors.poll();
            kNearestNeighbors.add(bndPair);
          }
          /*
           * minDistance should be the farthest point in the K nearest neighbor queue.
           */
          BoundablePair bp2 = kNearestNeighbors.peek();
          distanceLowerBound = bp2.getDistance();
        }
      } else {
        /*
         * Otherwise, expand one side of the pair,
         * (the choice of which side to expand is heuristically determined)
         * and insert the new expanded pairs into the queue
         */
        bndPair.expandToQueue(priQ, distanceLowerBound, index, format);
      }
    }
    // done - return items with min distance

    /*
     * Iterate the K Nearest Neighbour Queue and retrieve the item from each BoundablePair
     * in this queue
     */
    ArrayList<DataObjectWithId<T>> results = new ArrayList<>(kNearestNeighbors.size());
    while (!kNearestNeighbors.isEmpty()) {
      BoundablePair bp = kNearestNeighbors.poll();
      results.add((DataObjectWithId<T>) ((ItemBoundable) bp.getBoundable(0)).getItem());
    }
    return results;
  }
}
