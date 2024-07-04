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
package org.apache.sedona.core.spatialPartitioning;

import static org.apache.sedona.core.formatMapper.shapefileParser.ShapefileRDD.geometryFactory;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.sedona.core.knnJudgement.EuclideanItemDistance;
import org.apache.sedona.core.spatialPartitioning.quadtree.QuadRectangle;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * The class is used to build an R-tree over a random sample of another dataset and uses distance
 * bounds to ensure efficient local kNN joins.
 *
 * <p>By calculating distance bounds and using circle range queries, it ensures that the subsets Si,
 * containing all necessary points for accurate kNN results. The final union of local join results
 * provides the complete kNN join result for the datasets R and S.
 *
 * <p>It generates List<List<Integer>> expandedParitionedBoundaries based on the quad tree.
 */
public class QuadTreeRTPartitioning extends QuadtreePartitioning {
  // A query-only R-tree created using the Sort-Tile-Recursive (STR) algorithm.
  private STRtree strTree;

  // The expanded partitioned boundaries based on the quad tree
  private HashMap<Integer, List<Envelope>> mbrs;

  public QuadTreeRTPartitioning(List<Envelope> samples, Envelope boundary, int partitions) {
    super(samples, boundary, partitions);
  }

  public QuadTreeRTPartitioning(
      List<Envelope> samples, Envelope boundary, int partitions, int minTreeLevel) {
    super(samples, boundary, partitions, minTreeLevel);
  }

  public HashMap<Integer, List<Envelope>> getMbrs() {
    return mbrs;
  }

  /**
   * This function is used to build the STR tree from the quad-tree built from the samples. It is
   * used to expand the partitioned boundaries.
   *
   * @param samples the samples
   * @param k the number of neighbor samples
   * @return
   */
  public STRtree buildSTRTree(List<Envelope> samples, int k, double samplingProbability) {
    // The partitioned MBRs
    mbrs = new HashMap<>();

    // A query-only R-tree created using the Sort-Tile-Recursive (STR) algorithm.
    strTree = new STRtree();

    // Get all MBRs (partitions) from the quad-tree
    // The zones might include the one with null partition ids
    List<QuadRectangle> partitionMBRsList =
        partitionTree.getAllZones().stream()
            .filter(quadRect -> quadRect.partitionId != null)
            .collect(Collectors.toList());

    for (QuadRectangle quadRect : partitionMBRsList) {
      Envelope mbr = quadRect.getEnvelope();
      strTree.insert(mbr, mbr);
    }

    // Insert samples into an STR tree for k-nearest neighbor search
    STRtree sampleTree = new STRtree();
    for (Envelope sample : samples) {
      // convert sample to a point
      Point point =
          geometryFactory.createPoint(
              new Coordinate(sample.centre().getX(), sample.centre().getY()));
      sampleTree.insert(sample, point);
    }

    // For each MBR in the quad-tree
    for (QuadRectangle quadRect : partitionMBRsList) {
      Envelope partitionMBR = quadRect.getEnvelope();

      // 1- Calculate the centroid of each MBR in the STR tree
      double centroidX = (partitionMBR.getMinX() + partitionMBR.getMaxX()) / 2.0;
      double centroidY = (partitionMBR.getMinY() + partitionMBR.getMaxY()) / 2.0;
      Coordinate centroidCoord = new Coordinate(centroidX, centroidY);
      Point centroid = geometryFactory.createPoint(centroidCoord);

      // 2- Compute the maximum distance ui from the centroid to any point inside the partition
      double ui =
          Math.max(
              centroid.distance(
                  geometryFactory.createPoint(
                      new Coordinate(partitionMBR.getMinX(), partitionMBR.getMinY()))),
              Math.max(
                  centroid.distance(
                      geometryFactory.createPoint(
                          new Coordinate(partitionMBR.getMinX(), partitionMBR.getMaxY()))),
                  Math.max(
                      centroid.distance(
                          geometryFactory.createPoint(
                              new Coordinate(partitionMBR.getMaxX(), partitionMBR.getMinY()))),
                      centroid.distance(
                          geometryFactory.createPoint(
                              new Coordinate(partitionMBR.getMaxX(), partitionMBR.getMaxY()))))));

      // 3 - Find the k-nearest neighbors in the samples of the centroid in the STR tree
      Object[] kNearestNeighbors =
          sampleTree.nearestNeighbour(
              centroid.getEnvelopeInternal(), centroid, new EuclideanItemDistance(), k);

      // 4 - Calculate the distance to the farthest neighbor
      double maxDistance = 0;
      for (Object neighbor : kNearestNeighbors) {
        if (neighbor instanceof Envelope) {
          Envelope neighborEnvelope = (Envelope) neighbor;
          Coordinate neighborCoord =
              new Coordinate(neighborEnvelope.centre().getX(), neighborEnvelope.centre().getY());
          Point neighborPoint = geometryFactory.createPoint(neighborCoord);
          double distance = centroid.distance(neighborPoint);
          if (distance > maxDistance) {
            maxDistance = distance;
          }
        }
      }

      // 5 - Construct the circle with radius ui and center centroid
      // Calculate the radius of the circle
      double gamma_i = 2 * ui + maxDistance;
      // Since we're working with rectangles, this would be an envelope that fully contains the
      // circle
      Envelope circleEnvelope =
          new Envelope(
              centroidX - gamma_i, centroidX + gamma_i,
              centroidY - gamma_i, centroidY + gamma_i);

      // 6 - Compute all the MBRs that intersect with the circle and add them to a hash map
      List<Envelope> intersectingMBRs = strTree.query(circleEnvelope);
      mbrs.put(quadRect.partitionId, intersectingMBRs);
    }

    // 6 - Return the STR tree
    return strTree;
  }
}
