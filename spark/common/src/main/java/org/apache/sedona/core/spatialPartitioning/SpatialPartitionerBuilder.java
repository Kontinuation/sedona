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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.spatialPartitioning.quadtree.QuadRectangle;
import org.apache.sedona.core.spatialPartitioning.quadtree.StandardQuadTree;
import org.locationtech.jts.geom.Envelope;

/** Builder for creating spatial partitioner using sampled envelopes. */
public class SpatialPartitionerBuilder {
  private final PartitioningUtils tree;
  private final GridType gridType;
  private final Envelope boundary;
  private int neighborSampleNumber = -1;
  private double samplingProbability = 0.01f;

  /**
   * Construct a spatial partitioner builder.
   *
   * @param gridType the grid type
   * @param numPartitions the number of partitions
   * @param totalSamples the total number of samples
   * @param boundary the boundary of partitioned space
   */
  public SpatialPartitionerBuilder(
      GridType gridType, int numPartitions, int totalSamples, Envelope boundary) {
    this.gridType = gridType;
    this.boundary = boundary;
    switch (gridType) {
      case EQUALGRID:
        {
          // Force the quad-tree to grow up to a certain level
          // So the actual num of partitions might be slightly different
          int minLevel = (int) Math.max(Math.log(numPartitions) / Math.log(4), 0);
          QuadtreePartitioning quadtreePartitioning =
              new QuadtreePartitioning(totalSamples, boundary, numPartitions, minLevel);
          tree = quadtreePartitioning.getPartitionTree();
          break;
        }
      case QUADTREE:
        {
          QuadtreePartitioning quadtreePartitioning =
              new QuadtreePartitioning(totalSamples, boundary, numPartitions);
          tree = quadtreePartitioning.getPartitionTree();
          break;
        }
      case KDBTREE:
        {
          // According to the research efforts by W. Wright, the average utilization of B-tree page
          // is
          // ln(2), which is approximately 70%. So this is a more accurate estimation of
          // maxItemsPerNode:
          //
          // int maxItemsPerNode = Math.max((int)(totalSamples / (numPartitions * 0.70)), 1);
          //
          // We are not using this formula here because having slightly more spatial partitions
          // usually yields
          // better performance.
          int maxItemsPerNode = Math.max(totalSamples / numPartitions, 1);
          tree = new KDB(maxItemsPerNode, numPartitions, boundary);
          break;
        }
      case ZORDER:
        {
          tree = new IntervalTree(boundary, numPartitions);
          break;
        }

      default:
        throw new IllegalArgumentException("Unsupported spatial partitioning method " + gridType);
    }
  }

  /**
   * Add samples to the spatial partitioner, so that the partitioner can generate spatial partitions
   * according to the distribution of the samples.
   *
   * @param samples the samples
   */
  @SuppressWarnings("unchecked")
  public void addSamples(List<Envelope> samples) {
    switch (gridType) {
      case QUADTREE:
        StandardQuadTree<Integer> quadTree = (StandardQuadTree<Integer>) tree;
        for (final Envelope sample : samples) {
          if (boundary.covers(sample)) {
            quadTree.insert(new QuadRectangle(sample), 1);
          } else if (boundary.intersects(sample)) {
            Envelope truncatedSample = boundary.intersection(sample);
            quadTree.insert(new QuadRectangle(truncatedSample), 1);
          }
        }
        break;

      case KDBTREE:
        KDB kdbTree = (KDB) tree;
        for (final Envelope sample : samples) {
          if (boundary.covers(sample)) {
            kdbTree.insert(sample);
          } else if (boundary.intersects(sample)) {
            Envelope truncatedSample = boundary.intersection(sample);
            kdbTree.insert(truncatedSample);
          }
        }
        break;

      case ZORDER:
        IntervalTree linearTree = (IntervalTree) tree;
        for (final Envelope sample : samples) {
          if (boundary.covers(sample)) {
            linearTree.insert(sample);
          } else if (boundary.intersects(sample)) {
            Envelope truncatedSample = boundary.intersection(sample);
            linearTree.insert(truncatedSample);
          }
        }
        break;
    }
  }

  /**
   * Build the spatial partitioner.
   *
   * @return the spatial partitioner
   */
  @SuppressWarnings("unchecked")
  public SpatialPartitioner build() {
    switch (gridType) {
      case EQUALGRID:
      case QUADTREE:
        StandardQuadTree<Integer> quadTree = (StandardQuadTree<Integer>) tree;
        quadTree.assignPartitionIds();
        return new QuadTreePartitioner(quadTree);

      case KDBTREE:
        KDB kdbTree = (KDB) tree;
        kdbTree.assignLeafIds();
        return new KDBTreePartitioner(kdbTree);

      case ZORDER:
        IntervalTree linearTree = (IntervalTree) tree;
        linearTree.build(neighborSampleNumber, samplingProbability);
        return new ZOrderPartitioner(linearTree);

      default:
        throw new IllegalStateException("Unknown spatial partitioning method " + gridType);
    }
  }

  public enum SpatialPartitionBuildingStrategy {
    NAIVE,
    SUBSAMPLING
  }

  public static SpatialPartitioner buildSpatialPartitionerForSpatialJoin(
      SpatialPartitionBuildingStrategy strategy,
      GridType gridType,
      Envelope bound,
      int numPartitions,
      List<Envelope> samples,
      long estimatedLeftCount,
      List<Envelope> otherSamples,
      long estimatedRightCount) {
    switch (strategy) {
      case NAIVE:
        return buildSpatialPartitionerForSpatialJoinNaive(
            gridType,
            bound,
            numPartitions,
            samples,
            estimatedLeftCount,
            otherSamples,
            estimatedRightCount);
      case SUBSAMPLING:
        return buildSpatialPartitionerForSpatialJoinSubSampling(
            gridType,
            bound,
            numPartitions,
            samples,
            estimatedLeftCount,
            otherSamples,
            estimatedRightCount);
      default:
        throw new IllegalArgumentException(
            "Unsupported spatial partition building strategy " + strategy);
    }
  }

  public static SpatialPartitioner buildSpatialPartitionerForSpatialJoinNaive(
      GridType gridType,
      Envelope bound,
      int numPartitions,
      List<Envelope> samples,
      long estimatedLeftCount,
      List<Envelope> otherSamples,
      long estimatedRightCount) {
    int totalSamplesInBounds = samples.size() + otherSamples.size();
    SpatialPartitionerBuilder builder =
        new SpatialPartitionerBuilder(gridType, numPartitions, totalSamplesInBounds, bound);

    // We use the samples from the larger RDD as the base of the spatial partitioner.
    // The samples from the smaller RDD are for further partitioning to achieve better load balance.
    if (estimatedLeftCount > estimatedRightCount) {
      builder.addSamples(samples);
      builder.addSamples(otherSamples);
    } else {
      builder.addSamples(otherSamples);
      builder.addSamples(samples);
    }
    return builder.build();
  }

  public static SpatialPartitioner buildSpatialPartitionerForSpatialJoinSubSampling(
      GridType gridType,
      Envelope bound,
      int numPartitions,
      List<Envelope> left,
      long estimatedLeftCount,
      List<Envelope> right,
      long estimatedRightCount) {
    // Both left and right are in-bound samples. We need the total count of both datasets to do a
    // further sub-sampling
    double leftRatio = (estimatedLeftCount > 0 ? (double) left.size() / estimatedLeftCount : 0);
    double rightRatio = (estimatedRightCount > 0 ? (double) right.size() / estimatedRightCount : 0);
    List<Envelope> leftSubSamples;
    List<Envelope> rightSubSamples;
    if (leftRatio > rightRatio && leftRatio > 0) {
      // Sub-sample the left dataset
      double ratio = rightRatio / leftRatio;
      leftSubSamples = subSample(left, ratio);
      rightSubSamples = right;
    } else if (leftRatio < rightRatio && rightRatio > 0) {
      // Sub-sample the right dataset
      double ratio = leftRatio / rightRatio;
      rightSubSamples = subSample(right, ratio);
      leftSubSamples = left;
    } else {
      // No sub-sampling is needed
      leftSubSamples = left;
      rightSubSamples = right;
    }

    return buildSpatialPartitionerForSpatialJoinNaive(
        gridType,
        bound,
        numPartitions,
        leftSubSamples,
        estimatedLeftCount,
        rightSubSamples,
        estimatedRightCount);
  }

  public static List<Envelope> subSample(List<Envelope> samples, double ratio) {
    Random random = new Random();
    List<Envelope> subSamples = new ArrayList<>();
    for (Envelope envelope : samples) {
      if (random.nextDouble() < ratio) {
        subSamples.add(envelope);
      }
    }
    return subSamples;
  }

  public void setNeighborSampleNumber(int neighborSampleNumber) {
    this.neighborSampleNumber = neighborSampleNumber;
  }

  public void setSamplingProbability(double samplingProbability) {
    this.samplingProbability = samplingProbability;
  }
}
