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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.core.enums.DistanceMetric
import org.apache.sedona.core.enums.GridType
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.core.spatialPartitioning.QuadTreeRTPartitioner
import org.apache.sedona.core.spatialPartitioning.ZOrderPartitioner
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry

/**
 * KNN / AKNN joins requires target geometries (objects) to be in the same partition as the query
 * geometries. To create an overlap and guarantee matching geometries end up in the same
 * partition, the target geometry is expanded during partitioning.
 *
 * E.g., SELECT * FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, $numNeighbors,
 * true) SELECT * FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, $numNeighbors,
 * true)
 *
 * @param left
 *   left side of the join
 * @param right
 *   right side of the join
 * @param leftShape
 *   shape expression for the left side
 * @param rightShape
 *   shape expression for the right side
 * @param k
 *   \- number of neighbors to find
 * @param useApproximate
 *   whether to use approximate distance for the join
 * @param spatialPredicate
 *   spatial predicate as join condition
 * @param condition
 *   full join condition
 * @param extraCondition
 *   extra join condition other than spatialPredicate
 */
case class KNNJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftShape: Expression,
    rightShape: Expression,
    joinType: JoinType,
    k: Expression,
    searchRadius: Expression,
    useApproximate: Boolean,
    spatialPredicate: SpatialPredicate,
    isGeography: Boolean,
    condition: Expression,
    extraCondition: Option[Expression] = None)
    extends SedonaBinaryExecNode
    with TraitKNNJoinQueryExec
    with Logging {

  /**
   * Copy the plan with new children
   * @param newLeft
   * @param newRight
   * @return
   */
  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }

  /**
   * Execute the spatial partitioning for KNN join This is required to ensure that the target
   * geometries (objects) are in the same partition as the query geometries.
   *
   * Different KNN algorithms require different partitioning strategies. E.g., approximate KNN
   * join requires a different partitioning strategy than exact KNN join.
   *
   * @param objectsShapes
   *   the dominant shapes (objects)
   * @param queryShapes
   *   the follower shapes (queries)
   * @param numPartitions
   *   the number of partitions
   * @param sedonaConf
   *   the Sedona configuration
   */
  override def doSpatialPartitioning(
      objectsShapes: SpatialRDD[Geometry],
      queryShapes: SpatialRDD[Geometry],
      numPartitions: Integer,
      sedonaConf: SedonaConf): Unit = {
    require(numPartitions > 0, "The number of partitions must be greater than 0.")
    val kValue: Int = this.k.eval().asInstanceOf[Int]
    require(kValue >= 1, "The number of neighbors (k) must be equal or greater than 1.")
    objectsShapes.setNeighborSampleNumber(kValue)
    val searchRadius: Double =
      Option(this.searchRadius.eval()).map(_.asInstanceOf[Double]).getOrElse(-1)
    objectsShapes.setDistanceMetric(
      if (isGeography) DistanceMetric.HAVERSINE else DistanceMetric.EUCLIDEAN)
    if (searchRadius > 0) { objectsShapes.setSearchRadius(searchRadius) }
    if (useApproximate) {
      approximateSpatialPartitioning(objectsShapes, queryShapes, numPartitions)
    } else {
      exactSpatialPartitioning(objectsShapes, queryShapes, numPartitions)
    }
  }

  /**
   * Approximate spatial partitioning for KNN join
   * @param dominantShapes
   *   the dominant (objects) shapes
   * @param followerShapes
   *   the follower (queries) shapes
   * @param kValue
   */
  private def approximateSpatialPartitioning(
      dominantShapes: SpatialRDD[Geometry],
      followerShapes: SpatialRDD[Geometry],
      numPartitions: Integer): Unit = {
    // use z-order partitioning, as it is an approximate algorithm
    dominantShapes.spatialPartitioning(GridType.ZORDER, numPartitions)
    followerShapes.spatialPartitioning(
      dominantShapes.getPartitioner.asInstanceOf[ZOrderPartitioner].nonOverlappedPartitioner())
  }

  /**
   * Exact spatial partitioning for KNN join
   * @param dominantShapes
   *   the dominant (objects) shapes
   * @param followerShapes
   *   the follower (queries) shapes
   */
  private def exactSpatialPartitioning(
      dominantShapes: SpatialRDD[Geometry],
      followerShapes: SpatialRDD[Geometry],
      numPartitions: Integer): Unit = {
    // analyze the both RDDs to get the statistics (e.g., boundary)
    dominantShapes.advancedAnalyze()
    followerShapes.advancedAnalyze()

    // expand the boundary for partition to include both RDDs
    dominantShapes.getStatistics.getBoundary.expandToInclude(
      followerShapes.getStatistics.getBoundary)

    // use modified quadtree partitioning, as it is an exact algorithm
    dominantShapes.spatialPartitioning(GridType.QUADTREE_RTREE, numPartitions)
    followerShapes.spatialPartitioning(
      dominantShapes.getPartitioner
        .asInstanceOf[QuadTreeRTPartitioner]
        .nonOverlappedPartitioner())
  }
}
