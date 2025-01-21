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

import org.apache.sedona.core.enums.KNNJoinBroadcastSide
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry

case class BroadcastQuerySideKNNJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftShape: Expression,
    rightShape: Expression,
    joinSide: JoinSide,
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

  broadcastSide = KNNJoinBroadcastSide.QUERY_SIDE

  /**
   * Broadcast the dominant shapes (objects) to all the partitions
   *
   * This type of the join does not need to do spatial partition.
   *
   * For left side (queries) broadcast: the join needs to be reduced after the join. For right
   * side (objects) broadcast: the join does not need to be reduced after the join.
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

    // No need to do spatial partitioning for broadcast join
  }

  /**
   * Copy the plan with new children
   * @param newLeft
   * @param newRight
   * @return
   */
  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}
