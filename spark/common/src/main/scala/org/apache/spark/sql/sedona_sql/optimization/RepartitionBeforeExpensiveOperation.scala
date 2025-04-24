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
package org.apache.spark.sql.sedona_sql.optimization

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition}
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.sedona_sql.expressions.ST_Intersection
import org.apache.spark.sql.sedona_sql.optimization.RepartitionBeforeExpensiveOperation.{containsExpensiveFunctionCall, getTargetPartitionCount}
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastIndexJoinExec, LeftSide}

/**
 * Adds repartition operations before computationally expensive operations to improve parallelism
 * and performance.
 *
 * It targets operations containing expensive spatial function calls (like ST_Intersection). We
 * need this as a logical plan optimization because the physical plan rule may not be triggered on
 * plans that spark believe are too simple.
 *
 * The rule determines target partition count based on either:
 *   - A multiple of available cores when using static allocation
 *   - The `spark.sql.shuffle.partitions` configuration when using dynamic allocation
 *
 * In the future, we may consider implementing a Physical Node that has access to more information
 * about the RDD and its partitioning to make more informed decisions about repartitioning. This
 * optimization then can inject that node instead of a shuffle exchange.
 *
 * @param context
 *   The SparkContext used to access configuration and default parallelism
 */
class LogicalRepartitionBeforeExpensiveOperation(context: SparkContext)
    extends Rule[LogicalPlan] {

  override def apply(sparkPlan: LogicalPlan): LogicalPlan = {
    sparkPlan.transformUp {
      case plan @ (p: LogicalPlan) if containsExpensiveFunctionCall(p) =>
        addRepartitionToChildren(plan)
      case plan => plan
    }
  }

  private def addRepartitionToChildren(plan: LogicalPlan): LogicalPlan = {
    val numPartitions = getTargetPartitionCount(context)
    plan.withNewChildren(plan.children.map(generateRepartitionExec(numPartitions, _)))
  }

  private def generateRepartitionExec(
      targetPartitionCount: Int,
      child: LogicalPlan): LogicalPlan = {
    // There's no way to know the number of partitions in the child plan
    if (!child.isInstanceOf[Repartition]) {
      Repartition(targetPartitionCount, true, child)
    } else {
      child
    }
  }
}

/**
 * Adds repartition operations before computationally expensive operations to improve parallelism
 * and performance.
 *
 * It targets:
 *   - BroadcastIndexJoin operations - repartitions the stream side
 *   - Operations containing expensive spatial function calls (like ST_Intersection)
 *
 * The rule determines target partition count based on either:
 *   - A multiple of available cores when using static allocation
 *   - The `spark.sql.shuffle.partitions` configuration when using dynamic allocation
 *
 * In the future, we may consider implementing a Physical Node that has access to more information
 * about the RDD and its partitioning to make more informed decisions about repartitioning. This
 * optimization then can inject that node instead of a shuffle exchange.
 *
 * @param context
 *   The SparkContext used to access configuration and default parallelism
 */
class PhysicalRepartitionBeforeExpensiveOperation(context: SparkContext) extends Rule[SparkPlan] {
  private final val FORCE_REPARTITION_RATIO_THRESHOLD = 0.5

  override def apply(sparkPlan: SparkPlan): SparkPlan = {
    sparkPlan.transformUp {
      case plan: BroadcastIndexJoinExec => addRepartitionToStreamSide(plan)
      case plan @ (p: SparkPlan) if containsExpensiveFunctionCall(p) =>
        addRepartitionToChildren(plan)
      case plan => plan
    }
  }

  private def addRepartitionToChildren(plan: SparkPlan): SparkPlan = {
    val numPartitions = getTargetPartitionCount(context)
    plan.withNewChildren(plan.children.map(generateRepartitionExec(numPartitions, _)))
  }

  private def addRepartitionToStreamSide(plan: BroadcastIndexJoinExec) = {
    val numPartitions = getTargetPartitionCount(context)
    if (plan.indexBuildSide == LeftSide) {
      plan.copy(left = plan.left, right = generateRepartitionExec(numPartitions, plan.right))
    } else {
      plan.copy(left = generateRepartitionExec(numPartitions, plan.left), right = plan.right)
    }
  }

  private def generateRepartitionExec(targetPartitionCount: Int, child: SparkPlan): SparkPlan = {
    if (child.outputPartitioning.numPartitions / targetPartitionCount < FORCE_REPARTITION_RATIO_THRESHOLD && !child
        .isInstanceOf[ShuffleQueryStageExec]) {
      ShuffleExchangeExec(RoundRobinPartitioning(targetPartitionCount), child)
    } else {
      child
    }
  }
}

private[optimization] object RepartitionBeforeExpensiveOperation {
  def getTargetPartitionCount(context: SparkContext): Int = {
    // When core count is fixed, use some multiple of the number of cores
    if (!context.getConf.get("spark.dynamicAllocation.enabled", "false").toBoolean) {
      4 * context.defaultParallelism
    } else
      // Otherwise we can't assume we should use all currently
      // available cores or only currently  available cores
      context.getConf.get("spark.sql.shuffle.partitions", "200").toInt
  }

  def containsExpensiveFunctionCall(plan: QueryPlan[_]): Boolean =
    plan.expressions.exists(e =>
      e.isInstanceOf[ST_Intersection] ||
        e.children.exists(_.isInstanceOf[ST_Intersection]))
}
