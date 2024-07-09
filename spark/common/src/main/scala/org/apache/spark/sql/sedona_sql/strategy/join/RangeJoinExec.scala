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

import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode

/**
 * ST_Contains(left, right) - left contains right or ST_Intersects(left, right) - left and right
 * intersect
 *
 * @param left
 *   left side of the join
 * @param right
 *   right side of the join
 * @param leftShape
 *   expression for the first argument of spatialPredicate
 * @param rightShape
 *   expression for the second argument of spatialPredicate
 * @param joinType
 *   join type
 * @param spatialPredicate
 *   spatial predicate as join condition
 * @param condition
 *   full join condition
 * @param extraCondition
 *   extra join condition other than spatialPredicate
 * @param unneededLeftAttributes
 *   unneeded left attributes after joining
 * @param unneededRightAttributes
 *   unneeded right attributes after joining
 */
case class RangeJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftShape: Expression,
    rightShape: Expression,
    joinType: JoinType,
    spatialPredicate: SpatialPredicate,
    condition: Expression,
    extraCondition: Option[Expression],
    override val unneededLeftAttributes: Seq[Attribute],
    override val unneededRightAttributes: Seq[Attribute])
    extends SedonaBinaryExecNode
    with TraitAdvancedJoinQueryExec
    with Logging {

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}
