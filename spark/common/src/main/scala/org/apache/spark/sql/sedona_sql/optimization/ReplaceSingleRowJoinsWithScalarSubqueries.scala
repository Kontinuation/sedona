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

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sedona_sql.optimization.RewriteUtils.matchOrderToOriginalProjectList

// TODO: Support left, right, and outer joins
object ReplaceSingleRowJoinsWithScalarSubqueries extends Rule[LogicalPlan] {

  private def isScalarPlan(plan: LogicalPlan): Boolean = {
    // This will miss the case where there is a OneRowRelation that is not output by the plan
    plan.output.length == 1 && plan.maxRows.contains(1)
  }

  private def replaceJoin(
      scalarPlan: LogicalPlan,
      other: LogicalPlan,
      expectedOutput: Seq[NamedExpression],
      condition: Option[Expression]): LogicalPlan = {
    val project = Project(
      matchOrderToOriginalProjectList(
        other.output :+ Alias(ScalarSubquery(scalarPlan), scalarPlan.output.head.name)(
          scalarPlan.output.head.exprId),
        expectedOutput),
      other)
    condition match {
      case Some(c) =>
        Filter(
          c.withNewChildren(c.children.map {
            case expression: NamedExpression
                if expression.exprId == scalarPlan.output.head.exprId =>
              ScalarSubquery(scalarPlan)
            case child =>
              child
          }),
          project)
      case None =>
        project
    }
  }

  private def rewritePlan(join: Join): LogicalPlan =
    (isScalarPlan(join.left), isScalarPlan(join.right)) match {
      case (true, true) => replaceJoin(join.left, join.right, join.output, join.condition)
      case (true, false) => replaceJoin(join.left, join.right, join.output, join.condition)
      case (false, true) => replaceJoin(join.right, join.left, join.output, join.condition)
      case _ => join
    }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case join @ Join(_, _, _: InnerLike, _, _) => rewritePlan(join)
    case other => other
  }
}

// When a OneRowRelation does not have a parent Project, the output of the plan will not contain the members of the
// OneRowRelation. Since the OneRowRelation outputs no columns and has a exactly 1 row, a cross join's output is
// equivalent to the other branch of the join. There is some optimization where an Inner Join of a OneRowRelation and
// another plan which doesn't output the OneRowRelation's fields becomes a cross join with the condition move to be a
// filter in the other plan.
// TODO Integrate this logic into the one optimizing the Inner join into a cross join for OneRowRelations.
// TODO handle cases where there is a Condition in the join. Unclear if this is already always handled in the other rule
object OneRowRelationJoin extends Rule[LogicalPlan] {

  private def rewritePlan(join: Join): LogicalPlan =
    (join.left.isInstanceOf[OneRowRelation], join.right.isInstanceOf[OneRowRelation]) match {
      case (true, false) => join.right
      case (false, true) => join.left
      case _ => join // Unclear what to do when both sides are OneRowRelations
    }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case join @ Join(_, _, _: InnerLike, None, _) => rewritePlan(join)
    case other => other
  }
}
