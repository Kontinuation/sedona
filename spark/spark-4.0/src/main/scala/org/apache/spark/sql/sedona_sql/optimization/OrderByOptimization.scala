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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.rules.Rule

object OrderByOptimization extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown { case sort @ Sort(order, global, child, None) =>
      val nonAttributeReferences = order.collect {
        case so if !so.child.isInstanceOf[AttributeReference] => so
      }

      if (nonAttributeReferences.isEmpty) {
        sort
      } else {
        val aliases: Seq[Alias] = nonAttributeReferences.map { so =>
          createAlias(so.child)
        }

        val aliasedChild = Project(child.output ++ aliases, child)

        // Replace expressions in SortOrder with alias
        val updatedOrder: Seq[SortOrder] = order.map {
          case so if nonAttributeReferences.contains(so) =>
            so.copy(child = aliases.find(_.child == so.child).get.toAttribute)
          case other => other
        }

        // Wrap the transformed Sort node with a Project to restore original columns and order
        val transformedSort = Sort(updatedOrder, global, aliasedChild)
        Project(sort.output, transformedSort)
      }
    }
  }

  private def createAlias(expression: Expression): Alias = {
    val aliasName = s"function_eval"
    // by default, Alias will call NamedExpression.newExprId
    Alias(expression, aliasName)()
  }

}
