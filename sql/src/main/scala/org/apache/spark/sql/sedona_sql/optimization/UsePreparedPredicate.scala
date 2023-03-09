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

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sedona_sql.expressions._

/**
 * Rewrites spatial predicates involving literals into prepared predicates. Prepared predicates
 * have better performance and enables spatial filter pushdown optimizations.
 */
class UsePreparedPredicate extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter@Filter(condition, _) =>
      val newCondition = condition transform {
        case predicate: ST_Predicate =>
          val operands = predicate.inputExpressions match {
            case Seq(left, right: Literal) => Some(left, right, false)
            case Seq(left: Literal, right) => Some(right, left, true)
            case _ => None
          }
          operands.map { case (expr, literal, swapped) =>
            transformSpatialPredicate(predicate, expr, literal, swapped)
          }.getOrElse(predicate)
      }
      if (newCondition eq condition) {
        filter
      } else {
        filter.copy(condition = newCondition)
      }
  }

  private def transformSpatialPredicate(predicate: ST_Predicate, expr: Expression, literal: Literal,
                                        swapped: Boolean): ST_PreparedPredicate = {
    predicate match {
      case ST_Contains(_) => if (swapped) ST_PreparedWithin(expr, literal) else ST_PreparedContains(expr, literal)
      case ST_Within(_) => if (swapped) ST_PreparedContains(expr, literal) else ST_PreparedWithin(expr, literal)
      case ST_Covers(_) => if (swapped) ST_PreparedCoveredBy(expr, literal) else ST_PreparedCovers(expr, literal)
      case ST_CoveredBy(_) => if (swapped) ST_PreparedCovers(expr, literal) else ST_PreparedCoveredBy(expr, literal)
      case ST_Intersects(_) => ST_PreparedIntersects(expr, literal)
      case ST_Touches(_) => ST_PreparedTouches(expr, literal)
      case ST_Crosses(_) => ST_PreparedCrosses(expr, literal)
      case ST_Overlaps(_) => ST_PreparedOverlaps(expr, literal)
      case ST_Equals(_) => ST_PreparedEquals(expr, literal)
      case ST_OrderingEquals(_) => ST_PreparedOrderingEquals(expr, literal)
      case ST_Disjoint(_) => ST_PreparedDisjoint(expr, literal)
    }
  }
}
