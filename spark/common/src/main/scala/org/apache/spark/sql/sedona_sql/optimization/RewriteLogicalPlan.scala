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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExpressionSet, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._

abstract class RewriteLogicalPlan[FunctionType <: Expression: TypeTag] extends Rule[LogicalPlan] {

  /**
   * Replaces the existing Logical Plan with a new Logical plan where funcCall is replaced.
   *
   * @param funcCall
   *   The function call to rewrite
   * @param plan
   *   The Project operator containing the function call
   * @return
   *   The rewritten logical plan
   */
  protected def rewriteLogicalPlan(funcCall: FunctionType, plan: Project): LogicalPlan

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery if s.correlated => plan
    case _ =>
      plan
        .transformUp { case plan: LogicalPlan =>
          unnestFunctionCallsAndMigrateToProjects(plan)
        }
        .transformUp { case plan: LogicalPlan =>
          extract(plan)
        }
  }

  def expressionContainsFunction(expr: Expression): Boolean = {
    expr.find { e =>
      runtimeMirror(e.getClass.getClassLoader)
        .classSymbol(e.getClass)
        .toType =:= typeOf[FunctionType]
    }.isDefined
  }

  // this function is only written for child pattern
  // return is (one that goes in parent, ones that goes in child)
  // we don't have to handle nested cases here since they're not allowed by collectFunctionCallsFromExpressions
  private def replaceFunctionCallWithAttributeReference(
      expr: Expression): (Expression, Seq[Alias]) = {
    if (runtimeMirror(expr.getClass.getClassLoader)
        .classSymbol(expr.getClass)
        .toType =:= typeOf[FunctionType]) {
      val exprId = NamedExpression.newExprId

      (
        AttributeReference("function_eval", expr.dataType)(exprId),
        Seq(Alias(expr, "function_eval")(exprId)))
    } else if (expr.children.isEmpty) {
      (expr, Seq())

    } else {
      val (newParentExprs, newChildExprs) = handleAliases(expr.children)
      (expr.withNewChildren(newParentExprs), newChildExprs)
    }
  }

  private def handleAliases(
      exprs: Seq[Expression],
      splitAlias: Boolean = false): (Seq[Expression], Seq[Alias]) = {
    val childExprs = ListBuffer[Alias]()
    val parentExprs = exprs.map {
      case alias: Alias =>
        if (splitAlias) {
          val (reference, aliases) = replaceFunctionCallWithAttributeReference(alias)
          childExprs ++= aliases
          reference
        } else {
          val (reference, aliases) = replaceFunctionCallWithAttributeReference(alias.child)
          childExprs ++= aliases
          alias.withNewChildren(Seq(reference))
        }
      case ref: AttributeReference => ref
      case expr: Expression =>
        val (reference, aliases) = replaceFunctionCallWithAttributeReference(expr)
        childExprs ++= aliases
        reference
    }
    (
      parentExprs,
      childExprs.toSeq
    ) // toSeq is necessary to convert ListBuffer to Seq in scala 2.13
  }

  private def unnestFunctionCallsAndMigrateToProjects(plan: LogicalPlan): LogicalPlan = {
    if (collectFunctionCallsFromExpressions(plan.expressions).isEmpty) {
      return plan
    }

    val ret = plan match {
      case p: Project => // child strategy
        val (newProjectList, nweChildExprs) = handleAliases(p.projectList)
        p.copy(projectList = newProjectList
          .asInstanceOf[Seq[NamedExpression]])
          .withNewChildren(
            Seq(
              if (nweChildExprs.nonEmpty)
                Project(p.child.outputSet.toSeq ++ nweChildExprs, p.child)
              else p.child))
      case f: Filter =>
        // sandwich strategy
        val (newConditionExprs, newChildExprs) = handleAliases(Seq(f.condition))
        Project(
          f.output,
          f.copy(condition = newConditionExprs.head)
            .withNewChildren(Seq(Project(f.child.output ++ newChildExprs, f.child))))
      case s: Sort =>
        // sandwich strategy
        // child project materializes the function call results
        // parent project removes those added columns
        val childExpressions = ListBuffer[Alias]()
        Project(
          s.child.output,
          s.copy(order = s.order.map { s: SortOrder =>
            val (parents, children) = handleAliases(Seq(s.child))
            childExpressions ++= children
            SortOrder(parents.head, s.direction, s.nullOrdering, s.sameOrderExpressions)
          }).withNewChildren( // Consider short-circuiting if aliasedFunctionCalls is empty
            Seq(
              if (childExpressions.nonEmpty)
                Project(s.child.outputSet.toSeq ++ childExpressions, s.child)
              else s.child)))
      case a: Aggregate =>
        // Grouping Expression: Child Pattern
        val (newGroupingExpressions, newChildFunctionCalls) =
          handleAliases(a.groupingExpressions, splitAlias = true)
        val modifiedGroupingExpressions =
          a.groupingExpressions.zip(newGroupingExpressions).find(x => x._1 != x._2)
        val aAfterGrouping = a
          .copy(
            groupingExpressions = newGroupingExpressions,
            aggregateExpressions = a.aggregateExpressions.map {
              case alias: Alias =>
                Alias(
                  modifiedGroupingExpressions
                    .filter(_._1 == alias.child)
                    .map(_._2)
                    .getOrElse(alias.child),
                  alias.name)(alias.exprId)
              case n: NamedExpression => n
            })
          .withNewChildren(Seq(if (newChildFunctionCalls.nonEmpty)
            Project(a.child.outputSet.toSeq ++ newChildFunctionCalls, a.child)
          else a.child))
          .asInstanceOf[Aggregate]

        // Aggregate Expressions
        // TODO support these. James was having a hard time implementing this.
        // To handle these, there are three cases:
        // 1. The function call is on top of an AggregateFunction. Here we can create a parent Project
        // 2. The function call is neither above nor below an aggregate expression. More efficient to do as a parent project.
        // 3. The function call is below an AggregateFunction. Here we can create a child Project
        // If there are nested AggregateFunctions, that's probably not valid SQL.
        // This simplifies to 2 cases:
        // a. (From 1 and 2 above) There is not a FunctionType below an AggregateFunction
        // b. (From 3 above) There is a FunctionType call is below an AggregateFunction
        if (collectFunctionCallsFromExpressions(aAfterGrouping.aggregateExpressions).nonEmpty) {
          throw new IllegalArgumentException(
            f"Unsupported call to ${typeOf[FunctionType].baseClasses.head.name} in aggregate expression." +
              " If this is not the case, report a bug.")
        } else {
          aAfterGrouping
        }
      case _: BinaryNode =>
        throw new IllegalArgumentException(
          f"${typeOf[FunctionType].baseClasses.head.name} functions cannot be in a Join or other binary operator")
      case _ =>
        throw new IllegalArgumentException(
          f"${typeOf[FunctionType].baseClasses.head.name} functions found in unsupported operator. Report a bug.")
    }

    ret
  }

  private def collectFunctionCallsFromExpressions(
      expressions: Seq[Expression]): Seq[FunctionType] = {
    def collectFunctions(expr: Expression): Seq[FunctionType] = {

      val expressionType =
        runtimeMirror(expr.getClass.getClassLoader).classSymbol(expr.getClass).toType
      if (expressionType =:= typeOf[FunctionType]) {
        if (collectFunctionCallsFromExpressions(expr.children).nonEmpty) {
          throw new IllegalArgumentException(
            f"${typeOf[FunctionType].baseClasses.head.name} calls cannot be nested")
        }
        Seq(expr).asInstanceOf[Seq[FunctionType]]
      } else {
        expr.children.flatMap(collectFunctions)
      }
    }

    expressions.flatMap(collectFunctions)
  }

  /**
   * Extract all the geo-stats functions from the current operator and evaluate them before the
   * operator.
   */
  private def extract(plan: LogicalPlan): LogicalPlan = {
    val geoStatsFuncs =
      ExpressionSet(collectFunctionCallsFromExpressions(plan.expressions))
        // ignore the Function Calls that come from second/third aggregate, which is not used
        .filter(func => func.references.subsetOf(plan.inputSet))
        .filter(func => plan.children.exists(child => func.references.subsetOf(child.outputSet)))
        .toSeq
        .asInstanceOf[Seq[FunctionType]]
    if (geoStatsFuncs.isEmpty) {
      // If there aren't any, we are done.
      plan
    } else {
      // We'll call extract recursively later to transform other function calls
      val geoStatsFunc = geoStatsFuncs.head

      if (!plan.isInstanceOf[Project]) {
        throw new IllegalArgumentException(
          f"${typeOf[FunctionType].baseClasses.head.name} functions must be in a Project operator. Report a Bug.")
      }

      val rewritten = rewriteLogicalPlan(geoStatsFunc, plan.asInstanceOf[Project])

      // extract remaining function calls recursively
      extract(rewritten)
    }
  }

}
