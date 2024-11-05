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

import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.{HandleNullInputsForUDF, ResolveRelations}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, Complete}
import org.apache.spark.sql.catalyst.expressions.{Expression, _}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.sedona_sql.expressions.{ST_Distance, ST_ReverseGeocode}
import org.apache.spark.sql.sedona_sql.optimization.RewriteUtils.{assertGeocodeTableWellFormed, matchOrderToOriginalProjectList}
import org.apache.spark.sql.types.{IntegerType, StringType}

import scala.collection.convert.ImplicitConversions.`map AsScala`

object ReverseGeocodingFunction extends RewriteLogicalPlan[ST_ReverseGeocode] {

  private def getDistanceJoinDistanceThreshold(layerName: Expression): CaseWhen = {
    val thresholds = SedonaConf.fromActiveSession.getReverseGeocodingDistanceThresholds
    val cases = thresholds
      .filter(_._1 != "default")
      .map { case (layer, distance) =>
        (EqualTo(layerName, Literal(layer)), Literal(distance))
      }
      .toSeq

    CaseWhen(cases, Literal(thresholds("default")))
  }

  /**
   * Sorts the geocode results by the order of the layers array.
   *
   * Called as a UDF.
   *
   * @param results
   *   The geocode results to sort
   * @param order
   *   The order of the layers
   * @return
   *   The sorted geocode results
   */
  private def sortGeocodeResultsByLayersOrder(
      results: Seq[GenericRowWithSchema],
      order: Seq[GenericRowWithSchema]): Seq[GenericRowWithSchema] = {
    val sorted = order.sortBy(_.getInt(0)).map(_.getString(1))
    results.sortBy(r => sorted.indexOf(r.getString(1)))
  }

  override def rewriteLogicalPlan(funcCall: ST_ReverseGeocode, plan: Project): LogicalPlan = {
    val spark: SparkSession = SparkSession.getActiveSession.get

    val funcGeometryArg = funcCall.children(0)
    val funcLayerArg = funcCall.children(1)

    // Geocode table setup
    val geocodeTableName = SedonaConf.fromActiveSession().getReverseGeocodingTableName
    assertGeocodeTableWellFormed(geocodeTableName)

    var geocodePlan = ResolveRelations(
      EliminateSubqueryAliases(spark.table(geocodeTableName).logicalPlan))

    // If we don't remove the View node, we will get an error that there is no Plan for the view.
    geocodePlan = geocodePlan match {
      case view: View if view.desc.properties.contains("view.storingAnalyzedPlan") =>
        geocodePlan.children.head
      case _ => geocodePlan
    }

    val geocodePlanAttrs = geocodePlan.outputSet
    val geocodeLocation = geocodePlanAttrs.filter(_.name == "location").head
    val geocodeLayer = geocodePlanAttrs.filter(_.name == "layer").head
    val geocodeGeom = geocodePlanAttrs.filter(_.name == "geometry").head

    // Find geocode candidates
    val explodedLayerOutput = AttributeReference("layer", StringType)()
    val distanceExpression = ST_Distance(Seq(geocodeGeom, funcGeometryArg))

    val joinedPlan = Join(
      Generate(Explode(funcLayerArg), Nil, false, None, Seq(explodedLayerOutput), plan.child),
      geocodePlan,
      JoinType("left"),
      Some(
        And(
          LessThan(distanceExpression, getDistanceJoinDistanceThreshold(explodedLayerOutput)),
          EqualTo(geocodeLayer, explodedLayerOutput))),
      JoinHint.NONE)

    // Get the closest geocode for each specified layer
    val partitionSpec =
      plan.inputSet.toSeq :+ explodedLayerOutput // Join + Aggregate pattern assumes input rows are unique
    val orderSpec = Seq(SortOrder(distanceExpression, Ascending))

    val rankExpr = Alias(
      WindowExpression(
        RowNumber(), // Row number does not allow ties. Guarantees at most one result.
        WindowSpecDefinition(
          partitionSpec,
          orderSpec,
          SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))),
      "rank")()

    val windowedPlan = Window(Seq(rankExpr), partitionSpec, orderSpec, joinedPlan)

    val closestGeocodePerLayerPlan = Filter(
      EqualTo(windowedPlan.output.filter(_.exprId == rankExpr.exprId).head, Literal(1)),
      windowedPlan)

    // Aggregate by input row
    // Explicit order in CreateStruct to match ST_ReverseGeocode class output schema
    val collectListExpression = CollectList(
      CreateStruct(Seq(geocodeLocation, explodedLayerOutput, geocodeGeom)))

    val unsortedExprId = NamedExpression.newExprId
    val aggregatedPlan =
      Aggregate(
        plan.inputSet.toSeq, // Join + Aggregate pattern assumes input rows are unique
        plan.inputSet.toSeq :+ Alias(
          AggregateExpression(
            collectListExpression,
            Complete,
            false,
            None,
            NamedExpression.newExprId),
          "unsortedResult")(unsortedExprId),
        closestGeocodePerLayerPlan)

    // Sort results array to match the ordering of the layers array.
    val layerLambdaVar = NamedLambdaVariable("layer", StringType, false)
    val orderLambdaVar = NamedLambdaVariable("order", IntegerType, false)

    val zipExpression = ZipWith(
      Sequence(
        Literal(0),
        Subtract(Size(funcLayerArg), Literal(1)),
        Some(Literal(1)),
        Some(java.time.ZoneOffset.UTC.toString)),
      funcLayerArg,
      LambdaFunction(
        CreateStruct(Seq(orderLambdaVar, layerLambdaVar)),
        Seq(orderLambdaVar, layerLambdaVar)))

    val sortUDF = ScalaUDF(
      sortGeocodeResultsByLayersOrder _,
      collectListExpression.dataType,
      Seq(aggregatedPlan.output.filter(_.exprId == unsortedExprId).head, zipExpression))

    val replacedExpr = plan.projectList
      .filter(x => x.isInstanceOf[Alias] && x.asInstanceOf[Alias].child == funcCall)
      .head

    HandleNullInputsForUDF(
      Project(
        matchOrderToOriginalProjectList(
          plan.projectList.filter(x => x.exprId != replacedExpr.exprId) :+ Alias(
            sortUDF,
            replacedExpr.name)(exprId = replacedExpr.exprId),
          plan.projectList),
        aggregatedPlan))
  }
}
