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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.sedona_sql.expressions.{ST_Distance, ST_GetReverseGeocodingLayers, ST_ReverseGeocode, ST_SRID}
import org.apache.spark.sql.sedona_sql.optimization.RewriteUtils.{aliasOf, matchOrderToOriginalProjectList, retrieveGeocodeTablePlan}

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

  override def rewriteLogicalPlan(funcCall: ST_ReverseGeocode, plan: Project): LogicalPlan = {
    val funcGeometryArg = funcCall.children(0)
    val funcLayerArg = funcCall.children(1)

    val geocodePlan = retrieveGeocodeTablePlan()

    val geocodePlanAttrs = geocodePlan.outputSet
    val geocodeLayer = geocodePlanAttrs.filter(_.name == "layer").head
    val geocodeLocation = geocodePlanAttrs.filter(_.name == "location").head
    val geocodeGeom = geocodePlanAttrs.filter(_.name == "geometry").head

    val distanceExpression = ST_Distance(Seq(geocodeGeom, funcGeometryArg))

    val assertSRIDPlan = Filter(
      IsNull(
        AssertTrue(
          ArrayContains(
            CreateArray(Seq(Literal(0), Literal(4326))),
            ST_SRID(Seq(funcGeometryArg))))),
      plan.child)

    val assertLayerPlan = Filter(
      IsNull(AssertTrue(ArrayContains(ST_GetReverseGeocodingLayers(Seq()), funcLayerArg))),
      assertSRIDPlan)

    val joinedPlan = Join(
      if (SedonaConf.fromActiveSession.getReverseGeocodingAssertLayerExists) assertLayerPlan
      else assertSRIDPlan,
      geocodePlan,
      JoinType("left"),
      Some(
        And(
          LessThanOrEqual(distanceExpression, getDistanceJoinDistanceThreshold(funcLayerArg)),
          EqualTo(geocodeLayer, funcLayerArg))),
      JoinHint.NONE)

    // Get the closest geocode for each specified layer
    val partitionSpec =
      Seq(funcGeometryArg, funcLayerArg)
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

    val replacedExpr = aliasOf(funcCall, plan)

    val ret = Project(
      matchOrderToOriginalProjectList(
        plan.projectList.filter(x => x.exprId != replacedExpr.exprId) :+ Alias(
          CreateStruct(Seq(geocodeLocation, funcLayerArg, geocodeGeom)),
          replacedExpr.name)(replacedExpr.exprId),
        plan.projectList),
      closestGeocodePerLayerPlan)

    ret
  }
}
