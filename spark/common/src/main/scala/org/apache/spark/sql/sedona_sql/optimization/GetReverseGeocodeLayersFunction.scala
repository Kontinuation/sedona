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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectSet, Complete}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.sedona_sql.expressions.ST_GetReverseGeocodingLayers
import org.apache.spark.sql.sedona_sql.optimization.RewriteUtils.{aliasOf, matchOrderToOriginalProjectList, retrieveGeocodeTablePlan}

object GetReverseGeocodeLayersFunction extends RewriteLogicalPlan[ST_GetReverseGeocodingLayers] {
  override def rewriteLogicalPlan(
      funcCall: ST_GetReverseGeocodingLayers,
      plan: Project): LogicalPlan = {

    val geocodePlan = retrieveGeocodeTablePlan()

    val geocodeLayer = geocodePlan.outputSet.filter(_.name == "layer").head

    val funcAlias = aliasOf(funcCall, plan)

    val tempLayersExprId = NamedExpression.newExprId
    val LayersListPlan = Aggregate(
      Seq(),
      Seq(
        Alias(
          AggregateExpression(
            CollectSet(geocodeLayer),
            Complete,
            false,
            None,
            NamedExpression.newExprId),
          "placeHolderLayerColumnName")(tempLayersExprId)),
      geocodePlan)

    Project(
      matchOrderToOriginalProjectList(
        plan.projectList.filter(_ != funcAlias)
          :+ Alias(ScalarSubquery(LayersListPlan), funcAlias.name)(funcAlias.exprId),
        plan.projectList),
      plan.child)
  }
}
