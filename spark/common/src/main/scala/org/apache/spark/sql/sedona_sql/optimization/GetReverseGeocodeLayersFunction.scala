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
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.ResolveRelations
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectSet, Complete}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.sedona_sql.expressions.ST_GetReverseGeocodingLayers
import org.apache.spark.sql.sedona_sql.optimization.RewriteUtils.{assertGeocodeTableWellFormed, matchOrderToOriginalProjectList}

object GetReverseGeocodeLayersFunction extends RewriteLogicalPlan[ST_GetReverseGeocodingLayers] {
  override def rewriteLogicalPlan(
      funcCall: ST_GetReverseGeocodingLayers,
      plan: Project): LogicalPlan = {
    val spark: SparkSession = SparkSession.getActiveSession.get
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

    val geocodeLayer = geocodePlan.outputSet.filter(_.name == "layer").head

    val funcAlias = plan.projectList
      .filter(x => x.isInstanceOf[Alias] && x.asInstanceOf[Alias].child == funcCall)
      .head

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
