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
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.sedona_sql.DataFrameShims
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.StringType

object RewriteUtils {

  /**
   * Orders the currentExpressions to match the originalProjectList order based on the name of the
   * expressions.
   *
   * @param currentExpressions
   *   The expressions to order
   * @param originalProjectList
   *   The original project list of which to match the order
   * @return
   *   The currentExpressions ordered to match the originalProjectList order
   */
  def matchOrderToOriginalProjectList(
      currentExpressions: Seq[NamedExpression],
      originalProjectList: Seq[NamedExpression]): Seq[NamedExpression] = {
    val exprIds = originalProjectList.map(_.exprId)
    currentExpressions.sortBy(r => exprIds.indexOf(r.exprId))
  }

  /**
   * Asserts that the geocode table is well-formed.
   *
   * This helps users diagnose issues with their geocode table setup.
   *
   * @param geocodeTableName
   *   The name of the geocode table to check
   */
  def assertGeocodeTableWellFormed(geocodeTableName: String): Unit = {
    val baseMessage = f"spark.sedona.reverse.geocode.table set to $geocodeTableName."
    val geocodePlan =
      SparkSession.getActiveSession.get.table(geocodeTableName).queryExecution.optimizedPlan

    val geocodePlanAttrs = geocodePlan.outputSet

    require(
      geocodePlanAttrs.exists(_.name == "location"),
      f"$baseMessage $geocodeTableName does not have a location column")
    require(
      geocodePlanAttrs.exists(_.name == "layer"),
      f"$baseMessage $geocodeTableName does not have a layer column")
    require(
      geocodePlanAttrs.exists(_.name == "geometry"),
      f"$baseMessage $geocodeTableName does not have a geometry column")

    val geocodeLocation = geocodePlanAttrs.filter(_.name == "location").head
    val geocodeLayer = geocodePlanAttrs.filter(_.name == "layer").head
    val geocodeGeom = geocodePlanAttrs.filter(_.name == "geometry").head

    require(
      geocodeGeom.dataType.isInstanceOf[GeometryUDT],
      f"$baseMessage $geocodeTableName's geometry column is not type Geometry")
    require(
      geocodeLayer.dataType.isInstanceOf[StringType],
      f"$baseMessage $geocodeTableName's layer column is not type String")
    require(
      geocodeLocation.dataType.isInstanceOf[StringType],
      f"$baseMessage $geocodeTableName's location column is not type String")
  }

  def aliasOf(expr: Expression, plan: Project): Alias = {
    plan.projectList
      .filter(x => x.isInstanceOf[Alias] && x.asInstanceOf[Alias].child == expr)
      .head
      .asInstanceOf[Alias]
  }

  /**
   * Retrieve an optimized LogicalPlan for the geocode table or view.
   *
   * The geocode table retrieved is based on the reverse Geocoding table name set in the
   * SedonaConf.
   *
   * @return
   *   The optimized LogicalPlan for the geocode table
   */
  def retrieveGeocodeTablePlan(): LogicalPlan = {
    val geocodeTableName = SedonaConf.fromActiveSession().getReverseGeocodingTableName
    val geocodeDf = SparkSession.getActiveSession.get.table(geocodeTableName)
//    val geocodePlan = geocodeDf.asInstanceOf[ClassicDataFrame].logicalPlan
    val geocodePlan = DataFrameShims.getLogicalPlan(geocodeDf)

    assertGeocodeTableWellFormed(geocodeTableName)

    val geocodePlanAttrs = geocodePlan.outputSet

    // when there are multiple calls, we need these to be aliased to be disambiguated
    Project(
      Seq(
        Alias(geocodePlanAttrs.filter(_.name == "location").head, "location")(),
        Alias(geocodePlanAttrs.filter(_.name == "layer").head, "layer")(),
        Alias(geocodePlanAttrs.filter(_.name == "geometry").head, "geometry")()),
      geocodePlan)
  }

}
