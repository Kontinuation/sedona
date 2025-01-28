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

import org.apache.sedona.common.geometryObjects.Circle
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Expression, LessThan, LessThanOrEqual, Literal, Not, Or, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.parseColumnPath
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{GeoParquetFileFormatBase, GeoParquetSpatialFilter}
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetSpatialFilter.{AndFilter, LeafFilter, OrFilter}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions._
import org.apache.spark.sql.sedona_sql.optimization.ExpressionUtils.splitConjunctivePredicates
import org.apache.spark.sql.types.DoubleType
import org.locationtech.jts.geom.{Geometry, Point}

class SpatialFilterPushDownForGeoParquet(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val enableSpatialFilterPushDown =
      sparkSession.conf.get("spark.sedona.geoparquet.spatialFilterPushDown", "true").toBoolean
    if (!enableSpatialFilterPushDown) plan
    else {
      plan transform {
        case filter @ Filter(condition, lr: LogicalRelation) if isGeoParquetRelation(lr) =>
          val filters = splitConjunctivePredicates(condition)
          val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, lr.output)
          val (_, normalizedFiltersWithoutSubquery) =
            normalizedFilters.partition(SubqueryExpression.hasSubquery)
          val geoParquetSpatialFilters =
            translateToGeoParquetSpatialFilters(normalizedFiltersWithoutSubquery)
          val hadoopFsRelation = lr.relation.asInstanceOf[HadoopFsRelation]
          val fileFormat = hadoopFsRelation.fileFormat.asInstanceOf[GeoParquetFileFormatBase]
          if (geoParquetSpatialFilters.isEmpty) filter
          else {
            val combinedSpatialFilter = geoParquetSpatialFilters.reduce(AndFilter)
            val newFileFormat = fileFormat.withSpatialPredicates(combinedSpatialFilter)
            val newRelation = hadoopFsRelation.copy(fileFormat = newFileFormat)(sparkSession)
            filter.copy(child = lr.copy(relation = newRelation))
          }
      }
    }
  }

  private def isGeoParquetRelation(lr: LogicalRelation): Boolean =
    lr.relation.isInstanceOf[HadoopFsRelation] &&
      lr.relation.asInstanceOf[HadoopFsRelation].fileFormat.isInstanceOf[GeoParquetFileFormatBase]

  def translateToGeoParquetSpatialFilters(
      predicates: Seq[Expression]): Seq[GeoParquetSpatialFilter] = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled = false)
    predicates.flatMap { predicate =>
      translateToGeoParquetSpatialFilter(predicate, pushableColumn)
    }
  }

  private def translateToGeoParquetSpatialFilter(
      predicate: Expression,
      pushableColumn: PushableColumnBase): Option[GeoParquetSpatialFilter] = {
    predicate match {
      case And(left, right) =>
        val spatialFilterLeft = translateToGeoParquetSpatialFilter(left, pushableColumn)
        val spatialFilterRight = translateToGeoParquetSpatialFilter(right, pushableColumn)
        (spatialFilterLeft, spatialFilterRight) match {
          case (Some(l), Some(r)) => Some(AndFilter(l, r))
          case (Some(l), None) => Some(l)
          case (None, Some(r)) => Some(r)
          case _ => None
        }

      case Or(left, right) =>
        for {
          spatialFilterLeft <- translateToGeoParquetSpatialFilter(left, pushableColumn)
          spatialFilterRight <- translateToGeoParquetSpatialFilter(right, pushableColumn)
        } yield OrFilter(spatialFilterLeft, spatialFilterRight)

      case Not(_) => None

      case ST_PreparedContains(pushableColumn(name), Literal(v, _)) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.COVERS, GeometryUDT.deserialize(v)))
      case ST_PreparedCovers(pushableColumn(name), Literal(v, _)) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.COVERS, GeometryUDT.deserialize(v)))
      case ST_PreparedWithin(pushableColumn(name), Literal(v, _)) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.INTERSECTS, GeometryUDT.deserialize(v)))
      case ST_PreparedCoveredBy(pushableColumn(name), Literal(v, _)) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.INTERSECTS, GeometryUDT.deserialize(v)))

      case _: ST_PreparedEquals | _: ST_PreparedOrderingEquals =>
        for ((name, value) <- resolveNameAndLiteral(predicate.children, pushableColumn))
          yield LeafFilter(unquote(name), SpatialPredicate.COVERS, GeometryUDT.deserialize(value))

      case _: ST_PreparedIntersects | _: ST_PreparedCrosses | _: ST_PreparedOverlaps |
          _: ST_PreparedTouches =>
        for ((name, value) <- resolveNameAndLiteral(predicate.children, pushableColumn))
          yield LeafFilter(
            unquote(name),
            SpatialPredicate.INTERSECTS,
            GeometryUDT.deserialize(value))

      case LessThan(ST_Distance(distArgs), Literal(d, DoubleType)) =>
        for ((name, value) <- resolveNameAndLiteral(distArgs, pushableColumn))
          yield distanceFilter(name, GeometryUDT.deserialize(value), d.asInstanceOf[Double])

      case LessThanOrEqual(ST_Distance(distArgs), Literal(d, DoubleType)) =>
        for ((name, value) <- resolveNameAndLiteral(distArgs, pushableColumn))
          yield distanceFilter(name, GeometryUDT.deserialize(value), d.asInstanceOf[Double])

      case _ => None
    }
  }

  private def distanceFilter(name: String, geom: Geometry, distance: Double) = {
    val queryWindow = geom match {
      case point: Point => new Circle(point, distance)
      case _ =>
        val envelope = geom.getEnvelopeInternal
        envelope.expandBy(distance)
        geom.getFactory.toGeometry(envelope)
    }
    LeafFilter(unquote(name), SpatialPredicate.INTERSECTS, queryWindow)
  }

  private def unquote(name: String): String = {
    parseColumnPath(name).mkString(".")
  }

  private def resolveNameAndLiteral(
      expressions: Seq[Expression],
      pushableColumn: PushableColumnBase): Option[(String, Any)] = {
    expressions match {
      case Seq(pushableColumn(name), Literal(v, _)) => Some(name, v)
      case Seq(Literal(v, _), pushableColumn(name)) => Some(name, v)
      case _ => None
    }
  }
}
