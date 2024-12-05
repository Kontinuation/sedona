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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.sedona.stats.clustering.DBSCAN.dbscan
import org.apache.sedona.stats.outlierDetection.LocalOutlierFactor.localOutlierFactor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, GenericRowWithSchema, ImplicitCastInputTypes, Literal, ScalarSubquery, Unevaluable}
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.locationtech.jts.geom.Geometry

import scala.reflect.ClassTag

// We mark ST_GeoStatsFunction as non-deterministic to avoid the filter push-down optimization pass
// duplicates the ST_GeoStatsFunction when pushing down aliased ST_GeoStatsFunction through a
// Project operator. This will make ST_GeoStatsFunction being evaluated twice.
trait ST_GeoStatsFunction extends Expression with ImplicitCastInputTypes with Unevaluable {

  final override lazy val deterministic: Boolean = false

  override def nullable: Boolean = true

  private final lazy val sparkSession = SparkSession.getActiveSession.get

  protected final lazy val geometryColumnName = children(0) match {
    case ref: AttributeReference => ref.name
    case _ =>
      throw new IllegalArgumentException("geometry argument must be a column reference")
  }

  def getResultName(resultAttrs: Seq[Attribute]): String = resultAttrs match {
    case Seq(attr) => attr.name
    case _ => throw new IllegalArgumentException("resultAttrs must have exactly one attribute")
  }

  def doExecute(dataframe: DataFrame, resultAttrs: Seq[Attribute]): DataFrame

  def execute(plan: SparkPlan, resultAttrs: Seq[Attribute]): RDD[InternalRow] = {
    toInternalRowRDD(
      doExecute(
        Dataset.ofRows(sparkSession, LogicalRDD(plan.output, plan.execute())(sparkSession)),
        resultAttrs).rdd)
  }

  protected def toInternalRowRDD(rdd: RDD[Row]): RDD[InternalRow] = rdd.map(rowToInternal)

  private def rowToInternal(row: Row): InternalRow = {
    val values = row.toSeq.map {
      case geometry: Geometry => GeometrySerializer.serialize(geometry)
      case row: GenericRowWithSchema => rowToInternal(row)
      case elm => elm
    }.toArray
    new GenericInternalRow(values)
  }

  protected def getScalarValue[T](i: Int, name: String)(implicit ct: ClassTag[T]): T = {
    children(i) match {
      case Literal(l: T, _) => l
      case _: Literal =>
        throw new IllegalArgumentException(f"$name must be an instance of  ${ct.runtimeClass}")
      case s: ScalarSubquery =>
        s.eval() match {
          case t: T => t
          case _ =>
            throw new IllegalArgumentException(
              f"$name must be an instance of  ${ct.runtimeClass}")
        }
      case _ => throw new IllegalArgumentException(f"$name must be a scalar value")
    }
  }
}

case class ST_DBSCAN(children: Seq[Expression]) extends ST_GeoStatsFunction {

  override def dataType: DataType = StructType(
    Seq(StructField("isCore", BooleanType), StructField("cluster", LongType)))

  override def inputTypes: Seq[AbstractDataType] =
    Seq(GeometryUDT, DoubleType, IntegerType, BooleanType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def doExecute(dataframe: DataFrame, resultAttrs: Seq[Attribute]): DataFrame = {
    require(
      !dataframe.columns.contains("__isCore"),
      "__isCore is a  reserved name by the dbscan algorithm. Please rename the columns before calling the ST_DBSCAN function.")
    require(
      !dataframe.columns.contains("__cluster"),
      "__cluster is a  reserved name by the dbscan algorithm. Please rename the columns before calling the ST_DBSCAN function.")

    dbscan(
      dataframe,
      getScalarValue[Double](1, "epsilon"),
      getScalarValue[Int](2, "minPts"),
      geometryColumnName,
      SedonaConf.fromActiveSession().getDBSCANIncludeOutliers,
      getScalarValue[Boolean](3, "useSpheroid"),
      "__isCore",
      "__cluster")
      .withColumn(getResultName(resultAttrs), struct(col("__isCore"), col("__cluster")))
      .drop("__isCore", "__cluster")
  }
}
case class ST_LocalOutlierFactor(children: Seq[Expression]) extends ST_GeoStatsFunction {

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(GeometryUDT, IntegerType, BooleanType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def doExecute(dataframe: DataFrame, resultAttrs: Seq[Attribute]): DataFrame = {
    localOutlierFactor(
      dataframe,
      getScalarValue[Int](1, "k"),
      geometryColumnName,
      SedonaConf.fromActiveSession().getLOFApproximateKNN,
      SedonaConf.fromActiveSession().isIncludeTieBreakersInKNNJoins,
      getScalarValue[Boolean](2, "useSpheroid"),
      getResultName(resultAttrs))
  }
}
