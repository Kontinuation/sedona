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
package org.apache.spark.sql.sedona_sql.expressions.raster

import org.apache.sedona.common.raster.RasterEditors
import org.apache.sedona.sql.utils.RasterSerializer
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Generator
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.ImplicitCastInputTypes
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferrableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterEnhancer
import org.apache.spark.sql.types.AbstractDataType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.geotools.coverage.grid.GridCoverage2D

case class RS_SetSRID(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterEditors.setSrid _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_SetGeoReference(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(RasterEditors.setGeoReference),
      inferrableFunction3(RasterEditors.setGeoReference),
      inferrableFunction7(RasterEditors.setGeoReference)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_SetPixelType(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterEditors.setPixelType _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Resample(inputExpressions: Seq[Expression])
    extends InferredExpression(
      nullTolerantInferrableFunction4(RasterEditors.resample),
      nullTolerantInferrableFunction5(RasterEditors.resample),
      nullTolerantInferrableFunction7(RasterEditors.resample)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_NormalizeAll(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction1(RasterEditors.normalizeAll),
      inferrableFunction3(RasterEditors.normalizeAll),
      inferrableFunction4(RasterEditors.normalizeAll),
      inferrableFunction5(RasterEditors.normalizeAll),
      inferrableFunction6(RasterEditors.normalizeAll),
      inferrableFunction7(RasterEditors.normalizeAll)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_ReprojectMatch(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterEditors.reprojectMatch _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Interpolate(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction1(RasterEditors.interpolate),
      inferrableFunction2(RasterEditors.interpolate),
      inferrableFunction3(RasterEditors.interpolate),
      inferrableFunction4(RasterEditors.interpolate),
      inferrableFunction5(RasterEditors.interpolate),
      inferrableFunction6(RasterEditors.interpolate)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_StackTileExplode(children: Seq[Expression])
    extends Generator
    with ImplicitCastInputTypes
    with CodegenFallback {

  override def elementSchema: StructType = new StructType()
    .add("x", IntegerType, nullable = false)
    .add("y", IntegerType, nullable = false)
    .add("tile", RasterUDT, nullable = false)

  override def inputTypes: Seq[AbstractDataType] =
    Seq(ArrayType(RasterUDT), IntegerType, IntegerType, IntegerType, BooleanType, DoubleType)

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val rasterExpr = children.head
    val refRasterIndex = children(1).eval(input).asInstanceOf[Int]
    val tileWidth = children(2).eval(input).asInstanceOf[Int]
    val tileHeight = children(3).eval(input).asInstanceOf[Int]
    val padWithNoData = children(4).eval(input).asInstanceOf[Boolean]
    val noDataVal = children(5).eval(input).asInstanceOf[Double]

    val arrayData = rasterExpr.eval(input).asInstanceOf[ArrayData]
    val length = arrayData.numElements()
    val rasters = new Array[GridCoverage2D](length)
    for (i <- 0 until length) {
      rasters(i) = RasterSerializer.deserialize(arrayData.getBinary(i))
    }

    try {
      import scala.collection.JavaConverters._
      val tileIterator = RasterEditors.stackTileExplode(
        rasters,
        refRasterIndex,
        tileWidth,
        tileHeight,
        padWithNoData,
        noDataVal)
      tileIterator.setAutoDisposeSource(true)
      tileIterator.setDisposeFunction(() => rasters.foreach(_.dispose(true)))
      tileIterator.asScala.map { tile =>
        val gridCoverage2D = tile.getCoverage
        val row = InternalRow(tile.getTileX, tile.getTileY, gridCoverage2D.serialize)
        gridCoverage2D.dispose(true)
        row
      }
    } catch {
      case e: Exception =>
        rasters.foreach(_.dispose(true))
        throw e
    }
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}
