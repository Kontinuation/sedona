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

import org.apache.sedona.common.raster.RasterAIFunctions
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/**
 * Convert raster imagery segmentation results to geometries
 * @param inputExpressions
 */
case class RS_SEGMENT_TO_GEOMS(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(
    RasterUDT,
    ArrayType(DoubleType),
    ArrayType(IntegerType),
    MapType(StringType, IntegerType),
    DoubleType)

  override def dataType: DataType = new StructType()
    .add("geometry", ArrayType(GeometryUDT))
    .add("average_pixel_confidence_score", ArrayType(DoubleType))
    .add("label", ArrayType(IntegerType))
    .add("class_name", ArrayType(StringType))

  override def eval(input: InternalRow): Any = {
    val ref = inputExpressions.head.toRaster(input)
    try {
      val confidenceArray = inputExpressions(1).eval(input).asInstanceOf[ArrayData].toDoubleArray()
      val labelArray = inputExpressions(2).eval(input).asInstanceOf[ArrayData].toIntArray()
      val classMap = inputExpressions(3).eval(input).asInstanceOf[MapData]
      val classMapScala = (classMap.valueArray.toIntArray zip
        classMap.keyArray.toArray[UTF8String](StringType).map(_.toString)).toMap
      labelArray.foreach { label =>
        if (!(classMapScala contains label)) {
          throw new IllegalArgumentException(s"Label $label is not present in the class map.")
        }
      }
      val threshold = inputExpressions(4).eval(input).asInstanceOf[Double]
      val results = RasterAIFunctions.segmentToGeoms(ref, confidenceArray, labelArray, threshold)
      val geometries = results.asScala.map(_.geometry).map(GeometrySerializer.serialize).toArray
      val averagePixelConfidenceScores = results.asScala.map(_.averageConfidenceScore).toArray
      val labels = results.asScala.map(_.label).toArray
      val classNames = labels.map { label => UTF8String.fromString(classMapScala(label)) }
      InternalRow(
        ArrayData.toArrayData(geometries),
        ArrayData.toArrayData(averagePixelConfidenceScores),
        ArrayData.toArrayData(labels),
        ArrayData.toArrayData(classNames))
    } finally {
      ref.dispose(true)
    }
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): RS_SEGMENT_TO_GEOMS = {
    copy(inputExpressions = newChildren)
  }
}
