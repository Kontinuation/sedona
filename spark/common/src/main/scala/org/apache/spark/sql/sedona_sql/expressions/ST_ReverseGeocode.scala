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

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Unevaluable}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._

case class ST_ReverseGeocode(children: Seq[Expression])
    extends Expression
    with ImplicitCastInputTypes
    with Unevaluable {

  // Mark ST_ReverseGeocode as non-deterministic to avoid the filter push-down optimization which duplicates
  // the ST_ReverseGeocode function when pushing down aliased ST_ReverseGeocode calls.
  // This places ST_ReverseGeocode calls in non-Project statements, which isn't supported and duplicate execution would
  // be costly.
  final override lazy val deterministic: Boolean = false

  override def nullable: Boolean = true

  override def dataType: DataType =
    StructType(
      Seq(
        StructField("location", StringType),
        StructField("layer", StringType),
        StructField("geometry", GeometryUDT)))

  override def inputTypes: Seq[AbstractDataType] = Seq(GeometryUDT, StringType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

}

case class ST_GetReverseGeocodingLayers(children: Seq[Expression])
    extends Expression
    with ImplicitCastInputTypes
    with Unevaluable {

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(StringType)

  override def inputTypes: Seq[AbstractDataType] = Seq()

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

}
