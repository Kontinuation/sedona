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

import org.apache.hadoop.conf.Configuration
import org.apache.sedona.common.raster.RasterConstructors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterEnhancer
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

case class RS_FromArcInfoAsciiGrid(inputExpressions: Seq[Expression])
  extends InferredExpression(RasterConstructors.fromArcInfoAsciiGrid _) {
  override def foldable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_FromGeoTiff(inputExpressions: Seq[Expression])
  extends InferredExpression(RasterConstructors.fromGeoTiff _) {

  override def foldable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_FromPath(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with ExpectsInputTypes {

  override def children: Seq[Expression] = inputExpressions

  override def foldable: Boolean = false

  override def nullable: Boolean = true

  override def dataType: DataType = RasterUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  private val serializableConf = {
    val hadoopConf = SparkSession.getActiveSession match {
      case Some(sparkSession) => sparkSession.sparkContext.hadoopConfiguration
      case None => new Configuration()
    }
    new SerializableConfiguration(hadoopConf)
  }

  override def eval(input: InternalRow): Any = {
    var conf = serializableConf.value
    val path = inputExpressions(0).eval(input).asInstanceOf[UTF8String]
    val params = inputExpressions(1).eval(input).asInstanceOf[UTF8String]
    if (path == null) null else {
      if (params != null) {
        val overrideConf = params.toString.split(";").map(_.trim.split("="))
        if (overrideConf.nonEmpty) {
          conf = new Configuration(conf)
          overrideConf.foreach { case Array(key, value) => conf.set(key, value) }
        }
      }

      RasterConstructors.fromPath(path.toString, conf).serialize
    }
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_MakeEmptyRaster(inputExpressions: Seq[Expression])
  extends InferredExpression(
    inferrableFunction6(RasterConstructors.makeEmptyRaster),
    inferrableFunction10(RasterConstructors.makeEmptyRaster)) {

  override def foldable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
