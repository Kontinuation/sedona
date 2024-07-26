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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.common.geometryObjects.NullGeometry
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.{GeometrySerializer, RasterSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.strategy.join.TraitJoinQueryBase.createUnsafeRowProjector
import org.locationtech.jts.geom.Geometry

trait TraitJoinQueryBase {
  self: SparkPlan =>

  def isRasterJoin(leftShapeExpr: Expression, rightShapeExpr: Expression): Boolean =
    leftShapeExpr.dataType.isInstanceOf[RasterUDT] || rightShapeExpr.dataType
      .isInstanceOf[RasterUDT]

  def isGeographyDistanceJoin: Boolean = false

  def distanceExpression: Option[Expression] = None

  def toSpatialRddPair(
      leftRdd: RDD[UnsafeRow],
      leftShapeExpr: Expression,
      rightRdd: RDD[UnsafeRow],
      rightShapeExpr: Expression): (SpatialRDD[Geometry], SpatialRDD[Geometry]) = {
    if (isRasterJoin(leftShapeExpr, rightShapeExpr)) {
      (toWGS84EnvelopeRDD(leftRdd, leftShapeExpr), toWGS84EnvelopeRDD(rightRdd, rightShapeExpr))
    } else {
      (leftToSpatialRDD(leftRdd, leftShapeExpr), rightToSpatialRDD(rightRdd, rightShapeExpr))
    }
  }

  def toSpatialRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression,
      projection: Option[Seq[Expression]] = None): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .mapPartitions { iter =>
          val toUserData = createUnsafeRowProjector(projection)
          iter.map { row =>
            val serializedShape = shapeExpression.eval(row).asInstanceOf[Array[Byte]]
            val shape = if (serializedShape != null) {
              GeometrySerializer.deserialize(serializedShape)
            } else {
              new NullGeometry()
            }
            val userData = toUserData(row)
            shape.setUserData(userData)
            shape
          }
        }
        .toJavaRDD())
    spatialRdd
  }

  def leftToSpatialRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression,
      projection: Option[Seq[Expression]] = None): SpatialRDD[Geometry] = {
    toSpatialRDD(rdd, shapeExpression, projection)
  }

  def rightToSpatialRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression,
      projection: Option[Seq[Expression]] = None): SpatialRDD[Geometry] = {
    toSpatialRDD(rdd, shapeExpression, projection)
  }

  def toExpandedEnvelopeRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression,
      boundRadius: Expression,
      isGeography: Boolean,
      projection: Option[Seq[Expression]] = None): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .mapPartitions { iter =>
          val toUserData = createUnsafeRowProjector(projection)
          iter.map { row =>
            val serializedShape = shapeExpression.eval(row).asInstanceOf[Array[Byte]]
            val expandedEnvelope = if (serializedShape != null) {
              val shape = GeometrySerializer.deserialize(serializedShape)
              val distance = boundRadius.eval(row).asInstanceOf[Double]
              JoinedGeometry.geometryToExpandedEnvelope(shape, distance, isGeography)
            } else {
              new NullGeometry()
            }
            val userData = toUserData(row)
            expandedEnvelope.setUserData(userData)
            expandedEnvelope
          }
        }
        .toJavaRDD())
    spatialRdd
  }

  def toWGS84EnvelopeRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression): SpatialRDD[Geometry] = {
    // This RDD is for performing raster-geometry or raster-raster join, where we need to perform implicit CRS
    // transformation for both sides. We use expanded WGS84 envelope as the joined geometries and perform a
    // coarse-grained spatial join.
    val spatialRdd = new SpatialRDD[Geometry]
    val wgs84EnvelopeRdd = if (shapeExpression.dataType.isInstanceOf[RasterUDT]) {
      rdd.map { row =>
        val serializedRaster = shapeExpression.eval(row).asInstanceOf[Array[Byte]]
        val shape = if (serializedRaster != null) {
          val raster = RasterSerializer.deserialize(serializedRaster)
          try {
            JoinedGeometryRaster.rasterToWGS84Envelope(raster)
          } finally {
            raster.dispose(true)
          }
        } else {
          new NullGeometry()
        }
        shape.setUserData(row.copy)
        shape
      }
    } else {
      rdd.map { row =>
        val serializedShape = shapeExpression.eval(row).asInstanceOf[Array[Byte]]
        val shape = if (serializedShape != null) {
          val geom = GeometrySerializer.deserialize(serializedShape)
          JoinedGeometryRaster.geometryToWGS84Envelope(geom)
        } else {
          new NullGeometry()
        }
        shape.setUserData(row.copy)
        shape
      }
    }
    spatialRdd.setRawSpatialRDD(wgs84EnvelopeRdd)
    spatialRdd
  }

  def projectSpatialRDD(
      spatialRDD: SpatialRDD[Geometry],
      projection: Option[Seq[Expression]]): SpatialRDD[Geometry] = {
    projection match {
      case Some(_) =>
        val rawSpatialRDD = spatialRDD.rawSpatialRDD.rdd.mapPartitions { shapes =>
          val toUserData = createUnsafeRowProjector(projection)
          shapes.map { shape =>
            val rowData = shape.getUserData.asInstanceOf[UnsafeRow]
            val newUserData = toUserData(rowData)
            shape.setUserData(newUserData)
            shape
          }
        }
        val newSpatialRDD = new SpatialRDD[Geometry]
        newSpatialRDD.setRawSpatialRDD(rawSpatialRDD)
        newSpatialRDD.setStatistics(spatialRDD.getStatistics)
        newSpatialRDD
      case None => spatialRDD
    }
  }

  /**
   * Assemble the projection for the left and right side to eliminate unneeded geometry attributes
   * @param plan
   *   Spark plan
   * @param unneeded
   *   Unneeded attributes
   * @return
   *   Projection expressions
   */
  def projection(plan: SparkPlan, unneeded: Seq[Attribute]): Option[Seq[Expression]] = {
    if (unneeded.isEmpty) None
    else {
      Some(plan.output.map { ref =>
        if (unneeded.exists(_.semanticEquals(ref))) {
          Literal(null, ref.dataType)
        } else {
          BindReferences.bindReference(ref.asInstanceOf[Expression], plan.output)
        }
      })
    }
  }

  def doSpatialPartitioning(
      dominantShapes: SpatialRDD[Geometry],
      followerShapes: SpatialRDD[Geometry],
      numPartitions: Integer,
      sedonaConf: SedonaConf): Unit = {
    if (dominantShapes.approximateTotalCount > 0 && numPartitions > 0) {
      dominantShapes.spatialPartitioning(sedonaConf.getJoinGridType, numPartitions)
      followerShapes.spatialPartitioning(dominantShapes.getPartitioner)
    }
  }
}

object TraitJoinQueryBase {
  def createUnsafeRowProjector(
      projection: Option[Seq[Expression]],
      copy: Boolean = true): UnsafeRow => UnsafeRow = {
    projection match {
      case Some(attrs) =>
        val projection = GenerateUnsafeProjection.generate(attrs)
        if (copy) { (row: UnsafeRow) =>
          projection(row).copy
        } else { (row: UnsafeRow) =>
          projection(row)
        }
      case None =>
        if (copy) { (row: UnsafeRow) =>
          row.copy
        } else { (row: UnsafeRow) =>
          row
        }
    }
  }
}
