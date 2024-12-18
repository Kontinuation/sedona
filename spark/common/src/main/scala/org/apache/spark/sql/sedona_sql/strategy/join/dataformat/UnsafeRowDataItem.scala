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
package org.apache.spark.sql.sedona_sql.strategy.join.dataformat

import org.apache.sedona.common.geometrySerde.{GeometryBufferFactory, GeometrySerializer, SerializedCoordinateFilters}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.sedona_sql.expressions.SerdeAware
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}

/**
 * The data item for holding an UnsafeRow. The geometry data is lazily created and cached when the
 * geometry or prepared geometry is accessed.
 */
class UnsafeRowDataItem(val unsafeRow: UnsafeRow) {
  private var geometry: Geometry = _
  private var preparedGeometry: PreparedGeometry = _

  def getEnvelope(boundGeometryExpression: Expression): Envelope = {
    if (geometry == null) {
      boundGeometryExpression match {
        case serdeAware: SerdeAware =>
          // SerdeAware expression can be directly evaluated to objects
          geometry = serdeAware.evalWithoutSerialization(unsafeRow).asInstanceOf[Geometry]
          geometry.getEnvelopeInternal
        case _ =>
          // Directly extract the envelope from the serialized geometry when geometry is null
          val serializedGeometry =
            boundGeometryExpression.eval(unsafeRow).asInstanceOf[Array[Byte]]
          val buffer = GeometryBufferFactory.wrap(serializedGeometry)
          val collector = new SerializedCoordinateFilters.StatisticsCollector()
          collector.apply(buffer)
          collector.getEnvelope
      }
    } else {
      geometry.getEnvelopeInternal
    }
  }

  def getGeometry(boundGeometryExpression: Expression): Geometry = {
    if (geometry == null) {
      geometry = boundGeometryExpression match {
        case serdeAware: SerdeAware =>
          serdeAware.evalWithoutSerialization(unsafeRow).asInstanceOf[Geometry]
        case _ =>
          val serializedGeometry =
            boundGeometryExpression.eval(unsafeRow).asInstanceOf[Array[Byte]]
          GeometrySerializer.deserialize(serializedGeometry)
      }
    }
    geometry
  }

  def getPreparedGeometry(boundGeometryExpression: Expression): PreparedGeometry = {
    if (preparedGeometry == null) {
      val geom = getGeometry(boundGeometryExpression)
      preparedGeometry = UnsafeRowDataItem.PreparedGeometryFactory.create(geom)
    }
    preparedGeometry
  }
}

object UnsafeRowDataItem {
  private val PreparedGeometryFactory = new PreparedGeometryFactory()
}
