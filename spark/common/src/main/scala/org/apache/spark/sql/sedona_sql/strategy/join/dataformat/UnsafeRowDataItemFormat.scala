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

import org.apache.spark.sedona.core.index.DataItemFormat
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.geom.prep.PreparedGeometry

/**
 * The data item format for accessing geometry data in UnsafeRow. This is used for indexing
 * spatial data when running Spark SQL spatial join queries, where the data were represented by
 * UnsafeRows.
 */
class UnsafeRowDataItemFormat(
    private val numFields: Int,
    private val boundGeometryExpression: Expression)
    extends DataItemFormat[UnsafeRowDataItem] {

  override def serialize(dataItem: UnsafeRowDataItem): Array[Byte] = {
    dataItem.unsafeRow.getBytes
  }

  override def deserialize(bytes: Array[Byte]): UnsafeRowDataItem = {
    val unsafeRow = new UnsafeRow(numFields)
    unsafeRow.pointTo(bytes, bytes.length)
    new UnsafeRowDataItem(unsafeRow)
  }

  override def extractEnvelope(dataItem: UnsafeRowDataItem): Envelope = {
    dataItem.getEnvelope(boundGeometryExpression)
  }

  override def extractGeometry(dataItem: UnsafeRowDataItem): Geometry = {
    dataItem.getGeometry(boundGeometryExpression)
  }

  override def extractPreparedGeometry(dataItem: UnsafeRowDataItem): PreparedGeometry = {
    dataItem.getPreparedGeometry(boundGeometryExpression)
  }
}
