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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.column.{ColumnDescriptor, Dictionary}
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.locationtech.jts.io.WKBReader

import java.time.ZoneId

class GeoParquetVectorUpdaterFactory(
    logicalTypeAnnotation: LogicalTypeAnnotation,
    convertTz: ZoneId,
    datetimeRebaseMode: String,
    datetimeRebaseTz: String,
    int96RebaseMode: String,
    int96RebaseTz: String)
    extends ParquetVectorUpdaterFactory(
      logicalTypeAnnotation,
      convertTz,
      datetimeRebaseMode,
      datetimeRebaseTz,
      int96RebaseMode,
      int96RebaseTz) {

  /**
   * Default Parquet type for NULL columns. When the type is not known for an all null column the
   * writer uses int32 as the default type.
   */
  final val NULL_COLUMN_TYPE = PrimitiveTypeName.INT32;

  /**
   * Returns a ParquetVectorUpdater for the given column descriptor and spark type.
   * @param descriptor
   *   The column descriptor
   * @param sparkType
   *   The spark type, e.g. GeometryUDT
   * @param primitiveSparkType
   *   The primitive spark type, e.g. BinaryType
   * @return
   *   A ParquetVectorUpdater for the given column descriptor and spark type
   */
  def getUpdater(
      descriptor: ColumnDescriptor,
      sparkType: DataType,
      primitiveSparkType: DataType): ParquetVectorUpdater = {

    val typeName = descriptor.getPrimitiveType().getPrimitiveTypeName();

    sparkType match {
      case GeometryUDT =>
        new BinaryToGeoConverter()
      case StringType if typeName == NULL_COLUMN_TYPE =>
        new NullConverter()
      case _ =>
        super.getUpdater(descriptor, primitiveSparkType)
    }
  }

  class BinaryToGeoConverter extends ParquetVectorUpdater {

    override def readValues(
        total: Int,
        offset: Int,
        values: WritableColumnVector,
        valuesReader: VectorizedValuesReader): Unit = {
      for (i <- 0 until total) {
        readValue(offset + i, values, valuesReader)
      }
    }

    // This method is used to skip the values in the vectorized reader
    override def skipValues(total: Int, valuesReader: VectorizedValuesReader): Unit = {
      var remaining = total
      while (remaining > 0) {
        val length = valuesReader.readInteger()
        valuesReader.skipIntegers(1)
        valuesReader.skipBinary(length)
        remaining = remaining - 1
      }
    }

    override def readValue(
        offset: Int,
        values: WritableColumnVector,
        valuesReader: VectorizedValuesReader): Unit = {
      val length = valuesReader.readInteger()
      val bytes = valuesReader.readBinary(length).getBytes
      val geoBytes = convertToGeometry(bytes)
      values.putByteArray(offset, geoBytes)
    }

    override def decodeSingleDictionaryId(
        offset: Int,
        values: WritableColumnVector,
        dictionaryIds: WritableColumnVector,
        dictionary: Dictionary): Unit = {
      val v = dictionary.decodeToBinary(dictionaryIds.getDictId(offset))
      val geoBytes = convertToGeometry(v.getBytes)
      values.putByteArray(offset, geoBytes)
    }

    private def convertToGeometry(bytes: Array[Byte]): Array[Byte] = {
      val wkbReader = new WKBReader()
      val geom = wkbReader.read(bytes)
      GeometryUDT.serialize(geom)
    }
  }

  class NullConverter extends ParquetVectorUpdater {
    override def readValues(
        total: Int,
        offset: Int,
        values: WritableColumnVector,
        valuesReader: VectorizedValuesReader): Unit = {
      for (i <- 0 until total) {
        readValue(offset + i, values, valuesReader)
      }
    }

    override def skipValues(total: Int, valuesReader: VectorizedValuesReader): Unit = {
      for (_ <- 0 until total) {
        valuesReader.readInteger()
      }
    }

    override def readValue(
        offset: Int,
        values: WritableColumnVector,
        valuesReader: VectorizedValuesReader): Unit = {
      values.putNull(offset)
    }

    override def decodeSingleDictionaryId(
        offset: Int,
        values: WritableColumnVector,
        dictionaryIds: WritableColumnVector,
        dictionary: Dictionary): Unit = {
      values.putNull(offset)
    }
  }
}
