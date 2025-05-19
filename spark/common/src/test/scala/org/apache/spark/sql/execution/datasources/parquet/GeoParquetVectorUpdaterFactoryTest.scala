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

import org.apache.parquet.bytes.{ByteBufferInputStream, BytesInput}
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainBinaryDictionary
import org.apache.parquet.column.{ColumnDescriptor, Dictionary, Encoding}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Type}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.junit.Assert.{assertArrayEquals, assertFalse, assertTrue}
import org.junit.Test
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{WKBWriter, WKTReader}

import java.nio.{ByteBuffer, ByteOrder}
import java.time.ZoneId

class GeoParquetVectorUpdaterFactoryTest {
  val GEO_COLUMN = "geo_col": String
  val BINARY_COLUMN = "binary_col": String
  val BUFFER_SIZE = 1024

  val geoSparkSchema = StructType(
    Array(StructField(GEO_COLUMN, GeometryUDT), StructField(BINARY_COLUMN, BinaryType)))

  val binaryColDesc = new ColumnDescriptor(
    Array(BINARY_COLUMN),
    new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, BINARY_COLUMN),
    0,
    1000)

  val geoColDesc = new ColumnDescriptor(
    Array(GEO_COLUMN),
    new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, GEO_COLUMN),
    0,
    1000)

  val updaterFactory = new GeoParquetVectorUpdaterFactory(
    LogicalTypeAnnotation.intType(32, true),
    ZoneId.systemDefault(),
    "EXCEPTION",
    "UTC",
    "EXCEPTION",
    "UTC")

  val wktReader = new WKTReader()

  val geoVals: Array[Geometry] = Array(
    wktReader.read("POINT(1 1)"),
    wktReader.read("LINESTRING(1 1, 2 2)"),
    wktReader.read("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"))

  val binaryVals: Array[Array[Byte]] =
    Array(Array[Byte](1, 2, 3), Array[Byte](4, 5, 6), Array[Byte](7, 8, 9))

  @Test
  def getUpdaterWithGeoColumn() {
    val geoUpdater = updaterFactory.getUpdater(geoColDesc, GeometryUDT, BinaryType)
    assertTrue(geoUpdater.isInstanceOf[GeoParquetVectorUpdaterFactory#BinaryToGeoConverter])

    val nonGeoUpdater = updaterFactory.getUpdater(binaryColDesc, BinaryType, BinaryType)
    assertFalse(nonGeoUpdater.isInstanceOf[GeoParquetVectorUpdaterFactory#BinaryToGeoConverter])
  }

  @Test
  def readValueGeoColumn() {
    val updater = updaterFactory.getUpdater(geoColDesc, GeometryUDT, BinaryType)

    val reader = getMockValuesReader(GeometryUDT)
    val actual = new OnHeapColumnVector(BUFFER_SIZE, GeometryUDT)
    updater.readValue(0, actual, reader)

    val expected = getExpectedVector(1, GeometryUDT)
    compareBytes(actual, expected)
  }

  @Test
  def readValueNonGeoBynaryColumn() {
    val updater = updaterFactory.getUpdater(binaryColDesc, BinaryType, BinaryType)

    val reader = getMockValuesReader(BinaryType)
    val actual = new OnHeapColumnVector(BUFFER_SIZE, BinaryType)
    updater.readValue(0, actual, reader)

    val expected = getExpectedVector(1, BinaryType)
    compareBytes(actual, expected)
  }

  @Test
  def readValuesGeoColumn() {
    val updater = updaterFactory.getUpdater(geoColDesc, GeometryUDT, BinaryType)

    val reader = getMockValuesReader(GeometryUDT)
    val actual = new OnHeapColumnVector(BUFFER_SIZE, GeometryUDT)
    updater.readValues(3, 0, actual, reader)

    val expected = getExpectedVector(3, GeometryUDT)
    compareBytes(actual, expected)
  }

  @Test
  def readValuesGeoDictionary(): Unit = {
    val updater = updaterFactory.getUpdater(geoColDesc, GeometryUDT, BinaryType)

    val actual = new OnHeapColumnVector(BUFFER_SIZE, GeometryUDT)
    val dictionaryIds = new OnHeapColumnVector(BUFFER_SIZE, IntegerType)
    val dictionary = getMockDictionary(GeometryUDT)

    for (i <- 0 until 3) {
      dictionaryIds.putInt(i, i)
    }

    updater.decodeSingleDictionaryId(0, actual, dictionaryIds, dictionary)

    val expected = getExpectedVector(1, GeometryUDT)
    compareBytes(actual, expected)
  }

  def getExpectedVector(n: Int, t: DataType): WritableColumnVector = {
    getExpectedVector(n, 0, t)
  }

  def getExpectedVector(total: Int, offset: Int, t: DataType): WritableColumnVector = {
    val values = new OnHeapColumnVector(BUFFER_SIZE, t)
    for (i <- offset until offset + total) {
      t match {
        case BinaryType =>
          values.putByteArray(i, binaryVals(i))
        case GeometryUDT =>
          val geometry = geoVals(i)
          values.putByteArray(i, GeometryUDT.serialize(geometry))
      }
    }
    values
  }

  def getMockBuffer(t: DataType, includeLength: Boolean): ByteBuffer = {
    val wkbWriter = new WKBWriter()
    val mockBuffer = ByteBuffer.allocate(BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN)
    for (i <- 0 until 3) {
      t match {
        case BinaryType =>
          if (includeLength) {
            mockBuffer.putInt(binaryVals(i).length)
          }
          mockBuffer.put(binaryVals(i))
        case GeometryUDT =>
          val geoBytes = wkbWriter.write(geoVals(i))
          if (includeLength) {
            mockBuffer.putInt(geoBytes.length)
          }
          mockBuffer.put(geoBytes)
      }
    }

    mockBuffer.flip()
    mockBuffer
  }

  def getMockValuesReader(t: DataType): VectorizedValuesReader = {
    // Create three different geometries

    val mockBuffer = getMockBuffer(t, true)
    val reader = new VectorizedPlainValuesReader()

    val in = ByteBufferInputStream.wrap(mockBuffer)
    reader.initFromPage(3, in)

    reader
  }

  def getMockDictionary(t: DataType): Dictionary = {
    val mockBuffer = getMockBuffer(t, true)
    val dictionary = new PlainBinaryDictionary(
      new DictionaryPage(
        BytesInput.from(mockBuffer),
        3, // dictionary size
        Encoding.PLAIN))
    dictionary
  }

  def compareBytes(actual: WritableColumnVector, expected: WritableColumnVector): Unit = {
    val expectedBytes = expected.getChild(0).getBytes(0, BUFFER_SIZE)
    val actualBytes = actual.getChild(0).getBytes(0, BUFFER_SIZE)
    assertArrayEquals(expectedBytes, actualBytes)
  }
}
