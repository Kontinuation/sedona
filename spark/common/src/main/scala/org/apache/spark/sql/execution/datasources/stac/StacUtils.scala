/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sedona_sql.io.stac

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.locationtech.jts.geom._
import org.locationtech.jts.io.geojson.GeoJsonReader
import org.apache.spark.sql.types._


object StacUtils {
  def elevateProperties(row: InternalRow, schema: StructType): Map[String, Any] = {
    val propertiesFieldIndex = schema.fieldIndex("properties")
    val propertiesSchema = schema(propertiesFieldIndex).dataType.asInstanceOf[StructType]
    val propertiesRow = row.getStruct(propertiesFieldIndex, propertiesSchema.fields.length)

    propertiesSchema.fields.flatMap { field =>
      val fieldIndex = propertiesSchema.fieldIndex(field.name)
      val fieldValue = propertiesRow.get(fieldIndex, field.dataType)
      if (fieldValue != null) {
        Some(field.name -> fieldValue)
      } else {
        Some(field.name -> null)
      }
    }.toMap
  }

  def createExtendedSchema(originalSchema: StructType, elevatedProperties: Map[String, Any]): StructType = {
    val extendedFields = originalSchema.fields.toBuffer
    val propertiesField = originalSchema.find(_.name == "properties")

    val propertiesStruct = propertiesField.flatMap(f => f.dataType match {
      case st: StructType => Some(st)
      case _ => None
    })

    elevatedProperties.foreach { case (key, _) =>
      val originalFieldName = key
      val maybeOriginalField = propertiesStruct.flatMap(_.find(_.name == originalFieldName))
      maybeOriginalField match {
        case Some(field) =>
          extendedFields += StructField(key, field.dataType, nullable = true)
        case None =>
          extendedFields += StructField(key, StringType, nullable = true)
      }
    }

    StructType(extendedFields.toArray)
  }

  def createNewRow(originalRow: InternalRow, elevatedProperties: Map[String, Any], originalSchema: StructType, extendedSchema: StructType): InternalRow = {
    val values = new Array[Any](extendedSchema.fields.length)

    // Populate existing fields from the original row
    originalSchema.fields.indices.foreach { i =>
      values(i) = originalRow.get(i, originalSchema.fields(i).dataType)
    }

    // Populate new fields from elevatedProperties
    extendedSchema.fields.zipWithIndex.foreach { case (field, index) =>
      if (elevatedProperties.contains(field.name)) {
        values(index) = elevatedProperties(field.name)
      }
    }

    InternalRow.fromSeq(values)
  }

  def structToGeoJson(geometryStruct: InternalRow, geometrySchema: StructType): String = {
    val jsonBuilder = new StringBuilder("{")
    geometrySchema.fields.foreach { field =>
      field.name match {
        case "coordinates" =>
          val coords = geometryStruct.getArray(geometrySchema.fieldIndex(field.name))
          val coordJson = (0 until coords.numElements()).map { i =>
            val innerArray1 = coords.getArray(i)
            val innerJson1 = (0 until innerArray1.numElements()).map { j =>
              val innerArray2 = innerArray1.getArray(j)
              val coordPair = (0 until innerArray2.numElements()).map { k =>
                innerArray2.getDouble(k).toString
              }.mkString("[", ", ", "]")
              coordPair
            }.mkString("[", ", ", "]")
            innerJson1
          }.mkString("[", ", ", "]")
          jsonBuilder.append(s""""coordinates": $coordJson""")
        case _ =>
          val value = geometryStruct.get(geometrySchema.fieldIndex(field.name), field.dataType).toString
          jsonBuilder.append(s""""${field.name}": "$value"""")
      }
      jsonBuilder.append(", ")
    }
    jsonBuilder.setLength(jsonBuilder.length - 2)
    jsonBuilder.append("}")
    jsonBuilder.toString()
  }

  def convertGeometryStructToSerialized(geometryStruct: InternalRow, geometrySchema: StructType): Array[Byte] = {
    val geoJson = structToGeoJson(geometryStruct, geometrySchema)
    val reader = new GeoJsonReader()
    val geometry: Geometry = reader.read(geoJson)
    GeometryUDT.serialize(geometry)
  }

  def extractStacFields(row: InternalRow, originalSchema: StructType, stacSchema: StructType): InternalRow = {
    val values = stacSchema.fields.map(field =>
      row.get(originalSchema.fieldIndex(field.name), field.dataType)
    )
    InternalRow.fromSeq(values)
  }

}
