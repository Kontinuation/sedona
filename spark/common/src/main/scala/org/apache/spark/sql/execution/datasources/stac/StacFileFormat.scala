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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.json._
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.json.{JsonDataSource, JsonOutputWriter}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.types.StructType

class StacFileFormat extends TextBasedFileFormat with DataSourceRegister {
  override val shortName: String = "stac"

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = {
    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    val stacDataSource = JsonDataSource(parsedOptions)
    stacDataSource.isSplitable && super.isSplitable(sparkSession, options, path)
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {

    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    // Use existing logic to infer the full schema first
    val fullSchemaOption = JsonDataSource(parsedOptions).inferSchema(
      sparkSession, files, parsedOptions)

    fullSchemaOption.map { fullSchema =>
      // Add a new field for the UDT representation of geometry
      val geometryUDTField = StructField("geometryUDT", GeometryUDT, nullable = true)
      val newFields = fullSchema.fields :+ geometryUDTField

      if (fullSchema.fieldNames.contains("properties") && fullSchema("properties").dataType.isInstanceOf[StructType]) {
        val propertiesSchema = fullSchema("properties").dataType.asInstanceOf[StructType]

        // Use createExtendedSchema to elevate 'properties' fields to top level
        StacUtils.createExtendedSchema(StructType(newFields), propertiesSchema.fields.map(f => f.name -> f.dataType).toMap)
      } else {

        StructType(newFields)
      }
    }
  }

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {

    val conf = job.getConfiguration
    val parsedOptions = new JSONOptions(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    parsedOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {

        // Extract the names of nested property fields
        val propertiesFieldNames: Set[String] = dataSchema.find(_.name == "properties") match {
          case Some(structField) if structField.dataType.isInstanceOf[StructType] =>
            structField.dataType.asInstanceOf[StructType].fieldNames.toSet
          case _ => Set.empty[String]
        }

        // Define the STAC schema
        val stacSchema = StructType(dataSchema.fields.filterNot(field =>
          propertiesFieldNames.contains(field.name) || field.name == "geometryUDT" // exclude elevated properties and geometryUDT
        ))

        val transformToStac: InternalRow => InternalRow = row =>
          StacUtils.extractStacFields(row, dataSchema, stacSchema)

        new JsonOutputWriter(path, parsedOptions, stacSchema, context) {
          override def write(row: InternalRow): Unit = {
            // Transform the row to STAC format before writing
            super.write(transformToStac(row))
          }
        }
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".json" + CodecStreams.getCompressionExtension(context)
      }
    }
  }


  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    val actualSchema =
      StructType(requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    ExprUtils.verifyColumnNameOfCorruptRecord(dataSchema, parsedOptions.columnNameOfCorruptRecord)

    if (requiredSchema.length == 1 &&
      requiredSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
//      throw QueryCompilationErrors.queryFromRawFilesIncludeCorruptRecordColumnError()
    }

    (file: PartitionedFile) => {
      val parser = new JacksonParser(
        actualSchema,
        parsedOptions,
        allowArrayAsStructs = true)
      val dataSource = JsonDataSource(parsedOptions)

      dataSource.readFile(
        broadcastedHadoopConf.value.value,
        file,
        parser,
        requiredSchema).map(row => {

        // Extract and elevate properties from the row
        var elevatedProperties = StacUtils.elevateProperties(row, actualSchema)

        // If geometry field exists, convert it and add to elevatedProperties
        if (actualSchema.fieldNames.contains("geometry")) {
          val geometryFieldIndex = actualSchema.fieldIndex("geometry")
          val geometryInternalRow = row.getStruct(geometryFieldIndex, actualSchema("geometry").dataType.asInstanceOf[StructType].fields.length)
          val geometrySchema = actualSchema("geometry").dataType.asInstanceOf[StructType]
          val geometryUDT = StacUtils.convertGeometryStructToSerialized(geometryInternalRow, geometrySchema)
          elevatedProperties = elevatedProperties + ("geometryUDT" -> geometryUDT)
        }

        // Create a new row with elevated properties using the extended schema
        StacUtils.createNewRow(row, elevatedProperties, actualSchema, requiredSchema)
      })
    }
  }


  override def toString: String = "JSON"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[StacFileFormat]

  override def supportDataType(dataType: DataType): Boolean = dataType match {

    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType) }

    case ArrayType(elementType, _) => supportDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType) && supportDataType(valueType)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _: NullType => true

    case _ => false
  }
}
