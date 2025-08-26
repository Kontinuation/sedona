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
package org.apache.spark.sql.execution.datasources.geoparquet

import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.schema.PrimitiveType
import org.apache.spark.sql.execution.datasources.geoparquet.{ExtendableVectorizedColumnReader}
import org.apache.spark.sql.execution.datasources.geoparquet.internal.{ParquetVectorUpdater, ParquetVectorUpdaterFactory}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.DataType

import java.time.ZoneId

class GeoVectorizedColumnReader(
    descriptor: ColumnDescriptor,
    isRequired: Boolean,
    pageReadStore: PageReadStore,
    convertTz: ZoneId,
    datetimeRebaseMode: String,
    datetimeRebaseTz: String,
    int96RebaseMode: String,
    int96RebaseTz: String,
    writerVersion: ParsedVersion,
    updater: ParquetVectorUpdater,
    updaterFactory: ParquetVectorUpdaterFactory,
    sparkType: DataType)
    extends ExtendableVectorizedColumnReader(
      descriptor,
      isRequired,
      pageReadStore,
      convertTz,
      datetimeRebaseMode,
      datetimeRebaseTz,
      int96RebaseMode,
      int96RebaseTz,
      writerVersion,
      updater,
      updaterFactory,
      sparkType) {
  override def isLazyDecodingSupported(
      parquetTypeName: PrimitiveType.PrimitiveTypeName,
      sparkType: DataType): Boolean = {
    sparkType match {
      case _: GeometryUDT => false
      case _ => super.isLazyDecodingSupported(parquetTypeName, sparkType)
    }
  }
}
