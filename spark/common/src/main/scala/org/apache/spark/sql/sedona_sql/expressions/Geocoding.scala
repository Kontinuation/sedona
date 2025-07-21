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

import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.util.GeocodingUtils.createAddressIndex
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ImplicitCastInputTypes, Unevaluable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, functions => f}

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

case class ST_Geocode(children: Seq[Expression]) extends DataframePhysicalFunction {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def dataType: DataType = StructType(
    Seq(
      StructField("location", StringType),
      StructField("geometry", GeometryUDT),
      StructField("score", LongType),
      StructField("score_algorithm", StringType)))

  override protected def transformDataframe(
      dataframe: DataFrame,
      resultAttrs: Seq[Attribute]): DataFrame = {
    val sedona = org.apache.spark.sql.SparkSession.active
    val conf = SedonaConf.fromActiveSession()

    val dataFrameWithIds = dataframe
      .select(
        f.monotonically_increasing_id().alias("id"),
        getInputColumn(0, "address").alias("location"),
        (if (dataframe.columns.isEmpty) lit(null) else f.struct("*")).alias("dfContents"))
      .checkpoint()

    val ret = getAddressAndScore(
      dataFrameWithIds,
      sedona.table(conf.getGeocodingTableName).checkpoint(),
      getPairs(
        createAddressIndex(dataFrameWithIds),
        sedona.table(conf.getGeocodingIndexTableName)),
      getResultName(resultAttrs))

    ret
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }

  private def getPairs(df1: DataFrame, df2: DataFrame) = {
    val AddressDbPhraseMatched = df1
      .alias("l")
      .join(
        df2.alias("r"),
        f.col("l.token_phrase") === f.col("r.token_phrase") &&
          f.col("l.frequency") <= 100 &&
          f.col("r.frequency") <= 100,
        "inner")
      .select(
        f.col("l.token_phrase"),
        f.col("l.address_ids").alias("address_ids_1"),
        f.col("r.address_ids").alias("address_ids_2"))
      .repartition(f.col("address_ids_1"))

    AddressDbPhraseMatched
      .withColumn("address_id_1", f.explode(f.col("address_ids_1")))
      .withColumn("address_id_2", f.explode(f.col("address_ids_2")))
      .select("address_id_1", "address_id_2")
      .distinct()
  }

  private def getAddressAndScore(
      inputDf: DataFrame,
      referenceDf: DataFrame,
      matchDf: DataFrame,
      resultName: String) = {

    val window =
      Window
        .partitionBy("input.id")
        .orderBy(f.col("score").asc)

    val resultsDf = matchDf
      .alias("matches")
      .join(inputDf.alias("input"), f.col("matches.address_id_1") === f.col("input.id"), "right")
      .join(
        referenceDf.alias("reference"),
        f.col("matches.address_id_2") === f.col("reference.id"),
        "left")
      .withColumn("score", f.levenshtein(f.col("input.location"), f.col("reference.location")))
      .withColumn("rank", f.row_number().over(window))
      .filter(f.col("rank") === 1)

    val dfContentsField = inputDf.schema.fields.find(_.name == "dfContents")
    val resultColumn = f
      .struct(
        f.col("reference.location"),
        f.col("reference.geometry"),
        f.col("score").cast(LongType),
        f.lit("levenshtein").alias("score_algorithm"))
      .alias(resultName)
    if (dfContentsField.exists(field =>
        field.name == "dfContents" && !field.dataType.isInstanceOf[NullType])) {
      resultsDf.select(f.col("dfContents.*"), resultColumn)
    } else {
      resultsDf.select(resultColumn)
    }
  }
}
