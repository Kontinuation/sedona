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
package org.apache.spark.sql.sedona_sql.optimization

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sedona_sql.io.raster.RasterFileFormat
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * Automatically repartition relations loaded using the raster data source, since we'd like to
 * distribute the dataframe containing out-db raster column data across the cluster. This usually
 * won't introduce too much performance overhead, since the size of the out-db raster values are
 * usually way smaller than the raster data referenced by the out-db raster values, so even a
 * large raster dataset won't be too large when being loaded as a dataframe of out-db rasters.
 */

class AutoRepartitionRasterRelation(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (isAutoRepartitionEnabled) {
      val visited = mutable.HashSet.empty[LogicalRelation]
      plan transformDown {
        case p @ Repartition(_, _, r: LogicalRelation) =>
          visited.add(r)
          p
        case r: LogicalRelation if isRasterRelation(r) =>
          if (visited.contains(r)) {
            r
          } else {
            visited.add(r)
            val numPartitions = getNumPartitions
            Repartition(numPartitions, shuffle = true, r)
          }
      }
    } else {
      plan
    }

  private def isAutoRepartitionEnabled: Boolean = {
    sparkSession.conf.get("spark.wherobots.raster.load.autoRepartition", "true").toBoolean
  }

  private def isRasterRelation(lr: LogicalRelation): Boolean = {
    lr.relation match {
      case r: HadoopFsRelation => r.fileFormat.isInstanceOf[RasterFileFormat]
      case _ => false
    }
  }

  private def getNumPartitions: Int = {
    sparkSession.conf.get("spark.wherobots.raster.load.numPartitions", "0").toInt match {
      case 0 => sparkSession.sparkContext.defaultParallelism * 4
      case n => n
    }
  }
}
