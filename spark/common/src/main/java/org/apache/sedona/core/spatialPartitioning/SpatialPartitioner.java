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
package org.apache.sedona.core.spatialPartitioning;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.monitoring.JavaMetrics;
import org.apache.sedona.core.spatialRddTool.PlaceGeometryWithMetricsIterator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

public abstract class SpatialPartitioner extends Partitioner implements Serializable {

  protected final GridType gridType;
  protected final List<Envelope> grids;

  protected SpatialPartitioner() {
    gridType = null;
    grids = null;
  }

  protected SpatialPartitioner(GridType gridType) {
    this.gridType = gridType;
    this.grids = null;
  }

  /**
   * Partition the input RDD according to this partitioner. If enableMetrics is true, metrics about
   * the partitioning process will be collected and can be retrieved through Spark UI. The user data
   * of the partitioned RDD may not be the same with the input rawSpatialRDD. The user data may be
   * wrapped to be augmented with additional information for specialized tasks. For instance, when
   * performing spatial partitioning for outer-join, the user data will be wrapped by
   * OuterJoinUserData to assign unique IDs for each record. The unique ID will be used to remove
   * redundant joined rows with null non-outer side.
   *
   * @param rawSpatialRDD the input spatial RDD to be partitioned
   * @param enableMetrics whether to enable metrics collection
   * @return a PairRDD where the key is the partition ID and the value is the geometry
   * @param <T> the geometry type of the spatial RDD
   */
  public <T extends Geometry> JavaPairRDD<Integer, T> partitionRDD(
      JavaRDD<T> rawSpatialRDD, boolean enableMetrics) {
    JavaPairRDD<Integer, T> geometryWithPartId;
    JavaRDD<T> preparedRDD = prepareRDDForPartitioning(rawSpatialRDD);
    if (enableMetrics) {
      // Update metrics when iterating over partitioned geometries
      SparkContext sc = preparedRDD.context();
      LongAccumulator accInputCount = JavaMetrics.createMetric(sc, "inputCount");
      LongAccumulator accOutputCount = JavaMetrics.createMetric(sc, "outputCount");
      LongAccumulator accMaxDuplicates = JavaMetrics.createMetric(sc, "maxDuplicates");
      geometryWithPartId =
          preparedRDD.mapPartitionsToPair(
              (iterator) ->
                  new PlaceGeometryWithMetricsIterator<>(
                      iterator, this, accInputCount, accOutputCount, accMaxDuplicates));
    } else {
      geometryWithPartId = preparedRDD.flatMapToPair(this::placeObject);
    }

    return geometryWithPartId.partitionBy(this);
  }

  /**
   * Augment user data when necessary for performing specialized spatial partitioning. For instance,
   * outer join partitioner will wrap user data with OuterJoinUserData to assign unique IDs for each
   * record. In other cases, the input RDD is returned as is.
   *
   * @param rawSpatialRDD the input spatial RDD
   * @return the spatial RDD with user data augmented when necessary.
   * @param <T> the geometry type of the spatial RDD
   */
  protected <T extends Geometry> JavaRDD<T> prepareRDDForPartitioning(JavaRDD<T> rawSpatialRDD) {
    return rawSpatialRDD;
  }

  /**
   * Given a geometry, returns a list of partitions it overlaps.
   *
   * <p>For points, returns exactly one partition as long as grid type is non-overlapping. For other
   * geometry types or for overlapping grid types, may return multiple partitions.
   */
  public abstract <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
      throws Exception;

  @Nullable
  public abstract DedupParams getDedupParams();

  public GridType getGridType() {
    return gridType;
  }

  public abstract List<Envelope> getGrids();

  @Override
  public int getPartition(Object key) {
    return (int) key;
  }

  public boolean compatibleWith(SpatialPartitioner other) {
    return this.equals(other);
  }
}
