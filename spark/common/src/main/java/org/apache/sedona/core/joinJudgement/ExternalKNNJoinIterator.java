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
package org.apache.sedona.core.joinJudgement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.geometryObjects.UniqueGeometry;
import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sedona.core.index.ExternalSpatialIndex;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement;
import org.apache.spark.sedona.core.index.ExternalSpatialIndexWithRefinement.DataObjectWithId;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItem;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItemFormat;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.ItemDistance;

public class ExternalKNNJoinIterator<T extends Geometry, U extends Geometry>
    implements Iterator<Pair<T, U>>, AutoCloseable {
  private Iterator<T> querySideIterator;
  private final ExternalSpatialIndexWithRefinement<GeometryDataItem> externalSpatialIndex;

  private final int k;
  private final Double searchRadius;
  private final DistanceMetric distanceMetric;
  private final boolean includeTies;
  private final ItemDistance itemDistance;

  private final LongAccumulator streamCount;
  private final LongAccumulator resultCount;

  private final List<Pair<T, U>> currentResults = new ArrayList<>();
  private int currentResultIndex = 0;

  private final TaskContext taskContext;
  private final SparkEnv sparkEnv;

  private boolean isQuerySideSorted = false;

  public ExternalKNNJoinIterator(
      Iterator<T> querySideIterator,
      Iterator<U> buildSideIterator,
      int k,
      Double searchRadius,
      DistanceMetric distanceMetric,
      boolean includeTies,
      LongAccumulator buildCount,
      LongAccumulator streamCount,
      LongAccumulator resultCount,
      SedonaConf sedonaConf,
      SparkEnv sparkEnv,
      TaskContext taskContext) {
    this.querySideIterator = querySideIterator;

    this.k = k;
    this.searchRadius = searchRadius;
    this.distanceMetric = distanceMetric;
    this.includeTies = includeTies;
    this.itemDistance = KnnJoinIndexJudgement.getItemDistance(distanceMetric);
    this.streamCount = streamCount;
    this.resultCount = resultCount;

    // taskContext is non-null only when running unit tests, where a mock TaskContext object is
    // passed in. In production, this will always be null, and we'll always get the TaskContext
    // object from TaskContext.get().
    if (taskContext == null) {
      this.taskContext = TaskContext.get();
    } else {
      this.taskContext = taskContext;
    }

    // sparkEnv is non-null only when running unit tests, where a mock SparkEnv object is passed
    // in. In production, this will always be null, and we'll always get the SparkEnv object from
    // SparkEnv.get().
    if (sparkEnv == null) {
      this.sparkEnv = SparkEnv.get();
    } else {
      this.sparkEnv = sparkEnv;
    }

    this.externalSpatialIndex =
        buildExternalSpatialIndex(buildSideIterator, buildCount, sedonaConf);
    if (sedonaConf.forceSpillExternalSpatialIndex()) {
      try {
        this.externalSpatialIndex.spill();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private ExternalSpatialIndexWithRefinement<GeometryDataItem> buildExternalSpatialIndex(
      Iterator<U> buildIterator, LongAccumulator buildCount, SedonaConf conf) {
    BlockManager blockManager = sparkEnv.blockManager();
    TaskMemoryManager taskMemoryManager = taskContext.taskMemoryManager();
    long pageSizeBytes = taskMemoryManager.pageSizeBytes();
    ShuffleWriteMetrics spillMetrics = new ShuffleWriteMetrics();
    int leafPageCapacity = conf.getExternalSpatialIndexLeafPageCapacity();
    int internalNodeCapacity = conf.getExternalSpatialIndexInternalNodeCapacity();
    ExternalSpatialIndex inner =
        new ExternalSpatialIndex(
            taskContext,
            taskMemoryManager,
            blockManager,
            pageSizeBytes,
            leafPageCapacity,
            internalNodeCapacity,
            spillMetrics);

    ExternalSpatialIndexWithRefinement<GeometryDataItem> externalSpatialIndex =
        new ExternalSpatialIndexWithRefinement<>(
            inner, new GeometryDataItemFormat(), ExecutionMode.PREPARE_NONE, null, null);

    taskContext.addTaskCompletionListener(
        context -> {
          externalSpatialIndex.close();
        });

    int count = 0;
    for (; buildIterator.hasNext(); count++) {
      U geometry = buildIterator.next();
      GeometryDataItem dataItem = new GeometryDataItem(geometry);
      externalSpatialIndex.insert(dataItem);
    }
    try {
      externalSpatialIndex.build();
    } catch (IOException e) {
      externalSpatialIndex.close();
      throw new RuntimeException(e);
    }

    buildCount.add(count);
    return externalSpatialIndex;
  }

  @Override
  public boolean hasNext() {
    if (currentResultIndex < currentResults.size()) {
      return true;
    }

    currentResultIndex = 0;
    currentResults.clear();
    while (querySideIterator.hasNext()) {
      populateNextBatch();
      if (!currentResults.isEmpty()) {
        return true;
      }
    }

    externalSpatialIndex.close();
    return false;
  }

  @Override
  public Pair<T, U> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    return currentResults.get(currentResultIndex++);
  }

  private void populateNextBatch() {
    // If the external spatial index is spilled, we'll sort the query side to improve
    // the locality of the query windows, thus improve the I/O efficiency and cache hit rate
    // of the index.
    if (!isQuerySideSorted && externalSpatialIndex.hasSpilled()) {
      try {
        querySideIterator =
            SortedGeometryIterator.sortGeometryIterator(
                querySideIterator, sparkEnv, taskContext, externalSpatialIndex, false);
        isQuerySideSorted = true;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    T queryItem = querySideIterator.next();
    Geometry queryGeom;
    if (queryItem instanceof UniqueGeometry) {
      queryGeom = (Geometry) ((UniqueGeometry<?>) queryItem).getOriginalGeometry();
    } else {
      queryGeom = queryItem;
    }
    streamCount.add(1);

    Envelope queryEnv = queryGeom.getEnvelopeInternal();
    try {
      List<DataObjectWithId<GeometryDataItem>> nn =
          externalSpatialIndex.nearestNeighbours(queryEnv, queryGeom, itemDistance, k);
      Object[] localK = new Object[nn.size()];
      for (int i = 0; i < nn.size(); i++) {
        localK[i] = nn.get(i).dataObject.geometry;
      }

      if (includeTies) {
        localK = getUpdatedLocalKWithTies(queryGeom, localK, externalSpatialIndex);
      }
      if (searchRadius != null) {
        localK =
            KnnJoinIndexJudgement.getInSearchRadius(
                localK, queryGeom, distanceMetric, searchRadius);
      }

      for (Object obj : localK) {
        U candidate = (U) obj;
        Pair<T, U> pair = Pair.of(queryItem, candidate);
        currentResults.add(pair);
        resultCount.add(1);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Object[] getUpdatedLocalKWithTies(
      Geometry streamShape,
      Object[] localK,
      ExternalSpatialIndexWithRefinement<GeometryDataItem> externalSpatialIndex)
      throws IOException {
    Envelope searchEnvelope = streamShape.getEnvelopeInternal();
    // get the maximum distance from the k nearest neighbors
    double maxDistance = 0.0;
    LinkedHashSet<U> uniqueCandidates = new LinkedHashSet<>();
    for (Object obj : localK) {
      U candidate = (U) obj;
      uniqueCandidates.add(candidate);
      double distance = streamShape.distance(candidate);
      if (distance > maxDistance) {
        maxDistance = distance;
      }
    }
    searchEnvelope.expandBy(maxDistance);
    Iterator<DataObjectWithId<GeometryDataItem>> candidates =
        externalSpatialIndex.query(
            streamShape.getFactory().toGeometry(searchEnvelope),
            SpatialPredicateEvaluators.create(SpatialPredicate.INTERSECTS),
            null);
    List<Object> tiedResults = new ArrayList<>(localK.length);
    Collections.addAll(tiedResults, localK);
    while (candidates.hasNext()) {
      U candidate = (U) candidates.next().dataObject.geometry;
      double distance = streamShape.distance(candidate);
      if (distance == maxDistance && !uniqueCandidates.contains(candidate)) {
        tiedResults.add(candidate);
      }
    }
    return tiedResults.toArray();
  }

  @Override
  public void close() {
    externalSpatialIndex.close();
  }

  /** For testing purposes */
  public void forceSpill() throws IOException {
    externalSpatialIndex.spill();
  }
}
