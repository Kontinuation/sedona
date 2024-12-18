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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.subDivide.ExtentBasedGeometrySubDivider;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.HalfOpenRectangle;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.LocalJoinType;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.sedona.core.spatialPartitioning.OuterJoinSpatialPartitioner.OuterJoinUserData;
import org.apache.spark.api.java.function.Function2;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * The actual heavy lifting of the local spatial join is done by this iterator. It performs the
 * indexed spatial join in memory.
 *
 * @param <U> The type of the geometries on the build side
 * @param <T> The type of the geometries on the stream side
 */
class InMemorySpatialJoinIterator<U extends Geometry, T extends Geometry>
    implements Iterator<Pair<U, T>> {

  private static final PreparedGeometryFactory PREPARED_GEOMETRY_FACTORY =
      new PreparedGeometryFactory();

  private final LocalJoinType localJoinType;
  private final SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator;
  private final Function2<Geometry, Geometry, Boolean> extraFilter;
  private final ExecutionMode executionMode;
  private final HalfOpenRectangle extent;
  private final SubdivideOptions subdivideBuildOptions;
  private final SubdivideOptions subdivideStreamOptions;
  private final SpatialIndex spatialIndex;
  private final ArrayList<Object> indexedGeometries;
  private boolean[] indexedGeometryHasJoinResults = null;
  private final Iterator<T> streamIterator;

  // Iterator state
  private final List<Pair<U, T>> batch = new ArrayList<>();
  private boolean populatedIndexOuterBatch = false;
  private int batchIndex;

  // metrics
  private final SpatialJoinMetric metricStreamCount;
  private final SpatialJoinMetric metricResultCount;
  private final SpatialJoinMetric metricCandidateCount;

  InMemorySpatialJoinIterator(
      Iterator<U> buildIterator,
      Iterator<T> streamIterator,
      LocalJoinType localJoinType,
      IndexType indexType,
      SpatialPredicate predicate,
      Function2<Geometry, Geometry, Boolean> extraFilter,
      ExecutionMode executionMode,
      HalfOpenRectangle extent,
      SubdivideOptions subdivideBuildOptions,
      SubdivideOptions subdivideStreamOptions,
      SpatialJoinMetric buildCount,
      SpatialJoinMetric streamCount,
      SpatialJoinMetric resultCount,
      SpatialJoinMetric candidateCount,
      SpatialJoinMetric buildTime) {
    this.localJoinType = localJoinType;
    this.evaluator = SpatialPredicateEvaluators.create(predicate);
    this.extraFilter = extraFilter;
    this.executionMode = executionMode;
    this.extent = extent;
    this.subdivideBuildOptions = subdivideBuildOptions;
    this.subdivideStreamOptions = subdivideStreamOptions;
    this.indexedGeometries = new ArrayList<>();
    long start = System.nanoTime();
    this.spatialIndex = buildSpatialIndex(buildIterator, indexType, buildCount);
    buildTime.add(NANOSECONDS.toMillis(System.nanoTime() - start));
    this.streamIterator = streamIterator;
    this.batchIndex = 0;
    this.metricStreamCount = streamCount;
    this.metricResultCount = resultCount;
    this.metricCandidateCount = candidateCount;
  }

  private SpatialIndex buildSpatialIndex(
      Iterator<U> buildIterator, IndexType indexType, SpatialJoinMetric buildCount) {
    SpatialIndex spatialIndex = createEmptySpatialIndex(indexType);
    ExtentBasedGeometrySubDivider subDivider =
        subdivideBuildOptions != null
            ? new ExtentBasedGeometrySubDivider(subdivideBuildOptions)
            : null;
    int count = 0;
    for (; buildIterator.hasNext(); count++) {
      U geometry = buildIterator.next();
      if (executionMode == ExecutionMode.PREPARE_BUILD) {
        PreparedGeometry preparedGeometry = PREPARED_GEOMETRY_FACTORY.create(geometry);
        indexedGeometries.add(preparedGeometry);
      } else {
        indexedGeometries.add(geometry);
      }
      if (subDivider == null) {
        // No subdivision, put the index of the original geometry or prepared geometry in
        // the array into the spatial index
        Envelope envelope = geometry.getEnvelopeInternal();
        spatialIndex.insert(envelope, count);
      } else {
        // With subdivision, put subdivided parts into the spatial index, with an index into the
        // array containing the original geometries
        Iterator<Geometry> subGeomIter = subDivider.subdivide(geometry);
        while (subGeomIter.hasNext()) {
          Geometry geom = subGeomIter.next();
          Envelope envelope = geom.getEnvelopeInternal();
          spatialIndex.insert(envelope, count);
        }
      }
    }
    if (indexType == IndexType.RTREE) {
      ((STRtree) spatialIndex).build();
    }
    indexedGeometryHasJoinResults = new boolean[count];
    Arrays.fill(indexedGeometryHasJoinResults, false);
    buildCount.add(count);
    return spatialIndex;
  }

  private static SpatialIndex createEmptySpatialIndex(IndexType indexType) {
    switch (indexType) {
      case RTREE:
        return new STRtree();
      case QUADTREE:
        return new Quadtree();
      default:
        throw new IllegalArgumentException("Unsupported index type: " + indexType);
    }
  }

  @Override
  public boolean hasNext() {
    if (batch.isEmpty() || batchIndex >= batch.size()) {
      populateNextBatch();
    }

    return !batch.isEmpty();
  }

  @Override
  public Pair<U, T> next() {
    if (batch.isEmpty() || batchIndex >= batch.size()) {
      populateNextBatch();
    }

    if (batch.isEmpty()) {
      throw new NoSuchElementException();
    }

    Pair<U, T> pair = batch.get(batchIndex);
    batchIndex++;
    return pair;
  }

  private void populateNextBatch() {
    batch.clear();
    batchIndex = 0;
    if (subdivideBuildOptions != null || subdivideStreamOptions != null) {
      populateNextBatchWithSubdivision();
    } else {
      switch (executionMode) {
        case PREPARE_BUILD:
          populateNextBatchPrepareBuild();
          break;

        case PREPARE_STREAM:
          populateNextBatchPrepareStream();
          break;

        case PREPARE_NONE:
        default:
          populateNextBatchWithoutPreparedGeometry();
          break;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void populateNextBatchPrepareBuild() {
    long streamCount = 0;
    long candidateCount = 0;
    long resultCount = 0;

    while (streamIterator.hasNext()) {
      streamCount++;
      T geometry = streamIterator.next();
      Envelope envelope = geometry.getEnvelopeInternal();
      List<Integer> candidateIndexes = (List<Integer>) spatialIndex.query(envelope);
      candidateCount += candidateIndexes.size();
      for (int candidateIndex : candidateIndexes) {
        PreparedGeometry candidate = (PreparedGeometry) indexedGeometries.get(candidateIndex);
        if (evaluator.eval(candidate, geometry)) {
          if (extent == null || !GeomUtils.isDuplicate(candidate.getGeometry(), geometry, extent)) {
            try {
              if (extraFilter == null || extraFilter.call(candidate.getGeometry(), geometry)) {
                indexedGeometryHasJoinResults[candidateIndex] = true;
                batch.add(Pair.of((U) candidate.getGeometry(), geometry));
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          resultCount++;
        }
      }
      if (!batch.isEmpty()) {
        break;
      } else {
        if (tryPopulateStreamOuterBatch(geometry)) {
          break;
        }
      }
    }

    // When we get here and the batch is still empty, we must have consumed all geometries from
    // the stream side. We may need to populate elements for index outer join.
    if (batch.isEmpty()) {
      populateIndexOuterBatch();
    }

    // Update statistics
    metricStreamCount.add(streamCount);
    metricCandidateCount.add(candidateCount);
    metricResultCount.add(resultCount);
  }

  @SuppressWarnings("unchecked")
  private void populateNextBatchPrepareStream() {
    long streamCount = 0;
    long candidateCount = 0;
    long resultCount = 0;

    while (streamIterator.hasNext()) {
      streamCount++;
      T geometry = streamIterator.next();
      Envelope envelope = geometry.getEnvelopeInternal();
      List<Integer> candidateIndexes = (List<Integer>) spatialIndex.query(envelope);
      if (!candidateIndexes.isEmpty()) {
        candidateCount += candidateIndexes.size();
        PreparedGeometry preparedGeometry = PREPARED_GEOMETRY_FACTORY.create(geometry);
        for (int candidateIndex : candidateIndexes) {
          U candidate = (U) indexedGeometries.get(candidateIndex);
          if (evaluator.eval(candidate, preparedGeometry)) {
            if (extent == null || !GeomUtils.isDuplicate(candidate, geometry, extent)) {
              try {
                if (extraFilter == null || extraFilter.call(candidate, geometry)) {
                  indexedGeometryHasJoinResults[candidateIndex] = true;
                  batch.add(Pair.of(candidate, geometry));
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
            resultCount++;
          }
        }
      }
      if (!batch.isEmpty()) {
        break;
      } else {
        if (tryPopulateStreamOuterBatch(geometry)) {
          break;
        }
      }
    }

    // When we get here and the batch is still empty, we must have consumed all geometries from
    // the stream side. We may need to populate elements for index outer join.
    if (batch.isEmpty()) {
      populateIndexOuterBatch();
    }

    // Update statistics
    metricStreamCount.add(streamCount);
    metricCandidateCount.add(candidateCount);
    metricResultCount.add(resultCount);
  }

  @SuppressWarnings("unchecked")
  private void populateNextBatchWithoutPreparedGeometry() {
    long streamCount = 0;
    long candidateCount = 0;
    long resultCount = 0;

    while (streamIterator.hasNext()) {
      streamCount++;
      T geometry = streamIterator.next();
      Envelope envelope = geometry.getEnvelopeInternal();
      List<Integer> candidateIndexes = (List<Integer>) spatialIndex.query(envelope);
      candidateCount += candidateIndexes.size();
      for (int candidateIndex : candidateIndexes) {
        U candidate = (U) indexedGeometries.get(candidateIndex);
        if (evaluator.eval(candidate, geometry)) {
          if (extent == null || !GeomUtils.isDuplicate(candidate, geometry, extent)) {
            try {
              if (extraFilter == null || extraFilter.call(candidate, geometry)) {
                indexedGeometryHasJoinResults[candidateIndex] = true;
                batch.add(Pair.of(candidate, geometry));
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          resultCount++;
        }
      }
      if (!batch.isEmpty()) {
        break;
      } else {
        if (tryPopulateStreamOuterBatch(geometry)) {
          break;
        }
      }
    }

    // When we get here and the batch is still empty, we must have consumed all geometries from
    // the stream side. We may need to populate elements for index outer join.
    if (batch.isEmpty()) {
      populateIndexOuterBatch();
    }

    // Update statistics
    metricStreamCount.add(streamCount);
    metricCandidateCount.add(candidateCount);
    metricResultCount.add(resultCount);
  }

  @SuppressWarnings("unchecked")
  private void populateNextBatchWithSubdivision() {
    long streamCount = 0;
    long candidateCount = 0;
    long resultCount = 0;

    ExtentBasedGeometrySubDivider streamSubDivider =
        subdivideStreamOptions != null
            ? new ExtentBasedGeometrySubDivider(subdivideStreamOptions)
            : null;
    while (streamIterator.hasNext()) {
      streamCount++;
      T geometry = streamIterator.next();
      List<Object> candidates = new ArrayList<>();

      // The stream side should be subdivided when stream subdividing option is configured and
      // the number of points in the stream geometry is less than the number of indexed
      // geometries.
      // If there are just a few geometries on the build side, then the cost of subdividing the
      // stream side is not worth it.
      boolean shouldSubdivide = false;
      if (streamSubDivider != null) {
        shouldSubdivide =
            !(geometry instanceof Polygonal) || geometry.getNumPoints() < indexedGeometries.size();
      }

      if (shouldSubdivide) {
        // If the stream side should be subdivided, we need to first subdivide the geometry and
        // then query the spatial index.
        Iterator<Geometry> subGeomIter = streamSubDivider.subdivide(geometry);
        while (subGeomIter.hasNext()) {
          Geometry subGeom = subGeomIter.next();
          Envelope envelope = subGeom.getEnvelopeInternal();
          List<Object> results = spatialIndex.query(envelope);
          candidates.addAll(results);
        }
      } else {
        Envelope envelope = geometry.getEnvelopeInternal();
        candidates.addAll(spatialIndex.query(envelope));
      }

      if (!candidates.isEmpty()) {
        // Now there are 3 cases:
        // 1. The retrieved candidates are prepared geometries
        // 2. The retrieved candidates are ordinary geometries
        // 3. The retrieved candidates are subdivided parts of the original indexed geometries
        // In all cases, we need to deduplicate the candidates before evaluating the spatial
        // predicate.
        HashSet<Integer> candidateIndexes = new HashSet<>();
        for (Object candidate : candidates) {
          candidateIndexes.add((Integer) candidate);
        }
        candidateCount += candidateIndexes.size();

        // Evaluate spatial predicate on candidates
        PreparedGeometry preparedGeometry =
            (executionMode == ExecutionMode.PREPARE_STREAM)
                ? PREPARED_GEOMETRY_FACTORY.create(geometry)
                : null;
        for (int candidateIndex : candidateIndexes) {
          Object candidateObj = indexedGeometries.get(candidateIndex);
          Geometry candidateGeom;
          boolean evalResult;
          if (executionMode == ExecutionMode.PREPARE_BUILD) {
            PreparedGeometry candidate = (PreparedGeometry) candidateObj;
            candidateGeom = candidate.getGeometry();
            evalResult = evaluator.eval(candidate, geometry);
          } else {
            Geometry candidate = (Geometry) candidateObj;
            candidateGeom = candidate;
            evalResult =
                preparedGeometry != null
                    ? evaluator.eval(candidate, preparedGeometry)
                    : evaluator.eval(candidate, geometry);
          }
          if (evalResult) {
            if (extent == null || !GeomUtils.isDuplicate(candidateGeom, geometry, extent)) {
              try {
                if (extraFilter == null || extraFilter.call(candidateGeom, geometry)) {
                  indexedGeometryHasJoinResults[candidateIndex] = true;
                  batch.add(Pair.of((U) candidateGeom, geometry));
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
            resultCount++;
          }
        }
      }

      if (!batch.isEmpty()) {
        break;
      } else {
        if (tryPopulateStreamOuterBatch(geometry)) {
          break;
        }
      }
    }

    // When we get here and the batch is still empty, we must have consumed all geometries from
    // the stream side. We may need to populate elements for index outer join.
    if (batch.isEmpty()) {
      populateIndexOuterBatch();
    }

    // Update statistics
    metricStreamCount.add(streamCount);
    metricCandidateCount.add(candidateCount);
    metricResultCount.add(resultCount);
  }

  @SuppressWarnings("unchecked")
  private void populateIndexOuterBatch() {
    if (localJoinType == LocalJoinType.INDEX_OUTER && !populatedIndexOuterBatch) {
      populatedIndexOuterBatch = true;
      // Walk through all primary geometries in the index side that has no emitted join results,
      // and emit a record with stream side = null for them.
      for (int k = 0; k < indexedGeometryHasJoinResults.length; k++) {
        if (!indexedGeometryHasJoinResults[k]) {
          Object indexedObj = indexedGeometries.get(k);
          U geometry;
          if (executionMode == ExecutionMode.PREPARE_BUILD) {
            geometry = (U) ((PreparedGeometry) indexedObj).getGeometry();
          } else {
            geometry = (U) indexedObj;
          }
          OuterJoinUserData userData = (OuterJoinUserData) geometry.getUserData();
          if (userData.isPrimary) {
            batch.add(Pair.of(geometry, null));
          }
        }
      }
    }
  }

  private boolean tryPopulateStreamOuterBatch(T geometry) {
    // Check if the stream side is the primary geometry. if it is, we should emit a
    // record with index side = null
    if (localJoinType != LocalJoinType.STREAM_OUTER) {
      return false;
    }
    OuterJoinUserData userData = (OuterJoinUserData) geometry.getUserData();
    if (!userData.isPrimary) {
      return false;
    }
    batch.add(Pair.of(null, geometry));
    return true;
  }
}
