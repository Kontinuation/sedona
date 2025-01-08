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
package org.apache.spark.sedona.core.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.sedona.common.subDivide.ExtentBasedGeometrySubDivider;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.joinJudgement.SpatialJoinMetric;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators.SpatialPredicateEvaluator;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sedona.core.index.ExternalDataItemIndex.DataItemWithId;
import org.apache.spark.sedona.core.index.nearestneighbor.NearestNeighborSearch;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.strtree.ItemDistance;
import org.locationtech.jts.index.strtree.STRtree;

/** An external spatial index with candidate refinement using the original geometry. */
public class ExternalSpatialIndexWithRefinement<T> implements AutoCloseable {
  private static final PreparedGeometryFactory PREPARED_GEOMETRY_FACTORY =
      new PreparedGeometryFactory();

  private final ExternalSpatialIndex spatialIndex;
  private final DataItemFormat<T> dataItemFormat;

  private final ExecutionMode executionMode;

  private final ExtentBasedGeometrySubDivider buildSubDivider;
  private final ExtentBasedGeometrySubDivider streamSubDivider;

  private Cache<Integer, T> cachedGeometries;

  public ExternalSpatialIndexWithRefinement(
      ExternalSpatialIndex spatialIndex,
      DataItemFormat<T> dataItemFormat,
      ExecutionMode executionMode,
      SubdivideOptions subdivideBuildOptions,
      SubdivideOptions subdivideStreamOptions) {
    this.spatialIndex = spatialIndex;
    this.dataItemFormat = dataItemFormat;
    this.executionMode = executionMode;

    this.buildSubDivider =
        subdivideBuildOptions != null
            ? new ExtentBasedGeometrySubDivider(subdivideBuildOptions)
            : null;
    this.streamSubDivider =
        subdivideStreamOptions != null
            ? new ExtentBasedGeometrySubDivider(subdivideStreamOptions)
            : null;

    this.cachedGeometries = Caffeine.newBuilder().softValues().executor(Runnable::run).build();
  }

  public ExternalSpatialIndex getSpatialIndex() {
    return spatialIndex;
  }

  public DataItemFormat<T> getDataItemFormat() {
    return dataItemFormat;
  }

  @Override
  public void close() {
    spatialIndex.close();
    cachedGeometries = null;
  }

  /**
   * Number of indexed items
   *
   * @return the number of indexed items
   */
  public int count() {
    return spatialIndex.count();
  }

  /**
   * Insert a data object into the spatial index.
   *
   * @param dataObj the data object to insert
   * @return the item id of the inserted data object
   */
  public int insert(T dataObj) {
    byte[] serializedDataItem = dataItemFormat.serialize(dataObj);
    if (buildSubDivider != null) {
      Iterator<Envelope> subEnvIter =
          buildSubDivider.subdivideToEnvelopes(dataItemFormat.extractGeometry(dataObj));
      return spatialIndex.insert(subEnvIter, serializedDataItem);
    } else {
      Envelope itemEnv = dataItemFormat.extractEnvelope(dataObj);
      return spatialIndex.insert(itemEnv, serializedDataItem);
    }
  }

  public STRtree getNonLeafTree() {
    return spatialIndex.getNonLeafTree();
  }

  /** A data object with its item id. */
  public static class DataObjectWithId<T> {
    public final int itemId;
    public final T dataObject;

    public DataObjectWithId(int itemId, T dataItem) {
      this.itemId = itemId;
      this.dataObject = dataItem;
    }
  }

  /**
   * Fetch data objects with the specified item ids.
   *
   * @param itemIds the item ids
   * @return an iterator of data objects with the specified item ids
   */
  public Iterator<DataObjectWithId<T>> fetchDataObjects(IntList itemIds) {
    List<DataObjectWithId<T>> cachedResults = new ArrayList<>();
    IntList itemIdsToFetch = new IntArrayList();
    for (int itemId : itemIds) {
      T dataItem = cachedGeometries.getIfPresent(itemId);
      if (dataItem != null) {
        cachedResults.add(new DataObjectWithId<>(itemId, dataItem));
      } else {
        itemIdsToFetch.add(itemId);
      }
    }
    Iterator<DataItemWithId> fetchedItems = spatialIndex.getDataItemIndex().fetch(itemIdsToFetch);
    return new DataObjectIterator<>(fetchedItems, cachedResults.iterator(), dataItemFormat);
  }

  /**
   * An iterator that first iterates over the cached results, and then iterates over the fetched
   * items.
   */
  private static class DataObjectIterator<T> implements Iterator<DataObjectWithId<T>> {
    private final Iterator<DataItemWithId> iter;
    private final Iterator<DataObjectWithId<T>> cachedResultsIter;
    private final DataItemFormat<T> dataItemFormat;

    private DataObjectIterator(
        Iterator<DataItemWithId> iter,
        Iterator<DataObjectWithId<T>> cachedResultsIter,
        DataItemFormat<T> dataItemFormat) {
      this.iter = iter;
      this.cachedResultsIter = cachedResultsIter;
      this.dataItemFormat = dataItemFormat;
    }

    @Override
    public boolean hasNext() {
      if (cachedResultsIter.hasNext()) {
        return true;
      }
      return iter.hasNext();
    }

    @Override
    public DataObjectWithId<T> next() {
      if (cachedResultsIter.hasNext()) {
        return cachedResultsIter.next();
      }
      DataItemWithId dataItemWithId = iter.next();
      return new DataObjectWithId<>(
          dataItemWithId.itemId, dataItemFormat.deserialize(dataItemWithId.dataItem));
    }
  }

  /**
   * Query the spatial index for the data objects that match the search geometry.
   *
   * @param searchGeom the search geometry
   * @param spatialPredicateEvaluator the spatial predicate evaluator
   * @param extraFilter the extra filter
   * @return an iterator of data objects that match the search geometry
   * @throws IOException if an error occurs while querying the spatial index
   */
  public Iterator<DataObjectWithId<T>> query(
      Geometry searchGeom,
      SpatialPredicateEvaluator spatialPredicateEvaluator,
      Function2<Geometry, Geometry, Boolean> extraFilter)
      throws IOException {
    return query(searchGeom, spatialPredicateEvaluator, extraFilter, null);
  }

  /**
   * Query the spatial index for the data objects that match the search geometry.
   *
   * @param searchGeom the search geometry
   * @param spatialPredicateEvaluator the spatial predicate evaluator
   * @param extraFilter the extra filter
   * @param metricCandidateCount the metric candidate count
   * @return an iterator of data objects that match the search geometry
   * @throws IOException if an error occurs while querying the spatial index
   */
  public Iterator<DataObjectWithId<T>> query(
      Geometry searchGeom,
      SpatialPredicateEvaluator spatialPredicateEvaluator,
      Function2<Geometry, Geometry, Boolean> extraFilter,
      SpatialJoinMetric metricCandidateCount)
      throws IOException {
    // Query the spatial index for the item ids
    IntList itemIds;

    // The stream side should be subdivided when stream subdividing option is configured and
    // the number of points in the stream geometry is less than the number of indexed
    // geometries.
    // If there are just a few geometries on the build side, then the cost of subdividing the
    // stream side is not worth it.
    boolean shouldSubdivide = false;
    if (streamSubDivider != null) {
      shouldSubdivide =
          !(searchGeom instanceof Polygonal) || searchGeom.getNumPoints() < spatialIndex.count();
    }
    if (shouldSubdivide) {
      Iterator<Envelope> subEnvIter = streamSubDivider.subdivideToEnvelopes(searchGeom);
      itemIds = spatialIndex.queryItemIds(subEnvIter);
    } else {
      itemIds = spatialIndex.queryItemIds(searchGeom.getEnvelopeInternal());
    }
    if (metricCandidateCount != null) {
      metricCandidateCount.add(itemIds.size());
    }

    // Refine the item ids using the original geometry
    PreparedGeometry preparedSearchGeom = null;
    if (executionMode == ExecutionMode.PREPARE_STREAM && !itemIds.isEmpty()) {
      preparedSearchGeom = PREPARED_GEOMETRY_FACTORY.create(searchGeom);
    }

    // Step 1: process data items from the cache to determine which data items to keep
    List<DataObjectWithId<T>> resultsFromCache = new ArrayList<>();
    IntList itemIdsToFetch = new IntArrayList();
    for (int itemId : itemIds) {
      T cachedDataItem = cachedGeometries.getIfPresent(itemId);
      if (cachedDataItem == null) {
        itemIdsToFetch.add(itemId);
        continue;
      }
      if (refine(
          cachedDataItem, searchGeom, preparedSearchGeom, spatialPredicateEvaluator, extraFilter)) {
        resultsFromCache.add(new DataObjectWithId<>(itemId, cachedDataItem));
      }
    }

    // Step 2: load the data items from the data item index
    Iterator<DataItemWithId> fetchedDataItems =
        spatialIndex.getDataItemIndex().fetch(itemIdsToFetch);

    // Step 3: filter the data items based on the spatial predicate. We need to add the data item to
    // the cache, and evaluate the spatial predicate and extra filter.
    Iterator<DataObjectWithId<T>> resultsFromIndex =
        new RefinedDataItemIterator<>(
            this,
            fetchedDataItems,
            searchGeom,
            preparedSearchGeom,
            spatialPredicateEvaluator,
            extraFilter);

    // Finally, merge the cached results and the uncached results
    return new IteratorChain<>(resultsFromCache.iterator(), resultsFromIndex);
  }

  /**
   * Test if a data item matches the search geometry.
   *
   * @param dataItem the data item to test
   * @param searchGeom the search geometry
   * @param preparedSearchGeom the prepared search geometry
   * @param spatialPredicateEvaluator the spatial predicate evaluator
   * @param extraFilter the extra filter
   * @return true if the data item matches the search geometry, false otherwise
   */
  private boolean refine(
      T dataItem,
      Geometry searchGeom,
      PreparedGeometry preparedSearchGeom,
      SpatialPredicateEvaluators.SpatialPredicateEvaluator spatialPredicateEvaluator,
      Function2<Geometry, Geometry, Boolean> extraFilter) {
    Geometry geom;
    boolean matchSpatialPredicate;
    if (executionMode == ExecutionMode.PREPARE_BUILD) {
      PreparedGeometry preparedGeom = dataItemFormat.extractPreparedGeometry(dataItem);
      geom = preparedGeom.getGeometry();
      matchSpatialPredicate = spatialPredicateEvaluator.eval(preparedGeom, searchGeom);
    } else {
      geom = dataItemFormat.extractGeometry(dataItem);
      if (executionMode == ExecutionMode.PREPARE_STREAM) {
        matchSpatialPredicate = spatialPredicateEvaluator.eval(geom, preparedSearchGeom);
      } else {
        matchSpatialPredicate = spatialPredicateEvaluator.eval(geom, searchGeom);
      }
    }

    if (!matchSpatialPredicate) {
      return false;
    }

    if (extraFilter == null) {
      return true;
    }

    try {
      return extraFilter.call(geom, searchGeom);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** An iterator that refines the data items fetched from the data item index. */
  private static class RefinedDataItemIterator<T> implements Iterator<DataObjectWithId<T>> {
    private final Iterator<DataItemWithId> fetchedDataItems;
    private final Geometry searchGeom;
    private final PreparedGeometry preparedSearchGeom;
    private final SpatialPredicateEvaluators.SpatialPredicateEvaluator spatialPredicateEvaluator;
    private final Function2<Geometry, Geometry, Boolean> extraFilter;
    private final ExternalSpatialIndexWithRefinement<T> parent;

    private T nextDataItem;
    private int nextId;

    public RefinedDataItemIterator(
        ExternalSpatialIndexWithRefinement<T> parent,
        Iterator<DataItemWithId> fetchedDataItems,
        Geometry searchGeom,
        PreparedGeometry preparedSearchGeom,
        SpatialPredicateEvaluators.SpatialPredicateEvaluator spatialPredicateEvaluator,
        Function2<Geometry, Geometry, Boolean> extraFilter) {
      this.parent = parent;
      this.fetchedDataItems = fetchedDataItems;
      this.searchGeom = searchGeom;
      this.preparedSearchGeom = preparedSearchGeom;
      this.spatialPredicateEvaluator = spatialPredicateEvaluator;
      this.extraFilter = extraFilter;
      this.nextDataItem = null;
      this.nextId = -1;
    }

    @Override
    public boolean hasNext() {
      while (nextDataItem == null && fetchedDataItems.hasNext()) {
        loadNextDataItem();
      }
      return nextDataItem != null;
    }

    @Override
    public DataObjectWithId<T> next() {
      while (nextDataItem == null && fetchedDataItems.hasNext()) {
        loadNextDataItem();
      }
      if (nextDataItem == null) {
        throw new NoSuchElementException("No more data items");
      }
      T result = nextDataItem;
      int itemId = nextId;
      nextDataItem = null;
      return new DataObjectWithId<>(itemId, result);
    }

    private void loadNextDataItem() {
      if (!fetchedDataItems.hasNext()) {
        nextDataItem = null;
        return;
      }

      DataItemWithId dataItemWithId = fetchedDataItems.next();
      int itemId = dataItemWithId.itemId;

      // Refill the cache
      T dataItem = parent.dataItemFormat.deserialize(dataItemWithId.dataItem);
      parent.cachedGeometries.put(itemId, dataItem);

      // Check if the data item matches the spatial predicate and extra filter
      if (parent.refine(
          dataItem, searchGeom, preparedSearchGeom, spatialPredicateEvaluator, extraFilter)) {
        nextId = itemId;
        nextDataItem = dataItem;
      } else {
        nextDataItem = null;
      }
    }
  }

  public void build() throws IOException {
    spatialIndex.build();
  }

  public void spill() throws IOException {
    spatialIndex.spill();
  }

  public boolean hasSpilled() {
    return spatialIndex.hasSpilled();
  }

  /** For testing purposes only */
  public void invalidateCache() {
    cachedGeometries.invalidateAll();
    spatialIndex.getLeafPageIndex().invalidateCache();
  }

  /**
   * Finds up to k items in this tree which are the nearest neighbors to the given item, using
   * {@code itemDist} as the distance metric. This is ported from the STRtree implementation of JTS.
   *
   * <p>If the tree size is smaller than k fewer items will be returned.
   *
   * <p>If the tree is empty an array of size 0 is returned.
   *
   * @param env the envelope of the query item
   * @param item the item to find the nearest neighbours of
   * @param itemDist a distance metric applicable to the items in this tree and the query item
   * @param k the maximum number of nearest items to search for
   * @return a list of the nearest items found (with length between 0 and K)
   */
  public List<DataObjectWithId<T>> nearestNeighbours(
      Envelope env, Object item, ItemDistance itemDist, int k) throws IOException {
    if (k <= 0) {
      throw new IllegalArgumentException("k must be greater than 0");
    }
    if (buildSubDivider != null || streamSubDivider != null) {
      throw new UnsupportedOperationException(
          "Cannot perform nearest neighbor search with subdivider enabled");
    }

    return NearestNeighborSearch.nearestNeighbours(this, env, item, itemDist, k);
  }
}
