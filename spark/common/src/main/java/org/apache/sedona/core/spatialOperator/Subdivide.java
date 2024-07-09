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
package org.apache.sedona.core.spatialOperator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.enums.GeometryType;
import org.apache.sedona.common.geometrySerde.GeometrySerializer;
import org.apache.sedona.common.subDivide.ExtentBasedGeometrySubDivider;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.core.spatialRddTool.AdvancedStatCollector;
import org.apache.sedona.core.utils.GeometrySizeEstimator;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Subdivide {

  private static final Logger LOGGER = LoggerFactory.getLogger(Subdivide.class);

  /**
   * The user data of subdivided parts. It holds information for de-duplication and recovering the
   * original geometry.
   */
  public static class SubdividedPart implements Serializable, KryoSerializable {
    /**
     * Unique ID of the original geometry. All subdivided parts of one geometry have the same ID.
     */
    public long id;

    /** User data associated with the original geometry. */
    public Object userData;

    /**
     * Original geometry without user data. This could be null if the original geometry does not
     * need to be kept.
     */
    public Geometry origGeomWithoutUserData;

    public SubdividedPart(long id, Object userData, Geometry origGeomWithoutUserData) {
      this.id = id;
      this.userData = userData;
      this.origGeomWithoutUserData = origGeomWithoutUserData;
    }

    @Override
    public void write(Kryo kryo, Output output) {
      output.writeLong(id);
      if (userData instanceof UnsafeRow) {
        // fast path for joining DataFrames
        output.writeBoolean(true);
        ((UnsafeRow) userData).write(kryo, output);
      } else {
        output.writeBoolean(false);
        kryo.writeClassAndObject(output, userData);
      }
      if (origGeomWithoutUserData != null) {
        byte[] serializedGeom = GeometrySerializer.serialize(origGeomWithoutUserData);
        output.writeInt(serializedGeom.length);
        output.writeBytes(serializedGeom);
      } else {
        output.writeInt(0);
      }
    }

    @Override
    public void read(Kryo kryo, Input input) {
      id = input.readLong();
      if (input.readBoolean()) {
        // fast path for joining DataFrames
        userData = new UnsafeRow();
        ((UnsafeRow) userData).read(kryo, input);
      } else {
        userData = kryo.readClassAndObject(input);
      }
      int length = input.readInt();
      if (length > 0) {
        byte[] serializedGeom = input.readBytes(length);
        origGeomWithoutUserData = GeometrySerializer.deserialize(serializedGeom);
      } else {
        origGeomWithoutUserData = null;
      }
    }
  }

  /** Options for subdividing a spatial RDD. */
  public static class SubdivideRDDOptions implements Serializable {
    public final SubdivideOptions options;

    // Estimated statistics for the subdivided spatial RDD. This estimation could help us avoid
    // another
    // round of statistics calculation on the subdivided spatial RDD.
    public final AdvancedStatCollector stats;

    public final boolean keepOriginalGeometry;
    public boolean keepUserData;

    public SubdivideRDDOptions(
        SubdivideOptions options,
        AdvancedStatCollector stats,
        boolean keepOriginalGeometry,
        boolean keepUserData) {
      this.options = options;
      this.stats = stats;
      this.keepOriginalGeometry = keepOriginalGeometry;
      this.keepUserData = keepUserData;
    }

    public SubdivideRDDOptions(
        SubdivideOptions options, boolean keepOriginalGeometry, boolean keepUserData) {
      this(options, null, keepOriginalGeometry, keepUserData);
    }
  }

  /**
   * Subdivide the geometry in raw spatial RDD to have smaller extent. The subdivision is based on
   * the maximum number of coordinates, width, height, and area.
   *
   * @param rawSpatialRDD Raw spatial RDD.
   * @param options Subdivision options.
   * @param keepOriginalGeometry Whether to keep the original geometry in the subdivided parts.
   * @param keepUserData Whether to keep the user data in the subdivided parts.
   * @param geomTransformer A function to transform the geometry after subdividing.
   * @return Subdivided raw spatial RDD.
   * @param <T> The type of geometry.
   */
  public static <T extends Geometry> JavaRDD<Geometry> subdivideRawSpatialRDD(
      JavaRDD<T> rawSpatialRDD,
      SubdivideOptions options,
      boolean keepOriginalGeometry,
      boolean keepUserData,
      Function2<Geometry, Object, Geometry> geomTransformer) {
    int numPartitions = rawSpatialRDD.getNumPartitions();
    return rawSpatialRDD.mapPartitionsWithIndex(
        (index, geometryIterator) -> {
          ExtentBasedGeometrySubDivider subDivider = new ExtentBasedGeometrySubDivider(options);
          return new SubdivideIterator<>(
              subDivider,
              index,
              numPartitions,
              keepOriginalGeometry,
              keepUserData,
              geometryIterator,
              geomTransformer);
        },
        false);
  }

  /**
   * Subdivide the geometry in spatial RDD to have smaller extent.
   *
   * @param spatialRDD Spatial RDD.
   * @param options Subdivision options.
   * @param keepOriginalGeometry Whether to keep the original geometry in the subdivided parts.
   * @param keepUserData Whether to keep the user data in the subdivided parts.
   * @param geomTransformer A function to transform the geometry after subdividing.
   * @return Subdivided spatial RDD.
   * @param <T> The type of geometry in the spatial RDD.
   */
  public static <T extends Geometry> SpatialRDD<Geometry> subdivideSpatialRDD(
      SpatialRDD<T> spatialRDD,
      SubdivideOptions options,
      boolean keepOriginalGeometry,
      boolean keepUserData,
      Function2<Geometry, Object, Geometry> geomTransformer) {
    JavaRDD<Geometry> subdividedRawSpatialRDD =
        subdivideRawSpatialRDD(
            spatialRDD.getRawSpatialRDD(),
            options,
            keepOriginalGeometry,
            keepUserData,
            geomTransformer);
    SpatialRDD<Geometry> subdividedSpatialRDD = new SpatialRDD<>();
    subdividedSpatialRDD.setRawSpatialRDD(subdividedRawSpatialRDD);
    return subdividedSpatialRDD;
  }

  /**
   * Subdivide the geometry in spatial RDD to have smaller extent.
   *
   * @param spatialRDD Spatial RDD.
   * @param options Subdivision options.
   * @param keepOriginalGeometry Whether to keep the original geometry in the subdivided parts.
   * @param keepUserData Whether to keep the user data in the subdivided parts.
   * @return Subdivided spatial RDD.
   * @param <T> The type of geometry in the spatial RDD.
   */
  public static <T extends Geometry> SpatialRDD<Geometry> subdivideSpatialRDD(
      SpatialRDD<T> spatialRDD,
      SubdivideOptions options,
      boolean keepOriginalGeometry,
      boolean keepUserData) {
    return subdivideSpatialRDD(spatialRDD, options, keepOriginalGeometry, keepUserData, null);
  }

  /**
   * Subdivide the geometry in spatial RDD to have smaller extent.
   *
   * @param spatialRDD Spatial RDD.
   * @param options Subdivision options.
   * @param geomTransformer A function to transform the geometry after subdividing.
   * @return Subdivided spatial RDD.
   * @param <T> The type of geometry in the spatial RDD.
   */
  public static <T extends Geometry> SpatialRDD<Geometry> subdivideSpatialRDD(
      SpatialRDD<T> spatialRDD,
      SubdivideRDDOptions options,
      Function2<Geometry, Object, Geometry> geomTransformer) {
    SpatialRDD<Geometry> subdivided =
        subdivideSpatialRDD(
            spatialRDD,
            options.options,
            options.keepOriginalGeometry,
            options.keepUserData,
            geomTransformer);
    subdivided.setStatistics(options.stats);
    return subdivided;
  }

  /**
   * Subdivide the geometry in spatial RDD to have smaller extent.
   *
   * @param spatialRDD Spatial RDD.
   * @param options Subdivision options.
   * @return Subdivided spatial RDD.
   * @param <T> The type of geometry in the spatial RDD.
   */
  public static <T extends Geometry> SpatialRDD<Geometry> subdivideSpatialRDD(
      SpatialRDD<T> spatialRDD, SubdivideRDDOptions options) {
    return subdivideSpatialRDD(spatialRDD, options, null);
  }

  /**
   * Subdivide the geometry in raw spatial RDD to have smaller extent.
   *
   * @param <T>
   */
  private static class SubdivideIterator<T extends Geometry> implements Iterator<Geometry> {
    final ExtentBasedGeometrySubDivider subDivider;
    final boolean keepOriginalGeometry;
    final boolean keepUserData;
    final Iterator<Tuple2<Long, T>> geometriesWithIds;
    final Function2<Geometry, Object, Geometry> geomTransformer;
    Iterator<Geometry> subdividedGeometries = Collections.emptyIterator();
    long currentIndex = -1;
    Geometry currentGeometry = null;
    Geometry currentGeomWithoutUserData = null;
    Object currentUserData = null;

    SubdivideIterator(
        ExtentBasedGeometrySubDivider subDivider,
        int index,
        int step,
        boolean keepOriginalGeometry,
        boolean keepUserData,
        Iterator<T> geometries,
        Function2<Geometry, Object, Geometry> geomTransformer) {
      this.subDivider = subDivider;
      this.keepOriginalGeometry = keepOriginalGeometry;
      this.keepUserData = keepUserData;
      this.geometriesWithIds = new RecordWithIdIterator<>(index, step, geometries);
      this.geomTransformer = geomTransformer;
    }

    @Override
    public boolean hasNext() {
      while (!subdividedGeometries.hasNext()) {
        if (!geometriesWithIds.hasNext()) {
          return false;
        }
        // Consume and subdivide one geometry
        Tuple2<Long, T> geometryWithId = geometriesWithIds.next();
        currentIndex = geometryWithId._1();
        currentGeometry = geometryWithId._2();
        currentUserData = currentGeometry.getUserData();
        if (keepOriginalGeometry) {
          currentGeomWithoutUserData = currentGeometry.copy();
          currentGeomWithoutUserData.setUserData(null);
        }
        subdividedGeometries = subDivider.subdivide(currentGeometry);
      }
      return true;
    }

    @Override
    public Geometry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Geometry geom = subdividedGeometries.next();
      if (geomTransformer != null) {
        try {
          geom = geomTransformer.call(geom, currentUserData);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      SubdividedPart part =
          new SubdividedPart(
              currentIndex, keepUserData ? currentUserData : null, currentGeomWithoutUserData);
      geom.setUserData(part);
      return geom;
    }
  }

  /**
   * Don't subdivide the geometry, but only attach unique IDs to each record in the RDD. This is
   * useful when we want to join a subdivided spatial RDD with a non-subdivided spatial RDD.
   *
   * @param <T> The type of geometry in the spatial RDD.
   */
  private static class NoOpSubdivideIterator<T extends Geometry> implements Iterator<Geometry> {
    final Iterator<Tuple2<Long, T>> geometriesWithIds;

    NoOpSubdivideIterator(int index, int step, Iterator<T> iterator) {
      this.geometriesWithIds = new RecordWithIdIterator<>(index, step, iterator);
    }

    @Override
    public boolean hasNext() {
      return geometriesWithIds.hasNext();
    }

    @Override
    public Geometry next() {
      Tuple2<Long, T> tuple = geometriesWithIds.next();
      long index = tuple._1();
      Geometry geom = tuple._2();
      SubdividedPart part = new SubdividedPart(index, geom.getUserData(), null);
      Geometry newGeom = geom.copy();
      newGeom.setUserData(part);
      return newGeom;
    }
  }

  /**
   * Attach unique IDs to each record in the RDD, so that we can do deduplication on subdivided
   * RDDs, as well as joining subdivided RDD with original RDD.
   *
   * @param <T> The type of geometry in the spatial RDD.
   */
  private static class RecordWithIdIterator<T> implements Iterator<Tuple2<Long, T>> {
    long index;
    final int step;
    final Iterator<T> iterator;

    RecordWithIdIterator(int index, int step, Iterator<T> iterator) {
      this.index = index;
      this.step = step;
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Tuple2<Long, T> next() {
      index += step;
      return new Tuple2<>(index, iterator.next());
    }
  }

  /**
   * Attach unique IDs to each record in the RDD, so that it could be joined back with the
   * subdivided RDD later. The algorithm for assigning IDs is identical to the one in
   * SubdivideIterator, so that the IDs attached to original records are consistent with subdivided
   * records. This function is useful for retrieving information from the original RDD from a
   * subdivided RDD that does not carry the original geometries or user data.
   *
   * @param rdd The RDD to attach IDs to.
   * @return An RDD of (ID, T) pairs.
   * @param <T> The type of the records in the RDD.
   */
  public static <T> JavaRDD<Tuple2<Long, T>> attachId(JavaRDD<T> rdd) {
    int numPartitions = rdd.getNumPartitions();
    return rdd.mapPartitionsWithIndex(
        (index, iterator) -> new RecordWithIdIterator<>(index, numPartitions, iterator), false);
  }

  /**
   * Convert the user data of geometries to SubdividedPart objects and assign unique IDs to each
   * geometry. The resulting SpatialRDD has the same type of user data as being subdivided, but
   * there's no subdivision applied.
   *
   * @param spatialRDD The spatial RDD to subdivide.
   * @return A new SpatialRDD with unique IDs attached to each geometry.
   * @param <T> The type of geometry in the spatial RDD.
   */
  public static <T extends Geometry> SpatialRDD<Geometry> noOpSubdivideSpatialRDD(
      SpatialRDD<T> spatialRDD) {
    JavaRDD<T> rawSpatialRDD = spatialRDD.getRawSpatialRDD();
    int numPartitions = rawSpatialRDD.getNumPartitions();
    JavaRDD<Geometry> newRawSpatialRDD =
        rawSpatialRDD.mapPartitionsWithIndex(
            (index, geometryIterator) ->
                new NoOpSubdivideIterator<>(index, numPartitions, geometryIterator),
            false);
    SpatialRDD<Geometry> newSpatialRDD = new SpatialRDD<>();
    newSpatialRDD.setRawSpatialRDD(newRawSpatialRDD);
    newSpatialRDD.setStatistics(spatialRDD.getStatistics());
    return newSpatialRDD;
  }

  public static class SubdivideSpatialJoinOptions {
    public SubdivideRDDOptions globalOptions;
    public SubdivideOptions localOptions;

    public SubdivideSpatialJoinOptions(
        SubdivideRDDOptions globalOptions, SubdivideOptions localOptions) {
      this.globalOptions = globalOptions;
      this.localOptions = localOptions;
    }
  }

  /**
   * Determine the subdivision options for both spatial RDDs. This is done by applying some
   * heuristics to the statistics of the geometries in the spatial RDDs.
   *
   * <p>NOTE: we implement this method for processing 2 SpatialRDDs at the same time instead of only
   * implementing it for one SpatialRDD. This is because we may want to implement some complex
   * heuristics which requires the statistics of both SpatialRDDs. The subdivide options may also
   * tangle with each other, for example, keepUserData option can only be turned on when both sides
   * are accurate. This makes it necessary to determine the options given both SpatialRDDs.
   *
   * @param leftRDD Left joined Spatial RDD. This SpatialRDD should be analyzed using the {@link
   *     SpatialRDD#advancedAnalyze()} method.
   * @param rightRDD Right joined Spatial RDD. This SpatialRDD should be analyzed using the {@link
   *     SpatialRDD#advancedAnalyze()}
   * @param spatialPartitioner Spatial partitioner.
   * @param canDiscardLeftGeometry Whether to discard the left geometry after joining.
   * @param canDiscardRightGeometry Whether to discard the right geometry after joining.
   * @param sedonaConf Sedona configuration.
   * @return Subdivision options.
   * @param <T> The type of geometry in the spatial RDD.
   */
  public static <T extends Geometry>
      Pair<SubdivideSpatialJoinOptions, SubdivideSpatialJoinOptions> determineSubdivideOptions(
          SpatialRDD<T> leftRDD,
          SpatialRDD<T> rightRDD,
          SpatialPartitioner spatialPartitioner,
          boolean canDiscardLeftGeometry,
          boolean canDiscardRightGeometry,
          SedonaConf sedonaConf)
          throws Exception {
    if (leftRDD.getStatistics() == null) {
      throw new IllegalArgumentException(
          "The left spatial RDD must be analyzed using the advancedAnalyze method");
    }
    if (rightRDD.getStatistics() == null) {
      throw new IllegalArgumentException(
          "The right spatial RDD must be analyzed using the advancedAnalyze method");
    }

    SubdivideSpatialJoinOptions leftOptions =
        determineSubdivideOptions(
            leftRDD, spatialPartitioner, canDiscardLeftGeometry, sedonaConf, "left");
    SubdivideSpatialJoinOptions rightOptions =
        determineSubdivideOptions(
            rightRDD, spatialPartitioner, canDiscardRightGeometry, sedonaConf, "right");

    boolean isLeftSideAccurate = true;
    boolean isRightSideAccurate = true;
    if (leftOptions.globalOptions != null) {
      isLeftSideAccurate = isSubdivideAccurate(leftRDD, leftOptions.globalOptions.options);
    }
    if (rightOptions.globalOptions != null) {
      isRightSideAccurate = isSubdivideAccurate(rightRDD, rightOptions.globalOptions.options);
    }
    if (!(isLeftSideAccurate && isRightSideAccurate)) {
      // We cannot keep user data if either side is inaccurate, since we need the original geometry
      // to recover the user data. canDiscardLeftGeometry and canDiscardRightGeometry are no longer
      // valid.
      if (leftOptions.globalOptions != null && leftOptions.globalOptions.keepUserData) {
        leftOptions.globalOptions.keepUserData = false;
        LOGGER.info(
            "Not both sides are accurate after subdividing, keepUserData is turned off for the left side");
      }
      if (rightOptions.globalOptions != null && rightOptions.globalOptions.keepUserData) {
        rightOptions.globalOptions.keepUserData = false;
        LOGGER.info(
            "Not both sides are accurate after subdividing, keepUserData is turned off for the right side");
      }
    }

    return Pair.of(leftOptions, rightOptions);
  }

  private static <T extends Geometry> SubdivideSpatialJoinOptions determineSubdivideOptions(
      SpatialRDD<T> spatialRDD,
      SpatialPartitioner spatialPartitioner,
      boolean canDiscardGeometry,
      SedonaConf sedonaConf,
      String rddName)
      throws Exception {
    double subdivideWidth = Double.MAX_VALUE;
    double subdivideHeight = Double.MAX_VALUE;
    boolean globalSubdivide = false;
    SubdivideRDDOptions globalOptions = null;
    SubdivideOptions localOptions = null;

    // Get the boundary of partitioned space
    Envelope boundary = new Envelope();
    int numPartitions = spatialPartitioner.numPartitions();
    if (numPartitions == 0) {
      // Empty SpatialRDD, no need to subdivide
      return new SubdivideSpatialJoinOptions(null, null);
    }
    for (Envelope grid : spatialPartitioner.getGrids()) {
      boundary.expandToInclude(grid);
    }

    // Step 1: Spatial partition the sampled envelopes
    AdvancedStatCollector stat = spatialRDD.getStatistics();
    GeometryType dominantGeometryType = stat.getDominantGeometryType();
    SpatialPartitionStats spatialPartitionQuality =
        evaluateSpatialPartitionQuality(spatialPartitioner, stat);
    if (spatialPartitionQuality == null) {
      // Don't subdivide if there's no samples collected.
      return new SubdivideSpatialJoinOptions(null, null);
    }

    // Step 2: Calculate the duplication factor of the sampled envelopes and estimate shuffle write
    // size.
    // If it is too large, we should subdivide before spatial partitioning
    double duplicationFactor = spatialPartitionQuality.duplicationFactor;
    long inBoundCount = (long) (stat.getCount() * spatialPartitionQuality.partitionedPercentage);
    long totalGeomSize =
        (long) (stat.getEstimatedSizeWithoutUserDataInBytes() * inBoundCount * duplicationFactor);
    int duplicationFactorThreshold = sedonaConf.getSubdivideDuplicationFactorThreshold();
    long duplicatedGeometrySizeThreshold = sedonaConf.getSubdivideDupGeomSizeThreshold();
    int numPointsThreshold = sedonaConf.getSubdivideNumPointsThreshold();
    if (duplicationFactor >= duplicationFactorThreshold
        && totalGeomSize >= duplicatedGeometrySizeThreshold
        && stat.getMeanNumPoints() >= numPointsThreshold) {
      // If the duplication factor is too large, we should subdivide before spatial partitioning
      globalSubdivide = true;
      subdivideWidth = stat.getMeanEnvelopeWidth() / duplicationFactor;
      subdivideHeight = stat.getMeanEnvelopeHeight() / duplicationFactor;
    }

    // Step 3: Consider the largest geometries, if the shuffle write size is too large, we should
    // subdivide
    // before spatial partitioning
    LargeGeomStats largestDupByArea =
        calculateLargeGeomStats(stat.getTopAreaInfos(), spatialPartitioner);
    LargeGeomStats largestDupByWidth =
        calculateLargeGeomStats(stat.getTopWidthInfos(), spatialPartitioner);
    LargeGeomStats largestDupByHeight =
        calculateLargeGeomStats(stat.getTopHeightInfos(), spatialPartitioner);
    LargeGeomStats largestDup = largestDupByArea;
    if (largestDupByWidth.estimatedSize > largestDup.estimatedSize) {
      largestDup = largestDupByWidth;
    }
    if (largestDupByHeight.estimatedSize > largestDup.estimatedSize) {
      largestDup = largestDupByHeight;
    }
    if (largestDup.numDuplicates >= duplicationFactorThreshold
        && largestDup.estimatedSize >= duplicatedGeometrySizeThreshold
        && largestDup.numCoordinates >= numPointsThreshold) {
      // Duplication of large geometries caused large amount of data to be shuffled. We should
      // subdivide before
      // spatial partitioning
      globalSubdivide = true;
      subdivideWidth = Math.min(subdivideWidth, boundary.getWidth() / Math.sqrt(numPartitions));
      subdivideHeight = Math.min(subdivideHeight, boundary.getHeight() / Math.sqrt(numPartitions));
    }

    // Step 4: Calculate the size of sampled envelopes, if it is large compared to the partitioning
    // grid size and
    // has a high probability to intersect with each other, we should subdivide in local join phase.
    // NOTICE: If global subdivide is used, polygons are subdivided to boxes, there's no point to
    // subdivide them again.
    double meanCollisionFactor = spatialPartitionQuality.meanCollisionFactor;
    double meanExtentSizeRatio = spatialPartitionQuality.meanExtentSizeRatio;
    double collisionFactorThreshold = sedonaConf.getSubdivideCollisionFactorThreshold();
    double nonPolygonalCollisionFactorThreshold =
        sedonaConf.getSubdivideNonPolygonalCollisionFactorThreshold();
    double extentSizeRatioThreshold = sedonaConf.getSubdivideExtentSizeRatioThreshold();
    if (dominantGeometryType == GeometryType.POINT
        || dominantGeometryType == GeometryType.LINESTRING) {
      // We apply a more aggressive subdivision strategy for multipoint and linestring geometries,
      // since they
      // occupy no space but have large envelope size, so they are more likely to produce
      // false-postive
      // join result candidates.
      collisionFactorThreshold =
          Math.min(collisionFactorThreshold, nonPolygonalCollisionFactorThreshold);
    }
    if (!globalSubdivide) {
      if (meanCollisionFactor >= collisionFactorThreshold
          && meanExtentSizeRatio >= extentSizeRatioThreshold) {
        double polygonRatio = (double) stat.getPolygonalCount() / stat.getCount();
        if (polygonRatio >= 0.1) {
          // Apply a more conservative subdivision strategy for dataset containing a large number of
          // polygons,
          // since polygons are very costy to subdivide, and they are less likely to produce
          // false-positive
          // join result candidates.
          double factor = 1.0 / meanCollisionFactor;
          double localSubdivideWidth = stat.getMeanEnvelopeWidth() * factor;
          double localSubdivideHeight = stat.getMeanEnvelopeHeight() * factor;
          // Extent of subdivided geometries should not be too small, otherwise the polygon
          // subdividing will
          // be very slow. Extent size small enough compared to the spatial partitioning grid should
          // be good.
          localSubdivideWidth =
              Math.max(localSubdivideWidth, boundary.getWidth() / Math.sqrt(numPartitions) * 0.03);
          localSubdivideHeight =
              Math.max(
                  localSubdivideHeight, boundary.getHeight() / Math.sqrt(numPartitions) * 0.03);
          localOptions = new SubdivideOptions(localSubdivideWidth, localSubdivideHeight);
        } else {
          // Apply a more aggressive subdivision strategy for non-polygonal geometries, since they
          // are
          // easier to subdivide, and they are more likely to produce false-positive join result
          // candidates.
          double factor = 0.1;
          double localSubdivideWidth = stat.getMeanEnvelopeWidth() * factor;
          double localSubdivideHeight = stat.getMeanEnvelopeHeight() * factor;
          // Extent of subdivided geometries should be small enough compared to the partitioning
          // grid size
          localSubdivideWidth =
              Math.min(localSubdivideWidth, boundary.getWidth() / Math.sqrt(numPartitions) * 0.03);
          localSubdivideHeight =
              Math.min(
                  localSubdivideHeight, boundary.getHeight() / Math.sqrt(numPartitions) * 0.03);
          localOptions = new SubdivideOptions(localSubdivideWidth, localSubdivideHeight);
        }
      }
    } else if (dominantGeometryType == GeometryType.LINESTRING) {
      // If global subdivide is used:
      // * polygons are subdivided to boxes, there's no point to subdivide them again.
      // * multipoints are subdivided to points, cannot be further subdivided
      // The only case we need to handle is linestrings
      double localSubdivideWidth = subdivideWidth * 0.03;
      double localSubdivideHeight = subdivideHeight * 0.03;
      localOptions = new SubdivideOptions(localSubdivideWidth, localSubdivideHeight);
    }

    // Step 5: Determine if it is possible to keep user data. Please note that keepUserData may be
    // finally set to
    // false even though it is set to true here, since the other side may be inaccurate after being
    // subdivided.
    boolean isAccurate = true;
    if (globalSubdivide) {
      SubdivideOptions options = new SubdivideOptions(subdivideWidth, subdivideHeight);
      isAccurate = isSubdivideAccurate(spatialRDD, options);

      boolean keepUserData = (isAccurate && canDiscardGeometry);
      // Take size of non-geometry user data into consideration. If the user data is pretty large
      // compared to the
      // size of discarded geometry, we should not keep user data to reduce the shuffle write size
      // caused by
      // duplicating user data.
      long duplicatedUserDataSize =
          (long) (stat.getEstimatedUserDataSizeInBytes() * inBoundCount * duplicationFactor);
      long totalSize = stat.getEstimatedSizeInBytes() * inBoundCount;
      if (duplicatedUserDataSize > totalSize * 2) {
        keepUserData = false;
      }

      globalOptions = new SubdivideRDDOptions(options, stat, false, keepUserData);
    }

    // Print logs when debug logging is enabled
    LOGGER.info("===============================================");
    LOGGER.info("Subdividing parameters ({}):", rddName);
    LOGGER.info("===============================================");
    LOGGER.info("globalSubdivide: {}", globalOptions != null);
    if (globalOptions != null) {
      LOGGER.info("global subdivideWidth: {}", globalOptions.options.maxWidth);
      LOGGER.info("global subdivideHeight: {}", globalOptions.options.maxHeight);
      LOGGER.info("is subdivide accurate: {}", isAccurate);
      LOGGER.info("keepUserData (not final): {}", globalOptions.keepUserData);
    }
    LOGGER.info("localSubdivide: {}", localOptions != null);
    if (localOptions != null) {
      LOGGER.info("local subdivideWidth: {}", localOptions.maxWidth);
      LOGGER.info("local subdivideHeight: {}", localOptions.maxHeight);
    }
    LOGGER.info("===============================================");
    LOGGER.info("Metrics for auto-tuning subdividing parameters:");
    LOGGER.info("===============================================");
    LOGGER.info("numPartitions: {}", numPartitions);
    LOGGER.info("duplicationFactor: {}", duplicationFactor);
    LOGGER.info("estimatedSizeAfterDuplication: {}", totalGeomSize);
    LOGGER.info("largestDup.numDuplicates: {}", largestDup.numDuplicates);
    LOGGER.info("largestDup.estimatedSize (duplicated): {}", largestDup.estimatedSize);
    LOGGER.info("largestDup.numCoordinates (duplicated): {}", largestDup.numCoordinates);
    LOGGER.info("inBoundCount: {}, totalCount: {}", inBoundCount, stat.getCount());
    LOGGER.info(
        "Estimated element size: {}, Geometry size: {}, User data size: {}",
        stat.getEstimatedSizeInBytes(),
        stat.getEstimatedSizeWithoutUserDataInBytes(),
        stat.getEstimatedUserDataSizeInBytes());
    LOGGER.info("Mean num points: {}", stat.getMeanNumPoints());
    LOGGER.info(
        "numPuntal: {}, numMultiPoint: {}, numLineal: {}, numPolygonal: {}",
        stat.getPuntalCount(),
        stat.getMultiPointCount(),
        stat.getLinealCount(),
        stat.getPolygonalCount());
    LOGGER.info("Partitioned space: {}", boundary);
    LOGGER.info(
        "Mean envelope size: width = {}, height = {}",
        stat.getMeanEnvelopeWidth(),
        stat.getMeanEnvelopeHeight());
    LOGGER.info("meanCollisionFactor: {}", spatialPartitionQuality.meanCollisionFactor);
    LOGGER.info("meanExtentSizeRatio: {}", spatialPartitionQuality.meanExtentSizeRatio);

    return new SubdivideSpatialJoinOptions(globalOptions, localOptions);
  }

  private static class EnvelopeStat {
    Envelope envelope;
    /** Maximum percentage of grid area covered by this envelope */
    double gridOccupancy;
    /** Number of grids intersecting with this envelope */
    int numDuplicates;

    EnvelopeStat(Envelope envelope, double gridOccupancy, int numDuplicates) {
      this.envelope = envelope;
      this.gridOccupancy = gridOccupancy;
      this.numDuplicates = numDuplicates;
    }
  }

  private static class SpatialPartitionStats {
    List<Envelope> grids;
    List<EnvelopeStat> envelopeStats;
    long[] perPartitionSampleCount;
    /** Percentage of samples being partitioned (within the spatial partitioned range) */
    double partitionedPercentage;
    /** Average number of duplicates per geometry */
    double duplicationFactor;
    /** Estimated number of self-intersecting envelopes */
    double meanCollisionFactor;
    /** Mean value of envelope area / grid area */
    double meanExtentSizeRatio;

    SpatialPartitionStats(
        List<Envelope> grids,
        List<EnvelopeStat> envelopeStats,
        long[] perPartitionSampleCount,
        double partitionedPercentage,
        double duplicationFactor,
        double meanCollisionFactor,
        double meanExtentSizeRatio) {
      this.grids = grids;
      this.envelopeStats = envelopeStats;
      this.perPartitionSampleCount = perPartitionSampleCount;
      this.partitionedPercentage = partitionedPercentage;
      this.duplicationFactor = duplicationFactor;
      this.meanCollisionFactor = meanCollisionFactor;
      this.meanExtentSizeRatio = meanExtentSizeRatio;
    }
  }

  private static SpatialPartitionStats evaluateSpatialPartitionQuality(
      SpatialPartitioner spatialPartitioner, AdvancedStatCollector stat) throws Exception {
    List<Envelope> samples = stat.getSampledEnvelopes();
    if (samples.isEmpty()) {
      return null;
    }

    // Place the samples into the grids
    List<Envelope> grids = spatialPartitioner.getGrids();
    List<EnvelopeStat> envelopeStats = new ArrayList<>(samples.size());
    long[] perPartitionSampleCount = new long[grids.size()];
    GeometryFactory factory = new GeometryFactory();
    int samplesPartitioned = 0;
    for (Envelope sample : samples) {
      Geometry bound = factory.toGeometry(sample);
      Iterator<Tuple2<Integer, Geometry>> placements = spatialPartitioner.placeObject(bound);
      int numDuplicates = 0;
      double maxGridOccupancy = 0;
      while (placements.hasNext()) {
        Tuple2<Integer, Geometry> placement = placements.next();
        int partitionId = placement._1();
        perPartitionSampleCount[partitionId] += 1;
        numDuplicates += 1;
        Envelope grid = grids.get(partitionId);
        double area = sample.intersection(grid).getArea();
        double gridArea = grid.getArea();
        double gridOccupancy = gridArea > 0 ? area / gridArea : 1;
        maxGridOccupancy = Math.max(maxGridOccupancy, gridOccupancy);
      }
      if (numDuplicates > 0) {
        samplesPartitioned += 1;
      }
      envelopeStats.add(new EnvelopeStat(sample, maxGridOccupancy, numDuplicates));
    }

    // Calculating stats
    int totalDuplicates = envelopeStats.stream().mapToInt(x -> x.numDuplicates).sum();
    double duplicationFactor =
        (samplesPartitioned > 0 ? (double) totalDuplicates / samplesPartitioned : 1.0);
    double factor = (samples.isEmpty() ? 0.0 : (double) stat.getCount() / samples.size());
    double meanEnvelopeArea = stat.getMeanEnvelopeArea();
    double meanCollisionFactor = 0;
    double meanExtentSizeRatio = 0;
    for (int k = 0; k < perPartitionSampleCount.length; k++) {
      long sampleCount = perPartitionSampleCount[k];
      long estimatedCount = (long) (sampleCount * factor);
      double gridArea = grids.get(k).getArea();
      if (gridArea > 0) {
        double collisionFactor = estimatedCount * meanEnvelopeArea / gridArea;
        double extentSizeRatio = stat.getMeanEnvelopeArea() / gridArea;
        meanCollisionFactor += collisionFactor;
        meanExtentSizeRatio += extentSizeRatio;
      }
    }
    meanCollisionFactor /= perPartitionSampleCount.length;
    meanExtentSizeRatio /= perPartitionSampleCount.length;
    return new SpatialPartitionStats(
        grids,
        envelopeStats,
        perPartitionSampleCount,
        samplesPartitioned / (double) samples.size(),
        duplicationFactor,
        meanCollisionFactor,
        meanExtentSizeRatio);
  }

  private static class LargeGeomStats {
    /** Number of duplicates after spatial partitioning */
    int numDuplicates;
    /** Total number of coordinates after duplication */
    long numCoordinates;
    /** Estimated size in bytes of all duplicated geometries */
    long estimatedSize;

    LargeGeomStats(int numDuplicates, long numCoordinates) {
      this.numDuplicates = numDuplicates;
      this.numCoordinates = numCoordinates;
      this.estimatedSize = numCoordinates * GeometrySizeEstimator.BYTES_PER_COORDINATE;
    }
  }

  private static LargeGeomStats calculateLargeGeomStats(
      PriorityQueue<AdvancedStatCollector.LargeGeometryInfo> largeGeometryInfos,
      SpatialPartitioner spatialPartitioner)
      throws Exception {
    if (largeGeometryInfos.isEmpty()) {
      return new LargeGeomStats(0, 0);
    }

    LargeGeomStats worst = null;
    GeometryFactory factory = new GeometryFactory();
    for (AdvancedStatCollector.LargeGeometryInfo info : largeGeometryInfos) {
      Iterator<Tuple2<Integer, Geometry>> iterator =
          spatialPartitioner.placeObject(factory.toGeometry(info.extent));
      int numDuplications = 0;
      while (iterator.hasNext()) {
        iterator.next();
        numDuplications += 1;
      }
      long totalCoordinates = (long) info.numPoints * numDuplications;
      LargeGeomStats current = new LargeGeomStats(numDuplications, totalCoordinates);
      if (worst == null || current.estimatedSize > worst.estimatedSize) {
        worst = current;
      }
    }
    return worst;
  }

  /**
   * Determine if the subdivision performed on the spatial RDD is accurate based on the statistics
   * of the geometries and the subdivision options.
   *
   * @param spatialRDD Spatial RDD. This SpatialRDD should be analyzed using the {@link
   *     SpatialRDD#advancedAnalyze()}
   * @param options Subdivision options.
   * @return True if the subdivision is accurate, false otherwise.
   * @param <T> The type of geometry in the spatial RDD.
   */
  public static <T extends Geometry> boolean isSubdivideAccurate(
      SpatialRDD<T> spatialRDD, SubdivideOptions options) {
    AdvancedStatCollector stats = spatialRDD.getStatistics();
    boolean isAccurate = true;
    if (stats.getMultiPointCount() > 0) {
      isAccurate = options.multiPointSubDivider.isAccurate;
    }
    if (stats.getLinealCount() > 0) {
      isAccurate = (isAccurate && options.lineStringSubDivider.isAccurate);
    }
    if (stats.getPolygonalCount() > 0) {
      isAccurate = (isAccurate && options.polygonSubDivider.isAccurate);
    }
    if (stats.getGeometryCollectionCount() > 0) {
      isAccurate = (isAccurate && options.multiPointSubDivider.isAccurate);
      isAccurate = (isAccurate && options.lineStringSubDivider.isAccurate);
      isAccurate = (isAccurate && options.polygonSubDivider.isAccurate);
    }
    return isAccurate;
  }
}
