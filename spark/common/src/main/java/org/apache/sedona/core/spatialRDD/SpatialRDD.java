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
package org.apache.sedona.core.spatialRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.monitoring.JavaMetrics;
import org.apache.sedona.core.spatialPartitioning.*;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitionerBuilder.SpatialPartitionBuildingStrategy;
import org.apache.sedona.core.spatialPartitioning.quadtree.StandardQuadTree;
import org.apache.sedona.core.spatialRddTool.AdvancedStatCollector;
import org.apache.sedona.core.spatialRddTool.IndexBuilder;
import org.apache.sedona.core.spatialRddTool.PlaceGeometryWithMetricsIterator;
import org.apache.sedona.core.spatialRddTool.StatCalculator;
import org.apache.sedona.core.utils.RDDSampleUtils;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.random.SamplingUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.wololo.geojson.Feature;
import org.wololo.jts2geojson.GeoJSONWriter;
import scala.Tuple2;

// TODO: Auto-generated Javadoc

/** The Class SpatialRDD. */
public class SpatialRDD<T extends Geometry> implements Serializable {

  /** The Constant logger. */
  static final Logger logger = Logger.getLogger(SpatialRDD.class);

  /** The total number of records. */
  public long approximateTotalCount = -1;

  /** The boundary envelope. */
  public Envelope boundaryEnvelope = null;

  /** The spatial partitioned RDD. */
  public JavaRDD<T> spatialPartitionedRDD;

  /** The indexed RDD. */
  public JavaRDD<SpatialIndex> indexedRDD;

  /** The indexed raw RDD. */
  public JavaRDD<SpatialIndex> indexedRawRDD;

  /** The raw spatial RDD. */
  public JavaRDD<T> rawSpatialRDD;

  public List<String> fieldNames;
  /** The CR stransformation. */
  protected boolean CRStransformation = false;
  /** The source epsg code. */
  protected String sourceEpsgCode = "";
  /** The target epgsg code. */
  protected String targetEpgsgCode = "";

  private SpatialPartitioner partitioner;
  /** The sample number. */
  private int sampleNumber = -1;

  /** The neighbor sample number. */
  private int neighborSampleNumber = -1;

  /**
   * Comprehensive statistics of the spatial RDD with random samples collected for creating the
   * spatial partitioning grid.
   */
  private AdvancedStatCollector stat = null;

  public int getSampleNumber() {
    return sampleNumber;
  }

  /**
   * Sets the sample number.
   *
   * @param sampleNumber the new sample number
   */
  public void setSampleNumber(int sampleNumber) {
    this.sampleNumber = sampleNumber;
  }

  /**
   * Sets the neighbor sample number.
   *
   * @param neighborSampleNumber the new neighbor sample number
   */
  public void setNeighborSampleNumber(int neighborSampleNumber) {
    this.neighborSampleNumber = neighborSampleNumber;
  }

  /**
   * CRS transform.
   *
   * @param sourceEpsgCRSCode the source epsg CRS code
   * @param targetEpsgCRSCode the target epsg CRS code
   * @param lenient consider the difference of the geodetic datum between the two coordinate
   *     systems, if {@code true}, never throw an exception "Bursa-Wolf Parameters Required", but
   *     not recommended for careful analysis work
   * @return true, if successful
   */
  public boolean CRSTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode, boolean lenient) {
    this.CRStransformation = true;
    this.sourceEpsgCode = sourceEpsgCRSCode;
    this.targetEpgsgCode = targetEpsgCRSCode;
    this.rawSpatialRDD =
        this.rawSpatialRDD.map(
            (geom) ->
                (T) FunctionsGeoTools.transform(geom, sourceEpsgCRSCode, targetEpgsgCode, lenient));
    return true;
  }

  /**
   * CRS transform.
   *
   * @param sourceEpsgCRSCode the source epsg CRS code
   * @param targetEpsgCRSCode the target epsg CRS code
   * @return true, if successful
   */
  public boolean CRSTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode) {
    return CRSTransform(sourceEpsgCRSCode, targetEpsgCRSCode, false);
  }

  public boolean spatialPartitioning(GridType gridType) throws Exception {
    int numPartitions = this.rawSpatialRDD.rdd().partitions().length;
    spatialPartitioning(gridType, numPartitions);
    return true;
  }

  /**
   * Spatial partitioning.
   *
   * @param gridType the grid type
   * @throws Exception the exception
   */
  public void calc_partitioner(GridType gridType, int numPartitions) throws Exception {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Number of partitions must be > 0");
    }

    if (this.boundaryEnvelope == null) {
      throw new Exception(
          "[AbstractSpatialRDD][spatialPartitioning] SpatialRDD boundary is null. Please call analyze() first.");
    }
    if (this.approximateTotalCount == -1) {
      throw new Exception(
          "[AbstractSpatialRDD][spatialPartitioning] SpatialRDD total count is unknown. Please call analyze() first.");
    }

    List<Envelope> samples;
    if (this.stat != null && !this.stat.getSampledEnvelopes().isEmpty()) {
      // Use the samples collected in the stat calculator.
      samples = this.stat.getSampledEnvelopes();
    } else {
      // The legacy way of collecting samples: scan the raw spatial RDD to collect samples
      samples = sampleEnvelopes(numPartitions);
    }

    // Add some padding at the top and right of the boundaryEnvelope to make
    // sure all geometries lie within the half-open rectangle.
    final Envelope paddedBoundary =
        new Envelope(
            boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
            boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

    SpatialPartitionerBuilder builder =
        new SpatialPartitionerBuilder(gridType, numPartitions, samples.size(), paddedBoundary);
    builder.addSamples(samples);
    builder.setNeighborSampleNumber(neighborSampleNumber);
    builder.setSamplingProbability((double) samples.size() / (double) this.approximateTotalCount);
    partitioner = builder.build();
  }

  /**
   * The legacy way of collecting samples: scan the raw spatial RDD to collect samples. This is
   * superseded by {@link AdvancedStatCollector}, which collects statistics and samples in one pass.
   *
   * @param numPartitions Number of partitions
   * @return List of samples
   */
  private List<Envelope> sampleEnvelopes(int numPartitions) {
    // Calculate the number of samples we need to take.
    int sampleNumberOfRecords =
        RDDSampleUtils.getSampleNumbers(
            numPartitions, this.approximateTotalCount, this.sampleNumber);
    // Take Sample
    // RDD.takeSample implementation tends to scan the data multiple times to gather the exact
    // number of samples requested. Repeated scans increase the latency of the join. This increase
    // is significant for large datasets.
    // See
    // https://github.com/apache/spark/blob/412b0e8969215411b97efd3d0984dc6cac5d31e0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L508
    // Here, we choose to get samples faster over getting exactly specified number of samples.
    final double fraction =
        SamplingUtils.computeFractionForSampleSize(
            sampleNumberOfRecords, approximateTotalCount, false);
    List<Envelope> samples =
        this.rawSpatialRDD
            .sample(false, fraction)
            .map(
                new Function<T, Envelope>() {
                  @Override
                  public Envelope call(T geometry) throws Exception {
                    return geometry.getEnvelopeInternal();
                  }
                })
            .collect();

    logger.info("Collected " + samples.size() + " samples");
    return samples;
  }

  public void spatialPartitioning(GridType gridType, int numPartitions) throws Exception {
    calc_partitioner(gridType, numPartitions);
    this.spatialPartitionedRDD = partition(partitioner);
  }

  /**
   * Partition this spatial RDD and another spatial RDD using the same spatial partitioning grid.
   * The spatial partitioning grid is built to balance both RDDs.
   *
   * @param gridType Grid type
   * @param otherRdd Another spatial RDD
   * @param <U> Geometry type of the other spatial RDD
   */
  public <U extends Geometry> void spatialPartitioning(GridType gridType, SpatialRDD<U> otherRdd) {
    spatialPartitioning(gridType, otherRdd, -1);
  }

  /**
   * Create a spatial partitioner for this spatial RDD. The spatial partitioner is built to balance
   * the RDD.
   *
   * @param gridType Grid type
   * @param otherRdd Another spatial RDD
   * @param numPartitions Number of partitions
   * @param conf Sedona configuration
   * @return Spatial partitioner and metrics
   */
  public <U extends Geometry>
      Pair<SpatialPartitioner, SpatialPartitioningMetrics> createSpatialPartitioner(
          GridType gridType, SpatialRDD<U> otherRdd, int numPartitions, SedonaConf conf) {
    if (this.stat == null) {
      throw new IllegalArgumentException(
          "[SpatialRDD][spatialPartitioning] SpatialRDD stat is null. Please call advancedAnalyze() first.");
    }
    if (otherRdd.stat == null) {
      throw new IllegalArgumentException(
          "[SpatialRDD][spatialPartitioning] otherRdd stat is null. Please call otherRdd.advancedAnalyze() first.");
    }

    Envelope thisBoundary = this.stat.getBoundary();
    Envelope otherBoundary = otherRdd.stat.getBoundary();
    Envelope boundary = thisBoundary.intersection(otherBoundary);
    if (boundary.isNull()) {
      // The two datasets do not overlap. No need to partition. Running spatial join will return
      // empty result.
      return null;
    }

    // Add some padding at the top and right of the boundaryEnvelope to make sure all geometries lie
    // within the half-open rectangle.
    double deltaX = boundary.getWidth() > 0 ? boundary.getWidth() * 0.01 : 1e-6;
    double deltaY = boundary.getHeight() > 0 ? boundary.getHeight() * 0.01 : 1e-6;
    boundary.expandBy(deltaX, deltaY);

    SampledEnvelopesInBoundary thisSamplesInBoundary =
        filterSampledEnvelopesInBoundary(this.stat, boundary);
    SampledEnvelopesInBoundary otherSamplesInBoundary =
        filterSampledEnvelopesInBoundary(otherRdd.stat, boundary);
    SpatialPartitionBuildingStrategy strategy = conf.getSpatialPartitionBuildingStrategy();
    if (numPartitions == -1) {
      // Determine the number of partitions according to the statistics of both datasets.
      int thisPartitions = determineNumPartitions(this, thisSamplesInBoundary, conf);
      int otherPartitions = determineNumPartitions(otherRdd, otherSamplesInBoundary, conf);
      numPartitions = Math.max(thisPartitions, otherPartitions);
    }
    SpatialPartitioner spatialPartitioner =
        calc_partitioner(
            gridType, strategy, numPartitions, thisSamplesInBoundary, otherSamplesInBoundary);
    SpatialPartitioningMetrics metrics =
        new SpatialPartitioningMetrics(
            (double) thisSamplesInBoundary.estimatedInBoundaryGeometries / this.stat.getCount(),
            (double) otherSamplesInBoundary.estimatedInBoundaryGeometries
                / otherRdd.stat.getCount());
    return Pair.of(spatialPartitioner, metrics);
  }

  /**
   * Partition this spatial RDD and another spatial RDD using the same spatial partitioning grid.
   * The spatial partitioning grid is built to balance both RDDs.
   *
   * @param gridType Grid type
   * @param otherRdd Another spatial RDD
   * @param numPartitions Number of partitions
   * @param <U> Geometry type of the other spatial RDD
   */
  public <U extends Geometry> void spatialPartitioning(
      GridType gridType, SpatialRDD<U> otherRdd, int numPartitions) {
    SedonaConf conf = SedonaConf.fromActiveSession();
    Pair<SpatialPartitioner, SpatialPartitioningMetrics> result =
        createSpatialPartitioner(gridType, otherRdd, numPartitions, conf);
    if (result != null) {
      partitioner = result.getLeft();
      this.spatialPartitionedRDD = partition(this.partitioner, conf);
      otherRdd.spatialPartitioning(this.partitioner, conf);
    }
  }

  /**
   * Determine the number of spatial partitions using very simple heuristic.
   *
   * @param spatialRDD Spatial RDD
   * @param sampledEnvelopesInBoundary Sampled envelopes in the join extent
   * @param conf Sedona configuration
   * @return Number of partitions
   * @param <U> Geometry type
   */
  private static <U extends Geometry> int determineNumPartitions(
      SpatialRDD<U> spatialRDD,
      SampledEnvelopesInBoundary sampledEnvelopesInBoundary,
      SedonaConf conf) {
    // Estimate the number of geometries falling into the spatial join extent
    AdvancedStatCollector stat = spatialRDD.stat;
    long inBoundsCount = sampledEnvelopesInBoundary.estimatedInBoundaryGeometries;

    // Infer the amount of available executor memory for running local spatial join.
    SparkContext context = spatialRDD.rawSpatialRDD.context();
    long executorMemory = (long) context.executorMemory() * 1024 * 1024;
    double memoryFraction =
        Double.parseDouble(context.getConf().get("spark.memory.fraction", "0.6"));
    double storageFraction =
        Double.parseDouble(context.getConf().get("spark.memory.storageFraction", "0.5"));
    int executorCores = Integer.parseInt(context.getConf().get("spark.executor.cores", "1"));
    long availableMemory =
        (long) (executorMemory * memoryFraction * (1 - storageFraction) / executorCores);

    // Determine the number of spatial partitions to make partitions fit in executor memory.
    long thisTotalSizeInBytes = stat.getEstimatedSizeInBytes() * inBoundsCount;
    int partitionsBySize = (int) Math.ceil(thisTotalSizeInBytes * 2.0 / availableMemory);

    // Determine the number of spatial partitions to ensure that each partition has a reasonable
    // amount of
    // geometries.
    long perPartitionCount = conf.getExpectedPerPartitionCount();
    int maxNumPartitions = conf.getMaxGuessedPartitionNumber();
    int partitionsByCount =
        (int) Math.min(Math.ceil((double) inBoundsCount / perPartitionCount), maxNumPartitions);

    // Take the maximum of the two. If the spatial RDD is already partitioned to a larger number of
    // partitions,
    // we keep the larger number.
    int numPartitions = Math.max(partitionsBySize, partitionsByCount);
    numPartitions = Math.max(numPartitions, spatialRDD.rawSpatialRDD.getNumPartitions());

    // If numPartitions exceeds half of the number of estimated in-bound geometries, we may need to
    // reduce the
    // number of partitions.
    if (numPartitions * 2L > inBoundsCount) {
      numPartitions = Math.max((int) Math.ceil(inBoundsCount / 2.0), 1);
    }
    return numPartitions;
  }

  /**
   * Partition this spatial RDD and another spatial RDD using the same spatial partitioning grid.
   * The spatial partitioning grid is built to balance both RDDs.
   *
   * @param gridType Grid type
   * @param otherRdd Another spatial RDD
   * @param numPartitions Number of partitions
   * @param thisSamplesInBoundary Sampled envelopes within join extent of this spatial RDD
   * @param otherSamplesInBoundary Sampled envelopes within join extent of the other spatial RDD
   * @param conf Sedona configuration
   * @param <U> Geometry type of the other spatial RDD
   */
  private <U extends Geometry> void spatialPartitioning(
      GridType gridType,
      SpatialPartitionBuildingStrategy strategy,
      SpatialRDD<U> otherRdd,
      int numPartitions,
      SampledEnvelopesInBoundary thisSamplesInBoundary,
      SampledEnvelopesInBoundary otherSamplesInBoundary,
      SedonaConf conf) {
    this.partitioner =
        calc_partitioner(
            gridType, strategy, numPartitions, thisSamplesInBoundary, otherSamplesInBoundary);
    this.spatialPartitionedRDD = partition(this.partitioner, conf);
    otherRdd.spatialPartitioning(this.partitioner, conf);
  }

  private SpatialPartitioner calc_partitioner(
      GridType gridType,
      SpatialPartitionBuildingStrategy strategy,
      int numPartitions,
      SampledEnvelopesInBoundary thisSamplesInBoundary,
      SampledEnvelopesInBoundary otherSamplesInBoundary) {
    // We only need to partition the geometries overlapping with the intersection of the boundaries
    // of the two RDDs.
    Envelope bound = thisSamplesInBoundary.boundary;

    // Build a spatial partitioner using samples from both RDDs
    List<Envelope> samples = thisSamplesInBoundary.inBoundarySamples;
    List<Envelope> otherSamples = otherSamplesInBoundary.inBoundarySamples;
    // Shuffle the samples to obtain more balanced partitioning results, and avoid badly shaped
    // partition grids
    // when the samples are ordered by spatial proximity.
    Collections.shuffle(samples);
    Collections.shuffle(otherSamples);

    // TODO: find a better way to partition the space for spatial join
    return SpatialPartitionerBuilder.buildSpatialPartitionerForSpatialJoin(
        strategy,
        gridType,
        bound,
        numPartitions,
        samples,
        thisSamplesInBoundary.estimatedInBoundaryGeometries,
        otherSamples,
        otherSamplesInBoundary.estimatedInBoundaryGeometries);
  }

  /**
   * Sample envelopes within the specified boundary from the spatial RDD. The boundary is usually
   * the join extent, which is the intersection of the boundaries of the two joined RDDs.
   */
  private static class SampledEnvelopesInBoundary {
    private final Envelope boundary;
    private final List<Envelope> inBoundarySamples;
    private final long estimatedInBoundaryGeometries;

    private SampledEnvelopesInBoundary(
        Envelope boundary, List<Envelope> inBoundarySamples, long estimatedInBoundaryGeometries) {
      this.boundary = boundary;
      this.inBoundarySamples = inBoundarySamples;
      this.estimatedInBoundaryGeometries = estimatedInBoundaryGeometries;
    }
  }

  private static SampledEnvelopesInBoundary filterSampledEnvelopesInBoundary(
      AdvancedStatCollector stat, Envelope boundary) {
    List<Envelope> sampledEnvelopes = stat.getSampledEnvelopes();
    long inBoundsCount;
    List<Envelope> samplesInBoundary;
    if (!boundary.covers(stat.getBoundary()) && !sampledEnvelopes.isEmpty()) {
      samplesInBoundary = new ArrayList<>();
      for (Envelope envelope : sampledEnvelopes) {
        if (boundary.intersects(envelope)) {
          samplesInBoundary.add(envelope);
        }
      }
      double inBoundaryRatio = ((double) samplesInBoundary.size() / sampledEnvelopes.size());
      inBoundsCount = (long) (inBoundaryRatio * stat.getCount());
    } else {
      // All the geometries are within the boundary
      samplesInBoundary = sampledEnvelopes;
      inBoundsCount = stat.getCount();
    }
    return new SampledEnvelopesInBoundary(boundary, samplesInBoundary, inBoundsCount);
  }

  public SpatialPartitioner getPartitioner() {
    return partitioner;
  }

  public void spatialPartitioning(SpatialPartitioner partitioner) {
    spatialPartitioning(partitioner, null);
  }

  public void spatialPartitioning(SpatialPartitioner partitioner, SedonaConf conf) {
    this.partitioner = partitioner;
    this.spatialPartitionedRDD = partition(partitioner, conf);
  }

  /** @deprecated Use spatialPartitioning(SpatialPartitioner partitioner) */
  public boolean spatialPartitioning(final List<Envelope> otherGrids) throws Exception {
    this.partitioner = new FlatGridPartitioner(otherGrids);
    this.spatialPartitionedRDD = partition(partitioner);
    return true;
  }

  /** @deprecated Use spatialPartitioning(SpatialPartitioner partitioner) */
  public boolean spatialPartitioning(final StandardQuadTree partitionTree) throws Exception {
    this.partitioner = new QuadTreePartitioner(partitionTree);
    this.spatialPartitionedRDD = partition(partitioner);
    return true;
  }

  private JavaRDD<T> partition(final SpatialPartitioner partitioner) {
    return partition(partitioner, null);
  }

  private JavaRDD<T> partition(final SpatialPartitioner partitioner, SedonaConf conf) {
    JavaPairRDD<Integer, T> geometryWithPartId;
    if (conf != null && conf.metricsForSpatialPartitioningEnabled()) {
      // Update metrics when iterating over partitioned geometries
      SparkContext sc = rawSpatialRDD.context();
      LongAccumulator accInputCount = JavaMetrics.createMetric(sc, "inputCount");
      LongAccumulator accOutputCount = JavaMetrics.createMetric(sc, "outputCount");
      LongAccumulator accMaxDuplicates = JavaMetrics.createMetric(sc, "maxDuplicates");
      geometryWithPartId =
          this.rawSpatialRDD.mapPartitionsToPair(
              (iterator) ->
                  new PlaceGeometryWithMetricsIterator<>(
                      iterator, partitioner, accInputCount, accOutputCount, accMaxDuplicates));
    } else {
      geometryWithPartId = this.rawSpatialRDD.flatMapToPair(partitioner::placeObject);
    }
    return geometryWithPartId
        .partitionBy(partitioner)
        .mapPartitions(
            new FlatMapFunction<Iterator<Tuple2<Integer, T>>, T>() {
              @Override
              public Iterator<T> call(final Iterator<Tuple2<Integer, T>> tuple2Iterator)
                  throws Exception {
                return new Iterator<T>() {
                  @Override
                  public boolean hasNext() {
                    return tuple2Iterator.hasNext();
                  }

                  @Override
                  public T next() {
                    return tuple2Iterator.next()._2();
                  }

                  @Override
                  public void remove() {
                    throw new UnsupportedOperationException();
                  }
                };
              }
            },
            true);
  }

  /**
   * Count without duplicates.
   *
   * @return the long
   */
  public long countWithoutDuplicates() {

    List collectedResult = this.rawSpatialRDD.collect();
    HashSet resultWithoutDuplicates = new HashSet();
    for (int i = 0; i < collectedResult.size(); i++) {
      resultWithoutDuplicates.add(collectedResult.get(i));
    }
    return resultWithoutDuplicates.size();
  }

  /**
   * Count without duplicates SPRDD.
   *
   * @return the long
   */
  public long countWithoutDuplicatesSPRDD() {
    JavaRDD cleanedRDD = this.spatialPartitionedRDD;
    List collectedResult = cleanedRDD.collect();
    HashSet resultWithoutDuplicates = new HashSet();
    for (int i = 0; i < collectedResult.size(); i++) {
      resultWithoutDuplicates.add(collectedResult.get(i));
    }
    return resultWithoutDuplicates.size();
  }

  /**
   * Builds the index.
   *
   * @param indexType the index type
   * @param buildIndexOnSpatialPartitionedRDD the build index on spatial partitioned RDD
   * @throws Exception the exception
   */
  public void buildIndex(final IndexType indexType, boolean buildIndexOnSpatialPartitionedRDD)
      throws Exception {
    if (!buildIndexOnSpatialPartitionedRDD) {
      // This index is built on top of unpartitioned SRDD
      this.indexedRawRDD = this.rawSpatialRDD.mapPartitions(new IndexBuilder(indexType));
    } else {
      if (this.spatialPartitionedRDD == null) {
        throw new Exception(
            "[AbstractSpatialRDD][buildIndex] spatialPartitionedRDD is null. Please do spatial partitioning before build index.");
      }
      this.indexedRDD = this.spatialPartitionedRDD.mapPartitions(new IndexBuilder(indexType));
    }
  }

  /**
   * Boundary.
   *
   * @return the envelope
   * @deprecated Call analyze() instead
   */
  public Envelope boundary() {
    this.analyze();
    return this.boundaryEnvelope;
  }

  /**
   * Gets the raw spatial RDD.
   *
   * @return the raw spatial RDD
   */
  public JavaRDD<T> getRawSpatialRDD() {
    return rawSpatialRDD;
  }

  /**
   * Sets the raw spatial RDD.
   *
   * @param rawSpatialRDD the new raw spatial RDD
   */
  public void setRawSpatialRDD(JavaRDD<T> rawSpatialRDD) {
    this.rawSpatialRDD = rawSpatialRDD;
  }

  /**
   * Analyze.
   *
   * @param newLevel the new level
   * @return true, if successful
   */
  public boolean analyze(StorageLevel newLevel) {
    this.rawSpatialRDD = this.rawSpatialRDD.persist(newLevel);
    this.analyze();
    return true;
  }

  /**
   * Analyze.
   *
   * @return true, if successful
   */
  public boolean analyze() {
    final Function2 combOp =
        new Function2<StatCalculator, StatCalculator, StatCalculator>() {
          @Override
          public StatCalculator call(StatCalculator agg1, StatCalculator agg2) throws Exception {
            return StatCalculator.combine(agg1, agg2);
          }
        };

    final Function2 seqOp =
        new Function2<StatCalculator, Geometry, StatCalculator>() {
          @Override
          public StatCalculator call(StatCalculator agg, Geometry object) throws Exception {
            return StatCalculator.add(agg, object);
          }
        };

    StatCalculator agg = (StatCalculator) this.rawSpatialRDD.aggregate(null, seqOp, combOp);
    if (agg != null) {
      this.boundaryEnvelope = agg.getBoundary();
      this.approximateTotalCount = agg.getCount();
    } else {
      this.boundaryEnvelope = null;
      this.approximateTotalCount = 0;
    }
    return true;
  }

  /**
   * Analyze the raw spatial RDD using advanced statistics collector. This will collect more
   * comprehensive statistics as well as sampling the raw spatial RDD in one pass.
   *
   * @return true, if successful
   */
  @SuppressWarnings("unchecked")
  public boolean advancedAnalyze() {
    // Resolve parameters for collecting the statistics of the raw spatial RDD
    int numPartitions = this.rawSpatialRDD.getNumPartitions();
    SedonaConf conf = SedonaConf.fromActiveSession();
    long minSamples =
        numPartitions > 0
            ? Math.max(conf.getMinSamplesForSpatialPartitioning() / numPartitions, 1)
            : 0;
    long maxSamples = conf.getMaxSamplesForSpatialPartitioning();
    double minSamplingRate = conf.getMinSamplingRate();
    double sizeEstimationSampleGrowthRate = conf.getSizeEstimationSampleGrowthRate();
    int topKLargest = conf.getSubdivideConsiderTopKLargestGeometries();
    long seed = System.nanoTime();

    // Collect statistics of the raw spatial RDD
    final Function2<Integer, Iterator<T>, Iterator<AdvancedStatCollector>>
        aggregatePerPartitionStats =
            (partitionId, iterator) -> {
              AdvancedStatCollector statCalculator =
                  new AdvancedStatCollector(
                      minSamples,
                      maxSamples,
                      minSamplingRate,
                      sizeEstimationSampleGrowthRate,
                      topKLargest,
                      seed + partitionId);
              while (iterator.hasNext()) {
                Geometry geom = iterator.next();
                statCalculator.update(geom);
              }
              return (Iterator<AdvancedStatCollector>) new SingletonIterator(statCalculator);
            };
    AdvancedStatCollector agg;
    if (numPartitions > 0) {
      JavaRDD<AdvancedStatCollector> perPartitionStatsRdd =
          this.rawSpatialRDD.mapPartitionsWithIndex(aggregatePerPartitionStats, true);
      agg = perPartitionStatsRdd.reduce(AdvancedStatCollector::combine);
    } else {
      agg =
          new AdvancedStatCollector(
              minSamples,
              maxSamples,
              minSamplingRate,
              sizeEstimationSampleGrowthRate,
              topKLargest,
              seed);
    }

    // Set the boundary and count
    this.stat = agg;
    this.boundaryEnvelope = agg.getBoundary();
    this.approximateTotalCount = agg.getCount();
    return true;
  }

  /**
   * Retrieve advanced statistics of the spatial RDD
   *
   * @return Advanced statistics of this spatial RDD
   */
  public AdvancedStatCollector getStatistics() {
    return this.stat;
  }

  /**
   * Set advanced statistics of the spatial RDD. This method is only for internal use, and should
   * not be called directly by users.
   *
   * @param stat Advanced statistics of this spatial RDD
   */
  public void setStatistics(AdvancedStatCollector stat) {
    this.stat = stat;
  }

  /**
   * Free up memory used by the statistics data, especially sampled envelopes on both sides. This
   * can be done after using the statistics data to build spatial partitioning.
   */
  public void forgetStatistics() {
    this.stat = null;
  }

  public boolean analyze(Envelope datasetBoundary, Integer approximateTotalCount) {
    this.boundaryEnvelope = datasetBoundary;
    this.approximateTotalCount = approximateTotalCount;
    return true;
  }

  /**
   * Save as WKB.
   *
   * @param outputLocation the output location
   */
  public void saveAsWKB(String outputLocation) {
    if (this.rawSpatialRDD == null) {
      throw new NullArgumentException("save as WKB cannot operate on null RDD");
    }
    this.rawSpatialRDD
        .mapPartitions(
            new FlatMapFunction<Iterator<T>, String>() {
              @Override
              public Iterator<String> call(Iterator<T> iterator) throws Exception {
                WKBWriter writer = new WKBWriter(3, true);
                ArrayList<String> wkbs = new ArrayList<>();

                while (iterator.hasNext()) {
                  Geometry spatialObject = iterator.next();
                  String wkb = WKBWriter.toHex(writer.write(spatialObject));

                  if (spatialObject.getUserData() != null) {
                    wkbs.add(wkb + "\t" + spatialObject.getUserData());
                  } else {
                    wkbs.add(wkb);
                  }
                }
                return wkbs.iterator();
              }
            })
        .saveAsTextFile(outputLocation);
  }

  /** Save as WKT */
  public void saveAsWKT(String outputLocation) {
    if (this.rawSpatialRDD == null) {
      throw new NullArgumentException("save as WKT cannot operate on null RDD");
    }
    this.rawSpatialRDD
        .mapPartitions(
            new FlatMapFunction<Iterator<T>, String>() {
              @Override
              public Iterator<String> call(Iterator<T> iterator) throws Exception {
                WKTWriter writer = new WKTWriter(3);
                ArrayList<String> wkts = new ArrayList<>();

                while (iterator.hasNext()) {
                  Geometry spatialObject = iterator.next();
                  String wkt = writer.write(spatialObject);

                  if (spatialObject.getUserData() != null) {
                    wkts.add(wkt + "\t" + spatialObject.getUserData());
                  } else {
                    wkts.add(wkt);
                  }
                }
                return wkts.iterator();
              }
            })
        .saveAsTextFile(outputLocation);
  }

  /**
   * Save as geo JSON.
   *
   * @param outputLocation the output location
   */
  public void saveAsGeoJSON(String outputLocation) {
    this.rawSpatialRDD
        .mapPartitions(
            (FlatMapFunction<Iterator<T>, String>)
                iterator -> {
                  ArrayList<String> result = new ArrayList();
                  GeoJSONWriter writer = new GeoJSONWriter();
                  while (iterator.hasNext()) {
                    Geometry spatialObject = iterator.next();
                    Feature jsonFeature;
                    if (spatialObject.getUserData() != null) {
                      Map<String, Object> fields = new LinkedHashMap<String, Object>();
                      String[] fieldValues = spatialObject.getUserData().toString().split("\t");
                      if (fieldNames != null && fieldValues.length == fieldNames.size()) {
                        for (int i = 0; i < fieldValues.length; i++) {
                          fields.put(fieldNames.get(i), fieldValues[i]);
                        }
                      } else {
                        for (int i = 0; i < fieldValues.length; i++) {
                          fields.put("_c" + i, fieldValues[i]);
                        }
                      }
                      jsonFeature = new Feature(writer.write(spatialObject), fields);
                    } else {
                      jsonFeature = new Feature(writer.write(spatialObject), null);
                    }
                    String jsonstring = jsonFeature.toString();
                    result.add(jsonstring);
                  }
                  return result.iterator();
                })
        .saveAsTextFile(outputLocation);
  }

  /**
   * Minimum bounding rectangle.
   *
   * @return the rectangle RDD
   */
  @Deprecated
  public RectangleRDD MinimumBoundingRectangle() {
    JavaRDD<Polygon> rectangleRDD =
        this.rawSpatialRDD.map(
            new Function<T, Polygon>() {
              public Polygon call(T spatialObject) {
                Double x1, x2, y1, y2;
                LinearRing linear;
                Coordinate[] coordinates = new Coordinate[5];
                GeometryFactory fact = new GeometryFactory();
                final Envelope envelope = spatialObject.getEnvelopeInternal();
                x1 = envelope.getMinX();
                x2 = envelope.getMaxX();
                y1 = envelope.getMinY();
                y2 = envelope.getMaxY();
                coordinates[0] = new Coordinate(x1, y1);
                coordinates[1] = new Coordinate(x1, y2);
                coordinates[2] = new Coordinate(x2, y2);
                coordinates[3] = new Coordinate(x2, y1);
                coordinates[4] = coordinates[0];
                linear = fact.createLinearRing(coordinates);
                Polygon polygonObject = new Polygon(linear, null, fact);
                return polygonObject;
              }
            });
    return new RectangleRDD(rectangleRDD);
  }

  /**
   * Gets the CR stransformation.
   *
   * @return the CR stransformation
   */
  public boolean getCRStransformation() {
    return CRStransformation;
  }

  /**
   * Gets the source epsg code.
   *
   * @return the source epsg code
   */
  public String getSourceEpsgCode() {
    return sourceEpsgCode;
  }

  /**
   * Gets the target epgsg code.
   *
   * @return the target epgsg code
   */
  public String getTargetEpgsgCode() {
    return targetEpgsgCode;
  }

  public void flipCoordinates() {
    this.rawSpatialRDD =
        this.rawSpatialRDD.map(
            f -> {
              GeomUtils.flipCoordinates(f);
              return f;
            });
  }
}
