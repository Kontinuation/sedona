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
package org.apache.sedona.core.spatialRddTool;

import org.apache.sedona.core.utils.GeometrySizeEstimator;
import org.apache.spark.util.random.XORShiftRandom;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Stateful object for collecting statistics about a set of geometries. It allows collecting statistics about multiple
 * subsets of the geometries and then combining the statistics, thus enable distributed parallel processing.
 */
public class AdvancedStatCollector implements Serializable {
    public static final long DEFAULT_MIN_SAMPLES = 10000;
    public static final long DEFAULT_MAX_SAMPLES = 1000000;  // roughly 50 MB
    public static final double DEFAULT_MIN_SAMPLING_RATE = 0.01;
    public static final double DEFAULT_SIZE_ESTIMATION_SAMPLE_GROWTH_RATE = 1.2;

    /**
     * Whether to collect rich statistics, such as the standard deviation of the size of geometries, which is
     * not necessary for running spatial join, but somewhat useful when analyzing performance issues.
     */
    private final boolean enableRichStatistics;

    /**
     * The minimum number of samples to collect in {@link AdvancedStatCollector#update(Geometry)}.
     */
    private final long minNumSamples;

    /**
     * The maximum number of samples to collect and combine in {@link AdvancedStatCollector#update(Geometry)} and
     * {@link AdvancedStatCollector#combineWith(AdvancedStatCollector)}.
     * Limiting the number of samples is necessary for avoiding OutOfMemoryError on driver when collecting samples
     * or combining samples from multiple executors.
     * Please note that this is a soft limit. The actual number of samples combined may be slightly larger than this.
     */
    private final long maxNumSamples;

    /**
     * The minimum sampling rate. This is only relevant when the number of records in a partition exceeds minSamples.
     * Please note that this is a soft limit. The actual sampling rate may be smaller than this when the number of
     * records in a partition exceeds {@code maxNumSamples / minSamplingRate}.
     */
    private final double minSamplingRate;

    /**
     * Controls the base of the exponential which governs the rate of sampling. E.g., a value of 2 would mean we
     * sample at 1, 2, 4, 8, ... elements. This is used for avoiding estimating the size of the whole dataset, which
     * may be very expensive.
     */
    private final double sizeEstimationSampleGrowthRate;

    private final long reservoirSamplingMaxCount;

    /**
     * A deterministic random number generator for sampling geometries for collecting samples and size estimation.
     * This should generate the same sequence of numbers after seeding the same seed.
     */
    private final Random random;

    /**
     * The boundary of the geometries.
     */
    private final Envelope boundary = new Envelope();

    /**
     * The number of non-null geometries.
     */
    private long count = 0;

    /**
     * The number of geometries of each type.
     */
    private long puntalCount = 0;  // number of point and multipoint
    private long linealCount = 0;  // number linestring and multi linestring
    private long polygonalCount = 0;  // number of polygon and multi polygon
    private long geometryCollectionCount = 0;  // number of geometry collection
    private long multiPointCount = 0; // number of multipoint. We count it specially for subdividing.

    /**
     * The total number of points in sampled geometries.
     */
    private long totalNumPoints = 0;

    /**
     * The summed width, height and area of the envelopes.
     */
    private double totalEnvelopeWidth = 0;
    private double totalEnvelopeHeight = 0;
    private double totalEnvelopeArea = 0;

    /**
     * The total size (including user data) of sampled geometries in bytes. The number of sampled geometries is
     * numEstimatedGeometries
     */
    private long totalEstimatedSizeInBytes = 0;

    /**
     * The total size of user data in sampled geometries in bytes. The number of sampled geometries is
     * numEstimatedGeometries
     */
    private long totalEstimatedUserDataSizeInBytes = 0;

    /**
     * The number of geometries sampled for estimating size. Please note that this is not the same as the number of
     * items in samples, which is samples for building the partitioning grid.
     */
    private long numEstimatedGeometries = 0;

    /**
     * The index of the next geometry to be sampled for estimating size. It grows exponentially by a factor of
     * sizeEstimationSampleGrowthRate
     */
    private long nextEstimateNum = 1;

    /**
     * The samples for building the partitioning grid.
     */
    private List<Envelope> samples = new ArrayList<>();

    public AdvancedStatCollector(long minNumSamples, long maxNumSamples, double minSamplingRate,
                                 double sizeEstimationSampleGrowthRate,
                                 boolean enableRichStatistics, long seed) {
        this.minNumSamples = minNumSamples;
        this.maxNumSamples = maxNumSamples;
        this.minSamplingRate = minSamplingRate;
        this.sizeEstimationSampleGrowthRate = sizeEstimationSampleGrowthRate;
        this.enableRichStatistics = enableRichStatistics;
        this.random = new XORShiftRandom(seed);
        this.reservoirSamplingMaxCount = (long) (minNumSamples / minSamplingRate);
    }

    public AdvancedStatCollector(long seed) {
        this(DEFAULT_MIN_SAMPLES, DEFAULT_MAX_SAMPLES, DEFAULT_MIN_SAMPLING_RATE,
                DEFAULT_SIZE_ESTIMATION_SAMPLE_GROWTH_RATE, false, seed);
    }

    /**
     * Update the statistics after observing a new geometry.
     * @param geom The new geometry to be observed.
     */
    public void update(Geometry geom) {
        if (geom == null) {
            return;
        }
        count += 1;
        Envelope envelope = geom.getEnvelopeInternal();
        boundary.expandToInclude(envelope);

        if (geom instanceof Puntal) {
            puntalCount += 1;
            if (geom instanceof MultiPoint) {
                multiPointCount += 1;
            }
        } else if (geom instanceof Lineal) {
            linealCount += 1;
        } else if (geom instanceof Polygonal) {
            polygonalCount += 1;
        } else {
            geometryCollectionCount += 1;
        }

        // Sample the envelope. Here we want to keep at least minNumSamples samples, but not more than maxNumSamples.
        // We'll keep the sampling rate above minSamplingRate before hitting the maxNumSamples limit.
        if (samples.size() < minNumSamples) {
            // Stage 1: Filling the reservoir
            samples.add(envelope);
        } else if (count < reservoirSamplingMaxCount) {
            // Stage 2: Use reservoir sampling to keep minNumSamples samples before seeing reservoirSamplingMaxCount
            // samples, since for now we're above the minimum sampling rate.
            int index = (int) (random.nextDouble() * count);
            if (index < minNumSamples) {
                samples.set(index, envelope);
            }
        } else if (samples.size() < maxNumSamples) {
            // Stage 3: The reservoir cannot guarantee the minimum sampling rate. Use Bernoulli sampling in this stage
            if (random.nextDouble() < minSamplingRate) {
                samples.add(envelope);
            }
        } else {
            // Stage 4: We've reached the maximum number of samples. Use reservoir sampling so that we'll not exceed
            // the maximum number of samples.
            int index = (int) (random.nextDouble() * count);
            if (index < maxNumSamples) {
                samples.set(index, envelope);
            }
        }

        int numPoints = geom.getNumPoints();
        totalNumPoints += numPoints;
        totalEnvelopeWidth += envelope.getWidth();
        totalEnvelopeHeight += envelope.getHeight();
        totalEnvelopeArea += envelope.getArea();

        if (count == nextEstimateNum) {
            // Estimate size in bytes
            long geomSizeWithoutUserData = GeometrySizeEstimator.estimateSizeWithoutUserData(geom, numPoints);
            long userDataSize = GeometrySizeEstimator.estimateUserDataSize(geom.getUserData());
            long geomSize = geomSizeWithoutUserData + userDataSize;
            totalEstimatedSizeInBytes += geomSize;
            totalEstimatedUserDataSizeInBytes += userDataSize;

            // Update the number of sampled geometries, and set up a marker for the next estimation
            numEstimatedGeometries += 1;
            nextEstimateNum = (long) Math.ceil(nextEstimateNum * sizeEstimationSampleGrowthRate);
        }
    }

    /**
     * Merge the statistics of another StatCalculator object into this object.
     * @param other The other StatCalculator object to be merged.
     */
    public void combineWith(AdvancedStatCollector other) {
        if (other == null || other.count == 0) {
            return;
        }

        // Merge samples.
        double thisRatio = count > 0? ((double) samples.size() / count): 1.0;
        double otherRatio = (double) other.samples.size() / other.count;
        double takeThisProbability;
        double takeOtherProbability;
        if (thisRatio < otherRatio) {
            // This stat collector is sampling a larger fraction of the geometries than the other stat collector.
            // We need to drop some samples from this.samples to make merged samples conforming to a uniform sampling
            // rate.
            takeThisProbability = 1;
            takeOtherProbability = thisRatio / otherRatio;
        } else {
            // The opposite case.
            takeThisProbability = otherRatio / thisRatio;
            takeOtherProbability = 1;
        }

        // If the total number of samples is too large, we need to drop some samples from both sides to make the total
        // number of samples conforming to combineMaxSamples.
        long expectedTotalSamples = (long) (samples.size() * takeThisProbability + other.samples.size() * takeOtherProbability);
        if (expectedTotalSamples > maxNumSamples) {
            double combineSamplingFactor = (double) maxNumSamples / expectedTotalSamples;
            takeThisProbability = combineSamplingFactor * takeThisProbability;
            takeOtherProbability = combineSamplingFactor * takeOtherProbability;
        }

        if (takeThisProbability == 1.0) {
            // Need to drop some samples from other.samples before merging.
            for (Envelope envelope : other.samples) {
                if (random.nextDouble() < takeOtherProbability) {
                    samples.add(envelope);
                }
            }
        } else {
            // Need to drop some samples from both this.samples and other.samples before merging.
            List<Envelope> newSamples = new ArrayList<>();
            for (Envelope envelope : samples) {
                if (random.nextDouble() < takeThisProbability) {
                    newSamples.add(envelope);
                }
            }
            for (Envelope envelope : other.samples) {
                if (random.nextDouble() < takeOtherProbability) {
                    newSamples.add(envelope);
                }
            }
            samples = newSamples;
        }

        // Merge other statistics.
        count += other.count;
        boundary.expandToInclude(other.boundary);
        puntalCount += other.puntalCount;
        linealCount += other.linealCount;
        polygonalCount += other.polygonalCount;
        geometryCollectionCount += other.geometryCollectionCount;
        multiPointCount += other.multiPointCount;
        totalNumPoints += other.totalNumPoints;
        totalEnvelopeWidth += other.totalEnvelopeWidth;
        totalEnvelopeHeight += other.totalEnvelopeHeight;
        totalEnvelopeArea += other.totalEnvelopeArea;
        totalEstimatedSizeInBytes += other.totalEstimatedSizeInBytes;
        totalEstimatedUserDataSizeInBytes += other.totalEstimatedUserDataSizeInBytes;
        numEstimatedGeometries += other.numEstimatedGeometries;
    }

    public static AdvancedStatCollector combine(AdvancedStatCollector stat1, AdvancedStatCollector stat2) {
        stat1.combineWith(stat2);
        return stat1;
    }

    public Envelope getBoundary() {
        return boundary;
    }

    public long getCount() {
        return count;
    }

    public long getPuntalCount() {
        return puntalCount;
    }

    public long getLinealCount() {
        return linealCount;
    }

    public long getPolygonalCount() {
        return polygonalCount;
    }

    public long getGeometryCollectionCount() {
        return geometryCollectionCount;
    }

    public long getMultiPointCount() {
        return multiPointCount;
    }

    public long getNumEstimatedGeometries() {
        return numEstimatedGeometries;
    }

    public long getEstimatedSizeInBytes() {
        if (numEstimatedGeometries == 0) {
            return 0;
        }
        return totalEstimatedSizeInBytes / numEstimatedGeometries;
    }

    public long getEstimatedUserDataSizeInBytes() {
        if (numEstimatedGeometries == 0) {
            return 0;
        }
        return totalEstimatedUserDataSizeInBytes / numEstimatedGeometries;
    }

    public double getMeanNumPoints() {
        if (count == 0) {
            return 0;
        }
        return (double) totalNumPoints / count;
    }

    public double getMeanEnvelopeWidth() {
        if (count == 0) {
            return 0;
        }
        return totalEnvelopeWidth / count;
    }

    public double getMeanEnvelopeHeight() {
        if (count == 0) {
            return 0;
        }
        return totalEnvelopeHeight / count;
    }

    public double getMeanEnvelopeArea() {
        if (count == 0) {
            return 0;
        }
        return totalEnvelopeArea / count;
    }

    public List<Envelope> getSampledEnvelopes() {
        return samples;
    }
}
