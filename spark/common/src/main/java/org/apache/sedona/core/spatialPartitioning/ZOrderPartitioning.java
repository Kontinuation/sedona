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
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Range;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

public class ZOrderPartitioning implements Serializable {
  static final Logger log = Logger.getLogger(ZOrderPartitioning.class);

  // scale factor for coordinates
  private final double coordScaleFactor;
  // correction factor for Z-order range expansion due to ordering differences between Z-order and
  // Euclidean distance
  public static final int ZORDER_CORRECTION_FACTOR = 4;

  private final Envelope boundary;
  private final int numPartitions;

  List<Range<Long>> nonOverlappedRanges = new ArrayList<>();

  /**
   * Constructor to initialize ZOrderPartitioning with the given samples, boundary, and number of
   * partitions.
   *
   * @param boundary Envelope representing the overall boundary of the area to be partitioned.
   * @param numPartitions Number of partitions to create within the boundary.
   */
  public ZOrderPartitioning(Envelope boundary, int numPartitions) {
    this.boundary = boundary;
    this.numPartitions = numPartitions;
    this.coordScaleFactor = calculateScaleFactorForRange(boundary);
    log.info("ZOrderPartitioning coordScaleFactor: " + coordScaleFactor);
  }

  public List<Range<Long>> createZOrderRanges(List<Envelope> samples, int neighborSampleNumber) {
    return createZOrderRanges(samples, neighborSampleNumber, 0.01);
  }

  /**
   * Creates a list of ordered Z-order ranges based on the provided samples and boundary.
   *
   * <p>Algorithm Reference: Zhang, Chi, Feifei Li, and Jeffrey Jestes, Efficient parallel kNN joins
   * for large data in MapReduce. In Proceedings of the 15th international conference on extending
   * database technology, pp. 38-49. 2012.
   *
   * @param samples A list of sample envelopes.
   * @param neighborSampleNumber The number of nearest samples to consider for each range.
   * @param samplingProbability The probability of sampling points for each range.
   * @return A list of Z-order ranges, each covering approximately the same number of sample points.
   */
  public List<Range<Long>> createZOrderRanges(
      List<Envelope> samples, int neighborSampleNumber, double samplingProbability) {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Number of ranges must be greater than 0");
    }

    // Calculate Z-order values for the boundary corners
    long minZBoundary = calculateZOrder(new Coordinate(boundary.getMinX(), boundary.getMinY()));
    long maxZBoundary = calculateZOrder(new Coordinate(boundary.getMaxX(), boundary.getMaxY()));

    if (samples.isEmpty()) {
      // Return a single range covering the entire boundary from sample points
      return Collections.singletonList(Range.between(minZBoundary, maxZBoundary));
    }

    // Step 1: Filter points within the boundary and calculate Z-order values for each sample
    List<Long> zOrderValues =
        samples.stream()
            .map(envelope -> calculateZOrder(envelope))
            .sorted()
            .collect(Collectors.toList());

    // If there are no valid samples within the boundary
    if (zOrderValues.isEmpty()) {
      return Collections.emptyList();
    }

    // Step 2: Divide the sorted Z-order values into numPartitions ranges
    List<Range<Long>> ranges = new ArrayList<>();
    int samplesPerRange = (int) Math.ceil((double) zOrderValues.size() / numPartitions);

    long previousZOrder = minZBoundary - 1; // Start from the minimum Z-order value of the boundary
    for (int i = 0; i < numPartitions; i++) {
      int startIdx = i * samplesPerRange;
      int endIdx = Math.min((i + 1) * samplesPerRange, zOrderValues.size()) - 1;

      if (startIdx < zOrderValues.size()) {
        long startZ = previousZOrder + 1;
        long endZ = zOrderValues.get(endIdx);
        ranges.add(Range.between(startZ, endZ));
        previousZOrder = endZ;
      }
    }

    if (!ranges.isEmpty()) {
      // Adjust the minimum of the for range to the minimum Z-order value possible
      Range<Long> firstRange = ranges.get(0);
      ranges.set(0, Range.between(Long.MIN_VALUE, firstRange.getMaximum()));
      // Adjust the maximum of the last range to the maximum Z-order value possible
      Range<Long> lastRange = ranges.get(ranges.size() - 1);
      ranges.set(ranges.size() - 1, Range.between(lastRange.getMinimum(), Long.MAX_VALUE));
    }

    this.nonOverlappedRanges = ranges;
    // expand the ranges to include k nearest samples from neighboring ranges
    int scaleFactor = calculateCorrectionFactor(samplingProbability);
    int k = Math.max(neighborSampleNumber * scaleFactor, 0);
    List<Range<Long>> adjustedRanges = new ArrayList<>();

    // Adjust each range to expand to include k additional nearest samples from neighboring ranges
    for (int i = 0; i < ranges.size(); i++) {
      long newStart = ranges.get(i).getMinimum();
      long newEnd = ranges.get(i).getMaximum();

      // Expand start of current range with samples from the previous range
      if (i > 0) {
        int previousEndIdx = zOrderValues.indexOf(ranges.get(i - 1).getMaximum());
        int startExpansionIdx = Math.max(previousEndIdx - k + 1, 0);
        if (startExpansionIdx < previousEndIdx) {
          newStart = zOrderValues.get(startExpansionIdx);
        }
      }

      // Expand end of current range with samples from the next range
      if (i < ranges.size() - 1) {
        int currentEndIdx = zOrderValues.indexOf(ranges.get(i).getMaximum());
        int endExpansionIdx = Math.min(currentEndIdx + k, zOrderValues.size() - 1);
        if (endExpansionIdx > currentEndIdx) {
          newEnd = zOrderValues.get(endExpansionIdx);
        }
      }

      // Update the current range with the new expanded boundaries
      adjustedRanges.add(Range.between(newStart, newEnd));
    }

    return adjustedRanges;
  }

  public double calculateScaleFactorForRange(Envelope boundary) {
    double minX = boundary.getMinX();
    double maxX = boundary.getMaxX();
    double minY = boundary.getMinY();
    double maxY = boundary.getMaxY();

    // The scale factor should cover the largest range to ensure all coordinates fit within the
    // int32 range
    double minCoord = Math.max(Math.abs(minX), Math.abs(minY));
    double maxCoord = Math.max(Math.abs(maxX), Math.abs(maxY));
    double maxRange = Math.max(minCoord, maxCoord);

    // Calculate the initial scale factor
    double initialScaleFactor = (double) Integer.MAX_VALUE / maxRange;

    // Calculate the scale factor as a power of 10
    double scaleFactor = Math.pow(10, Math.floor(Math.log10(initialScaleFactor)) - 1);

    // Ensure the scale factor doesn't result in values exceeding the Integer range
    while ((maxRange * scaleFactor) > Integer.MAX_VALUE) {
      scaleFactor /= 10;
    }

    return scaleFactor;
  }

  // Method to calculate the correction factor for Z-order range expansion
  int calculateCorrectionFactor(double samplingProbability) {
    // larger scaling factor for higher sampling probability
    if (samplingProbability >= 0.1) return ZORDER_CORRECTION_FACTOR;
    else return ZORDER_CORRECTION_FACTOR / 2;
  }

  // Method to calculate the Z-order value for a given coordinate with default precision
  long calculateZOrder(Coordinate coordinate) {
    return calculateZOrder(coordinate, coordScaleFactor);
  }

  // Method to calculate the Z-order value for a given coordinate with default precision
  public long calculateZOrder(Envelope envelope) {
    return calculateZOrder(envelope, coordScaleFactor);
  }

  private static long calculateZOrder(Envelope envelope, double precision) {
    int x = scaleAndConvert((envelope.getMinX() + envelope.getMaxX()) / 2, precision);
    int y = scaleAndConvert((envelope.getMinY() + envelope.getMaxY()) / 2, precision);
    return interleaveBits(x, y);
  }

  // Method to calculate the Z-order value for a given coordinate with specified precision
  private long calculateZOrder(Coordinate coordinate, double precision) {
    // Scale the coordinates and convert to integers
    int x = scaleAndConvert(coordinate.x, precision);
    int y = scaleAndConvert(coordinate.y, precision);
    return interleaveBits(x, y);
  }

  // Method to scale and convert double to int
  private static int scaleAndConvert(double value, double precision) {
    return (int) (value * precision);
  }

  /**
   * The provided code interleaves the bits of two integers, x and y, to generate a Z-order value
   * (Morton code). It uses the full bit length of the integers (typically 32 bits for int in Java).
   *
   * <p>Bit Interleaving: The code interleaves the bits of x and y: For each bit position i (from 0
   * to 31, since Integer.SIZE is 32): The i-th bit of x is shifted to position 2 * i in the result
   * z. The i-th bit of y is shifted to position 2 * i + 1 in the result z.
   *
   * <p>Grid Size: The grid size that this code can handle depends on the bit length of the
   * coordinates: Since x and y are 32-bit integers, the code can handle coordinates ranging from 0
   * to 2^31 − 1 This corresponds to a grid with side length up to 2^31 -1 in each dimension.
   *
   * @param x
   * @param y
   * @return
   */
  private static long interleaveBits(int x, int y) {
    long z = 0;
    for (int i = 0; i < Integer.SIZE; i++) {
      z |= ((x >> i) & 1L) << (2 * i) | ((y >> i) & 1L) << (2 * i + 1);
    }
    return z;
  }
}
