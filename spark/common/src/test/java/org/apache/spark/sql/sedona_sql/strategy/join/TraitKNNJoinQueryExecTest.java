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
package org.apache.spark.sql.sedona_sql.strategy.join;

import junit.framework.TestCase;

public class TraitKNNJoinQueryExecTest extends TestCase {

  public void testKnnJoinPartitionNumOptimizer() {
    // Define a list of test cases with input parameters and expected results
    Object[][] testCases = {
      // Standard test cases
      {6_000_000_000L, 1_000L, 1_000_000_000L, 1000L, 100, 100, 524288L, 4, 1907},
      {6_000_000_000L, 1_000L, 500_000_000L, 2000L, 50, 20, 524288L, 4, 953},
      {6_000_000_000L, 1_000L, 250_000_000L, 500L, 25, 50, 524288L, 16, 476},
      {6_000_000_000L, 1_000L, 750_000_000L, 1500L, 800, 150, 524288L, 64, 1430},

      // Under-partitioned cases
      {6_000_000_000L, 1_000L, 1_000_000_000L, 1000L, 1, 1, 524288L, 4, 1907},
      {6_000_000_000L, 1_000L, 1_000_000_000L, 1000L, 1, 100, 524288L, 4, 1907},

      // Over-partitioned cases
      {6_000_000_000L, 1_000L, 1_000_000_000L, 1000L, 10_000, 1, 524288L, 4, 10_000},
      {6_000_000_000L, 1_000L, 1_000_000_000L, 1000L, 10_000, 1000, 524288L, 4, 10_000},

      // Edge cases
      {6_000_000_000L, 1_000L, 0L, 0L, 1, 1, 524288L, 1, 1}, // No objects, minimal partitions
      {6_000_000_000L, 1_000L, 1L, 1L, 1, 1, 524288L, 1, 1}, // Minimal non-zero counts

      // Cases where objectSidePartNum is greater than candidatePartitionNum
      {6_000_000_000L, 1_000L, 1_000_000_000L, 1_000_000L, 1000, 100, 524288L, 16, 1907},

      // Cases where query side dominates
      {6_000_000_000L, 1_000L, 1_000_000L, 1_000_000_000L, 100, 1000, 524288L, 8, 1907},

      // Cases with small object side and query side count
      {6_000_000_000L, 1_000L, 10_000L, 1_000L, 100, 100, 524288L, 256, 100},
      {6_000_000_000L, 1_000L, 1000L, 1000L, 10, 10, 524288L, 4, 10},

      // Cases with small numNeighbor
      {6_000_000_000L, 1_000L, 1_000_000_000L, 1_000_000L, 100, 100, 524288L, 1, 1907},

      // Large scale with different counts and neighbors
      {6_000_000_000L, 1_000L, 10_000_000_000L, 1_000_000L, 500, 100, 524288L, 16, 19073},
      {6_000_000_000L, 1_000L, 5_000_000_000L, 2_000_000L, 400, 200, 524288L, 8, 9536},
      {6_000_000_000L, 1_000L, 5_000_000_000L, 2_000_000L, 10_000, 200, 524288L, 8, 10_000},
    };

    // Loop through the test cases
    for (Object[] testCase : testCases) {
      long availableMemory = (long) testCase[0];
      long estimatedSizeInBytes = (long) testCase[1];
      long objectSideCount = (long) testCase[2];
      long querySideCount = (long) testCase[3];
      int objectSidePartNum = (int) testCase[4];
      int querySidePartNum = (int) testCase[5];
      long maxRowsPerPartition = (long) testCase[6];
      int numNeighbor = (int) testCase[7];
      int expectedValue = (int) testCase[8];

      // Run the method and get the result
      int result =
          TraitKNNJoinQueryExec.knnJoinPartitionNumOptimizer(
              availableMemory,
              estimatedSizeInBytes,
              objectSidePartNum,
              querySidePartNum,
              objectSideCount,
              querySideCount,
              maxRowsPerPartition,
              numNeighbor);

      // Assert that the result matches the expected value
      assertEquals(
          "Test failed for inputs: " + java.util.Arrays.toString(testCase), expectedValue, result);
    }
  }
}
