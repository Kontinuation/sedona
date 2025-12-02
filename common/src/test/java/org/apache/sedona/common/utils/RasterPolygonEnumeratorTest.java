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
package org.apache.sedona.common.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class RasterPolygonEnumeratorTest {
  @Test
  public void doubleEqualsTest() {
    // Test NaNs
    assertFalse(RasterPolygonEnumerator.doubleEquals(Double.NaN, Double.NaN));
    assertFalse(RasterPolygonEnumerator.doubleEquals(Double.NaN, 0.0));
    assertFalse(RasterPolygonEnumerator.doubleEquals(0.0, Double.NaN));
    assertFalse(RasterPolygonEnumerator.doubleEquals(Double.NaN, 100.0));
    assertFalse(RasterPolygonEnumerator.doubleEquals(-50.0, Double.NaN));

    // Test value ranges
    assertDoubleEqualsOnUlpRanges(2.7); // positive values
    assertDoubleEqualsOnUlpRanges(-5.7); //  negative values
    assertDoubleEqualsOnUlpRanges(1.0e15); // large values
    assertDoubleEqualsOnUlpRanges(1.0e-100); // tiny values
    assertDoubleEqualsOnUlpRanges(-1.0e-100); // tiny values
    assertDoubleEqualsOnUlpRanges(0.0); // at zero
  }

  private static void assertDoubleEqualsOnUlpRanges(double value) {
    // Equality
    assertTrue(RasterPolygonEnumerator.doubleEquals(value, value));

    // positive ULPs to threshold
    assertTrue(RasterPolygonEnumerator.doubleEquals(value, adjustByUlps(value, 1)));
    assertTrue(RasterPolygonEnumerator.doubleEquals(value, adjustByUlps(value, 2)));
    assertFalse(RasterPolygonEnumerator.doubleEquals(value, adjustByUlps(value, 3)));

    // negative ULPs to threshold
    assertTrue(RasterPolygonEnumerator.doubleEquals(value, adjustByUlps(value, -1)));
    assertTrue(RasterPolygonEnumerator.doubleEquals(value, adjustByUlps(value, -2)));
    assertFalse(RasterPolygonEnumerator.doubleEquals(value, adjustByUlps(value, -3)));
  }

  private static double adjustByUlps(double value, int ulps) {
    double result = value;
    if (ulps > 0) {
      for (int i = 0; i < ulps; i++) {
        result = Math.nextAfter(result, Double.POSITIVE_INFINITY);
      }
    } else if (ulps < 0) {
      for (int i = 0; i < Math.abs(ulps); i++) {
        result = Math.nextAfter(result, Double.NEGATIVE_INFINITY);
      }
    }
    return result;
  }

  @Test
  public void testInt64NotSupported() {
    // This test ensures we do NOT support 64-bit integer (long) raster data types.
    //
    // LIMITATION: The current polygonization implementation converts all raster values to doubles
    // and uses doubleEquals() with MAX_ULPS=2 for comparisons. This works correctly for:
    // - All floating-point types (float, double)
    // - Integer types up to 32-bit (byte, short, int)
    //
    // However, 64-bit integers (longs) cannot be safely represented as doubles because:
    // - Doubles have only 53 bits of precision in the mantissa
    // - Consecutive integers beyond 2^53 are NOT exactly representable as doubles
    // - This means distinct long values could incorrectly merge into the same polygon
    //
    // TO FIX IF int64 SUPPORT IS NEEDED:
    // Split the polygonization into separate code paths:
    // 1. Use doubleEquals() for floating-point rasters (float, double)
    // 2. Use exact equality (==) for integral rasters (byte, short, int, long)
    // Then use isDataTypeIntegral to call the correct polygonization

    // This test verifies that the supported types haven't changed to include any int64 variants.
    // These are the currently supported types (verified as of implementation date)
    String[] knownSupportedTypes = {"B", "S", "US", "I", "F", "D"};
    int[] expectedDataTypeCodes = {0, 2, 1, 3, 4, 5};

    // Verify each known type still maps to the expected code
    for (int i = 0; i < knownSupportedTypes.length; i++) {
      int actualCode = RasterUtils.getDataTypeCode(knownSupportedTypes[i]);
      assertEquals(
          "Data type code for '" + knownSupportedTypes[i] + "' has changed",
          expectedDataTypeCodes[i],
          actualCode);
    }

    // Verify that isDataTypeIntegral only returns true for types up to 32-bit
    for (int i = 0; i < expectedDataTypeCodes.length; i++) {
      boolean isIntegral = RasterUtils.isDataTypeIntegral(expectedDataTypeCodes[i]);
      boolean shouldBeIntegral = (i < 4); // B, S, US, I are integral; F, D are not
      assertEquals(
          "isDataTypeIntegral for type '" + knownSupportedTypes[i] + "' is incorrect",
          shouldBeIntegral,
          isIntegral);
    }

    // If this test fails after adding new data types, you MUST:
    // 1. Review the new type - is it a 64-bit integer?
    // 2. If yes, update polygonization to use exact equality for integral types
    // 3. If no, Update this test to include the new type in the appropriate category
  }
}
