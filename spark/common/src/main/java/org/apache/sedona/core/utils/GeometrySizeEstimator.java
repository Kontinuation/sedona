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
package org.apache.sedona.core.utils;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.util.SizeEstimator;
import org.locationtech.jts.geom.Geometry;

/**
 * Estimate the size of a geometry object.
 */
public class GeometrySizeEstimator {
    private GeometrySizeEstimator() {}

    /**
     * Estimate the size of a geometry object. User data is also included in the estimation.
     * @param geom The geometry object.
     * @return The estimated size of the geometry object.
     */
    public static long estimateSize(Geometry geom) {
        return estimateSize(geom, geom.getNumPoints());
    }

    /**
     * Estimate the size of a geometry object. User data is also included in the estimation.
     * @param geom The geometry object.
     * @param numPoints The number of points in the geometry object.
     * @return The estimated size of the geometry object.
     */
    public static long estimateSize(Geometry geom, int numPoints) {
        long geomSize = estimateSizeWithoutUserData(geom, numPoints);
        Object userData = geom.getUserData();
        long userDataSize;
        if (userData instanceof UnsafeRow) {
            // We can use a fast estimation if the user data is an UnsafeRow, which is the case when we run spatial join
            // on DataFrame. 64 is an estimated overhead of UnsafeRow object.
            userDataSize = ((UnsafeRow) userData).getSizeInBytes() + 64;
        } else if (userData instanceof String) {
            // We can use a fast estimation if the user data is a String, which is usually the case when using the
            // RDD API. 64 is an estimated overhead of String object.
            // We multiply the length of the string by 2 for conservative estimation (assuming internal usage of
            // UTF-16 encoding)
            userDataSize = ((String) userData).length() * 2L + 64;
        } else if (userData instanceof byte[]) {
            userDataSize = ((byte[]) userData).length + 16;
        } else {
            // Here we use the SizeEstimator from Spark to estimate the size of the geometry. This is very expensive
            // since it involves lots of reflection, unless the user has implemented the KnownSizeEstimation trait.
            userDataSize = SizeEstimator.estimate(userData);
        }
        return geomSize + userDataSize;
    }

    /**
     * Estimate the size of a geometry object. User data is not included in the estimation.
     * @param geom The geometry object.
     * @return The estimated size of the geometry object.
     */
    public static long estimateSizeWithoutUserData(Geometry geom) {
        return estimateSizeWithoutUserData(geom, geom.getNumPoints());
    }

    /**
     * Estimate the size of a geometry object. User data is not included in the estimation.
     * @param geom The geometry object.
     * @param numPoints The number of points in the geometry object.
     * @return The estimated size of the geometry object.
     */
    public static long estimateSizeWithoutUserData(Geometry geom, int numPoints) {
        if (numPoints == -1) {
            numPoints = geom.getNumPoints();
        }
        return 200L + numPoints * 48L;  // This estimation is verified by experiments.
    }
}
