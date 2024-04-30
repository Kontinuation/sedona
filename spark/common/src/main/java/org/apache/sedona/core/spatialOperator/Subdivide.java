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
import org.apache.sedona.common.geometrySerde.GeometrySerializer;
import org.apache.sedona.common.subDivide.ExtentBasedGeometrySubDivider;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.core.spatialRddTool.AdvancedStatCollector;
import org.apache.sedona.core.utils.SedonaConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public class Subdivide {

    /**
     * The user data of subdivided parts. It holds information for de-duplication and recovering the original geometry.
     */
    public static class SubdividedPart implements Serializable, KryoSerializable {
        /**
         * Unique ID of the original geometry. All subdivided parts of one geometry have the same ID.
         */
        public long id;

        /**
         * User data associated with the original geometry.
         */
        public Object userData;

        /**
         * Original geometry without user data. This could be null if the original geometry does not need to be kept.
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
            kryo.writeClassAndObject(output, userData);
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
            userData = kryo.readClassAndObject(input);
            int length = input.readInt();
            if (length > 0) {
                byte[] serializedGeom = input.readBytes(length);
                origGeomWithoutUserData = GeometrySerializer.deserialize(serializedGeom);
            } else {
                origGeomWithoutUserData = null;
            }
        }
    }

    /**
     * Options for subdividing a spatial RDD.
     */
    public static class SubdivideRDDOptions implements Serializable {
        public final SubdivideOptions options;

        // Estimated statistics for the subdivided spatial RDD. This estimation could help us avoid another
        // round of statistics calculation on the subdivided spatial RDD.
        public final AdvancedStatCollector stats;

        public final boolean keepOriginalGeometry;
        public final boolean keepUserData;

        public SubdivideRDDOptions(SubdivideOptions options, AdvancedStatCollector stats,
                                   boolean keepOriginalGeometry, boolean keepUserData) {
            this.options = options;
            this.stats = stats;
            this.keepOriginalGeometry = keepOriginalGeometry;
            this.keepUserData = keepUserData;
        }

        public SubdivideRDDOptions(SubdivideOptions options, boolean keepOriginalGeometry, boolean keepUserData) {
            this(options, null, keepOriginalGeometry, keepUserData);
        }
    }

    /**
     * Subdivide the geometry in raw spatial RDD to have smaller extent.
     * The subdivision is based on the maximum number of coordinates, width, height, and area.
     * @param rawSpatialRDD Raw spatial RDD.
     * @param options Subdivision options.
     * @param keepOriginalGeometry Whether to keep the original geometry in the subdivided parts.
     * @param keepUserData Whether to keep the user data in the subdivided parts.
     * @param geomTransformer A function to transform the geometry after subdividing.
     * @return Subdivided raw spatial RDD.
     * @param <T> The type of geometry.
     */
    public static <T extends Geometry> JavaRDD<Geometry> subdivideRawSpatialRDD(
            JavaRDD<T> rawSpatialRDD, SubdivideOptions options, boolean keepOriginalGeometry, boolean keepUserData,
            Function2<Geometry, Object, Geometry> geomTransformer) {
        int numPartitions = rawSpatialRDD.getNumPartitions();
        return rawSpatialRDD.mapPartitionsWithIndex((index, geometryIterator) -> {
            ExtentBasedGeometrySubDivider subDivider = new ExtentBasedGeometrySubDivider(options);
            return new SubdivideIterator<>(subDivider, index, numPartitions, keepOriginalGeometry, keepUserData,
                    geometryIterator, geomTransformer);
        }, false);
    }

    /**
     * Subdivide the geometry in spatial RDD to have smaller extent.
     * @param spatialRDD Spatial RDD.
     * @param options Subdivision options.
     * @param keepOriginalGeometry Whether to keep the original geometry in the subdivided parts.
     * @param keepUserData Whether to keep the user data in the subdivided parts.
     * @param geomTransformer A function to transform the geometry after subdividing.
     * @return Subdivided spatial RDD.
     * @param <T> The type of geometry in the spatial RDD.
     */
    public static <T extends Geometry> SpatialRDD<Geometry> subdivideSpatialRDD(
            SpatialRDD<T> spatialRDD, SubdivideOptions options, boolean keepOriginalGeometry, boolean keepUserData,
            Function2<Geometry, Object, Geometry> geomTransformer) {
        JavaRDD<Geometry> subdividedRawSpatialRDD = subdivideRawSpatialRDD(
                spatialRDD.getRawSpatialRDD(), options, keepOriginalGeometry, keepUserData, geomTransformer);
        SpatialRDD<Geometry> subdividedSpatialRDD = new SpatialRDD<>();
        subdividedSpatialRDD.setRawSpatialRDD(subdividedRawSpatialRDD);
        return subdividedSpatialRDD;
    }

    /**
     * Subdivide the geometry in spatial RDD to have smaller extent.
     * @param spatialRDD Spatial RDD.
     * @param options Subdivision options.
     * @param keepOriginalGeometry Whether to keep the original geometry in the subdivided parts.
     * @param keepUserData Whether to keep the user data in the subdivided parts.
     * @return Subdivided spatial RDD.
     * @param <T> The type of geometry in the spatial RDD.
     */
    public static <T extends Geometry> SpatialRDD<Geometry> subdivideSpatialRDD(
            SpatialRDD<T> spatialRDD, SubdivideOptions options, boolean keepOriginalGeometry, boolean keepUserData) {
        return subdivideSpatialRDD(spatialRDD, options, keepOriginalGeometry, keepUserData, null);
    }

    /**
     * Subdivide the geometry in spatial RDD to have smaller extent.
     * @param spatialRDD Spatial RDD.
     * @param options Subdivision options.
     * @param geomTransformer A function to transform the geometry after subdividing.
     * @return Subdivided spatial RDD.
     * @param <T> The type of geometry in the spatial RDD.
     */
    public static <T extends Geometry> SpatialRDD<Geometry> subdivideSpatialRDD(
            SpatialRDD<T> spatialRDD, SubdivideRDDOptions options,
            Function2<Geometry, Object, Geometry> geomTransformer) {
        SpatialRDD<Geometry> subdivided = subdivideSpatialRDD(spatialRDD, options.options, options.keepOriginalGeometry,
                options.keepUserData, geomTransformer);
        subdivided.setStatistics(options.stats);
        return subdivided;
    }

    /**
     * Subdivide the geometry in spatial RDD to have smaller extent.
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

        SubdivideIterator(ExtentBasedGeometrySubDivider subDivider, int index, int step,
                          boolean keepOriginalGeometry, boolean keepUserData,
                          Iterator<T> geometries, Function2<Geometry, Object, Geometry> geomTransformer) {
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
            SubdividedPart part = new SubdividedPart(currentIndex, keepUserData? currentUserData: null,
                    currentGeomWithoutUserData);
            geom.setUserData(part);
            return geom;
        }
    }

    /**
     * Don't subdivide the geometry, but only attach unique IDs to each record in the RDD. This is useful when we want
     * to join a subdivided spatial RDD with a non-subdivided spatial RDD.
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
     * Attach unique IDs to each record in the RDD, so that we can do deduplication on subdivided RDDs, as well as
     * joining subdivided RDD with original RDD.
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
     * Attach unique IDs to each record in the RDD, so that it could be joined back with the subdivided RDD later.
     * The algorithm for assigning IDs is identical to the one in SubdivideIterator, so that the IDs attached to
     * original records are consistent with subdivided records. This function is useful for retrieving information
     * from the original RDD from a subdivided RDD that does not carry the original geometries or user data.
     * @param rdd The RDD to attach IDs to.
     * @return An RDD of (ID, T) pairs.
     * @param <T> The type of the records in the RDD.
     */
    public static <T> JavaRDD<Tuple2<Long, T>> attachId(JavaRDD<T> rdd) {
        int numPartitions = rdd.getNumPartitions();
        return rdd.mapPartitionsWithIndex((index, iterator) ->
                new RecordWithIdIterator<>(index, numPartitions, iterator), false);
    }

    /**
     * Convert the user data of geometries to SubdividedPart objects and assign unique IDs to each geometry. The
     * resulting SpatialRDD has the same type of user data as being subdivided, but there's no subdivision applied.
     * @param spatialRDD The spatial RDD to subdivide.
     * @return A new SpatialRDD with unique IDs attached to each geometry.
     * @param <T> The type of geometry in the spatial RDD.
     */
    public static <T extends Geometry> SpatialRDD<Geometry> noOpSubdivideSpatialRDD(SpatialRDD<T> spatialRDD) {
        JavaRDD<T> rawSpatialRDD = spatialRDD.getRawSpatialRDD();
        int numPartitions = rawSpatialRDD.getNumPartitions();
        JavaRDD<Geometry> newRawSpatialRDD = rawSpatialRDD.mapPartitionsWithIndex((index, geometryIterator) ->
                new NoOpSubdivideIterator<>(index, numPartitions, geometryIterator), false);
        SpatialRDD<Geometry> newSpatialRDD = new SpatialRDD<>();
        newSpatialRDD.setRawSpatialRDD(newRawSpatialRDD);
        newSpatialRDD.setStatistics(spatialRDD.getStatistics());
        return newSpatialRDD;
    }

    /**
     * Determine the subdivision options based on the spatial RDD. This is done by applying some heuristics to the
     * statistics of the geometries in the spatial RDD.
     * @param spatialRDD Spatial RDD. This SpatialRDD should be analyzed using the {@link SpatialRDD#advancedAnalyze()}
     *                   method.
     * @param sedonaConf Sedona configuration.
     * @return Subdivision options.
     * @param <T> The type of geometry in the spatial RDD.
     */
    public static <T extends Geometry> Optional<SubdivideRDDOptions> determineSubdivideOptions(
            SpatialRDD<T> spatialRDD, SedonaConf sedonaConf) {
        AdvancedStatCollector stats = spatialRDD.getStatistics();
        if (stats == null) {
            throw new IllegalArgumentException("The spatial RDD must be analyzed using the advancedAnalyze method");
        }
        // TODO: implement this method
        // return Optional.empty();
        SubdivideOptions options = new SubdivideOptions();
        return Optional.of(new SubdivideRDDOptions(options, stats, false, false));
    }

    /**
     * Determine if the subdivision performed on the spatial RDD is accurate based on the statistics of the geometries
     * and the subdivision options.
     * @param spatialRDD Spatial RDD. This SpatialRDD should be analyzed using the {@link SpatialRDD#advancedAnalyze()}
     * @param subdivideRDDOptions Subdivision RDD options.
     * @return True if the subdivision is accurate, false otherwise.
     * @param <T> The type of geometry in the spatial RDD.
     */
    public static <T extends Geometry> boolean isSubdivideAccurate(SpatialRDD<T> spatialRDD,
                                                                   SubdivideRDDOptions subdivideRDDOptions) {
        AdvancedStatCollector stats = spatialRDD.getStatistics();
        SubdivideOptions options = subdivideRDDOptions.options;
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
