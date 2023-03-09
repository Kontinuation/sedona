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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

import java.util.List;

/**
 * Create a Refiner for refining the join result candidates. To understand what a refiner do, we'll take a brief review
 * of the join process on each partition. The join process on each partition takes two sets of geometries as input, one
 * is the stream side, and the other one is build side. The geometries on the build side could be restructured for
 * efficient MBR based spatial query, or simply leave it as a flat list (nested loop join). The join process is:
 *
 * <ol>
 *     <li>For each geometry on the stream side, find all candidates on the build side that may satisfy the spatial
 *     predicate.
 *     <li>For each candidate, evaluate the spatial predicate to determine whether the pair of geometries actually
 *     satisfy the spatial predicate.
 * </ol>
 *
 * The refiner is for the second step. It takes a geometry on the stream side, and a list of candidate geometries retrieved
 * from the build side, and returns a list of pairs of geometries that actually satisfy the spatial predicate. The stream
 * side can be either side of joined RDDs, and we can also choose to prepare the stream side geometry to speed up
 * evaluating the spatial predicate. These many combinations leads to various implementations of the refiner.
 */
public class JoinResultCandidateRefiner {
    public interface Refiner {
        <T, U> void refine(Geometry streamGeometry, List<Geometry> candidateGeometries, List<Pair<T, U>> results);
    }

    static class StreamGeometryOnLeft implements Refiner {
        SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator;
        StreamGeometryOnLeft(SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        @Override
        public <T, U> void refine(Geometry streamGeometry, List<Geometry> candidateGeometries, List<Pair<T, U>> results) {
            for (Geometry candidateGeometry : candidateGeometries) {
                if (evaluator.eval(streamGeometry, candidateGeometry)) {
                    results.add(Pair.of((T) streamGeometry, (U) candidateGeometry));
                }
            }
        }
    }

    static class StreamGeometryOnRight implements Refiner {
        SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator;
        StreamGeometryOnRight(SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        @Override
        public <T, U> void refine(Geometry streamGeometry, List<Geometry> candidateGeometries, List<Pair<T, U>> results) {
            for (Geometry candidateGeometry : candidateGeometries) {
                if (evaluator.eval(candidateGeometry, streamGeometry)) {
                    results.add(Pair.of((T) candidateGeometry, (U) streamGeometry));
                }
            }
        }
    }

    static class PreparedStreamGeometryOnLeft implements Refiner {
        SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator;
        PreparedGeometryFactory factory = new PreparedGeometryFactory();
        PreparedStreamGeometryOnLeft(SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        @Override
        public <T, U> void refine(Geometry streamGeometry, List<Geometry> candidateGeometries, List<Pair<T, U>> results) {
            if (candidateGeometries.isEmpty()) {
                return;
            }
            PreparedGeometry preparedStreamGeometry = factory.create(streamGeometry);
            for (Geometry candidateGeometry : candidateGeometries) {
                if (evaluator.eval(preparedStreamGeometry, candidateGeometry)) {
                    results.add(Pair.of((T) streamGeometry, (U) candidateGeometry));
                }
            }
        }
    }

    static class PreparedStreamGeometryOnRight implements Refiner {
        SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator;
        PreparedGeometryFactory factory = new PreparedGeometryFactory();
        PreparedStreamGeometryOnRight(SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        @Override
        public <T, U> void refine(Geometry streamGeometry, List<Geometry> candidateGeometries, List<Pair<T, U>> results) {
            if (candidateGeometries.isEmpty()) {
                return;
            }
            PreparedGeometry preparedStreamGeometry = factory.create(streamGeometry);
            for (Geometry candidateGeometry : candidateGeometries) {
                if (evaluator.eval(candidateGeometry, preparedStreamGeometry)) {
                    results.add(Pair.of((T) candidateGeometry, (U) streamGeometry));
                }
            }
        }
    }

    /**
     * Create a refiner for refining the join result candidates.
     * @param streamLeft If the stream geometry is from the left side of the joined RDDs.
     * @param spatialPredicate The spatial predicate to be evaluated.
     * @return A refiner for refining the join result candidates.
     */
    public static Refiner create(boolean streamLeft, SpatialPredicate spatialPredicate) {
        SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator = SpatialPredicateEvaluators.create(spatialPredicate);
        if (streamLeft) {
            // Prepared geometries does not always speed up the evaluation of spatial predicates. Only intersects,
            // contains and covers have an optimized implementation for prepared geometries in JTS, while other
            // predicates such as within, covered_by and overlaps simply fallback to the non-prepared implementation.
            // We'll have some extra overhead for constructing prepared geometries without getting any speed-ups when
            // evaluating such spatial predicates. So we'll only enable prepared geometries for intersects, contains and
            // covers when stream geometry is on the left side.
            if (spatialPredicate == SpatialPredicate.INTERSECTS ||
                    spatialPredicate == SpatialPredicate.CONTAINS ||
                    spatialPredicate == SpatialPredicate.COVERS) {
                return new PreparedStreamGeometryOnLeft(evaluator);
            } else {
                return new StreamGeometryOnLeft(evaluator);
            }
        } else {
            // Similarly, when stream geometry is on the right side, we'll only benefit from prepared geometries when
            // evaluating intersects, within and covered_by. For other predicates, we'll simply use the non-prepared
            // implementation.
            if (spatialPredicate == SpatialPredicate.INTERSECTS ||
                    spatialPredicate == SpatialPredicate.WITHIN ||
                    spatialPredicate == SpatialPredicate.COVERED_BY) {
                return new PreparedStreamGeometryOnRight(evaluator);
            } else {
                return new StreamGeometryOnRight(evaluator);
            }
        }
    }
}
