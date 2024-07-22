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
package org.apache.sedona.core.enums;

import org.apache.sedona.common.enums.GeometryType;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialRddTool.AdvancedStatCollector;

/**
 * Execution mode defines how to prepare geometries for running local spatial join. A proper
 * execution mode will significantly improve the performance of local spatial join.
 */
public enum ExecutionMode {
  /** Create prepared geometries for the build side */
  PREPARE_BUILD,

  /** Create prepared geometries for the stream side */
  PREPARE_STREAM,

  /** Don't use prepared geometry for evaluating spatial predicates */
  PREPARE_NONE;

  public static ExecutionMode getOptimalExecutionMode(
      SpatialPredicate predicate,
      AdvancedStatCollector indexedStat,
      AdvancedStatCollector streamStat) {
    GeometryType indexGeomType = indexedStat.getDominantGeometryType();
    GeometryType streamGeomType = streamStat.getDominantGeometryType();
    double indexMeanNumPoints = indexedStat.getMeanNumPoints();
    double streamMeanNumPoints = streamStat.getMeanNumPoints();
    switch (predicate) {
      case CONTAINS:
      case COVERS:
        return ExecutionMode.PREPARE_BUILD;

      case WITHIN:
      case COVERED_BY:
        return ExecutionMode.PREPARE_STREAM;

      case INTERSECTS:
        if (indexGeomType == GeometryType.POINT && streamGeomType != GeometryType.POINT) {
          return ExecutionMode.PREPARE_STREAM;
        } else if (indexGeomType != GeometryType.POINT && streamGeomType == GeometryType.POINT) {
          return ExecutionMode.PREPARE_BUILD;
        } else if (indexGeomType != GeometryType.POINT) {
          // Both sides are not points. We need to determine the execution mode based on the
          // complexity of the geometries.
          if (streamMeanNumPoints > 0 && indexMeanNumPoints / streamMeanNumPoints > 10) {
            // The index side is much more complex than the stream side. We create prepared
            // geometries for the build side.
            return ExecutionMode.PREPARE_BUILD;
          } else {
            // In all other cases, we create prepared geometries for the stream side. Preparing the
            // stream side has better performance in general since it has a higher cache hit rate.
            return ExecutionMode.PREPARE_STREAM;
          }
        } else {
          return ExecutionMode.PREPARE_STREAM;
        }

      default:
        return ExecutionMode.PREPARE_NONE;
    }
  }
}
