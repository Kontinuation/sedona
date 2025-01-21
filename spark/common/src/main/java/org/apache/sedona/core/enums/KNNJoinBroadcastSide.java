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

import java.io.Serializable;

public enum KNNJoinBroadcastSide implements Serializable {

  /** No broadcast is performed. Spatial partitioning is used to optimize the join operation. */
  NONE,

  /**
   * When the object side is broadcasted, we can simply broadcast the spatial index built using all
   * geometries on the object side and perform local KNN join on each partition of the query side.
   */
  OBJECT_SIDE,

  /**
   * When the query side is broadcasted, we have to broadcast the list of geometries on the query
   * side to each partition of the object side. We run local KNN join on each partition of the
   * object side by querying the spatial index built using geometries on the object side. The query
   * results has to be further aggregated to obtain the global KNN results from per-partition KNN
   * results.
   */
  QUERY_SIDE
}
