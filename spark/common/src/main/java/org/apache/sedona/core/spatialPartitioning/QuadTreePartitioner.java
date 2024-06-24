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

import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.quadtree.StandardQuadTree;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

public class QuadTreePartitioner extends SpatialPartitioner {
  private final StandardQuadTree<?> quadTree;
  private transient List<Envelope> grids;

  public QuadTreePartitioner(StandardQuadTree<?> quadTree) {
    super(GridType.QUADTREE);
    this.quadTree = quadTree;
    this.grids = null;

    // Make sure not to broadcast all the samples used to build the Quad
    // tree to all nodes which are doing partitioning
    this.quadTree.dropElements();
  }

  @Override
  public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry spatialObject) throws Exception {
    return quadTree.placeObject(spatialObject);
  }

  @Nullable
  @Override
  public DedupParams getDedupParams() {
    return new DedupParams(getGrids());
  }

  @Override
  public List<Envelope> getGrids() {
    if (grids == null) {
      grids = quadTree.fetchLeafZones();
    }
    return grids;
  }

  @Override
  public int numPartitions() {
    return getGrids().size();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof QuadTreePartitioner)) {
      return false;
    }

    final QuadTreePartitioner other = (QuadTreePartitioner) o;
    return other.quadTree.equals(this.quadTree);
  }
}
