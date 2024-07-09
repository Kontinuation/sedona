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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.sedona.common.geometryObjects.NullGeometry;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * The SpatialPartitioner wraps an ordinary SpatialPartitioner to support outer join. Conventional
 * SpatialPartitioner such as {@link KDBTreePartitioner} and {@link QuadTreePartitioner} ignores
 * geometries that are out of the partitioned space, which is only suitable for inner join. This
 * partitioner places geometries that are out of the partitioned space into additional
 * "out-of-bound" partitions, which can be used for outer join by generating joined pairs containing
 * nulls.
 */
public class OuterJoinSpatialPartitioner extends SpatialPartitioner {
  private final SpatialPartitioner partitioner;
  private final int baseOutOfBoundPartitionId;
  private final int numOutOfBoundPartitions;
  private final boolean partitionOutOfBoundsGeometries;

  public OuterJoinSpatialPartitioner(
      SpatialPartitioner partitioner,
      int numOutOfBoundPartitions,
      boolean partitionOutOfBoundsGeometries) {
    super(partitioner.gridType);
    this.partitioner = partitioner;
    this.baseOutOfBoundPartitionId = partitioner.numPartitions();
    this.numOutOfBoundPartitions = numOutOfBoundPartitions;
    this.partitionOutOfBoundsGeometries = partitionOutOfBoundsGeometries;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
      throws Exception {
    if (!(spatialObject instanceof NullGeometry)) {
      Iterator<Tuple2<Integer, T>> iter = partitioner.placeObject(spatialObject);
      if (iter.hasNext()) {
        return new WithOuterJoinUserDataIterator<>(iter);
      }
    }

    if (partitionOutOfBoundsGeometries) {
      // Place it in one of the out-of-bound partitions
      int hashCode = spatialObject.hashCode();
      int partitionId =
          (hashCode & Integer.MAX_VALUE) % numOutOfBoundPartitions + baseOutOfBoundPartitionId;
      Geometry newSpatialObject = spatialObject.copy();
      newSpatialObject.setUserData(new OuterJoinUserData(spatialObject.getUserData(), true));
      return (Iterator<Tuple2<Integer, T>>)
          new SingletonIterator(new Tuple2<>(partitionId, (T) newSpatialObject));
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public DedupParams getDedupParams() {
    return partitioner.getDedupParams();
  }

  @Override
  public List<Envelope> getGrids() {
    return partitioner.getGrids();
  }

  @Override
  public int numPartitions() {
    return baseOutOfBoundPartitionId + numOutOfBoundPartitions;
  }

  @Override
  public boolean compatibleWith(SpatialPartitioner other) {
    if (other instanceof OuterJoinSpatialPartitioner) {
      OuterJoinSpatialPartitioner otherOuterJoin = (OuterJoinSpatialPartitioner) other;
      return partitioner.compatibleWith(otherOuterJoin.partitioner)
          && numOutOfBoundPartitions == otherOuterJoin.numOutOfBoundPartitions;
    }
    return false;
  }

  /**
   * Attach isPrimary flag to geometries for performing outer-join. A geometry may be duplicated to
   * multiple spatial partitions, but only one of them will be marked as primary. If the primary
   * geometry does not intersect with any geometry in the other dataset, it will be paired with a
   * null. This won't happen to non-primary geometries. This ensures that each geometry is only
   * paired with null at most once.
   */
  public static class OuterJoinUserData implements KryoSerializable, Serializable {
    public Object userData;
    public boolean isPrimary;

    public OuterJoinUserData(Object userData, boolean isPrimary) {
      this.userData = userData;
      this.isPrimary = isPrimary;
    }

    @Override
    public void write(Kryo kryo, Output output) {
      if (userData instanceof UnsafeRow) {
        // fast path for joining DataFrames
        output.writeBoolean(true);
        ((UnsafeRow) userData).write(kryo, output);
      } else {
        output.writeBoolean(false);
        kryo.writeClassAndObject(output, userData);
      }
      output.writeBoolean(isPrimary);
    }

    @Override
    public void read(Kryo kryo, Input input) {
      if (input.readBoolean()) {
        // fast path for joining DataFrames
        userData = new UnsafeRow();
        ((UnsafeRow) userData).read(kryo, input);
      } else {
        userData = kryo.readClassAndObject(input);
      }
      isPrimary = input.readBoolean();
    }
  }

  /**
   * An iterator that attaches isPrimary flag to geometries for performing outer-join. Only the
   * geometry duplicated to the first partition will be marked as primary.
   *
   * @param <T>
   */
  public static class WithOuterJoinUserDataIterator<T extends Geometry>
      implements Iterator<Tuple2<Integer, T>> {
    private final Iterator<Tuple2<Integer, T>> iterator;
    private boolean isFirst = true;

    public WithOuterJoinUserDataIterator(Iterator<Tuple2<Integer, T>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tuple2<Integer, T> next() {
      Tuple2<Integer, T> next = iterator.next();
      Geometry geom = next._2.copy();
      Object userData = geom.getUserData();
      geom.setUserData(new OuterJoinUserData(userData, isFirst));
      isFirst = false;
      return new Tuple2<>(next._1, (T) geom);
    }
  }
}
