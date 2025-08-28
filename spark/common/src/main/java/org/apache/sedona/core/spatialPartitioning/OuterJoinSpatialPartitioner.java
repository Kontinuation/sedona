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
import org.apache.sedona.core.utils.UniqueIDUtils;
import org.apache.spark.api.java.JavaRDD;
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
      Object userData = spatialObject.getUserData();
      if (userData instanceof OuterJoinUserData) {
        Geometry newSpatialObject = spatialObject.copy();
        OuterJoinUserData outerJoinUserData = (OuterJoinUserData) userData;
        newSpatialObject.setUserData(
            new OuterJoinUserData(outerJoinUserData.userData, true, outerJoinUserData.uniqueId));
        return (Iterator<Tuple2<Integer, T>>)
            new SingletonIterator(new Tuple2<>(partitionId, (T) newSpatialObject));
      } else {
        return (Iterator<Tuple2<Integer, T>>)
            new SingletonIterator(new Tuple2<>(partitionId, spatialObject));
      }
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
    public long uniqueId;

    public OuterJoinUserData(Object userData, boolean isPrimary, long uniqueId) {
      this.userData = userData;
      this.isPrimary = isPrimary;
      this.uniqueId = uniqueId;
    }

    public OuterJoinUserData() {
      this(null, false, 0);
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
      output.writeLong(uniqueId);
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
      uniqueId = input.readLong();
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
      T geom = next._2;
      Object userData = geom.getUserData();
      if (userData instanceof OuterJoinUserData) {
        Geometry newGeom = next._2.copy();
        OuterJoinUserData outerJoinUserData = (OuterJoinUserData) userData;
        newGeom.setUserData(
            new OuterJoinUserData(outerJoinUserData.userData, isFirst, outerJoinUserData.uniqueId));
        isFirst = false;
        return new Tuple2<>(next._1, (T) newGeom);
      } else {
        return next;
      }
    }
  }

  /**
   * Prepare a Geometry RDD for spatial partitioning with OuterJoinSpatialPartitioner. The userData
   * of each geometry will be wrapped in an OuterJoinUserData object, with uniqueId set to a
   * universally identify a record. This is useful when we remove the redundant pairs with null as
   * the non-outer side produced by each partition during the local join.
   *
   * @param rdd the input Geometry RDD
   * @return the prepared Geometry RDD
   * @param <T> the type of Geometry
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T extends Geometry> JavaRDD<T> prepareRDDForPartitioning(JavaRDD<T> rdd) {
    JavaRDD<Tuple2<Long, T>> rddWithId = UniqueIDUtils.attachId(rdd);
    return rddWithId.map(
        (tuple) -> {
          long uniqueId = tuple._1;
          T geom = tuple._2;
          if (geom.getUserData() instanceof OuterJoinUserData) {
            return geom;
          } else {
            Geometry newGeom = geom.copy();
            newGeom.setUserData(new OuterJoinUserData(geom.getUserData(), false, uniqueId));
            return (T) newGeom;
          }
        });
  }
}
