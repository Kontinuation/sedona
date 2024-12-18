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
package org.apache.spark.sedona.core.index;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;

/**
 * DataItemFormat is an interface that defines the format of data items. It defines methods for
 * serializing and deserializing data items, extracting geometry data from the data items, and
 * extracting envelope of the geometry directly from the data items.
 */
public interface DataItemFormat<T> {
  /**
   * Serialize the data item to a byte array.
   *
   * @param dataItem the data item to serialize
   * @return the byte array representation of the data item
   */
  byte[] serialize(T dataItem);

  /**
   * Deserialize the data item from a byte array.
   *
   * @param bytes the byte array to deserialize
   * @return the data item
   */
  T deserialize(byte[] bytes);

  /**
   * Extract the envelope of the geometry directly from the data item.
   *
   * @param dataItem the data item to extract the envelope from
   * @return the envelope of the geometry
   */
  Envelope extractEnvelope(T dataItem);

  /**
   * Extract the geometry data from the data item.
   *
   * @param dataItem the data item to extract the geometry from
   * @return the geometry data
   */
  Geometry extractGeometry(T dataItem);

  /**
   * Extract the prepared geometry data from the data item.
   *
   * @param dataItem the data item to extract the prepared geometry from
   * @return the prepared geometry data
   */
  PreparedGeometry extractPreparedGeometry(T dataItem);
}
