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
package org.apache.sedona.core.serde;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItemFormat;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

/**
 * This is a specialized serializer for shuffle read/write spatial partitioned geometries. It uses
 * GeometryDataItemFormat to serialize/deserialize geometries, which is more efficient when handling
 * user data attached to geometry objects. It also gets rid of serializing/deserializing keys since
 * they are dropped immediately after spatial partitioning.
 */
public class ShuffledGeometrySerializer extends Serializer implements Serializable {

  @Override
  public SerializerInstance newInstance() {
    return new GeometrySerializerInstance();
  }

  @Override
  public boolean supportsRelocationOfSerializedObjects() {
    // The underlying serialization is based on kryo with auto-reset enabled, so the serialized
    // objects are relocatable.
    return true;
  }

  private static class GeometrySerializerInstance extends SerializerInstance {

    @Override
    public SerializationStream serializeStream(OutputStream out) {
      return new GeometrySerializationStream(out);
    }

    @Override
    public DeserializationStream deserializeStream(InputStream in) {
      return new GeometryDeserializationStream(in);
    }

    // The following methods are never called by shuffle code.

    @Override
    public <T> ByteBuffer serialize(T t, ClassTag<T> classTag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> classTag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassTag<T> classTag) {
      throw new UnsupportedOperationException();
    }
  }

  private static class GeometrySerializationStream extends SerializationStream {
    DataOutputStream out;
    GeometryDataItemFormat format;

    public GeometrySerializationStream(OutputStream out) {
      this.out = new DataOutputStream(new BufferedOutputStream(out));
      this.format = new GeometryDataItemFormat();
    }

    @Override
    public <T> SerializationStream writeKey(T key, ClassTag<T> classTag) {
      return this;
    }

    @Override
    public <T> SerializationStream writeValue(T value, ClassTag<T> classTag) {
      byte[] data = format.serialize((Geometry) value);
      try {
        out.writeInt(data.length);
        out.write(data, 0, data.length);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    @Override
    public void flush() {
      try {
        out.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      try {
        out.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public <T> SerializationStream writeAll(Iterator<T> iter, ClassTag<T> classTag$T$0) {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> SerializationStream writeObject(T t, ClassTag<T> classTag$T$0) {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException();
    }
  }

  private static class GeometryDeserializationStream extends DeserializationStream {
    DataInputStream in;
    GeometryDataItemFormat format;

    public GeometryDeserializationStream(InputStream in) {
      this.in = new DataInputStream(new BufferedInputStream(in));
      this.format = new GeometryDataItemFormat();
    }

    @Override
    public Iterator<Tuple2<Object, Object>> asKeyValueIterator() {
      java.util.Iterator<Tuple2<Object, Object>> iter =
          new java.util.Iterator<Tuple2<Object, Object>>() {
            private int size = 0;
            private byte[] data = new byte[1024];

            private int readSize() {
              try {
                return in.readInt();
              } catch (EOFException e) {
                try {
                  in.close();
                  data = null;
                  return -1;
                } catch (IOException ex) {
                  throw new RuntimeException(ex);
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public boolean hasNext() {
              if (size == 0) {
                size = readSize();
              }
              return size != -1;
            }

            @Override
            public Tuple2<Object, Object> next() {
              if (!hasNext()) {
                throw new NoSuchElementException();
              }
              if (data.length < size) {
                data = new byte[size];
              }
              try {
                in.readFully(data, 0, size);
                size = 0;
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              Geometry geometry = format.deserializeToGeometry(data);
              return new Tuple2<>(geometry, geometry);
            }
          };
      return scala.collection.JavaConverters.asScalaIterator(iter);
    }

    @Override
    public <T> T readKey(ClassTag<T> classTag) {
      return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readValue(ClassTag<T> classTag) {
      try {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        Geometry geometry = format.deserializeToGeometry(data);
        return (T) geometry;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      try {
        in.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Iterator<Object> asIterator() {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T readObject(ClassTag<T> classTag$T$0) {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException();
    }
  }
}
