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
package org.apache.spark.sql.sedona_sql

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.BinaryEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.TransformingEncoder
import org.apache.spark.sql.catalyst.encoders.Codec
import org.apache.spark.sql.catalyst.expressions.objects.SerializerSupport
import org.apache.spark.sql.errors.ExecutionErrors

import java.lang.reflect.Modifier
import java.nio.ByteBuffer
import scala.reflect.ClassTag
import scala.reflect.classTag

/**
 * This is for working around https://issues.apache.org/jira/browse/SPARK-52819, this bug prevents
 * us from using `Encoders.kryo` directly in Spark 4.0.
 */
object EncodersShim {
  def kryo[T: ClassTag]: Encoder[T] = genericSerializer(() => new KryoSerializationCodecImpl())

  private def genericSerializer[T: ClassTag](
                                              provider: () => Codec[Any, Array[Byte]]): Encoder[T] = {
    if (classTag[T].runtimeClass.isPrimitive) {
      throw ExecutionErrors.primitiveTypesNotSupportedError()
    }

    validatePublicClass[T]()

    TransformingEncoder(classTag[T], BinaryEncoder, provider)
  }

  private def validatePublicClass[T: ClassTag](): Unit = {
    if (!Modifier.isPublic(classTag[T].runtimeClass.getModifiers)) {
      throw ExecutionErrors.notPublicClassError(classTag[T].runtimeClass.getName)
    }
  }
}

/**
 * A serializable variant of [[org.apache.spark.sql.catalyst.encoders.KryoSerializationCodecImpl]]
 * to work around https://issues.apache.org/jira/browse/SPARK-52819
 */
class KryoSerializationCodecImpl extends Codec[Any, Array[Byte]] {
  private lazy val serializer = SerializerSupport.newSerializer(useKryo = true)
  override def encode(in: Any): Array[Byte] =
    serializer.serialize(in).array()

  override def decode(out: Array[Byte]): Any =
    serializer.deserialize(ByteBuffer.wrap(out))
}
