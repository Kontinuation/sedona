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
package org.apache.sedona.common.raster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.sun.media.jai.rmi.DataBufferState;
import com.sun.media.jai.rmi.RasterState;
import com.sun.media.jai.rmi.SampleModelState;
import com.sun.media.jai.util.DataBufferUtils;

import javax.media.jai.remote.SerializerFactory;
import java.awt.Point;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferInt;
import java.awt.image.DataBufferShort;
import java.awt.image.DataBufferUShort;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

/**
 * Kryo serializers for JAI Raster and related classes.
 */
public class JAISerializableStateSerializers {
    public static class RasterStateSerializer extends Serializer<RasterState> {
        @Override
        public void write(Kryo kryo, Output output, RasterState object) {
            Raster raster = (Raster) object.getObject();
            Raster r;
            if (raster.getParent() != null) {
                r = raster.createCompatibleWritableRaster(raster.getBounds());
                ((WritableRaster)r).setRect(raster);
            } else {
                r = raster;
            }

            output.writeInt(r.getMinX());
            output.writeInt(r.getMinY());
            kryo.writeObject(output, SerializerFactory.getState(r.getSampleModel(), null));
            kryo.writeObject(output, SerializerFactory.getState(r.getDataBuffer(), null));
        }

        @Override
        public RasterState read(Kryo kryo, Input input, Class<RasterState> type) {
            int minX = input.readInt();
            int minY = input.readInt();
            Point location = new Point(minX, minY);
            SampleModelState sampleModelState = kryo.readObject(input, SampleModelState.class);
            DataBufferState dataBufferState = kryo.readObject(input, DataBufferState.class);
            SampleModel sampleModel = (SampleModel) sampleModelState.getObject();
            DataBuffer dataBuffer = (DataBuffer) dataBufferState.getObject();
            Raster raster = Raster.createRaster(sampleModel, dataBuffer, location);
            return new RasterState(raster.getClass(), raster,null);
        }
    }

    public static class DataBufferStateSerializer extends Serializer<DataBufferState> {
        @Override
        public void write(Kryo kryo, Output output, DataBufferState object) {
            DataBuffer dataBuffer = (DataBuffer) object.getObject();
            int dataType = dataBuffer.getDataType();
            output.writeInt(dataType);
            kryo.writeObject(output, dataBuffer.getOffsets());
            output.writeInt(dataBuffer.getSize());
            switch (dataType) {
                case DataBuffer.TYPE_BYTE:
                    byte[][] byteDataArray = ((DataBufferByte)dataBuffer).getBankData();
                    kryo.writeObject(output, byteDataArray);
                    break;
                case DataBuffer.TYPE_USHORT:
                    short[][] uShortDataArray = ((DataBufferUShort) dataBuffer).getBankData();
                    kryo.writeObject(output, uShortDataArray);
                    break;
                case DataBuffer.TYPE_SHORT:
                    short[][] shortDataArray = ((DataBufferShort)dataBuffer).getBankData();
                    kryo.writeObject(output, shortDataArray);
                    break;
                case DataBuffer.TYPE_INT:
                    int[][] intDataArray = ((DataBufferInt) dataBuffer).getBankData();
                    kryo.writeObject(output, intDataArray);
                    break;
                case DataBuffer.TYPE_FLOAT:
                    float[][] floatDataArray = DataBufferUtils.getBankDataFloat(dataBuffer);
                    kryo.writeObject(output, floatDataArray);
                    break;
                case DataBuffer.TYPE_DOUBLE:
                    double[][] doubleDataArray = DataBufferUtils.getBankDataDouble(dataBuffer);
                    kryo.writeObject(output, doubleDataArray);
                    break;
                default:
                    throw new RuntimeException("Unknown data type: " + dataType);
            }
        }

        @Override
        public DataBufferState read(Kryo kryo, Input input, Class<DataBufferState> type) {
            int dataType = input.readInt();
            int[] offsets = kryo.readObject(input, int[].class);
            int size = input.readInt();
            DataBuffer dataBuffer;
            switch (dataType) {
                case DataBuffer.TYPE_BYTE:
                    byte[][] byteDataArray = kryo.readObject(input, byte[][].class);
                    dataBuffer = new DataBufferByte(byteDataArray, size, offsets);
                    break;
                case DataBuffer.TYPE_USHORT:
                    short[][] uShortDataArray = kryo.readObject(input, short[][].class);
                    dataBuffer = new DataBufferUShort(uShortDataArray, size, offsets);
                    break;
                case DataBuffer.TYPE_SHORT:
                    short[][] shortDataArray = kryo.readObject(input, short[][].class);
                    dataBuffer = new DataBufferShort(shortDataArray, size, offsets);
                    break;
                case DataBuffer.TYPE_INT:
                    int[][] intDataArray = kryo.readObject(input, int[][].class);
                    dataBuffer = new DataBufferInt(intDataArray, size, offsets);
                    break;
                case DataBuffer.TYPE_FLOAT:
                    float[][] floatDataArray = kryo.readObject(input, float[][].class);
                    dataBuffer = DataBufferUtils.createDataBufferFloat(floatDataArray, size, offsets);
                    break;
                case DataBuffer.TYPE_DOUBLE:
                    double[][] doubleDataArray = kryo.readObject(input, double[][].class);
                    dataBuffer = DataBufferUtils.createDataBufferDouble(doubleDataArray, size, offsets);
                    break;
                default:
                    throw new RuntimeException("Unknown data type: " + dataType);
            }
            return new DataBufferState(dataBuffer.getClass(), dataBuffer, null);
        }
    }
}
