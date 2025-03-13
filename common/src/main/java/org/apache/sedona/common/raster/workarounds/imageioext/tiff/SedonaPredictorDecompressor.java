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
package org.apache.sedona.common.raster.workarounds.imageioext.tiff;

import it.geosolutions.imageio.plugins.tiff.BaselineTIFFTagSet;
import it.geosolutions.imageio.plugins.tiff.TIFFDecompressor;
import java.nio.ByteOrder;
import java.util.Arrays;
import javax.imageio.IIOException;

/** Class applying the Predictor algorithm to restore the data from its compressed form. */
public class SedonaPredictorDecompressor {

  /** predictor's type */
  private final int predictor;

  private int[] bitsPerSample;
  private int[] sampleFormat;
  private int samplesPerPixel;
  private ByteOrder byteOrder;

  public SedonaPredictorDecompressor(
      int predictor,
      int[] bitsPerSample,
      int[] sampleFormat,
      int samplesPerPixel,
      ByteOrder byteOrder) {
    this.predictor = predictor;
    this.bitsPerSample = bitsPerSample;
    this.sampleFormat = sampleFormat;
    this.samplesPerPixel = samplesPerPixel;
    this.byteOrder = byteOrder;
  }

  /** Decompress the buffer content by applying the proper predictor algorithm */
  public void decompress(
      byte[] buf, int bufOffset, int dstOffset, int srcHeight, int srcWidth, int bytesPerRow)
      throws IIOException {
    if (predictor == BaselineTIFFTagSet.PREDICTOR_HORIZONTAL_DIFFERENCING) {
      if (bitsPerSample[0] == 8) {
        for (int j = 0; j < srcHeight; j++) {
          int count = bufOffset + samplesPerPixel * (j * srcWidth + 1);
          for (int i = samplesPerPixel; i < srcWidth * samplesPerPixel; i++) {
            buf[count] += buf[count - samplesPerPixel];
            count++;
          }
        }
      } else if (bitsPerSample[0] == 16) {
        if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
          for (int j = 0; j < srcHeight; j++) {
            int count = dstOffset + samplesPerPixel * (j * srcWidth + 1) * 2;
            for (int i = samplesPerPixel; i < srcWidth * samplesPerPixel; i++) {
              int curr = (((int) buf[count]) & 0xFF) | (buf[count + 1] << 8);
              int prev =
                  (((int) buf[count - samplesPerPixel * 2]) & 0xFF)
                      | (buf[count + 1 - samplesPerPixel * 2] << 8);
              curr += prev;
              buf[count] = (byte) curr;
              buf[count + 1] = (byte) (curr >> 8);
              count += 2;
            }
          }
        } else {
          for (int j = 0; j < srcHeight; j++) {
            int count = dstOffset + samplesPerPixel * (j * srcWidth + 1) * 2;
            for (int i = samplesPerPixel; i < srcWidth * samplesPerPixel; i++) {
              int curr = (((int) buf[count + 1]) & 0xFF) | (buf[count] << 8);
              int prev =
                  (((int) buf[count + 1 - samplesPerPixel * 2]) & 0xFF)
                      | (buf[count - samplesPerPixel * 2] << 8);
              curr += prev;
              buf[count + 1] = (byte) curr;
              buf[count] = (byte) (curr >> 8);
              count += 2;
            }
          }
        }
      } else if (bitsPerSample[0] == 32) {
        if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
          for (int j = 0; j < srcHeight; j++) {
            int count = dstOffset + samplesPerPixel * (j * srcWidth + 1) * 4;
            for (int i = samplesPerPixel; i < srcWidth * samplesPerPixel; i++) {
              int prevBase = count - samplesPerPixel * 4;
              int prev =
                  TIFFDecompressor.readIntegerFromBuffer(
                      buf, prevBase, prevBase + 1, prevBase + 2, prevBase + 3);
              int curr =
                  TIFFDecompressor.readIntegerFromBuffer(
                      buf, count, count + 1, count + 2, count + 3);
              int sum = curr + prev;
              buf[count] = (byte) (sum & 0xFF);
              buf[count + 1] = (byte) ((sum >> 8) & 0xFF);
              buf[count + 2] = (byte) ((sum >> 16) & 0xFF);
              buf[count + 3] = (byte) ((sum >> 24) & 0xFF);
              count += 4;
            }
          }
        } else {
          for (int j = 0; j < srcHeight; j++) {
            int count = dstOffset + samplesPerPixel * (j * srcWidth + 1) * 4;
            for (int i = samplesPerPixel; i < srcWidth * samplesPerPixel; i++) {
              int prevBase = count - samplesPerPixel * 4;
              int prev =
                  TIFFDecompressor.readIntegerFromBuffer(
                      buf, prevBase + 3, prevBase + 2, prevBase + 1, prevBase);
              int curr =
                  TIFFDecompressor.readIntegerFromBuffer(
                      buf, count + 3, count + 2, count + 1, count);
              int sum = curr + prev;
              buf[count + 3] = (byte) (sum & 0xFF);
              buf[count + 2] = (byte) (sum >> 8 & 0xFF);
              buf[count + 1] = (byte) (sum >> 16 & 0xFF);
              buf[count] = (byte) (sum >> 24 & 0xFF);
              count += 4;
            }
          }
        }
      } else
        throw new IIOException(
            "Unexpected branch of Horizontal differencing Predictor, bps=" + bitsPerSample[0]);
    } else if (predictor == BaselineTIFFTagSet.PREDICTOR_FLOATING_POINT) {
      int bytesPerSample = bitsPerSample[0] / 8;
      if (bytesPerRow % (bytesPerSample * samplesPerPixel) != 0) {
        throw new IIOException(
            "The number of bytes in a row ("
                + bytesPerRow
                + ") is not divisible"
                + "by the number of bytes per pixel ("
                + bytesPerSample * samplesPerPixel
                + ")");
      }

      for (int j = 0; j < srcHeight; j++) {
        int offset = bufOffset + j * bytesPerRow;
        int count = offset + samplesPerPixel;
        for (int i = samplesPerPixel; i < bytesPerRow; i++) {
          buf[count] += buf[count - samplesPerPixel];
          count++;
        }

        // Reorder the semi-BigEndian bytes.
        byte[] tmp = Arrays.copyOfRange(buf, offset, offset + bytesPerRow);
        int samplesPerRow = srcWidth * samplesPerPixel;
        if (byteOrder == ByteOrder.BIG_ENDIAN) {
          for (int i = 0; i < samplesPerRow; i++) {
            for (int k = 0; k < bytesPerSample; k++) {
              buf[offset + i * bytesPerSample + k] = tmp[k * samplesPerRow + i];
            }
          }
        } else {
          for (int i = 0; i < samplesPerRow; i++) {
            for (int k = 0; k < bytesPerSample; k++) {
              buf[offset + i * bytesPerSample + k] =
                  tmp[(bytesPerSample - k - 1) * samplesPerRow + i];
            }
          }
        }
      }
    }
  }

  /** Validate the current predictor setup */
  public void validate() throws IIOException {
    // Check bitsPerSample.
    if (predictor == BaselineTIFFTagSet.PREDICTOR_HORIZONTAL_DIFFERENCING) {
      int len = bitsPerSample.length;
      final int bps = bitsPerSample[0];
      if (bps != 8 && bps != 16 && bps != 32) {
        throw new IIOException(
            bps + "-bit samples " + "are not supported for Horizontal " + "differencing Predictor");
      }
      for (int i = 0; i < len; i++) {
        if (bitsPerSample[i] != bps) {
          throw new IIOException(
              "Varying sample width is not "
                  + "supported for Horizontal "
                  + "differencing Predictor (first: "
                  + bps
                  + ", unexpected:"
                  + bitsPerSample[i]
                  + ")");
        }
      }
    } else if (predictor == BaselineTIFFTagSet.PREDICTOR_FLOATING_POINT) {
      int len = bitsPerSample.length;
      final int bps = bitsPerSample[0];
      if (bps != 16 && bps != 24 && bps != 32 && bps != 64) {
        throw new IIOException(
            bps + "-bit samples " + "are not supported for Floating " + "point Predictor");
      }
      for (int i = 0; i < len; i++) {
        if (bitsPerSample[i] != bps) {
          throw new IIOException(
              "Varying sample width is not "
                  + "supported for Floating "
                  + "point Predictor (first: "
                  + bps
                  + ", unexpected:"
                  + bitsPerSample[i]
                  + ")");
        }
      }
      for (int sf : sampleFormat) {
        if (sf != BaselineTIFFTagSet.SAMPLE_FORMAT_FLOATING_POINT) {
          throw new IIOException(
              "Floating point Predictor not supported" + "with " + sf + " data format");
        }
      }
    }
  }
}
