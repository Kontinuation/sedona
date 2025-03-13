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

import it.geosolutions.imageio.compression.CompressionFinder;
import it.geosolutions.imageio.compression.CompressionType;
import it.geosolutions.imageio.compression.Decompressor;
import it.geosolutions.imageio.plugins.tiff.BaselineTIFFTagSet;
import it.geosolutions.imageio.plugins.tiff.TIFFDecompressor;
import java.io.IOException;
import java.util.zip.DataFormatException;
import javax.imageio.IIOException;

public class SedonaTIFFDeflateDecompressor extends TIFFDecompressor {

  private static final boolean DEBUG = false;
  int predictor;
  Decompressor deflateDecompressor;

  public SedonaTIFFDeflateDecompressor(int predictor) throws IIOException {
    if (predictor != BaselineTIFFTagSet.PREDICTOR_NONE
        && predictor != BaselineTIFFTagSet.PREDICTOR_HORIZONTAL_DIFFERENCING
        && predictor != BaselineTIFFTagSet.PREDICTOR_FLOATING_POINT) {
      throw new IIOException("Illegal value for Predictor in TIFF file");
    }

    if (DEBUG) {
      System.out.println("Using horizontal differencing predictor");
    }

    this.predictor = predictor;
    deflateDecompressor = CompressionFinder.getDecompressor(CompressionType.DEFLATE);
  }

  public synchronized void decodeRaw(byte[] b, int dstOffset, int bitsPerPixel, int scanlineStride)
      throws IOException {

    SedonaPredictorDecompressor predictorDecompressor =
        new SedonaPredictorDecompressor(
            predictor,
            bitsPerSample,
            sampleFormat,
            planar ? 1 : samplesPerPixel,
            stream.getByteOrder());
    predictorDecompressor.validate();

    // Seek to current tile data offset.
    stream.seek(offset);

    // Read the deflated data.
    byte[] srcData = new byte[byteCount];
    stream.readFully(srcData);

    int bytesPerRow = (srcWidth * bitsPerPixel + 7) / 8;
    byte[] buf;
    int bufOffset;
    if (bytesPerRow == scanlineStride) {
      buf = b;
      bufOffset = dstOffset;
    } else {
      buf = new byte[bytesPerRow * srcHeight];
      bufOffset = 0;
    }

    deflateDecompressor.setInput(srcData);
    try {
      deflateDecompressor.decompress(buf, bufOffset, bytesPerRow * srcHeight);
    } catch (DataFormatException dfe) {
      throw new IIOException("DataFormatException occurred when decompressing data", dfe);
    } finally {
      deflateDecompressor.done();
    }
    predictorDecompressor.decompress(buf, bufOffset, dstOffset, srcHeight, srcWidth, bytesPerRow);

    if (bytesPerRow != scanlineStride) {
      if (DEBUG) {
        System.out.println("bytesPerRow != scanlineStride");
      }
      int off = 0;
      for (int y = 0; y < srcHeight; y++) {
        System.arraycopy(buf, off, b, dstOffset, bytesPerRow);
        off += bytesPerRow;
        dstOffset += scanlineStride;
      }
    }
  }
}
