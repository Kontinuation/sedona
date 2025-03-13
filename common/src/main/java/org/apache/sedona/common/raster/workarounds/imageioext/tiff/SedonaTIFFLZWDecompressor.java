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
import java.io.IOException;
import javax.imageio.IIOException;

public class SedonaTIFFLZWDecompressor extends TIFFDecompressor {

  private static final boolean DEBUG = false;

  private static final int andTable[] = {511, 1023, 2047, 4095};

  int predictor;

  byte[] srcData;
  byte[] dstData;

  int srcIndex;
  int dstIndex;

  byte stringTable[][];
  int tableIndex, bitsToGet = 9;

  int nextData = 0;
  int nextBits = 0;

  public SedonaTIFFLZWDecompressor(int predictor) throws IIOException {
    super();

    if (predictor != BaselineTIFFTagSet.PREDICTOR_NONE
        && predictor != BaselineTIFFTagSet.PREDICTOR_HORIZONTAL_DIFFERENCING
        && predictor != BaselineTIFFTagSet.PREDICTOR_FLOATING_POINT) {
      throw new IIOException("Illegal value for Predictor in " + "TIFF file");
    }

    if (DEBUG) {
      System.out.println("Using horizontal differencing predictor");
    }

    this.predictor = predictor;
  }

  public void decodeRaw(byte[] b, int dstOffset, int bitsPerPixel, int scanlineStride)
      throws IOException {
    stream.seek(offset);

    byte[] sdata = new byte[byteCount];
    stream.readFully(sdata);

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

    int numBytesDecoded = decode(sdata, 0, buf, bufOffset, bytesPerRow);

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

  public int decode(byte[] sdata, int srcOffset, byte[] ddata, int dstOffset, int bytesPerRow)
      throws IOException {
    if (sdata[0] == (byte) 0x00 && sdata[1] == (byte) 0x01) {
      throw new IIOException("TIFF 5.0-style LZW compression is not supported!");
    }

    SedonaPredictorDecompressor predictorDecompressor =
        new SedonaPredictorDecompressor(
            predictor,
            bitsPerSample,
            sampleFormat,
            planar ? 1 : samplesPerPixel,
            stream.getByteOrder());
    predictorDecompressor.validate();

    this.srcData = sdata;
    this.dstData = ddata;

    this.srcIndex = srcOffset;
    this.dstIndex = dstOffset;

    this.nextData = 0;
    this.nextBits = 0;

    initializeStringTable();

    int code, oldCode = 0;
    byte[] string;

    while ((code = getNextCode()) != 257) {
      if (code == 256) {
        initializeStringTable();
        code = getNextCode();
        if (code == 257) {
          break;
        }

        writeString(stringTable[code]);
        oldCode = code;
      } else {
        if (code < tableIndex) {
          string = stringTable[code];

          writeString(string);
          addStringToTable(stringTable[oldCode], string[0]);
          oldCode = code;
        } else {
          string = stringTable[oldCode];
          string = composeString(string, string[0]);
          writeString(string);
          addStringToTable(string);
          oldCode = code;
        }
      }
    }

    predictorDecompressor.decompress(
        dstData, srcOffset, dstOffset, srcHeight, srcWidth, bytesPerRow);

    return dstIndex - dstOffset;
  }

  /** Initialize the string table. */
  public void initializeStringTable() {
    stringTable = new byte[4096][];

    for (int i = 0; i < 256; i++) {
      stringTable[i] = new byte[1];
      stringTable[i][0] = (byte) i;
    }

    tableIndex = 258;
    bitsToGet = 9;
  }

  /** Write out the string just uncompressed. */
  public void writeString(byte string[]) {
    if (dstIndex < dstData.length) {
      int maxIndex = Math.min(string.length, dstData.length - dstIndex);

      for (int i = 0; i < maxIndex; i++) {
        dstData[dstIndex++] = string[i];
      }
    }
  }

  /** Add a new string to the string table. */
  public void addStringToTable(byte oldString[], byte newString) {
    int length = oldString.length;
    byte string[] = new byte[length + 1];
    System.arraycopy(oldString, 0, string, 0, length);
    string[length] = newString;

    // Add this new String to the table
    stringTable[tableIndex++] = string;

    if (tableIndex == 511) {
      bitsToGet = 10;
    } else if (tableIndex == 1023) {
      bitsToGet = 11;
    } else if (tableIndex == 2047) {
      bitsToGet = 12;
    }
  }

  /** Add a new string to the string table. */
  public void addStringToTable(byte string[]) {
    // Add this new String to the table
    stringTable[tableIndex++] = string;

    if (tableIndex == 511) {
      bitsToGet = 10;
    } else if (tableIndex == 1023) {
      bitsToGet = 11;
    } else if (tableIndex == 2047) {
      bitsToGet = 12;
    }
  }

  /** Append <code>newString</code> to the end of <code>oldString</code>. */
  public byte[] composeString(byte oldString[], byte newString) {
    int length = oldString.length;
    byte string[] = new byte[length + 1];
    System.arraycopy(oldString, 0, string, 0, length);
    string[length] = newString;

    return string;
  }

  // Returns the next 9, 10, 11 or 12 bits
  public int getNextCode() {
    // Attempt to get the next code. The exception is caught to make
    // this robust to cases wherein the EndOfInformation code has been
    // omitted from a strip. Examples of such cases have been observed
    // in practice.

    try {
      nextData = (nextData << 8) | (srcData[srcIndex++] & 0xff);
      nextBits += 8;

      if (nextBits < bitsToGet) {
        nextData = (nextData << 8) | (srcData[srcIndex++] & 0xff);
        nextBits += 8;
      }

      int code = (nextData >> (nextBits - bitsToGet)) & andTable[bitsToGet - 9];
      nextBits -= bitsToGet;

      return code;
    } catch (ArrayIndexOutOfBoundsException e) {
      // Strip not terminated as expected: return EndOfInformation code.
      return 257;
    }
  }
}
