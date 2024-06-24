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
package org.apache.sedona.common.raster.inputstream.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * A buffered drop-in replacement for java.io.RandomAccessFile. Instances of this class realise
 * substantial speed increases over java.io.RandomAccessFile through the use of buffering. This is a
 * subclass of Object, as it was not possible to subclass java.io.RandomAccessFile because many of
 * the methods are final.
 *
 * <p>The implementation of this class is inspired by and simplified from the
 * EnhancedRandomAccessFile in imageio-ext developed by GeoSolutions.
 */
public class BufferedRandomAccessFile implements AutoCloseable {

  /** The default buffer size, in bytes. */
  public static final int DEFAULT_BUFFER_SIZE = 4096;

  /** The underlying java.io.RandomAccessFile. */
  protected RandomAccessFile raf;

  /** The offset in bytes from the raf start, of the next read or write operation. */
  protected long filePosition;

  /** The buffer used to load the data. */
  protected byte[] buffer;

  /** The offset in bytes of the start of the buffer, from the start of the raf. */
  protected long bufferStart;

  /**
   * The offset in bytes of the end of the data in the buffer, from the start of the raf. This can
   * be calculated from <code>bufferStart + dataSize</code>, but it is cached to speed up the read(
   * ) method.
   */
  protected long dataEnd;

  /**
   * The size of the data stored in the buffer, in bytes. This may be less than the size of the
   * buffer.
   */
  protected int dataSize;

  /**
   * Constructor, default buffer size.
   *
   * @param file file of the raf
   * @param mode same as for java.io.RandomAccessFile
   */
  public BufferedRandomAccessFile(File file, String mode) throws IOException {
    this(file, mode, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Constructor.
   *
   * @param file file of the raf
   * @param mode same as for java.io.RandomAccessFile
   * @param bufferSize size of buffer to use.
   */
  public BufferedRandomAccessFile(File file, String mode, int bufferSize) throws IOException {
    this.raf = new java.io.RandomAccessFile(file, mode);
    filePosition = 0;
    buffer = new byte[bufferSize];
    invalidateBuffer();
  }

  /**
   * Close the raf, and release any associated system resources.
   *
   * @exception IOException if an I/O error occurs.
   */
  public void close() throws IOException {
    if (raf == null) {
      return;
    }

    // Close the underlying raf object.
    raf.close();
    raf = null;
    buffer = null;
    invalidateBuffer();
  }

  /**
   * Set the position in the raf for the next read or write.
   *
   * @param pos the offset (in bytes) from the start of the raf.
   */
  public void seek(long pos) {
    // If the seek is into the buffer, just update the raf pointer.
    filePosition = pos;
    if ((pos < bufferStart) || (pos >= dataEnd)) {
      // Invalidate the buffer
      bufferStart = 0;
      dataEnd = 0;
      dataSize = 0;
    }
  }

  /**
   * Read up to <code>len</code> bytes into an array, at a specified offset. This will block until
   * at least one byte has been read.
   *
   * @param b the byte array to receive the bytes.
   * @param off the offset in the array where copying will start.
   * @param len the number of bytes to copy.
   * @return the actual number of bytes read, or -1 if there is not more data due to the end of the
   *     raf being reached.
   * @exception IOException if an I/O error occurs.
   */
  public int read(byte[] b, int off, int len) throws IOException {
    if (len < 0) {
      throw new IllegalArgumentException("len < 0");
    } else if (len == 0) {
      return 0;
    }

    // Copy as much as we can from the buffer.
    int copyLength = 0;
    int remaining = len;
    if (filePosition >= bufferStart && filePosition < dataEnd) {
      int bytesAvailable = (int) (dataEnd - filePosition);
      copyLength = Math.min(bytesAvailable, len);
      System.arraycopy(buffer, (int) (filePosition - bufferStart), b, off, copyLength);
      filePosition += copyLength;
      remaining = len - copyLength;
      if (remaining == 0) {
        return copyLength;
      }
    }

    // There is more to copy. If the amount remaining is more than a buffer's length, read it
    // directly from the raf.
    int appendLength = 0;
    if (remaining > buffer.length) {
      appendLength = read_(filePosition, b, off + copyLength, remaining);
    } else {
      // Refill the buffer and copy from it.
      bufferStart = filePosition;
      int ret = read_(bufferStart, buffer, 0, buffer.length);
      if (ret >= 0) {
        dataEnd = bufferStart + ret;
        dataSize = ret;
      } else {
        dataEnd = bufferStart;
        dataSize = 0;
        return copyLength == 0 ? -1 : copyLength;
      }
      appendLength = Math.min(dataSize, remaining);
      System.arraycopy(buffer, 0, b, off + copyLength, appendLength);
    }

    filePosition += appendLength;
    return copyLength + appendLength;
  }

  protected int read_(long pos, byte[] b, int offset, int len) throws IOException {
    raf.seek(pos);
    return raf.read(b, offset, len);
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array starting at offset <code>off</code>
   * to this raf.
   *
   * @param b the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @exception IOException if an I/O error occurs.
   */
  public void write(byte b[], int off, int len) throws IOException {
    raf.seek(filePosition);
    raf.write(b, off, len);
    filePosition = raf.getFilePointer();
    invalidateBuffer();
  }

  private void invalidateBuffer() {
    bufferStart = 0;
    dataEnd = 0;
    dataSize = 0;
  }
}
