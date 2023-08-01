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
package org.apache.sedona.common.raster.inputstream;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import javax.imageio.stream.FileCacheImageInputStream;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageInputStreamImpl;

import org.apache.sedona.common.raster.inputstream.util.ByteRange;
import org.apache.sedona.common.raster.inputstream.util.ByteRangeSet;

/**
 * A wrapper of ImageInputStream objects that caches already read image data to a temporary file.
 * This is useful for speeding up access to image files stored on remote file systems, such as S3.
 * The difference between this class and {@link FileCacheImageInputStream} is that this class
 * wraps a seekable stream and handles seeks in a more efficient way. It will create files with
 * holes on file systems supporting such files.
 */
public class DiskCachedImageInputStream extends ImageInputStreamImpl {

    private final ImageInputStream stream;
    private final File cacheFile;
    private final RandomAccessFile cache;
    private final ByteRangeSet cachedRanges;
    private final int readAheadSize;

    // streamLength starts off to be the maximum value, and will shrink as we read from the stream
    // and hit EOF.
    private long streamLength;

    public DiskCachedImageInputStream(ImageInputStream stream, int readAheadSize, File cacheDir) throws IOException {
        if (stream == null) {
            throw new IllegalArgumentException("stream == null!");
        }
        if ((cacheDir != null) && !(cacheDir.isDirectory())) {
            throw new IllegalArgumentException("Not a directory!");
        }
        this.stream = stream;
        if (cacheDir == null) {
            this.cacheFile = Files.createTempFile("image-stream-cache-", ".tmp").toFile();
        } else {
            this.cacheFile = Files.createTempFile(cacheDir.toPath(), "image-stream-cache-", ".tmp")
                    .toFile();
        }
        this.cacheFile.deleteOnExit();
        this.cache = new RandomAccessFile(cacheFile, "rw");
        this.cachedRanges = new ByteRangeSet();
        this.readAheadSize = readAheadSize;
        this.streamLength = Long.MAX_VALUE;
    }

    public DiskCachedImageInputStream(ImageInputStream stream, int readAheadSize) throws IOException {
        this(stream, readAheadSize, null);
    }

    public ImageInputStream getStream() {
        return stream;
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            stream.close();
        } finally {
            try {
                cache.close();
            } finally {
                cacheFile.delete();
            }
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        checkClosed();
        stream.seek(pos);
        cache.seek(pos);
        super.seek(pos);
    }

    @Override
    public int read() throws IOException {
        byte[] buf = new byte[1];
        int ret_len = read(buf, 0, 1);
        if (ret_len < 0) {
            return ret_len;
        }
        return buf[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkClosed();
        bitOffset = 0;

        if (len < 0) {
            throw new IllegalArgumentException("len < 0");
        } else if (len == 0) {
            return 0;
        }
        len = (int) Math.min(len, streamLength - streamPos);
        if (len <= 0) {
            // Reading beyond the end of the stream
            return -1;
        }

        // Find the ranges that are not cached.
        ByteRange range = new ByteRange(streamPos, streamPos + len);
        List<ByteRange> overlappingRanges = cachedRanges.findOverlappingRanges(range, 3);
        List<ByteRange> missingRanges = resolveMissingRanges(range, overlappingRanges, readAheadSize);
        for (ByteRange missingRange : missingRanges) {
            // Some bytes are not cached. Read them from the underlying stream.
            stream.seek(missingRange.inclusiveStart);
            int missingLen = (int) (missingRange.exclusiveEnd - missingRange.inclusiveStart);
            byte[] buf = new byte[missingLen];
            int ret_len = stream.read(buf, 0, missingLen);
            if (ret_len <= 0) {
                // This range is beyond the end of the stream. No need to check the subsequent ranges
                // since the ranges are sorted.
                streamLength = Math.min(streamLength, missingRange.inclusiveStart);
                break;
            }
            if (ret_len < missingLen) {
                // This range is partially beyond the end of the stream. We should only cache the bytes
                // that are actually read. We'll do another read if there is a subsequent range to confirm
                // that EOF is definitely reached.
                missingRange.exclusiveEnd = missingRange.inclusiveStart + ret_len;
                streamLength = Math.min(streamLength, missingRange.exclusiveEnd);
            }

            cache.seek(missingRange.inclusiveStart);
            cache.write(buf, 0, ret_len);
            cachedRanges.addRange(missingRange);
            if (ret_len < missingLen) {
                break;
            }
        }

        // Now everything is cached
        if (!missingRanges.isEmpty()) {
            cache.seek(streamPos);
        }
        return readFromCache(b, off, len);
    }

    @Override
    public boolean isCached() {
        return true;
    }

    @Override
    public boolean isCachedFile() {
        return true;
    }

    private int readFromCache(byte[] b, int off, int len) throws IOException {
        int remaining = len;
        while (remaining > 0) {
            int ret_len = cache.read(b, off, remaining);
            if (ret_len < 0) {
                // Hit EOF, no more data to read.
                if (len - remaining > 0) {
                    return len - remaining;
                } else {
                    // We have not read any data, return EOF.
                    return ret_len;
                }
            }
            off += ret_len;
            remaining -= ret_len;
            streamPos += ret_len;
        }
        return len - remaining;
    }

    /**
     * Find the ranges that are not covered by the given ranges. This method is for finding which
     * parts of the read request cannot be satisfied by the disk cache file, so that we can read
     * those parts from the underlying stream and fill the cache.
     *
     * @param range         the range to subtract from
     * @param minusRanges   the ranges to subtract. This list must be sorted.
     * @param readAheadSize add additional size to the last range if the tail of the range is not covered
     * @return a list of ByteRange objects that are not covered by the given ranges. The list is
     * sorted
     */
    public static List<ByteRange> resolveMissingRanges(ByteRange range, List<ByteRange> minusRanges, int readAheadSize) {
        List<ByteRange> result = new ArrayList<>();
        long start = range.inclusiveStart;
        for (ByteRange minusRange : minusRanges) {
            if (minusRange.inclusiveStart > start) {
                result.add(new ByteRange(start, minusRange.inclusiveStart));
            }
            start = minusRange.exclusiveEnd;
        }
        if (start < range.exclusiveEnd) {
            long exclusiveEnd = Math.max(start + readAheadSize, range.exclusiveEnd);
            result.add(new ByteRange(start, exclusiveEnd));
        }
        return result;
    }
}
