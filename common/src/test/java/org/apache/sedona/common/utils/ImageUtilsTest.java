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
package org.apache.sedona.common.utils;

import org.junit.Assert;
import org.junit.Test;

import javax.media.jai.RasterFactory;
import java.awt.Point;
import java.awt.image.DataBuffer;
import java.awt.image.WritableRaster;

public class ImageUtilsTest {
    @Test
    public void testCopyRasterWithoutPadding() {
        double[] samples = {
                1, 2, 3, 4,
                5, 6, 7, 8,
                9, 0, 1, 2
        };
        WritableRaster source = RasterFactory.createBandedRaster(DataBuffer.TYPE_BYTE, 4, 3, 1, null);
        WritableRaster dest = RasterFactory.createBandedRaster(DataBuffer.TYPE_BYTE, 4, 3, 2, null);
        source.setSamples(0, 0, 4, 3, 0, samples);

        ImageUtils.copyRasterWithPadding(source, 0, dest, 1, 100);
        double[] actualSamples = dest.getSamples(0, 0, 4, 3, 1, (double[]) null);
        Assert.assertArrayEquals(samples, actualSamples, 0.0);
    }

    @Test
    public void testCopyRasterWithPadding() {
        double[] samples = {
                1, 2, 3, 4,
                5, 6, 7, 8,
                9, 0, 1, 2
        };
        WritableRaster source = RasterFactory.createBandedRaster(DataBuffer.TYPE_BYTE, 4, 3, 1, null);
        WritableRaster dest = RasterFactory.createBandedRaster(DataBuffer.TYPE_BYTE, 5, 5, 2, null);
        source.setSamples(0, 0, 4, 3, 0, samples);
        ImageUtils.copyRasterWithPadding(source, 0, dest, 1, 100);

        double[] expectedSamples = {
                1, 2, 3, 4, 100,
                5, 6, 7, 8, 100,
                9, 0, 1, 2, 100,
                100, 100, 100, 100, 100,
                100, 100, 100, 100, 100
        };
        double[] actualSamples = dest.getSamples(0, 0, 5, 5, 1, (double[]) null);
        Assert.assertArrayEquals(expectedSamples, actualSamples, 0.0);
    }

    @Test
    public void testCopyTranslatedRasterWithPadding() {
        double[] samples = {
                1, 2, 3, 4,
                5, 6, 7, 8,
                9, 0, 1, 2
        };
        Point sourceLocation = new Point(100, 120);
        WritableRaster source = RasterFactory.createBandedRaster(DataBuffer.TYPE_BYTE, 4, 3, 1, sourceLocation);
        Point destLocation = new Point(1000, 1100);
        WritableRaster dest = RasterFactory.createBandedRaster(DataBuffer.TYPE_BYTE, 5, 5, 2, destLocation);
        source.setSamples(100, 120, 4, 3, 0, samples);
        ImageUtils.copyRasterWithPadding(source, 0, dest, 1, 100);

        double[] expectedSamples = {
                1, 2, 3, 4, 100,
                5, 6, 7, 8, 100,
                9, 0, 1, 2, 100,
                100, 100, 100, 100, 100,
                100, 100, 100, 100, 100
        };
        double[] actualSamples = dest.getSamples(1000, 1100, 5, 5, 1, (double[]) null);
        Assert.assertArrayEquals(expectedSamples, actualSamples, 0.0);
    }

    @Test
    public void testCopyRasterWithInvalidDest() {
        WritableRaster source = RasterFactory.createBandedRaster(DataBuffer.TYPE_BYTE, 4, 3, 1, null);
        WritableRaster dest = RasterFactory.createBandedRaster(DataBuffer.TYPE_BYTE, 4, 2, 2, null);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ImageUtils.copyRasterWithPadding(source, 0, dest, 1, 100));
        WritableRaster dest2 = RasterFactory.createBandedRaster(DataBuffer.TYPE_BYTE, 3, 3, 2, null);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ImageUtils.copyRasterWithPadding(source, 0, dest2, 1, 100));
    }
}
