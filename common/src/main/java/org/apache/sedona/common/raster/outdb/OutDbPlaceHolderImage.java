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
package org.apache.sedona.common.raster.outdb;

import java.awt.*;
import java.awt.image.BandedSampleModel;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.util.Vector;

/**
 * This image object works as a placeholder in OutDbGridCoverage2D. OutDbGridCoverage2D supports loading the referenced
 * raster image file in a deferred manner: it may not load the image file on object construction, and user can happily
 * retrieve geo-referencing information from the object. When user retrieves pixel data from the object, or calls
 * getRenderedImage(), OutDbGridCoverage2D will load the actual raster image file.
 *
 * <p>Before the actual raster image is loaded, OutDbGridCoverage2D will use this placeholder object to represent the
 * image. This placeholder object contains the width, height, sample model and color model of the image, so that the
 * OutGridCoverage2D object will be successfully constructed by calling the constructor of the parent (GridCoverage2D)
 * class. The placeholder object will be replaced by the actual raster image object when the actual raster image file is
 * loaded.
 */
public class OutDbPlaceHolderImage implements RenderedImage {
    private final int width;
    private final int height;
    private final int tileWidth;
    private final int tileHeight;
    private final int numXTiles;
    private final int numYTiles;
    private final SampleModel sampleModel;
    private final ColorModel colorModel;

    public OutDbPlaceHolderImage(int width, int height, int numBand, int dataType) {
        this.width = width;
        this.height = height;

        // The placeholder image is tiled. We have to make sure that the tile size is small, otherwise
        // there will be an exception when constructing the sample model object.
        this.tileWidth = Math.min(256, width);
        this.tileHeight = Math.min(256, height);
        this.numXTiles = (width + tileWidth - 1) / tileWidth;
        this.numYTiles = (height + tileHeight - 1) / tileHeight;

        // It doesn't matter what sample model we are using here, as long as it gives us the correct values for
        // the number of bands then we'll make the constructor of GridCoverage2D happy.
        this.sampleModel = new BandedSampleModel(dataType, this.tileWidth, this.tileHeight, numBand);
        this.colorModel = null;
    }

    @Override
    public Vector<RenderedImage> getSources() {
        return new Vector<>(0);
    }

    @Override
    public Object getProperty(String name) {
        return null;
    }

    @Override
    public String[] getPropertyNames() {
        return new String[0];
    }

    @Override
    public ColorModel getColorModel() {
        return colorModel;
    }

    @Override
    public SampleModel getSampleModel() {
        return sampleModel;
    }

    @Override
    public int getWidth() {
        return width;
    }

    @Override
    public int getHeight() {
        return height;
    }

    @Override
    public int getMinX() {
        return 0;
    }

    @Override
    public int getMinY() {
        return 0;
    }

    @Override
    public int getNumXTiles() {
        return numXTiles;
    }

    @Override
    public int getNumYTiles() {
        return numYTiles;
    }

    @Override
    public int getMinTileX() {
        return 0;
    }

    @Override
    public int getMinTileY() {
        return 0;
    }

    @Override
    public int getTileWidth() {
        return tileWidth;
    }

    @Override
    public int getTileHeight() {
        return tileHeight;
    }

    @Override
    public int getTileGridXOffset() {
        return 0;
    }

    @Override
    public int getTileGridYOffset() {
        return 0;
    }

    @Override
    public Raster getTile(int tileX, int tileY) {
        throw new UnsupportedOperationException("getTile is not supported in OutDbPlaceHolderImage");
    }

    @Override
    public Raster getData() {
        throw new UnsupportedOperationException("getData is not supported in OutDbPlaceHolderImage");
    }

    @Override
    public Raster getData(Rectangle rect) {
        throw new UnsupportedOperationException("getData is not supported in OutDbPlaceHolderImage");
    }

    @Override
    public WritableRaster copyData(WritableRaster raster) {
        throw new UnsupportedOperationException("copyData is not supported in OutDbPlaceHolderImage");
    }
}
