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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.raster.outdb.OutDbGridCoverage2D;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.metadata.spatial.PixelOrientation;
import org.geotools.api.referencing.datum.PixelInCell;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.processing.operation.Crop;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.geotools.api.parameter.ParameterValueGroup;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.operation.TransformException;

import javax.media.jai.RasterFactory;
import java.awt.geom.Point2D;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Arrays;
import java.util.Collections;

public class RasterBandEditors {
    /**
     * Adds no-data value to the raster.
     * @param raster Source raster to add no-data value
     * @param bandIndex Band index to add no-data value
     * @param noDataValue Value to set as no-data value, if null then remove existing no-data value
     * @param replace if true replaces the previous no-data value with the specified no-data value
     * @return Raster with no-data value
     */
    public static GridCoverage2D setBandNoDataValue(GridCoverage2D raster, int bandIndex, Double noDataValue, boolean replace) {
        RasterUtils.ensureBand(raster, bandIndex);
        Double rasterNoData = RasterBandAccessors.getBandNoDataValue(raster, bandIndex);

        // Remove no-Data if it is null
        if (noDataValue == null) {
            if (RasterBandAccessors.getBandNoDataValue(raster) == null) {
                return raster;
            }
            GridSampleDimension[] sampleDimensions = raster.getSampleDimensions();
            sampleDimensions [bandIndex - 1] = RasterUtils.removeNoDataValue(sampleDimensions[bandIndex - 1]);
            return RasterUtils.clone(raster.getRenderedImage(), null, sampleDimensions, raster, null, true);
        }

        if ( rasterNoData != null && rasterNoData.equals(noDataValue)) {
            return raster;
        }
        GridSampleDimension[] bands = raster.getSampleDimensions();
        bands[bandIndex - 1] = RasterUtils.createSampleDimensionWithNoDataValue(bands[bandIndex - 1], noDataValue);

        if (replace) {
            if (rasterNoData == null) {
                throw new IllegalArgumentException("The raster provided doesn't have a no-data value. Please provide a raster that has a no-data value to use `replace` option.");
            }

            Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());
            int dataTypeCode = rasterData.getDataBuffer().getDataType();
            int numBands = RasterAccessors.numBands(raster);
            int height = RasterAccessors.getHeight(raster);
            int width = RasterAccessors.getWidth(raster);
            WritableRaster wr = RasterFactory.createBandedRaster(dataTypeCode, width, height, numBands, null);
            double[] bandData = rasterData.getSamples(0, 0, width, height, bandIndex - 1, (double[]) null);
            for (int i = 0; i < bandData.length; i++) {
                if (bandData[i] == rasterNoData) {
                    bandData[i] = noDataValue;
                }
            }
            wr.setSamples(0, 0, width, height, bandIndex - 1, bandData);
            return RasterUtils.clone(wr, null, bands, raster, null, true);
        }

        return RasterUtils.clone(raster.getRenderedImage(), null, bands, raster, null, true);
    }

    /**
     * Adds no-data value to the raster.
     * @param raster Source raster to add no-data value
     * @param bandIndex Band index to add no-data value
     * @param noDataValue Value to set as no-data value, if null then remove existing no-data value
     * @return Raster with no-data value
     */
    public static GridCoverage2D setBandNoDataValue(GridCoverage2D raster, int bandIndex, Double noDataValue) {
        return setBandNoDataValue(raster, bandIndex, noDataValue, false);
    }

    /**
     * Adds no-data value to the raster.
     * @param raster Source raster to add no-data value
     * @param noDataValue Value to set as no-data value, if null then remove existing no-data value
     * @return Raster with no-data value
     */
    public static GridCoverage2D setBandNoDataValue(GridCoverage2D raster, Double noDataValue) {
        return setBandNoDataValue(raster, 1, noDataValue, false);
    }

    /**
     * @param toRaster Raster to add the band
     * @param fromRaster Raster from the band will be copied
     * @param fromBand Index of Raster band that will be copied
     * @param toRasterIndex Index of Raster where the copied band will be changed
     * @return A raster with either replacing a band or adding a new band at the end the toRaster, with the specified band values extracted from the fromRaster at given band
     */
    public static GridCoverage2D addBand(GridCoverage2D toRaster, GridCoverage2D fromRaster, int fromBand, int toRasterIndex) {
        RasterUtils.ensureBand(fromRaster, fromBand);
        ensureBandAppend(toRaster, toRasterIndex);
        RasterUtils.isRasterSameShape(toRaster, fromRaster);

        int width = RasterAccessors.getWidth(toRaster), height = RasterAccessors.getHeight(toRaster);

        Raster rasterData = RasterUtils.getRaster(fromRaster.getRenderedImage());

        // get datatype of toRaster to preserve data type
        int dataTypeCode = RasterUtils.getRaster(toRaster.getRenderedImage()).getDataBuffer().getDataType();
        int numBands = RasterAccessors.numBands(toRaster);
        Double noDataValue = RasterBandAccessors.getBandNoDataValue(fromRaster, fromBand);

        if (RasterUtils.isDataTypeIntegral(dataTypeCode)) {
            int[] bandValues = rasterData.getSamples(0, 0, width, height, fromBand - 1, (int[]) null);
            if (numBands + 1 == toRasterIndex) {
                return RasterUtils.copyRasterAndAppendBand(toRaster, Arrays.stream(bandValues).boxed().toArray(Integer[]::new), noDataValue);
            } else {
                return RasterUtils.copyRasterAndReplaceBand(toRaster, fromBand, Arrays.stream(bandValues).boxed().toArray(Integer[]::new), noDataValue, false);
            }
        } else {
            double[] bandValues = rasterData.getSamples(0, 0, width, height, fromBand - 1, (double[]) null);
            if (numBands + 1 == toRasterIndex) {
                return RasterUtils.copyRasterAndAppendBand(toRaster, Arrays.stream(bandValues).boxed().toArray(Double[]::new), noDataValue);
            } else {
                return RasterUtils.copyRasterAndReplaceBand(toRaster, fromBand, Arrays.stream(bandValues).boxed().toArray(Double[]::new), noDataValue, false);
            }
        }
    }

    /**
     * The new band will be added to the end of the toRaster
     * @param toRaster Raster to add the band
     * @param fromRaster Raster from the band will be copied
     * @param fromBand Index of Raster band that will be copied
     * @return A raster with either replacing a band or adding a new band at the end the toRaster, with the specified band values extracted from the fromRaster at given band
     */
    public static GridCoverage2D addBand(GridCoverage2D toRaster, GridCoverage2D fromRaster, int fromBand) {
        int endBand = RasterAccessors.numBands(toRaster) + 1;
        return addBand(toRaster, fromRaster, fromBand, endBand);
    }

    /**
     * Index of fromRaster will be taken as 1 and will be copied at the end of the toRaster
     * @param toRaster Raster to add the band
     * @param fromRaster Raster from the band will be copied
     * @return A raster with either replacing a band or adding a new band at the end the toRaster, with the specified band values extracted from the fromRaster at given band
     */
    public static GridCoverage2D addBand(GridCoverage2D toRaster, GridCoverage2D fromRaster) {
        return addBand(toRaster, fromRaster, 1);
    }

    /**
     * Check if the band index is either present or at the end of the raster
     * @param raster Raster to check
     * @param band Band index to append to the raster
     */
    private static void ensureBandAppend(GridCoverage2D raster, int band) {
        if (band < 1 || band > RasterAccessors.numBands(raster) + 1) {
            throw new IllegalArgumentException(String.format("Provided band index %d is not present in the raster", band));
        }
    }

    /**
     * Return a clipped raster with the specified ROI by the geometry
     * @param raster Raster to clip
     * @param band Band number to perform clipping
     * @param geometry Specify ROI
     * @param noDataValue no-Data value for empty cells
     * @param crop Specifies to keep the original extent or not
     * @return A clip Raster with defined ROI by the geometry
     */
    public static GridCoverage2D clip(GridCoverage2D raster, int band, Geometry geometry, double noDataValue, boolean crop) throws FactoryException, TransformException {
        if (raster instanceof OutDbGridCoverage2D && crop) {
            // Test if no-data value is left unspecified
            boolean specifiedNoDataValue;
            boolean isDataTypeIntegral = RasterUtils.isDataTypeIntegral(RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, band)));
            if (isDataTypeIntegral) {
                specifiedNoDataValue = (Double.compare(noDataValue, Integer.MIN_VALUE) != 0);
            } else {
                specifiedNoDataValue = (Double.compare(noDataValue, Double.MIN_VALUE) != 0);
            }

            // Test if the geometry is in the same CRS as the raster, and it is a rectangle.
            int rasterSRID = RasterAccessors.srid(raster);
            if (rasterSRID == 0) {
                rasterSRID = 4326;
            }
            int geomSRID = Functions.getSRID(geometry);
            if (geomSRID == 0) {
                geomSRID = 4326;
            }
            boolean geometryIsRectangle = (geomSRID == rasterSRID && geometry.isRectangle());

            // Test if the raster has no skew.
            AffineTransform2D affine = RasterUtils.getAffineTransform(raster, PixelOrientation.CENTER);
            boolean isNoSkew = (affine.getShearX() == 0 && affine.getShearY() == 0);

            if (!specifiedNoDataValue && geometryIsRectangle && isNoSkew) {
                // We can create a clipped out-db raster, without even touching the pixel data.
                Envelope env = geometry.getEnvelopeInternal();
                double x0 = env.getMinX();
                double y0 = env.getMaxY();
                double regionWidth = env.getWidth();
                double regionHeight = env.getHeight();
                int[] bands = {band};
                return clipOutDb((OutDbGridCoverage2D) raster, bands, x0, y0, regionWidth, regionHeight);
            }
        }
        return clipInDB(raster, band, geometry, noDataValue, crop);
    }

    public static GridCoverage2D clipInDB(GridCoverage2D raster, int band, Geometry geometry, double noDataValue, boolean crop) throws FactoryException, TransformException {

        // Selecting the band from original raster
        RasterUtils.ensureBand(raster, band);
        GridCoverage2D singleBandRaster = RasterBandAccessors.getBand(raster, new int[]{band});

        Pair<GridCoverage2D, Geometry> pair = RasterUtils.setDefaultCRSAndTransform(singleBandRaster, geometry);
        singleBandRaster = pair.getLeft();
        geometry = pair.getRight();

        // Crop the raster
        // this will shrink the extent of the raster to the geometry
        Crop cropObject = new Crop();
        ParameterValueGroup parameters = cropObject.getParameters();
        parameters.parameter("Source").setValue(singleBandRaster);
        parameters.parameter(Crop.PARAMNAME_DEST_NODATA).setValue(new double[]{noDataValue});
        parameters.parameter(Crop.PARAMNAME_ROI).setValue(geometry);

        GridCoverage2D newRaster = (GridCoverage2D) cropObject.doOperation(parameters, null);

        if (!crop) {
            double[] metadataOriginal = RasterAccessors.metadata(raster);
            int widthOriginalRaster = (int) metadataOriginal[2], heightOriginalRaster = (int) metadataOriginal[3];
            Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());


            // create a new raster and set a default value that's the no-data value
            String bandType = RasterBandAccessors.getBandType(raster, 1);
            int dataTypeCode = RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, 1));
            boolean isDataTypeIntegral = RasterUtils.isDataTypeIntegral(dataTypeCode);
            WritableRaster resultRaster = RasterFactory.createBandedRaster(dataTypeCode, widthOriginalRaster, heightOriginalRaster, 1, null);
            int sizeOfArray = widthOriginalRaster * heightOriginalRaster;
            if (isDataTypeIntegral) {
                int[] array = ArrayUtils.toPrimitive(Collections.nCopies(sizeOfArray, (int) noDataValue).toArray(new Integer[sizeOfArray]));
                resultRaster.setSamples(0, 0, widthOriginalRaster, heightOriginalRaster, 0, array);
            } else {
                double[] array = ArrayUtils.toPrimitive(Collections.nCopies(sizeOfArray, noDataValue).toArray(new Double[sizeOfArray]));
                resultRaster.setSamples(0, 0, widthOriginalRaster, heightOriginalRaster, 0, array);
            }

            // rasterize the geometry to iterate over the clipped raster
            GridCoverage2D rasterized = RasterConstructors.asRaster(geometry, raster, bandType, 150);
            Raster rasterizedData = RasterUtils.getRaster(rasterized.getRenderedImage());
            double[] metadataRasterized = RasterAccessors.metadata(rasterized);
            int widthRasterized = (int) metadataRasterized[2], heightRasterized = (int) metadataRasterized[3];

            for (int j = 0; j < heightRasterized; j++) {
                for(int i = 0; i < widthRasterized; i++) {
                    Point2D point = RasterUtils.getWorldCornerCoordinates(rasterized, i, j);
                    int[] rasterCoord = RasterUtils.getGridCoordinatesFromWorld(raster, point.getX(), point.getY());
                    int x = Math.abs(rasterCoord[0]), y = Math.abs(rasterCoord[1]);

                    if (rasterizedData.getPixel(i, j, (int[]) null)[0] == 0) {
                        continue;
                    }

                    if (isDataTypeIntegral) {
                        int[] pixelValue = rasterData.getPixel(x, y, (int[]) null);

                        resultRaster.setPixel(x, y, new int[]{pixelValue[band - 1]});
                    } else {
                        double[] pixelValue = rasterData.getPixel(x, y, (double[]) null);

                        resultRaster.setPixel(x, y, new double[]{pixelValue[band - 1]});
                    }
                }
            }
            newRaster = RasterUtils.clone(resultRaster, raster.getGridGeometry(), newRaster.getSampleDimensions(), newRaster, noDataValue, true);
        } else {
            RenderedImage image = newRaster.getRenderedImage();
            int minX = image.getMinX();
            int minY = image.getMinY();
            if (minX != 0 || minY != 0) {
                newRaster = RasterUtils.shiftRasterToZeroOrigin(newRaster, noDataValue);
            } else {
                newRaster = RasterUtils.clone(newRaster.getRenderedImage(), newRaster.getGridGeometry(), newRaster.getSampleDimensions(), newRaster, noDataValue, true);
            }
        }

        return newRaster;
    }

    public static GridCoverage2D clipOutDb(OutDbGridCoverage2D raster, int[] bandIndices, double x0, double y0, double regionWidth, double regionHeight) {
        ReferencedEnvelope rasterEnvelope = raster.getEnvelope2D();
        ReferencedEnvelope clipEnvelope = ReferencedEnvelope.rect(x0, y0 - regionHeight, regionWidth, regionHeight, rasterEnvelope.getCoordinateReferenceSystem());
        if (!rasterEnvelope.intersects((Envelope) clipEnvelope)) {
            throw new IllegalArgumentException("The region to clip is outside the raster bounds");
        }

        int[] outDbBandIndices = raster.getOutDbBandIndices();
        int[] newBandIndices = new int[bandIndices.length];
        for (int i = 0; i < bandIndices.length; i++) {
            int bandIndex = bandIndices[i] - 1;
            newBandIndices[i] = outDbBandIndices[bandIndex];
        }

        RenderedImage renderedImage = raster.getRenderedImage();
        int imageWidth = renderedImage.getWidth();
        int imageHeight = renderedImage.getHeight();

        AffineTransform2D affine = RasterUtils.getAffineTransform(raster, PixelOrientation.CENTER);
        double scaleX = affine.getScaleX();
        double scaleY = affine.getScaleY();
        double ipX = affine.getTranslateX();
        double ipY = affine.getTranslateY();

        // Derive affine transformation and crop region of the cropped out-db raster
        int endpointX0 = (int) ((x0 - (ipX - 0.5 * scaleX)) / scaleX);
        int endpointX1 = (int) ((x0 + regionWidth - (ipX - 0.5 * scaleX)) / scaleX);
        int offsetX = Math.min(endpointX0, endpointX1);
        int endX = Math.max(endpointX0, endpointX1);
        offsetX = Math.min(Math.max(offsetX, 0), imageWidth - 1);
        endX = Math.min(Math.max(endX, 0), imageWidth - 1);
        double newIpX = ipX + offsetX * scaleX;

        int endpointY0 = (int) ((y0 - (ipY - 0.5 * scaleY)) / scaleY);
        int endpointY1 = (int) ((y0 - regionHeight - (ipY - 0.5 * scaleY)) / scaleY);
        int offsetY = Math.min(endpointY0, endpointY1);
        int endY = Math.max(endpointY0, endpointY1);
        offsetY = Math.min(Math.max(offsetY, 0), imageHeight - 1);
        endY = Math.min(Math.max(endY, 0), imageHeight - 1);
        double newIpY = ipY + offsetY * scaleY;

        // Derive the size of the cropped region in pixels
        int newImageWidth = endX - offsetX + 1;
        int newImageHeight = endY - offsetY + 1;

        // Construct the cropped out-db raster
        AffineTransform2D affineNew = new AffineTransform2D(scaleX, 0, 0, scaleY, newIpX, newIpY);
        GridGeometry2D gridGeometry2D = new GridGeometry2D(
                new GridEnvelope2D(0, 0, newImageWidth, newImageHeight),
                PixelInCell.CELL_CENTER,
                affineNew, raster.getCoordinateReferenceSystem(), null);
        GridSampleDimension[] sampleDimensions = new GridSampleDimension[newBandIndices.length];
        for (int k = 0; k < newBandIndices.length; k++) {
            int bandIndex = newBandIndices[k];
            sampleDimensions[k] = raster.getSampleDimension(bandIndex);
        }
        return OutDbGridCoverage2D.create(raster.getName(), gridGeometry2D,
                sampleDimensions, newBandIndices,
                raster.getOutDbPath(), raster.getSerializedConfiguration(), raster.getOutDbParams());
    }

    /**
     * Return a clipped raster with the specified ROI by the geometry.
     * @param raster Raster to clip
     * @param band Band number to perform clipping
     * @param geometry Specify ROI
     * @param noDataValue no-Data value for empty cells
     * @return A clip Raster with defined ROI by the geometry
     */
    public static GridCoverage2D clip(GridCoverage2D raster, int band, Geometry geometry, double noDataValue) throws FactoryException, TransformException {
        return clip(raster, band, geometry, noDataValue, true);
    }

    /**
     * Return a clipped raster with the specified ROI by the geometry. No-data value will be taken as the lowest possible value for the data type and crop will be `true`.
     * @param raster Raster to clip
     * @param band Band number to perform clipping
     * @param geometry Specify ROI
     * @return A clip Raster with defined ROI by the geometry
     */
    public static GridCoverage2D clip(GridCoverage2D raster, int band, Geometry geometry) throws FactoryException, TransformException {
        boolean isDataTypeIntegral = RasterUtils.isDataTypeIntegral(RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, band)));

        if (isDataTypeIntegral) {
            double noDataValue = Integer.MIN_VALUE;
            return clip(raster, band, geometry, noDataValue, true);
        } else {
            double noDataValue = Double.MIN_VALUE;
            return clip(raster, band, geometry, noDataValue, true);
        }
    }
}
