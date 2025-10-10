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

import java.awt.geom.Point2D;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Collections;
import javax.media.jai.RasterFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.raster.outdb.OutDbGridCoverage2D;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.metadata.spatial.PixelOrientation;
import org.geotools.api.parameter.ParameterValueGroup;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.datum.PixelInCell;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.processing.CannotCropException;
import org.geotools.coverage.processing.operation.Crop;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

public class RasterBandEditors {
  /**
   * Adds no-data value to the raster.
   *
   * @param raster Source raster to add no-data value
   * @param bandIndex Band index to add no-data value
   * @param noDataValue Value to set as no-data value, if null then remove existing no-data value
   * @param replace if true replaces the previous no-data value with the specified no-data value
   * @return Raster with no-data value
   */
  public static GridCoverage2D setBandNoDataValue(
      GridCoverage2D raster, int bandIndex, Double noDataValue, boolean replace) {
    RasterUtils.ensureBand(raster, bandIndex);
    Double rasterNoData = RasterBandAccessors.getBandNoDataValue(raster, bandIndex);

    // Remove no-Data if it is null
    if (noDataValue == null) {
      if (RasterBandAccessors.getBandNoDataValue(raster) == null) {
        return raster;
      }
      GridSampleDimension[] sampleDimensions = raster.getSampleDimensions();
      sampleDimensions[bandIndex - 1] =
          RasterUtils.removeNoDataValue(sampleDimensions[bandIndex - 1]);
      return RasterUtils.clone(
          raster.getRenderedImage(), null, sampleDimensions, raster, null, true);
    }

    if (rasterNoData != null && rasterNoData.equals(noDataValue)) {
      return raster;
    }
    GridSampleDimension[] bands = raster.getSampleDimensions();
    bands[bandIndex - 1] =
        RasterUtils.createSampleDimensionWithNoDataValue(bands[bandIndex - 1], noDataValue);

    if (replace) {
      if (rasterNoData == null) {
        throw new IllegalArgumentException(
            "The raster provided doesn't have a no-data value. Please provide a raster that has a no-data value to use `replace` option.");
      }

      Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());
      int dataTypeCode = rasterData.getDataBuffer().getDataType();
      int numBands = RasterAccessors.numBands(raster);
      int height = RasterAccessors.getHeight(raster);
      int width = RasterAccessors.getWidth(raster);
      WritableRaster wr =
          RasterFactory.createBandedRaster(dataTypeCode, width, height, numBands, null);
      double[] bandData =
          rasterData.getSamples(0, 0, width, height, bandIndex - 1, (double[]) null);
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
   *
   * @param raster Source raster to add no-data value
   * @param bandIndex Band index to add no-data value
   * @param noDataValue Value to set as no-data value, if null then remove existing no-data value
   * @return Raster with no-data value
   */
  public static GridCoverage2D setBandNoDataValue(
      GridCoverage2D raster, int bandIndex, Double noDataValue) {
    return setBandNoDataValue(raster, bandIndex, noDataValue, false);
  }

  /**
   * Adds no-data value to the raster.
   *
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
   * @return A raster with either replacing a band or adding a new band at the end the toRaster,
   *     with the specified band values extracted from the fromRaster at given band
   */
  public static GridCoverage2D addBand(
      GridCoverage2D toRaster, GridCoverage2D fromRaster, int fromBand, int toRasterIndex) {
    RasterUtils.ensureBand(fromRaster, fromBand);
    ensureBandAppend(toRaster, toRasterIndex);
    RasterUtils.isRasterSameShape(toRaster, fromRaster);

    int width = RasterAccessors.getWidth(toRaster), height = RasterAccessors.getHeight(toRaster);

    Raster rasterData = RasterUtils.getRaster(fromRaster.getRenderedImage());

    // get datatype of toRaster to preserve data type
    int dataTypeCode =
        RasterUtils.getRaster(toRaster.getRenderedImage()).getDataBuffer().getDataType();
    int numBands = RasterAccessors.numBands(toRaster);
    Double noDataValue = RasterBandAccessors.getBandNoDataValue(fromRaster, fromBand);

    if (RasterUtils.isDataTypeIntegral(dataTypeCode)) {
      int[] bandValues = rasterData.getSamples(0, 0, width, height, fromBand - 1, (int[]) null);
      if (numBands + 1 == toRasterIndex) {
        return RasterUtils.copyRasterAndAppendBand(toRaster, bandValues, noDataValue);
      } else {
        return RasterUtils.copyRasterAndReplaceBand(
            toRaster, fromBand, bandValues, noDataValue, false);
      }
    } else {
      double[] bandValues =
          rasterData.getSamples(0, 0, width, height, fromBand - 1, (double[]) null);
      if (numBands + 1 == toRasterIndex) {
        return RasterUtils.copyRasterAndAppendBand(toRaster, bandValues, noDataValue);
      } else {
        return RasterUtils.copyRasterAndReplaceBand(
            toRaster, fromBand, bandValues, noDataValue, false);
      }
    }
  }

  /**
   * The new band will be added to the end of the toRaster
   *
   * @param toRaster Raster to add the band
   * @param fromRaster Raster from the band will be copied
   * @param fromBand Index of Raster band that will be copied
   * @return A raster with either replacing a band or adding a new band at the end the toRaster,
   *     with the specified band values extracted from the fromRaster at given band
   */
  public static GridCoverage2D addBand(
      GridCoverage2D toRaster, GridCoverage2D fromRaster, int fromBand) {
    int endBand = RasterAccessors.numBands(toRaster) + 1;
    return addBand(toRaster, fromRaster, fromBand, endBand);
  }

  /**
   * Index of fromRaster will be taken as 1 and will be copied at the end of the toRaster
   *
   * @param toRaster Raster to add the band
   * @param fromRaster Raster from the band will be copied
   * @return A raster with either replacing a band or adding a new band at the end the toRaster,
   *     with the specified band values extracted from the fromRaster at given band
   */
  public static GridCoverage2D addBand(GridCoverage2D toRaster, GridCoverage2D fromRaster) {
    return addBand(toRaster, fromRaster, 1);
  }

  public static GridCoverage2D rasterUnion(
      GridCoverage2D raster1,
      GridCoverage2D raster2,
      GridCoverage2D raster3,
      GridCoverage2D raster4,
      GridCoverage2D raster5,
      GridCoverage2D raster6,
      GridCoverage2D raster7) {
    return rasterUnion(rasterUnion(raster1, raster2, raster3, raster4, raster5, raster6), raster7);
  }

  public static GridCoverage2D rasterUnion(
      GridCoverage2D raster1,
      GridCoverage2D raster2,
      GridCoverage2D raster3,
      GridCoverage2D raster4,
      GridCoverage2D raster5,
      GridCoverage2D raster6) {
    return rasterUnion(rasterUnion(raster1, raster2, raster3, raster4, raster5), raster6);
  }

  public static GridCoverage2D rasterUnion(
      GridCoverage2D raster1,
      GridCoverage2D raster2,
      GridCoverage2D raster3,
      GridCoverage2D raster4,
      GridCoverage2D raster5) {
    return rasterUnion(rasterUnion(raster1, raster2, raster3, raster4), raster5);
  }

  public static GridCoverage2D rasterUnion(
      GridCoverage2D raster1,
      GridCoverage2D raster2,
      GridCoverage2D raster3,
      GridCoverage2D raster4) {
    return rasterUnion(rasterUnion(raster1, raster2, raster3), raster4);
  }

  public static GridCoverage2D rasterUnion(
      GridCoverage2D raster1, GridCoverage2D raster2, GridCoverage2D raster3) {
    return rasterUnion(rasterUnion(raster1, raster2), raster3);
  }

  public static GridCoverage2D rasterUnion(GridCoverage2D raster1, GridCoverage2D raster2) {
    // Ensure both rasters have the same dimensions
    RasterUtils.isRasterSameShape(raster1, raster2);

    int numBands1 = RasterAccessors.numBands(raster1);
    int numBands2 = RasterAccessors.numBands(raster2);

    // Start with raster1 as the initial result
    GridCoverage2D result = raster1;

    // Append each band from raster2 to the result
    for (int bandIndex = 1; bandIndex <= numBands2; bandIndex++) {
      result = addBand(result, raster2, bandIndex);
    }

    return result;
  }

  /**
   * Check if the band index is either present or at the end of the raster
   *
   * @param raster Raster to check
   * @param band Band index to append to the raster
   */
  private static void ensureBandAppend(GridCoverage2D raster, int band) {
    if (band < 1 || band > RasterAccessors.numBands(raster) + 1) {
      throw new IllegalArgumentException(
          String.format("Provided band index %d is not present in the raster", band));
    }
  }

  /**
   * Return a clipped raster with the specified ROI by the geometry
   *
   * @param raster Raster to clip
   * @param band Band number to perform clipping
   * @param geometry Specify ROI
   * @param allTouched Include all pixels touched by roi geometry
   * @param noDataValue no-Data value for empty cells
   * @param crop Specifies to keep the original extent or not
   * @param lenient Return null if the raster and geometry do not intersect when set to true,
   *     otherwise will throw an exception
   * @return A clip Raster with defined ROI by the geometry
   */
  public static GridCoverage2D clip(
      GridCoverage2D raster,
      int band,
      Geometry geometry,
      boolean allTouched,
      double noDataValue,
      boolean crop,
      boolean lenient)
      throws FactoryException, TransformException {

    if (!RasterPredicates.rsIntersects(raster, geometry)) {
      if (lenient) {
        return null;
      }
      throw new IllegalArgumentException("Geometry does not intersect Raster.");
    }

    if (raster instanceof OutDbGridCoverage2D && crop) {
      // Test if no-data value is left unspecified
      boolean specifiedNoDataValue;
      boolean isDataTypeIntegral =
          RasterUtils.isDataTypeIntegral(
              RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, band)));
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

        int[] bands = {band};
        return clipOutDb((OutDbGridCoverage2D) raster, bands, env, lenient);
      }
    }
    return clipInDB(raster, band, geometry, allTouched, noDataValue, crop, lenient);
  }

  public static GridCoverage2D crop(
      GridCoverage2D raster, double noDataValue, Geometry geomExtent, boolean lenient) {
    // Crop the raster
    // this will shrink the extent of the raster to the geometry
    Crop cropObject = new Crop();
    ParameterValueGroup parameters = cropObject.getParameters();
    parameters.parameter("Source").setValue(raster);
    parameters.parameter(Crop.PARAMNAME_DEST_NODATA).setValue(new double[] {noDataValue});
    parameters.parameter(Crop.PARAMNAME_ROI).setValue(geomExtent);

    // crop the raster to the geometry extent
    try {
      raster = (GridCoverage2D) cropObject.doOperation(parameters, null);
    } catch (CannotCropException e) {
      if (lenient) {
        return null;
      } else {
        throw e;
      }
    }

    RenderedImage image = raster.getRenderedImage();
    int minX = image.getMinX();
    int minY = image.getMinY();
    if (minX != 0 || minY != 0) {
      raster = RasterUtils.shiftRasterToZeroOrigin(raster, noDataValue);
    } else {
      raster =
          RasterUtils.clone(
              raster.getRenderedImage(),
              raster.getGridGeometry(),
              raster.getSampleDimensions(),
              raster,
              noDataValue,
              true);
    }

    return raster;
  }

  public static GridCoverage2D clipInDB(
      GridCoverage2D raster,
      int band,
      Geometry geometry,
      boolean allTouched,
      double noDataValue,
      boolean crop,
      boolean lenient)
      throws FactoryException, TransformException {

    // Selecting the band from original raster
    RasterUtils.ensureBand(raster, band);
    Pair<GridCoverage2D, Geometry> pair = RasterUtils.setDefaultCRSAndTransform(raster, geometry);
    geometry = pair.getRight();

    double[] rasterMetadata = RasterAccessors.metadata(raster);
    int rasterWidth = (int) rasterMetadata[2], rasterHeight = (int) rasterMetadata[3];
    Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());

    // create a new raster and set a default value that's the no-data value
    String bandType = RasterBandAccessors.getBandType(raster, band);
    int dataTypeCode = RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, band));
    boolean isDataTypeIntegral = RasterUtils.isDataTypeIntegral(dataTypeCode);
    WritableRaster writableRaster =
        RasterFactory.createBandedRaster(dataTypeCode, rasterWidth, rasterHeight, 1, null);
    int sizeOfArray = rasterWidth * rasterHeight;
    if (isDataTypeIntegral) {
      int[] array =
          ArrayUtils.toPrimitive(
              Collections.nCopies(sizeOfArray, (int) noDataValue)
                  .toArray(new Integer[sizeOfArray]));
      writableRaster.setSamples(0, 0, rasterWidth, rasterHeight, 0, array);
    } else {
      double[] array =
          ArrayUtils.toPrimitive(
              Collections.nCopies(sizeOfArray, noDataValue).toArray(new Double[sizeOfArray]));
      writableRaster.setSamples(0, 0, rasterWidth, rasterHeight, 0, array);
    }

    // rasterize the geometry to iterate over the clipped raster
    GridCoverage2D maskRaster =
        RasterConstructors.asRaster(geometry, raster, bandType, allTouched, 150);
    Raster maskData = RasterUtils.getRaster(maskRaster.getRenderedImage());
    double[] maskMetadata = RasterAccessors.metadata(maskRaster);
    int maskWidth = (int) maskMetadata[2], maskHeight = (int) maskMetadata[3];

    // Calculate offset
    Point2D point = RasterUtils.getWorldCornerCoordinates(maskRaster, 1, 1);

    // Add half the pixel length to account for floating-point precision issues.
    // This adjustment increases the margin of error because the upper-left corner of a pixel
    // is very close to neighboring pixels. Without this adjustment, floating-point inaccuracies
    // can cause the calculated world coordinates to incorrectly fall into an adjacent pixel.
    // For example, the upperLeftY of a maskRaster pixel might be 243924.000000000001,
    // while the upperLeftY of the original raster pixel is 243924.000000000000.
    int[] rasterCoord =
        RasterUtils.getGridCoordinatesFromWorld(
            raster, point.getX() + (rasterMetadata[4] / 2), point.getY() + (rasterMetadata[5] / 2));
    double offsetX = rasterCoord[0];
    double offsetY = rasterCoord[1];

    for (int j = 0; j < maskHeight; j++) {
      for (int i = 0; i < maskWidth; i++) {
        // Calculate mapped raster index
        int x = (int) (i + offsetX), y = (int) (j + offsetY);

        // Check if the pixel in the maskRaster data is valid
        if (maskData.getPixel(i, j, (int[]) null)[0] == 0) {
          continue;
        }

        if (isDataTypeIntegral) {
          int[] pixelValue = rasterData.getPixel(x, y, (int[]) null);
          writableRaster.setPixel(x, y, new int[] {pixelValue[band - 1]});
        } else {
          double[] pixelValue = rasterData.getPixel(x, y, (double[]) null);
          writableRaster.setPixel(x, y, new double[] {pixelValue[band - 1]});
        }
      }
    }

    // Not cropped but clipped
    GridCoverage2D newRaster =
        RasterUtils.clone(
            writableRaster,
            raster.getGridGeometry(),
            new GridSampleDimension[] {raster.getSampleDimension(band - 1)},
            raster,
            noDataValue,
            true);

    if (crop) {
      Geometry maskRasterExtent = GeometryFunctions.envelope(maskRaster);
      return crop(newRaster, noDataValue, maskRasterExtent, lenient);
    }

    return newRaster;
  }

  public static GridCoverage2D clipOutDb(
      OutDbGridCoverage2D raster, int[] bandIndices, Envelope roi, boolean lenient) {
    ReferencedEnvelope rasterEnvelope = raster.getEnvelope2D();
    if (!rasterEnvelope.intersects(roi)) {
      if (lenient) {
        return null;
      } else {
        throw new IllegalArgumentException("The region to clip is outside the raster bounds");
      }
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
    double x0 = roi.getMinX();
    double y0 = roi.getMaxY();
    double regionWidth = roi.getWidth();
    double regionHeight = roi.getHeight();

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
    GridGeometry2D gridGeometry2D =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, newImageWidth, newImageHeight),
            PixelInCell.CELL_CENTER,
            affineNew,
            raster.getCoordinateReferenceSystem(),
            null);
    GridSampleDimension[] sampleDimensions = new GridSampleDimension[newBandIndices.length];
    for (int k = 0; k < newBandIndices.length; k++) {
      int bandIndex = newBandIndices[k];
      sampleDimensions[k] = raster.getSampleDimension(bandIndex);
    }
    return OutDbGridCoverage2D.create(
        raster.getName(),
        gridGeometry2D,
        sampleDimensions,
        newBandIndices,
        raster.getOutDbPath(),
        raster.getSerializedConfiguration(),
        raster.getOutDbParams());
  }

  /**
   * Return a clipped raster with the specified ROI by the geometry
   *
   * @param raster Raster to clip
   * @param band Band number to perform clipping
   * @param geometry Specify ROI
   * @param allTouched Include all pixels touched by roi geometry
   * @param noDataValue no-Data value for empty cells
   * @param crop Specifies to keep the original extent or not
   * @return A clip Raster with defined ROI by the geometry
   */
  public static GridCoverage2D clip(
      GridCoverage2D raster,
      int band,
      Geometry geometry,
      boolean allTouched,
      double noDataValue,
      boolean crop)
      throws FactoryException, TransformException {
    return clip(raster, band, geometry, allTouched, noDataValue, crop, true);
  }

  /**
   * Return a clipped raster with the specified ROI by the geometry.
   *
   * @param raster Raster to clip
   * @param band Band number to perform clipping
   * @param geometry Specify ROI
   * @param allTouched Include all pixels touched by roi geometry
   * @param noDataValue no-Data value for empty cells
   * @return A clip Raster with defined ROI by the geometry
   */
  public static GridCoverage2D clip(
      GridCoverage2D raster, int band, Geometry geometry, boolean allTouched, double noDataValue)
      throws FactoryException, TransformException {
    return clip(raster, band, geometry, allTouched, noDataValue, true);
  }

  /**
   * Return a clipped raster with the specified ROI by the geometry. No-data value will be taken as
   * the lowest possible value for the data type and crop will be `true`.
   *
   * @param raster Raster to clip
   * @param band Band number to perform clipping
   * @param geometry Specify ROI
   * @param allTouched Include all pixels touched by roi geometry
   * @return A clip Raster with defined ROI by the geometry
   */
  public static GridCoverage2D clip(
      GridCoverage2D raster, int band, Geometry geometry, boolean allTouched)
      throws FactoryException, TransformException {
    boolean isDataTypeIntegral =
        RasterUtils.isDataTypeIntegral(
            RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, band)));

    double noDataValue;
    if (isDataTypeIntegral) {
      noDataValue = Integer.MIN_VALUE;
    } else {
      noDataValue = Double.MIN_VALUE;
    }
    return clip(raster, band, geometry, allTouched, noDataValue, true);
  }

  /**
   * Return a clipped raster with the specified ROI by the geometry. No-data value will be taken as
   * the lowest possible value for the data type and crop will be `true`.
   *
   * @param raster Raster to clip
   * @param band Band number to perform clipping
   * @param geometry Specify ROI
   * @return A clip Raster with defined ROI by the geometry
   */
  public static GridCoverage2D clip(GridCoverage2D raster, int band, Geometry geometry)
      throws FactoryException, TransformException {
    return clip(raster, band, geometry, false);
  }
}
