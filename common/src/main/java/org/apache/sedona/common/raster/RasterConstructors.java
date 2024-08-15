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

import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.media.jai.RasterFactory;
import org.apache.hadoop.fs.Path;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.raster.TileGenerator.Tile;
import org.apache.sedona.common.raster.TileGenerator.TileIterator;
import org.apache.sedona.common.raster.inputstream.ByteArrayImageInputStream;
import org.apache.sedona.common.raster.netcdf.NetCdfReader;
import org.apache.sedona.common.raster.outdb.LazyLoadOutDbGridCoverage2D;
import org.apache.sedona.common.raster.outdb.OutDbGridCoverage2D;
import org.apache.sedona.common.raster.outdb.OutDbResourcePool;
import org.apache.sedona.common.raster.workarounds.RuntimePatches;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.gce.arcgrid.ArcGridReader;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.vector.VectorToRasterProcess;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

public class RasterConstructors {

  public static GridCoverage2D fromArcInfoAsciiGrid(byte[] bytes) throws IOException {
    ArcGridReader reader =
        new ArcGridReader(
            new ByteArrayImageInputStream(bytes),
            new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
    return reader.read(null);
  }

  public static GridCoverage2D fromGeoTiff(byte[] bytes) throws IOException {
    return fromGeoTiff(bytes, true);
  }

  public static GridCoverage2D fromGeoTiff(byte[] bytes, boolean autoRescale) throws IOException {
    AbstractGridCoverage2DReader geoTiffReader =
        RuntimePatches.createGeoTiffReader(
            new ByteArrayImageInputStream(bytes),
            new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
    ParameterValue<Boolean> rescalePixels = AbstractGridFormat.RESCALE_PIXELS.createValue();
    rescalePixels.setValue(autoRescale);
    GeneralParameterValue[] parameters = {rescalePixels};
    return geoTiffReader.read(parameters);
  }

  public static GridCoverage2D fromPath(
      String path, byte[] serializedConf, Map<String, String> params, boolean eagerLoadMetadata)
      throws IOException {
    OutDbResourcePool.ResourceKey resourceKey =
        new OutDbResourcePool.ResourceKey(new Path(path), serializedConf, params);
    if (eagerLoadMetadata) {
      return OutDbGridCoverage2D.create("outDbCoverage", resourceKey);
    } else {
      return new LazyLoadOutDbGridCoverage2D("outDbCoverage", resourceKey);
    }
  }

  public static GridCoverage2D fromNetCDF(
      byte[] bytes, String variableName, String lonDimensionName, String latDimensionName)
      throws IOException, FactoryException {
    NetcdfFile netcdfFile = openNetCdfBytes(bytes);
    return NetCdfReader.getRaster(netcdfFile, variableName, latDimensionName, lonDimensionName);
  }

  public static GridCoverage2D fromNetCDF(byte[] bytes, String recordVariableName)
      throws IOException, FactoryException {
    NetcdfFile netcdfFile = openNetCdfBytes(bytes);
    return NetCdfReader.getRaster(netcdfFile, recordVariableName);
  }

  public static String getRecordInfo(byte[] bytes) throws IOException {
    NetcdfFile netcdfFile = openNetCdfBytes(bytes);
    return NetCdfReader.getRecordInfo(netcdfFile);
  }

  private static NetcdfFile openNetCdfBytes(byte[] bytes) throws IOException {
    return NetcdfFiles.openInMemory("", bytes);
  }

  /**
   * Returns a raster that is converted from the geometry provided.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster
   * @param value The value of the pixel of the resultant raster
   * @param noDataValue The noDataValue of the resultant raster
   * @param useGeometryExtent The way to generate extent of the resultant raster. Use the extent of
   *     the geometry to convert if true, else use the extent of the reference raster
   * @return Rasterized Geometry
   * @throws FactoryException
   */
  public static GridCoverage2D asRaster(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      double value,
      Double noDataValue,
      boolean useGeometryExtent)
      throws FactoryException {
    List<Object> objects =
        rasterization(geom, raster, pixelType, value, noDataValue, useGeometryExtent);
    WritableRaster writableRaster = (WritableRaster) objects.get(0);
    GridCoverage2D rasterized = (GridCoverage2D) objects.get(1);

    return RasterUtils.clone(
        writableRaster,
        rasterized.getSampleDimensions(),
        rasterized,
        noDataValue,
        false); // no need to original raster metadata since this is a new raster.
  }

  /**
   * Returns a raster that is converted from the geometry provided. A convenience function for
   * asRaster.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster
   * @param value The value of the pixel of the resultant raster
   * @param noDataValue The noDataValue of the resultant raster
   * @return Rasterized Geometry
   * @throws FactoryException
   */
  public static GridCoverage2D asRaster(
      Geometry geom, GridCoverage2D raster, String pixelType, double value, Double noDataValue)
      throws FactoryException {
    return asRaster(geom, raster, pixelType, value, noDataValue, true);
  }

  /**
   * Returns a raster that is converted from the geometry provided. A convenience function for
   * asRaster.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster.
   * @return Rasterized Geometry
   * @throws FactoryException
   */
  public static GridCoverage2D asRaster(Geometry geom, GridCoverage2D raster, String pixelType)
      throws FactoryException {
    return asRaster(geom, raster, pixelType, 1, null);
  }

  /**
   * Returns a raster that is converted from the geometry provided. A convenience function for
   * asRaster.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster.
   * @param value The value of the pixel of the resultant raster
   * @return Rasterized Geometry
   * @throws FactoryException
   */
  public static GridCoverage2D asRaster(
      Geometry geom, GridCoverage2D raster, String pixelType, double value)
      throws FactoryException {
    return asRaster(geom, raster, pixelType, value, null);
  }

  private static List<Object> rasterization(
      Geometry geom,
      GridCoverage2D raster,
      String pixelType,
      double value,
      Double noDataValue,
      boolean useGeometryExtent)
      throws FactoryException {
    DefaultFeatureCollection featureCollection =
        getFeatureCollection(geom, raster.getCoordinateReferenceSystem());

    double[] metadata = RasterAccessors.metadata(raster);
    // The current implementation doesn't support rasters with properties below
    // It is not a problem as most rasters don't have these properties
    // ScaleX < 0
    if (metadata[4] < 0) {
      throw new IllegalArgumentException(
          String.format("ScaleX %f of the raster is negative, it should be positive", metadata[4]));
    }
    // ScaleY > 0
    if (metadata[5] > 0) {
      throw new IllegalArgumentException(
          String.format(
              "ScaleY %f of the raster is positive. It should be negative.", metadata[5]));
    }
    // SkewX should be zero
    if (metadata[6] != 0) {
      throw new IllegalArgumentException(
          String.format("SkewX %d of the raster is not zero.", metadata[6]));
    }
    // SkewY should be zero
    if (metadata[7] != 0) {
      throw new IllegalArgumentException(
          String.format("SkewY %d of the raster is not zero.", metadata[7]));
    }

    Envelope2D bound = null;

    int width, height;
    if (useGeometryExtent) {
      bound =
          JTS.getEnvelope2D(geom.getEnvelopeInternal(), raster.getCoordinateReferenceSystem2D());
      double scaleX = Math.abs(metadata[4]), scaleY = Math.abs(metadata[5]);
      width = Math.max((int) Math.ceil(bound.getWidth() / scaleX), 1);
      height = Math.max((int) Math.ceil(bound.getHeight() / scaleY), 1);
      bound =
          new Envelope2D(
              bound.getCoordinateReferenceSystem(),
              bound.getMinX(),
              bound.getMinY(),
              width * scaleX,
              height * scaleY);
    } else {
      ReferencedEnvelope envelope =
          ReferencedEnvelope.create(raster.getEnvelope(), raster.getCoordinateReferenceSystem());
      bound = JTS.getEnvelope2D(envelope, raster.getCoordinateReferenceSystem2D());
      GridEnvelope2D gridRange = raster.getGridGeometry().getGridRange2D();
      width = gridRange.width;
      height = gridRange.height;
    }

    VectorToRasterProcess rasterProcess = new VectorToRasterProcess();
    GridCoverage2D rasterized =
        rasterProcess.execute(
            featureCollection, width, height, "value", Double.toString(value), bound, null);
    if (noDataValue != null) {
      rasterized = RasterBandEditors.setBandNoDataValue(rasterized, 1, noDataValue);
    }
    WritableRaster writableRaster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(pixelType), width, height, 1, null);
    double[] samples =
        RasterUtils.getRaster(rasterized.getRenderedImage())
            .getSamples(0, 0, width, height, 0, (double[]) null);
    writableRaster.setSamples(0, 0, width, height, 0, samples);

    List<Object> objects = new ArrayList<>();
    objects.add(writableRaster);
    objects.add(rasterized);

    return objects;
  }

  /**
   * For internal use only! Returns a raster that is converted from the geometry provided with the
   * extent of the reference raster.
   *
   * @param geom The geometry to convert
   * @param raster The reference raster
   * @param pixelType The data type of pixel/cell of resultant raster
   * @param value The value of the pixel of the resultant raster
   * @param noDataValue The noDataValue of the resultant raster
   * @return Rasterized Geometry with reference raster's extent
   * @throws FactoryException
   */
  public static GridCoverage2D asRasterWithRasterExtent(
      Geometry geom, GridCoverage2D raster, String pixelType, double value, Double noDataValue)
      throws FactoryException {
    return asRaster(geom, raster, pixelType, value, noDataValue, false);
  }

  public static DefaultFeatureCollection getFeatureCollection(
      Geometry geom, CoordinateReferenceSystem crs) {
    SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
    simpleFeatureTypeBuilder.setName("Raster");
    simpleFeatureTypeBuilder.setCRS(crs);
    simpleFeatureTypeBuilder.add("geometry", Geometry.class);

    SimpleFeatureType featureType = simpleFeatureTypeBuilder.buildFeatureType();
    SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
    featureBuilder.add(geom);
    SimpleFeature simpleFeature = featureBuilder.buildFeature("1");
    DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
    featureCollection.add(simpleFeature);

    return featureCollection;
  }

  /**
   * Convenience function setting DOUBLE as datatype for the bands Create a new empty raster with
   * the given number of empty bands. The bounding envelope is defined by the upper left corner and
   * the scale. The math formula of the envelope is: minX = upperLeftX = lowerLeftX, minY
   * (lowerLeftY) = upperLeftY - height * pixelSize
   *
   * <ul>
   *   <li>The raster is defined by the width and height
   *   <li>The upper left corner is defined by the upperLeftX and upperLeftY
   *   <li>The scale is defined by pixelSize. The scaleX is equal to pixelSize and scaleY is equal
   *       to -pixelSize
   *   <li>skewX and skewY are zero, which means no shear or rotation.
   *   <li>SRID is default to 0 which means the default CRS (Generic 2D)
   * </ul>
   *
   * @param numBand the number of bands
   * @param widthInPixel the width of the raster, in pixel
   * @param heightInPixel the height of the raster, in pixel
   * @param upperLeftX the upper left corner of the raster. Note that: the minX of the envelope is
   *     equal to the upperLeftX
   * @param upperLeftY the upper left corner of the raster. Note that: the minY of the envelope is
   *     equal to the upperLeftY - height * pixelSize
   * @param pixelSize the size of the pixel in the unit of the CRS
   * @return the new empty raster
   */
  public static GridCoverage2D makeEmptyRaster(
      int numBand,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double pixelSize)
      throws FactoryException {
    return makeEmptyRaster(
        numBand,
        widthInPixel,
        heightInPixel,
        upperLeftX,
        upperLeftY,
        pixelSize,
        -pixelSize,
        0,
        0,
        0);
  }

  /**
   * Convenience function allowing explicitly setting the datatype for all the bands
   *
   * @param numBand
   * @param dataType
   * @param widthInPixel
   * @param heightInPixel
   * @param upperLeftX
   * @param upperLeftY
   * @param pixelSize
   * @return
   * @throws FactoryException
   */
  public static GridCoverage2D makeEmptyRaster(
      int numBand,
      String dataType,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double pixelSize)
      throws FactoryException {
    return makeEmptyRaster(
        numBand,
        dataType,
        widthInPixel,
        heightInPixel,
        upperLeftX,
        upperLeftY,
        pixelSize,
        -pixelSize,
        0,
        0,
        0);
  }

  /**
   * Convenience function for creating a raster with data type DOUBLE for all the bands
   *
   * @param numBand
   * @param widthInPixel
   * @param heightInPixel
   * @param upperLeftX
   * @param upperLeftY
   * @param scaleX
   * @param scaleY
   * @param skewX
   * @param skewY
   * @param srid
   * @return
   * @throws FactoryException
   */
  public static GridCoverage2D makeEmptyRaster(
      int numBand,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY,
      int srid)
      throws FactoryException {
    return makeEmptyRaster(
        numBand,
        "d",
        widthInPixel,
        heightInPixel,
        upperLeftX,
        upperLeftY,
        scaleX,
        scaleY,
        skewX,
        skewY,
        srid);
  }

  /**
   * Create a new empty raster with the given number of empty bands
   *
   * @param numBand the number of bands
   * @param bandDataType the data type of the raster, one of D | B | I | F | S | US
   * @param widthInPixel the width of the raster, in pixel
   * @param heightInPixel the height of the raster, in pixel
   * @param upperLeftX the upper left corner of the raster, in the CRS unit. Note that: the minX of
   *     the envelope is equal to the upperLeftX
   * @param upperLeftY the upper left corner of the raster, in the CRS unit. Note that: the minY of
   *     the envelope is equal to the upperLeftY + height * scaleY
   * @param scaleX the scale of the raster (pixel size on X), in the CRS unit
   * @param scaleY the scale of the raster (pixel size on Y), in the CRS unit
   * @param skewX the skew of the raster on X, in the CRS unit
   * @param skewY the skew of the raster on Y, in the CRS unit
   * @param srid the srid of the CRS. 0 means the default CRS (Cartesian 2D)
   * @return the new empty raster
   * @throws FactoryException
   */
  public static GridCoverage2D makeEmptyRaster(
      int numBand,
      String bandDataType,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY,
      int srid)
      throws FactoryException {
    CoordinateReferenceSystem crs;
    if (srid == 0) {
      crs = DefaultEngineeringCRS.GENERIC_2D;
    } else {
      // Create the CRS from the srid
      // Longitude first, Latitude second
      crs = FunctionsGeoTools.sridToCRS(srid);
    }

    // Create a new empty raster
    WritableRaster raster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(bandDataType), widthInPixel, heightInPixel, numBand, null);
    MathTransform transform =
        new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
            PixelInCell.CELL_CORNER,
            transform,
            crs,
            null);
    return RasterUtils.create(raster, gridGeometry, null, null);
  }

  public static GridCoverage2D makeNonEmptyRaster(
      int numBand,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY,
      int srid,
      double[][] data,
      Map<String, List<String>> properties,
      Double noDataValue,
      PixelInCell anchor)
      throws FactoryException {
    CoordinateReferenceSystem crs;
    if (srid == 0) {
      crs = DefaultEngineeringCRS.GENERIC_2D;
    } else {
      // Create the CRS from the srid
      // Longitude first, Latitude second
      crs = CRS.decode("EPSG:" + srid, true);
    }

    // Create a new empty raster
    WritableRaster raster =
        RasterFactory.createBandedRaster(
            5, widthInPixel, heightInPixel, numBand, null); // create a raster with double values
    for (int i = 0; i < numBand; i++) {
      raster.setSamples(0, 0, widthInPixel, heightInPixel, i, data[i]);
    }
    // raster.setPixels(0, 0, widthInPixel, heightInPixel, data);
    MathTransform transform =
        new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, widthInPixel, heightInPixel), anchor, transform, crs, null);
    return RasterUtils.create(raster, gridGeometry, null, noDataValue, properties);
  }

  public static GridCoverage2D makeNonEmptyRaster(
      int numBands,
      String bandDataType,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY,
      int srid,
      double[][] rasterValues) {
    CoordinateReferenceSystem crs;
    if (srid == 0) {
      crs = DefaultEngineeringCRS.GENERIC_2D;
    } else {
      // Create the CRS from the srid
      // Longitude first, Latitude second
      crs = FunctionsGeoTools.sridToCRS(srid);
    }

    // Create a new empty raster
    WritableRaster raster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(bandDataType), widthInPixel, heightInPixel, numBands, null);
    for (int i = 0; i < numBands; i++)
      raster.setSamples(0, 0, widthInPixel, heightInPixel, i, rasterValues[i]);
    MathTransform transform =
        new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
            PixelInCell.CELL_CORNER,
            transform,
            crs,
            null);
    return RasterUtils.create(raster, gridGeometry, null);
  }

  /**
   * Make a non-empty raster from a reference raster and a set of values. The constructed raster
   * will have the same CRS, geo-reference metadata, width and height as the reference raster. The
   * number of bands of the reference raster is determined by the size of values. The size of values
   * should be multiple of width * height of the reference raster.
   *
   * @param ref the reference raster
   * @param bandDataType the data type of the band
   * @param values the values to set
   * @return the constructed raster
   */
  public static GridCoverage2D makeNonEmptyRaster(
      GridCoverage2D ref, String bandDataType, double[] values) {
    CoordinateReferenceSystem crs = ref.getCoordinateReferenceSystem();
    int widthInPixel = ref.getRenderedImage().getWidth();
    int heightInPixel = ref.getRenderedImage().getHeight();
    int valuesPerBand = widthInPixel * heightInPixel;
    if (values.length == 0) {
      throw new IllegalArgumentException("The size of values should be greater than 0");
    }
    if (values.length % valuesPerBand != 0) {
      throw new IllegalArgumentException(
          "The size of values should be multiple of width * height of the reference raster");
    }
    int numBands = values.length / valuesPerBand;
    WritableRaster raster =
        RasterFactory.createBandedRaster(
            RasterUtils.getDataTypeCode(bandDataType), widthInPixel, heightInPixel, numBands, null);
    for (int i = 0; i < numBands; i++) {
      double[] bandValues = Arrays.copyOfRange(values, i * valuesPerBand, (i + 1) * valuesPerBand);
      raster.setSamples(0, 0, widthInPixel, heightInPixel, i, bandValues);
    }
    MathTransform transform = ref.getGridGeometry().getGridToCRS(PixelInCell.CELL_CENTER);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
            PixelInCell.CELL_CENTER,
            transform,
            crs,
            null);
    return RasterUtils.create(raster, gridGeometry, null);
  }

  /**
   * Convert an out-db raster to an in-db raster.
   *
   * @param gridCoverage2D the out-db raster
   * @return the in-db raster
   */
  public static GridCoverage2D asInDbRaster(GridCoverage2D gridCoverage2D) {
    if (!(gridCoverage2D instanceof OutDbGridCoverage2D)) {
      // Already an in-db raster, simply return the input
      return gridCoverage2D;
    }
    OutDbGridCoverage2D outDbGridCoverage2D = (OutDbGridCoverage2D) gridCoverage2D;
    RenderedImage renderedImage = outDbGridCoverage2D.getRenderedImage();
    // This will fetch all pixel data from the out-db image
    Raster raster = RasterUtils.getRaster(renderedImage);

    // Create a new in-db raster using that raster
    int dataType = raster.getDataBuffer().getDataType();
    int width = raster.getWidth();
    int height = raster.getHeight();
    int numBands = raster.getNumBands();
    WritableRaster wr = RasterFactory.createBandedRaster(dataType, width, height, numBands, null);
    wr.setRect(raster);
    return RasterUtils.create(
        wr, gridCoverage2D.getGridGeometry(), gridCoverage2D.getSampleDimensions());
  }

  /**
   * Generate tiles from a grid coverage
   *
   * @param gridCoverage2D the grid coverage
   * @param bandIndices the indices of the bands to select (1-based), can be null or empty to
   *     include all the bands.
   * @param tileWidth the width of the tiles
   * @param tileHeight the height of the tiles
   * @param padWithNoData whether to pad the tiles with no data value
   * @param padNoDataValue the no data value for padded tiles, only used when padWithNoData is true.
   *     If the value is NaN, the no data value of the original band will be used.
   * @return the tiles
   */
  public static TileIterator generateTiles(
      GridCoverage2D gridCoverage2D,
      int[] bandIndices,
      int tileWidth,
      int tileHeight,
      boolean padWithNoData,
      double padNoDataValue) {
    int numBands = gridCoverage2D.getNumSampleDimensions();
    if (bandIndices == null || bandIndices.length == 0) {
      // Select all the bands
      bandIndices = new int[numBands];
      for (int i = 0; i < numBands; i++) {
        bandIndices[i] = i + 1;
      }
    } else {
      // Check the band indices
      for (int bandIndex : bandIndices) {
        if (bandIndex <= 0 || bandIndex > numBands) {
          throw new IllegalArgumentException(
              String.format("Provided band index %d is not present in the raster", bandIndex));
        }
      }
    }
    if (gridCoverage2D instanceof OutDbGridCoverage2D) {
      return TileGenerator.generateOutDbTiles(
          (OutDbGridCoverage2D) gridCoverage2D, bandIndices, tileWidth, tileHeight);
    } else {
      return TileGenerator.generateInDbTiles(
          gridCoverage2D, bandIndices, tileWidth, tileHeight, padWithNoData, padNoDataValue);
    }
  }

  public static GridCoverage2D[] rsTile(
      GridCoverage2D gridCoverage2D,
      int[] bandIndices,
      int tileWidth,
      int tileHeight,
      boolean padWithNoData,
      Double padNoDataValue) {
    if (gridCoverage2D == null) {
      return null;
    }
    if (padNoDataValue == null) {
      padNoDataValue = Double.NaN;
    }
    TileIterator tileIterator =
        generateTiles(
            gridCoverage2D, bandIndices, tileWidth, tileHeight, padWithNoData, padNoDataValue);
    GridCoverage2D[] result = new GridCoverage2D[tileIterator.getNumTiles()];
    int i = 0;
    while (tileIterator.hasNext()) {
      Tile tile = tileIterator.next();
      result[i++] = tile.getCoverage();
    }
    return result;
  }

  public static GridCoverage2D[] rsTile(
      GridCoverage2D gridCoverage2D,
      int[] bandIndices,
      int tileWidth,
      int tileHeight,
      boolean padWithNoData) {
    return rsTile(gridCoverage2D, bandIndices, tileWidth, tileHeight, padWithNoData, Double.NaN);
  }

  public static GridCoverage2D[] rsTile(
      GridCoverage2D gridCoverage2D, int[] bandIndices, int tileWidth, int tileHeight) {
    return rsTile(gridCoverage2D, bandIndices, tileWidth, tileHeight, false);
  }

  public static GridCoverage2D[] rsTile(
      GridCoverage2D gridCoverage2D, int tileWidth, int tileHeight) {
    return rsTile(gridCoverage2D, null, tileWidth, tileHeight);
  }
}
