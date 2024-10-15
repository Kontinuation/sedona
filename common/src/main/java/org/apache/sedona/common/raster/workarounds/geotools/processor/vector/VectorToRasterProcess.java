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
package org.apache.sedona.common.raster.workarounds.geotools.processor.vector;

import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.util.List;
import javax.media.jai.RasterFactory;
import javax.media.jai.TiledImage;
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.jts.Geometries;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.vector.VectorProcess;
import org.geotools.process.vector.VectorToRasterException;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.geometry.Envelope;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

/**
 * This is a modified version of the {@link org.geotools.process.vector.VectorToRasterProcess} class
 * from GeoTools.The original class has significant performance overhead when processing the ECQL
 * expression. We want to get rid of that in Sedona.
 *
 * <p>A Process to rasterize vector features in an input FeatureCollection.
 *
 * <p>A feature attribute is specified from which to extract the numeric values that will be written
 * to the output grid coverage. At present only int or float values are written to the output grid
 * coverage. If the attribute is of type Long it will be coerced to int values and a warning will be
 * logged. Similarly, if the attribute is of type Double it will be coerced to float and a warning
 * logged.
 *
 * @author Steve Ansari, NOAA
 * @author Michael Bedward
 * @since 2.6
 * @version $Id$
 */
@DescribeProcess(
    title = "Transform",
    description =
        "Converts some or all of a feature collection to a raster grid, using an attribute to specify cell values.")
public class VectorToRasterProcess implements VectorProcess {

  private static final int COORD_GRID_CHUNK_SIZE = 1000;

  public enum TransferType {
    INTEGRAL,
    FLOAT
  }

  private TransferType transferType;

  private ReferencedEnvelope extent;
  private Geometry extentGeometry;
  private GridGeometry2D gridGeom;

  private boolean transformFeatures;
  private MathTransform featureToRasterTransform;

  private int[] coordGridX = new int[COORD_GRID_CHUNK_SIZE];
  private int[] coordGridY = new int[COORD_GRID_CHUNK_SIZE];

  TiledImage image;
  Graphics2D graphics;

  public static class Feature {
    public Geometry geometry;
    public double value;

    public Feature(Geometry geometry, double value) {
      this.geometry = geometry;
      this.value = value;
    }
  }

  /**
   * A static helper method that can be called directly to run the process.
   *
   * <p>The process interface is useful for advertising functionality to dynamic applications, but
   * for 'hands on' coding this method is much more convenient than working via the {@linkplain
   * org.geotools.process.Process#execute }.
   *
   * @param features the feature collection to be (wholly or partially) rasterized
   * @param crs the coordinate reference system of the features
   * @param gridDim size of the output raster
   * @param bounds bounds (world coordinates) of the output raster
   * @param transferType the data type of the output raster
   * @param covName a name for the output raster
   * @return a new grid coverage
   */
  public static GridCoverage2D process(
      List<Feature> features,
      CoordinateReferenceSystem crs,
      Dimension gridDim,
      Envelope bounds,
      TransferType transferType,
      String covName)
      throws VectorToRasterException {
    VectorToRasterProcess process = new VectorToRasterProcess();
    return process.convert(features, crs, gridDim, bounds, transferType, covName);
  }

  @DescribeResult(description = "Rasterized grid")
  public GridCoverage2D execute(
      @DescribeParameter(name = "features", description = "Features to process", min = 1, max = 1)
          List<Feature> features,
      @DescribeParameter(name = "crs", description = "CRS of the features", min = 1, max = 1)
          CoordinateReferenceSystem crs,
      @DescribeParameter(
              name = "rasterWidth",
              description = "Width of the output grid in pixels",
              min = 1,
              max = 1,
              minValue = 1)
          Integer rasterWidth,
      @DescribeParameter(
              name = "rasterHeight",
              description = "Height of the output grid in pixels",
              min = 1,
              max = 1,
              minValue = 1)
          Integer rasterHeight,
      @DescribeParameter(
              name = "transferType",
              description = "Data type of pixels",
              min = 1,
              max = 1)
          TransferType transferType,
      @DescribeParameter(
              name = "title",
              description = "Title to use for the output grid",
              min = 0,
              max = 1,
              defaultValue = "raster")
          String title,
      @DescribeParameter(
              name = "bounds",
              description = "Bounding box of the area to rasterize",
              min = 0,
              max = 1)
          Envelope bounds) {

    return convert(
        features, crs, new Dimension(rasterWidth, rasterHeight), bounds, transferType, title);
  }

  /**
   * This method is called by {@linkplain #execute} to rasterize an individual feature.
   *
   * @param feature the feature to be rasterized
   */
  protected void processFeature(Feature feature) throws Exception {
    Geometry geometry = feature.geometry;
    if (geometry.intersects(extentGeometry)) {
      Double value = feature.value;
      graphics.setColor(valueToColor(value));
      Geometries geomType = Geometries.get(geometry);
      switch (geomType) {
        case MULTIPOLYGON:
        case MULTILINESTRING:
        case MULTIPOINT:
          final int numGeom = geometry.getNumGeometries();
          for (int i = 0; i < numGeom; i++) {
            Geometry geomN = geometry.getGeometryN(i);
            drawGeometry(Geometries.get(geomN), geomN);
          }
          break;

        case POLYGON:
        case LINESTRING:
        case POINT:
          drawGeometry(geomType, geometry);
          break;

        default:
          throw new UnsupportedOperationException(
              "Unsupported geometry type: " + geomType.getName());
      }
    }
  }

  private GridCoverage2D convert(
      List<Feature> features,
      CoordinateReferenceSystem crs,
      Dimension gridDim,
      Envelope bounds,
      TransferType transferType,
      String covName)
      throws VectorToRasterException {

    this.transferType = transferType;
    initialize(features, crs, bounds, gridDim);

    for (Feature feature : features) {
      try {
        processFeature(feature);
      } catch (Exception e) {
        throw new VectorToRasterException(e);
      }
    }

    flattenImage();

    GridCoverageFactory gcf = new GridCoverageFactory();
    return gcf.create(covName, image, extent);
  }

  private void initialize(
      List<Feature> features, CoordinateReferenceSystem crs, Envelope bounds, Dimension gridDim)
      throws VectorToRasterException {

    try {
      setBounds(features, crs, bounds);
    } catch (TransformException ex) {
      throw new VectorToRasterException(ex);
    }

    createImage(gridDim);

    gridGeom = new GridGeometry2D(new GridEnvelope2D(0, 0, gridDim.width, gridDim.height), extent);
  }

  /**
   * Sets the output coverage bounds and checks whether features need to be transformed into the
   * output CRS.
   */
  private void setBounds(List<Feature> features, CoordinateReferenceSystem crs, Envelope bounds)
      throws TransformException {

    ReferencedEnvelope featureBounds = calculateBounds(features, crs);

    if (bounds == null) {
      extent = featureBounds;
    } else {
      extent = new ReferencedEnvelope(bounds);
    }

    extentGeometry = (new GeometryFactory()).toGeometry(extent);

    // Compare the CRS of features and requested output bounds. If they
    // are different (and both non-null) flag that we need to transform
    // features to the output CRS prior to rasterizing them.

    CoordinateReferenceSystem featuresCRS = featureBounds.getCoordinateReferenceSystem();
    CoordinateReferenceSystem boundsCRS = extent.getCoordinateReferenceSystem();

    transformFeatures = false;
    if (featuresCRS != null
        && boundsCRS != null
        && !CRS.equalsIgnoreMetadata(boundsCRS, featuresCRS)) {

      try {
        featureToRasterTransform = CRS.findMathTransform(featuresCRS, boundsCRS, true);

      } catch (Exception ex) {
        throw new TransformException(
            "Unable to transform features into output coordinate reference system", ex);
      }

      transformFeatures = true;
    }
  }

  /** Calculate bounds from features */
  private static ReferencedEnvelope calculateBounds(
      List<Feature> features, CoordinateReferenceSystem crs) {
    ReferencedEnvelope extent = ReferencedEnvelope.create(crs);
    for (Feature feature : features) {
      ReferencedEnvelope bbox = JTS.bounds(feature.geometry, crs);
      if (bbox == null || bbox.isEmpty() || bbox.isNull()) {
        continue;
      }
      extent.expandToInclude(bbox);
    }
    return extent;
  }

  /**
   * Create the tiled image and the associated graphics object that we will be used to draw the
   * vector features into a raster.
   *
   * <p>Note, the graphics objects will be an instance of TiledImageGraphics which is a subclass of
   * Graphics2D.
   */
  private void createImage(Dimension gridDim) {

    ColorModel cm = ColorModel.getRGBdefault();
    SampleModel sm = cm.createCompatibleSampleModel(gridDim.width, gridDim.height);

    image = new TiledImage(0, 0, gridDim.width, gridDim.height, 0, 0, sm, cm);
    graphics = image.createGraphics();
    graphics.setPaintMode();
    graphics.setComposite(AlphaComposite.Src);
  }

  /**
   * Takes the 4-band ARGB image that we have been drawing into and converts it to a single-band
   * image.
   *
   * @todo There is probably a much easier / faster way to do this that still takes advantage of
   *     image tiling (?)
   */
  private void flattenImage() {

    if (transferType == TransferType.FLOAT) {
      flattenImageToFloat();
    } else {
      flattenImageToInt();
    }
  }

  /**
   * Takes the 4-band ARGB image that we have been drawing into and converts it to a single-band int
   * image.
   */
  private void flattenImageToInt() {
    int numXTiles = image.getNumXTiles();
    int numYTiles = image.getNumYTiles();

    SampleModel sm =
        RasterFactory.createPixelInterleavedSampleModel(
            DataBuffer.TYPE_INT, image.getWidth(), image.getHeight(), 1);

    TiledImage destImage =
        new TiledImage(
            0,
            0,
            image.getWidth(),
            image.getHeight(),
            0,
            0,
            sm,
            new ComponentColorModel(
                ColorSpace.getInstance(ColorSpace.CS_GRAY),
                false,
                false,
                Transparency.OPAQUE,
                DataBuffer.TYPE_INT));

    for (int yt = 0; yt < numYTiles; yt++) {
      for (int xt = 0; xt < numXTiles; xt++) {
        Raster srcTile = image.getTile(xt, yt);
        WritableRaster destTile = destImage.getWritableTile(xt, yt);

        int[] data = new int[srcTile.getDataBuffer().getSize()];
        srcTile.getDataElements(
            srcTile.getMinX(), srcTile.getMinY(), srcTile.getWidth(), srcTile.getHeight(), data);

        Rectangle bounds = destTile.getBounds();
        destTile.setPixels(bounds.x, bounds.y, bounds.width, bounds.height, data);
        destImage.releaseWritableTile(xt, yt);
      }
    }

    image = destImage;
  }

  /**
   * Takes the 4-band ARGB image that we have been drawing into and converts it to a single-band
   * float image
   */
  private void flattenImageToFloat() {
    int numXTiles = image.getNumXTiles();
    int numYTiles = image.getNumYTiles();

    SampleModel sm =
        RasterFactory.createPixelInterleavedSampleModel(
            DataBuffer.TYPE_FLOAT, image.getWidth(), image.getHeight(), 1);
    TiledImage destImage =
        new TiledImage(
            0,
            0,
            image.getWidth(),
            image.getHeight(),
            0,
            0,
            sm,
            new ComponentColorModel(
                ColorSpace.getInstance(ColorSpace.CS_GRAY),
                false,
                false,
                Transparency.OPAQUE,
                DataBuffer.TYPE_FLOAT));

    for (int yt = 0; yt < numYTiles; yt++) {
      for (int xt = 0; xt < numXTiles; xt++) {
        Raster srcTile = image.getTile(xt, yt);
        WritableRaster destTile = destImage.getWritableTile(xt, yt);

        int[] data = new int[srcTile.getDataBuffer().getSize()];
        srcTile.getDataElements(
            srcTile.getMinX(), srcTile.getMinY(), srcTile.getWidth(), srcTile.getHeight(), data);

        Rectangle bounds = destTile.getBounds();

        int k = 0;
        for (int dy = bounds.y, drow = 0; drow < bounds.height; dy++, drow++) {
          for (int dx = bounds.x, dcol = 0; dcol < bounds.width; dx++, dcol++, k++) {
            destTile.setSample(dx, dy, 0, Float.intBitsToFloat(data[k]));
          }
        }

        destImage.releaseWritableTile(xt, yt);
      }
    }

    image = destImage;
  }

  private void drawGeometry(Geometries geomType, Geometry geometry) throws TransformException {
    if (transformFeatures) {
      try {
        JTS.transform(geometry, featureToRasterTransform);
      } catch (MismatchedDimensionException ex) {
        throw new RuntimeException(ex);
      }
    }

    Coordinate[] coords = geometry.getCoordinates();

    // enlarge if needed
    if (coords.length > coordGridX.length) {
      int n = coords.length / COORD_GRID_CHUNK_SIZE + 1;
      coordGridX = new int[n * COORD_GRID_CHUNK_SIZE];
      coordGridY = new int[n * COORD_GRID_CHUNK_SIZE];
    }

    // Go through coordinate array in order received
    DirectPosition2D worldPos = new DirectPosition2D();
    for (int n = 0; n < coords.length; n++) {
      worldPos.setLocation(coords[n].x, coords[n].y);
      GridCoordinates2D gridPos = gridGeom.worldToGrid(worldPos);
      coordGridX[n] = gridPos.x;
      coordGridY[n] = gridPos.y;
    }

    switch (geomType) {
      case POLYGON:
        graphics.fillPolygon(coordGridX, coordGridY, coords.length);
        break;

      case LINESTRING: // includes LinearRing
        graphics.drawPolyline(coordGridX, coordGridY, coords.length);
        break;

      case POINT:
        graphics.fillRect(coordGridX[0], coordGridY[0], 1, 1);
        break;

      default:
        throw new IllegalArgumentException("Invalid geometry type: " + geomType.getName());
    }
  }

  /**
   * Encode a value as a Color. The value will be Integer or Float.
   *
   * @param value the value to encode
   * @return the resulting sRGB Color
   */
  private Color valueToColor(Number value) {
    int intBits;
    if (transferType == TransferType.FLOAT) {
      intBits = Float.floatToIntBits(value.floatValue());
    } else {
      intBits = value.intValue();
    }

    return new Color(intBits, true);
  }
}
