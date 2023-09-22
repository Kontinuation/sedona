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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sedona.common.raster.CRSSerializer;
import org.apache.sedona.common.raster.inputstream.HadoopImageInputStreamFactory;
import org.apache.sedona.common.utils.ImageUtils;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.DataSourceException;
import org.geotools.gce.arcgrid.ArcGridFormat;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.util.factory.Hints;
import org.opengis.coverage.CannotEvaluateException;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.PlanarImage;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Locale;
import java.util.Map;

/**
 * A grid coverage referencing raster images stored in cloud storages. The grid coverage may only
 * reference a sub portion of the whole raster image. This is a common practice to populate multiple
 * tiles referencing the same raster image.
 *
 * <p>The resources held by this class is managed by thread-local {@link OutDbResourcePool} instances.
 * Please make sure to call {@link #dispose(boolean)} to release the resources when the grid coverage
 * is no longer needed, and don't pass the grid coverage to other threads.
 */
public class OutDbGridCoverage2D extends GridCoverage2D {
    private final OutDbResourcePool.ResourceKey resourceKey;
    private OutDbResourcePool.OutDbResource pooledResource;
    private final int[] bandIndices;

    public OutDbGridCoverage2D(final CharSequence name,
                               final PlanarImage image,
                               GridGeometry2D gridGeometry,
                               final GridSampleDimension[] bands,
                               int[] bandIndices,
                               OutDbResourcePool.OutDbResource pooledResource) {
        super(name, image, gridGeometry, bands, null, null, null);
        this.resourceKey = pooledResource.resourceKey;
        this.bandIndices = bandIndices;
        this.pooledResource = pooledResource;
    }

    public OutDbGridCoverage2D(final CharSequence name,
                               GridGeometry2D gridGeometry,
                               int dataType,
                               final GridSampleDimension[] bands,
                               int[] bandIndices,
                               OutDbResourcePool.ResourceKey resourceKey) {
        // We use a placeholder image object to make sure that the parent class (GridCoverage2D) can be constructed.
        // The placeholder image will be replaced by the real image when the pixel data is needed. Please refer to
        // replacePlaceHolderImage() for more details.
        super(name, createPlaceHolderImage(gridGeometry, dataType, bands, bandIndices), gridGeometry, bands, null, null, null);
        this.resourceKey = resourceKey;
        this.pooledResource = null;
        this.bandIndices = bandIndices;
    }

    @Override
    public synchronized boolean dispose(final boolean force) {
        boolean ret = super.dispose(force);
        if (ret) {
            if (pooledResource != null) {
                OutDbResourcePool pool = ThreadLocalOutDbResourcePool.get();
                pool.release(pooledResource);
                pooledResource = null;
            }
        }
        return ret;
    }

    public Path getOutDbPath() {
        return resourceKey.path;
    }

    public byte[] getSerializedConfiguration() {
        return resourceKey.serializedConf;
    }

    public int[] getOutDbBandIndices() {
        return bandIndices;
    }

    public Map<String, String> getOutDbParams() {
        return resourceKey.params;
    }

    @Override
    public boolean isDataEditable() {
        return false;
    }

    @Override
    public Object evaluate(final DirectPosition point) throws CannotEvaluateException {
        replacePlaceHolderImage();
        return super.evaluate(point);
    }

    @Override
    public int[] evaluate(final Point2D coord, final int[] dest) throws CannotEvaluateException {
        replacePlaceHolderImage();
        return super.evaluate(coord, dest);
    }

    @Override
    public float[] evaluate(final Point2D coord, final float[] dest)
            throws CannotEvaluateException {
        replacePlaceHolderImage();
        return super.evaluate(coord, dest);
    }

    @Override
    public double[] evaluate(final Point2D coord, final double[] dest)
            throws CannotEvaluateException {
        replacePlaceHolderImage();
        return super.evaluate(coord, dest);
    }

    @Override
    public int[] evaluate(final GridCoordinates2D coord, final int[] dest) {
        replacePlaceHolderImage();
        return super.evaluate(coord, dest);
    }

    @Override
    public float[] evaluate(final GridCoordinates2D coord, final float[] dest) {
        replacePlaceHolderImage();
        return super.evaluate(coord, dest);
    }

    @Override
    public double[] evaluate(final GridCoordinates2D coord, final double[] dest) {
        replacePlaceHolderImage();
        return super.evaluate(coord, dest);
    }

    @Override
    public synchronized String getDebugString(final DirectPosition coord) {
        replacePlaceHolderImage();
        return super.getDebugString(coord);
    }

    @Override
    public int[] getOptimalDataBlockSizes() {
        replacePlaceHolderImage();
        return super.getOptimalDataBlockSizes();
    }

    @Override
    public RenderedImage getRenderedImage() {
        replacePlaceHolderImage();
        return super.getRenderedImage();
    }

    @Override
    public void prefetch(final Rectangle2D area) {
        replacePlaceHolderImage();
        super.prefetch(area);
    }

    /**
     * Replace the placeholder image with the actual image. This method is called when the pixel data of the grid
     * coverage is needed.
     */
    private void replacePlaceHolderImage() {
        if (pooledResource == null) {
            OutDbResourcePool pool = ThreadLocalOutDbResourcePool.get();
            OutDbResourcePool.OutDbResource resource = null;
            try {
                resource = getOrCreateOutDbResource(pool, resourceKey);
                PlanarImage planarImage = buildImageForGridGeometry(gridGeometry, getSampleDimensions(), bandIndices, resource.gridCoverage2D);

                // image field is final in GridCoverage2D, so we need to use reflection to set it.
                final Field field = GridCoverage2D.class.getDeclaredField("image");
                field.setAccessible(true);
                field.set(this, planarImage);

                pooledResource = resource;
            } catch (Exception e) {
                if (resource != null) {
                    pool.release(resource);
                }
                throw new RuntimeException("Failed to build planar image for out-db grid coverage, path=" +
                        resourceKey.path, e);
            }
        }
    }

    public static class SerializableState implements Serializable {
        public CharSequence name;

        // The following three components are used to construct a GridGeometry2D object.
        // We serialize CRS separately because the default serializer is pretty slow, we use a
        // cached serializer to speed up the serialization and reuse CRS on deserialization.
        public GridEnvelope2D gridEnvelope2D;
        public MathTransform gridToCRS;
        public byte[] serializedCRS;

        public GridSampleDimension[] bands;
        public int dataType;
        public int[] bandIndices;
        public Path path;
        public byte[] serializedConf;
        public Map<String, String> params;

        public OutDbGridCoverage2D restore() {
            if (serializedConf == null) {
                throw new IllegalStateException("Configuration was not serialized, cannot restore without user specified configuration");
            }
            return restore(serializedConf);
        }

        public OutDbGridCoverage2D restore(byte[] serializedConf) {
            if (serializedConf == null) {
                throw new IllegalStateException("Cannot restore out-db grid coverage without configuration");
            }
            GridGeometry2D gridGeometry = new GridGeometry2D(gridEnvelope2D, gridToCRS, CRSSerializer.deserialize(serializedCRS));
            OutDbResourcePool.ResourceKey resourceKey = new OutDbResourcePool.ResourceKey(path, serializedConf, params);
            return create(name, gridGeometry, dataType, bands, bandIndices, resourceKey);
        }
    }

    public SerializableState getSerializableState(boolean withConfiguration) {
        SerializableState state = new SerializableState();
        GridGeometry2D gridGeometry = getGridGeometry();
        state.name = getName();
        state.gridEnvelope2D = gridGeometry.getGridRange2D();
        state.gridToCRS = gridGeometry.getGridToCRS2D();
        state.serializedCRS = CRSSerializer.serialize(gridGeometry.getCoordinateReferenceSystem());
        state.bands = getSampleDimensions();
        state.dataType = image.getSampleModel().getDataType();
        state.bandIndices = bandIndices;
        state.path = resourceKey.path;
        state.params = resourceKey.params;
        // Serialized configuration is pretty large (usually >= 80KB). We serialize configuration only if requested.
        if (withConfiguration) {
            state.serializedConf = resourceKey.serializedConf;
        } else {
            state.serializedConf = null;
        }
        return state;
    }

    /**
     * Create an out-db grid coverage from resource key
     * @param name name of the grid coverage
     * @param gridGeometry grid geometry
     * @param dataType data type of raster data buffer, e.g. DataBuffer.TYPE_FLOAT. can be -1 to infer data type from
     *                 bands (this is not always accurate). Currently, this parameter is used by havasu to construct
     *                 out-db grid coverages from its internal raster objects.
     * @param bands sample dimensions
     * @param bandIndices indices of bands to be used
     * @param resourceKey resource key
     * @return out-db grid coverage
     */
    public static OutDbGridCoverage2D create(CharSequence name, GridGeometry2D gridGeometry,
                                             int dataType,
                                             GridSampleDimension[] bands,
                                             int[] bandIndices,
                                             OutDbResourcePool.ResourceKey resourceKey) {
        if (bandIndices == null) {
            bandIndices = new int[bands.length];
            for (int i = 0; i < bands.length; i++) {
                bandIndices[i] = i;
            }
        }
        return new OutDbGridCoverage2D(name, gridGeometry, dataType, bands, bandIndices, resourceKey);
    }

    public static OutDbGridCoverage2D create(CharSequence name, GridGeometry2D gridGeometry,
                                             GridSampleDimension[] bands,
                                             int[] bandIndices,
                                             Path path,
                                             Configuration conf) {
        OutDbResourcePool.ResourceKey resourceKey = new OutDbResourcePool.ResourceKey(path, conf);
        return create(name, gridGeometry, -1, bands, bandIndices, resourceKey);
    }

    public static OutDbGridCoverage2D create(CharSequence name, GridGeometry2D gridGeometry,
                                             GridSampleDimension[] bands,
                                             int[] bandIndices,
                                             Path path,
                                             byte[] serializedConf,
                                             Map<String, String> params) {
        OutDbResourcePool.ResourceKey resourceKey = new OutDbResourcePool.ResourceKey(path, serializedConf, params);
        return create(name, gridGeometry, -1, bands, bandIndices, resourceKey);
    }

    public static OutDbGridCoverage2D create(CharSequence name, Path path, Configuration conf) throws IOException {
        OutDbResourcePool.ResourceKey resourceKey = new OutDbResourcePool.ResourceKey(path, conf);
        return create(name, resourceKey);
    }

    public static OutDbGridCoverage2D create(CharSequence name, OutDbResourcePool.ResourceKey resourceKey) throws IOException {
        OutDbResourcePool pool = ThreadLocalOutDbResourcePool.get();
        OutDbResourcePool.OutDbResource resource = getOrCreateOutDbResource(pool, resourceKey);
        GridCoverage2D sourceGrid = resource.gridCoverage2D;
        try {
            int[] bandIndices = new int[sourceGrid.getNumSampleDimensions()];
            for (int i = 0; i < bandIndices.length; i++) {
                bandIndices[i] = i;
            }
            PlanarImage planarImage = PlanarImage.wrapRenderedImage(sourceGrid.getRenderedImage());
            return new OutDbGridCoverage2D(name, planarImage, sourceGrid.getGridGeometry(), sourceGrid.getSampleDimensions(), bandIndices, resource);
        } catch (Exception e) {
            pool.release(resource);
            throw new DataSourceException("Failed to create out-db grid coverage", e);
        }
    }

    private static OutDbResourcePool.OutDbResource getOrCreateOutDbResource(OutDbResourcePool pool,
                                                                            OutDbResourcePool.ResourceKey key)
            throws IOException {
        OutDbResourcePool.OutDbResource resource = pool.acquire(key);
        if (resource == null) {
            AbstractGridFormat format = getFileFormat(key.path);
            ImageInputStream stream = HadoopImageInputStreamFactory.create(key.path, key.getConfWithParams());
            try {
                GridCoverage2D sourceGrid = readGridCoverage(format, stream);
                resource = new OutDbResourcePool.OutDbResource(key, sourceGrid, stream);
                pool.add(resource);
            } catch (Exception e) {
                stream.close();
                throw new DataSourceException("Failed to create out-db grid coverage", e);
            }
        }
        return resource;
    }

    private static PlanarImage buildImageForGridGeometry(GridGeometry2D gridGeometry,
                                                         GridSampleDimension[] bands,
                                                         int[] bandIndices,
                                                         GridCoverage2D sourceGrid) throws IOException {
        // Construct planar image from parameters. This planar image may contain bands read from
        // different streams, and it could be a sub image of the original image streams.
        if (bandIndices.length != bands.length) {
            throw new DataSourceException("Number of band numbers and bands do not match.");
        }

        // Validate the geo-referencing information of source grid, and resolve the sub portion needed
        // by this grid coverage.
        GridGeometry2D sourceGridGeometry = sourceGrid.getGridGeometry();
        RenderedImage image = sourceGrid.getRenderedImage();
        GridCoordinates2D offset = calculateImageOffset(sourceGridGeometry, gridGeometry);

        // Crop a sub portion of the image and translate it to the origin of the grid geometry
        int offsetX = offset.x;
        int offsetY = offset.y;
        GridEnvelope2D gridRange = gridGeometry.getGridRange2D();
        GridCoordinates2D bound = gridRange.getHigh();
        int width = bound.x + 1;
        int height = bound.y + 1;
        RenderedImage croppedImage = ImageUtils.cropAndTranslateImage(image, offsetX, offsetY, width, height);

        // Select a subset of bands from the source grid
        for (int i = 0; i < bands.length; i++) {
            int bandIndex = bandIndices[i];
            GridSampleDimension sampleDimension = sourceGrid.getSampleDimension(bandIndex);
            if (!sampleDimension.getSampleDimensionType().equals(bands[i].getSampleDimensionType())) {
                throw new DataSourceException("Sample dimension type does not match.");
            }
        }
        return PlanarImage.wrapRenderedImage(ImageUtils.selectBands(croppedImage, bandIndices));
    }

    private static PlanarImage createPlaceHolderImage(GridGeometry2D gridGeometry,
                                                      int dataType,
                                                      GridSampleDimension[] bands,
                                                      int[] bandIndices) {
        int numBand = bandIndices.length;
        if (numBand == 0) {
            throw new IllegalArgumentException("Number of bands must be positive");
        }
        int widthInPixel = gridGeometry.getGridRange2D().width;
        int heightInPixel = gridGeometry.getGridRange2D().height;

        GridSampleDimension band = bands[bandIndices[0]];

        if (dataType < 0) {
            dataType = TypeMap.getDataBufferType(band.getSampleDimensionType());
        }
        final RenderedImage image = new OutDbPlaceHolderImage(widthInPixel, heightInPixel, numBand, dataType);
        return PlanarImage.wrapRenderedImage(image);
    }

    private static AbstractGridFormat getFileFormat(Path path) {
        String fileName = path.getName().toUpperCase(Locale.ROOT);
        AbstractGridFormat format;
        if (fileName.endsWith(".TIFF") || fileName.endsWith(".TIF")) {
            format = new GeoTiffFormat();
        } else if (fileName.endsWith(".ASC")) {
            format = new ArcGridFormat();
        } else {
            // If we cannot infer the file type, we assume that it is GeoTIFF.
            format = new GeoTiffFormat();
        }
        return format;
    }

    private static GridCoverage2D readGridCoverage(AbstractGridFormat format, ImageInputStream stream) throws IOException {
        Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
        return format.getReader(stream, hints).read(null);
    }

    /**
     * Calculate the grid coordinate of the origin of gridGeom1 in gridGeom0.
     * It also checks if the grid geometries of two grid coverages align with each other, and throw exception if they do
     * not match.
     *
     * @param gridGeom0 grid geometry to match
     * @param gridGeom1 another grid geometry to match
     * @return the grid coordinate of the origin of gridGeom1 in gridGeom0
     */
    private static GridCoordinates2D calculateImageOffset(GridGeometry2D gridGeom0, GridGeometry2D gridGeom1) {
        CoordinateReferenceSystem crs0 = gridGeom0.getCoordinateReferenceSystem();
        CoordinateReferenceSystem crs1 = gridGeom1.getCoordinateReferenceSystem();
        AffineTransform2D transform0 = (AffineTransform2D) gridGeom0.getGridToCRS2D();
        AffineTransform2D transform1 = (AffineTransform2D) gridGeom1.getGridToCRS2D();

        // CRS should match. However, due to the weird behavior of GeoTools, sometimes the CRS won't equal to itself
        // after formatting to WKT then read back, so let's just log a warning here.
        if (!CRS.equalsIgnoreMetadata(crs0, crs1)) {
            LOGGER.warning(String.format("The grid coverages have different CRS (%s vs %s)",
                    crs0.toWKT(), crs1.toWKT()));
        }

        // Scale must match. Please note that PostGIS allows the scales to have the different sign, but the same
        // absolute value. We do not support this case.
        if (DBL_NEQ(transform0.getScaleX(), transform1.getScaleX())) {
            throw new IllegalStateException("The grid coverages have different scales on the X axis");
        }
        if (DBL_NEQ(transform0.getScaleY(), transform1.getScaleY())) {
            throw new IllegalStateException("The grid coverages have different scales on the Y axis");
        }

        // Skew must match
        if (DBL_NEQ(transform0.getShearX(), transform1.getShearX())) {
            throw new IllegalStateException("The grid coverages have different shears on the X axis");
        }
        if (DBL_NEQ(transform0.getShearY(), transform1.getShearY())) {
            throw new IllegalStateException("The grid coverages have different shears on the Y axis");
        }

        // Calculate the grid coordinate offset of the two grid geometries
        double x0 = transform0.getTranslateX();
        double y0 = transform0.getTranslateY();
        double x1 = transform1.getTranslateX();
        double y1 = transform1.getTranslateY();
        double scaleX = transform0.getScaleX();
        double scaleY = transform1.getScaleY();
        double gridOffsetX = (x1 - x0) / scaleX;
        double gridOffsetY = (y1 - y0) / scaleY;
        return new GridCoordinates2D((int) Math.round(gridOffsetX), (int) Math.round(gridOffsetY));
    }

    private static boolean DBL_NEQ(double a, double b) {
        return a != b && Math.abs(a - b) > Double.MIN_NORMAL;
    }
}
