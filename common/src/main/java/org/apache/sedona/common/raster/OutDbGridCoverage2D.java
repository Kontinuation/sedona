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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sedona.common.raster.inputstream.DiskCachedImageInputStream;
import org.apache.sedona.common.raster.inputstream.HadoopImageInputStream;
import org.apache.sedona.common.raster.inputstream.HadoopImageInputStreamFactory;
import org.geotools.coverage.GridSampleDimension;
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
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedOp;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;
import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;

/**
 * A grid coverage referencing raster images stored in cloud storages. The grid coverage may only
 * reference a sub portion of the whole raster image. This is a common practice to populate multiple
 * tiles referencing the same raster image.
 */
public class OutDbGridCoverage2D extends GridCoverage2D {
    private transient ImageInputStream stream;
    private final int[] bandIndices;

    public OutDbGridCoverage2D(final CharSequence name,
                               final PlanarImage image,
                               GridGeometry2D gridGeometry,
                               final GridSampleDimension[] bands,
                               int[] bandIndices,
                               ImageInputStream stream) {
        super(name, image, gridGeometry, bands, null, null, null);
        this.bandIndices = bandIndices;
        this.stream = stream;
    }

    @Override
    public synchronized boolean dispose(final boolean force) {
        boolean ret = super.dispose(force);
        if (ret) {
            try {
                stream.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close the input stream", e);
            }
        }
        return ret;
    }

    public static OutDbGridCoverage2D create(CharSequence name, GridGeometry2D gridGeometry,
                                             GridSampleDimension[] bands,
                                             int[] bandIndices,
                                             Path path,
                                             Configuration conf) throws IOException {
        if (bandIndices == null) {
            bandIndices = new int[bands.length];
            for (int i = 0; i < bands.length; i++) {
                bandIndices[i] = i;
            }
        }
        AbstractGridFormat format = getFileFormat(path);
        ImageInputStream stream = HadoopImageInputStreamFactory.create(path, conf);
        try {
            PlanarImage image = buildImageForGridGeometry(gridGeometry, bands, bandIndices, format, stream);
            return new OutDbGridCoverage2D(name, image, gridGeometry, bands, bandIndices, stream);
        } catch (Exception e) {
            stream.close();
            throw new DataSourceException("Failed to create out-db grid coverage", e);
        }
    }

    public static OutDbGridCoverage2D create(CharSequence name, Path path, Configuration conf) throws IOException {
        AbstractGridFormat format = getFileFormat(path);
        ImageInputStream stream = HadoopImageInputStreamFactory.create(path, conf);
        try {
            GridCoverage2D sourceGrid = readGridCoverage(format, stream);
            PlanarImage planarImage = PlanarImage.wrapRenderedImage(sourceGrid.getRenderedImage());
            int[] bandIndices = new int[sourceGrid.getNumSampleDimensions()];
            for (int i = 0; i < bandIndices.length; i++) {
                bandIndices[i] = i;
            }
            return new OutDbGridCoverage2D(name, planarImage, sourceGrid.getGridGeometry(), sourceGrid.getSampleDimensions(), bandIndices, stream);
        } catch (Exception e) {
            stream.close();
            throw new DataSourceException("Failed to create out-db grid coverage", e);
        }
    }

    public static class SerializableState implements Serializable {
        public CharSequence name;
        public GridGeometry2D gridGeometry;
        public GridSampleDimension[] bands;
        public int[] bandIndices;
        public Path path;
        public transient Configuration conf;

        public OutDbGridCoverage2D restore() throws IOException {
            return create(name, gridGeometry, bands, bandIndices, path, conf);
        }

        private void writeObject(java.io.ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();
            conf.write(out);
        }

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            conf = new Configuration(false);
            conf.readFields(in);
        }
    }

    public SerializableState getSerializableState() throws IOException {
        // Serialize the path and configuration to construct the image input stream.
        ImageInputStream serializedStream = stream;
        while (serializedStream instanceof DiskCachedImageInputStream) {
            serializedStream = ((DiskCachedImageInputStream) serializedStream).getStream();
        }
        if (!(serializedStream instanceof HadoopImageInputStream)) {
            throw new IllegalStateException("out-db grid coverage only supports HadoopImageInputStream");
        }
        HadoopImageInputStream hadoopStream = (HadoopImageInputStream) serializedStream;
        Path path = hadoopStream.getPath();
        Configuration conf = hadoopStream.getConf();
        if (path == null || conf == null) {
            throw new IllegalStateException("out-db grid coverage only supports HadoopImageInputStream with path and configuration");
        }

        SerializableState state = new SerializableState();
        state.name = getName();
        state.gridGeometry = getGridGeometry();
        state.bands = getSampleDimensions();
        state.bandIndices = bandIndices;
        state.path = path;
        state.conf = conf;
        return state;
    }

    private static PlanarImage buildImageForGridGeometry(GridGeometry2D gridGeometry,
                                                           GridSampleDimension[] bands,
                                                           int[] bandIndices,
                                                           AbstractGridFormat format,
                                                           ImageInputStream stream) throws IOException {
        // Construct planar image from parameters. This planar image may contain bands read from
        // different streams, and it could be a sub image of the original image streams.
        if (bandIndices.length != bands.length) {
            throw new DataSourceException("Number of band numbers and bands do not match.");
        }
        int numBands = bands.length;

        // Read grid coverages from provided streams
        GridCoverage2D sourceGrid = readGridCoverage(format, stream);
        if (sourceGrid == null) {
            throw new DataSourceException("Failed to read grid coverage from stream");
        }

        try {
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
            RenderedOp croppedImage = cropAndTranslateImage(image, offsetX, offsetY, width, height);

            // Select a subset of bands from the source grid
            for (int i = 0; i < numBands; i++) {
                int bandIndex = bandIndices[i];
                GridSampleDimension sampleDimension = sourceGrid.getSampleDimension(bandIndex);
                if (!sampleDimension.getSampleDimensionType().equals(bands[i].getSampleDimensionType())) {
                    throw new DataSourceException("Sample dimension type does not match.");
                }
            }
            return selectBands(croppedImage, bandIndices);
        } finally {
            sourceGrid.dispose(true);
        }
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

        // CRS should match
        if (!CRS.equalsIgnoreMetadata(crs0, crs1)) {
            throw new IllegalStateException("The grid coverages have different CRS");
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

    private static RenderedOp cropAndTranslateImage(RenderedImage image, int offsetX, int offsetY, int width, int height) {
        ParameterBlock cropParams = new ParameterBlock();
        cropParams.addSource(image);
        cropParams.add((float) offsetX);
        cropParams.add((float) offsetY);
        cropParams.add((float) width);
        cropParams.add((float) height);
        RenderedOp croppedImage = JAI.create("crop", cropParams);

        ParameterBlock translateParams = new ParameterBlock();
        translateParams.addSource(croppedImage);
        translateParams.add((float) -offsetX);
        translateParams.add((float) -offsetY);
        return JAI.create("translate", translateParams);
    }

    private static RenderedOp selectBands(RenderedOp croppedImage, int[] bandIndices) {
        ParameterBlock bandSelectParams = new ParameterBlock();
        bandSelectParams.addSource(croppedImage);
        bandSelectParams.add(bandIndices);
        return JAI.create("bandSelect", bandSelectParams);
    }

    private static boolean DBL_NEQ(double a, double b) {
        return a != b && Math.abs(a - b) > Double.MIN_NORMAL;
    }
}
