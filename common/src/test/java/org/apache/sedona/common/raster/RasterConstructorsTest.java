/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common.raster;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sedona.common.raster.outdb.OutDbGridCoverage2D;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RasterConstructorsTest
        extends RasterTestBase {

    @Test
    public void fromArcInfoAsciiGrid() throws IOException, FactoryException {
        GridCoverage2D gridCoverage2D = RasterConstructors.fromArcInfoAsciiGrid(arc.getBytes(StandardCharsets.UTF_8));

        Geometry envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(3600, envelope.getArea(), 0.1);
        assertEquals(378922d + 30, envelope.getCentroid().getX(), 0.1);
        assertEquals(4072345d + 30, envelope.getCentroid().getY(), 0.1);
        assertEquals(2, gridCoverage2D.getRenderedImage().getTileHeight());
        assertEquals(2, gridCoverage2D.getRenderedImage().getTileWidth());
        assertEquals(0d, RasterUtils.getNoDataValue(gridCoverage2D.getSampleDimension(0)), 0.1);
        assertEquals(3d, gridCoverage2D.getRenderedImage().getData().getPixel(1, 1, (double[])null)[0], 0.1);
    }

    @Test
    public void fromGeoTiff() throws IOException, FactoryException {
        GridCoverage2D gridCoverage2D = RasterConstructors.fromGeoTiff(geoTiff);

        Geometry envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(100, envelope.getArea(), 0.1);
        assertEquals(5, envelope.getCentroid().getX(), 0.1);
        assertEquals(5, envelope.getCentroid().getY(), 0.1);
        assertEquals(10, gridCoverage2D.getRenderedImage().getTileHeight());
        assertEquals(10, gridCoverage2D.getRenderedImage().getTileWidth());
        assertEquals(10d, gridCoverage2D.getRenderedImage().getData().getPixel(5, 5, (double[])null)[0], 0.1);
        assertEquals(4, gridCoverage2D.getNumSampleDimensions());
    }

    @Test
    public void makeEmptyRaster() throws FactoryException {
        double upperLeftX = 0;
        double upperLeftY = 0;
        int widthInPixel = 1;
        int heightInPixel = 2;
        double pixelSize = 2;
        int numBands = 1;
        String dataType = "I";

        GridCoverage2D gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize);
        Geometry envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
        assertEquals(upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
        assertEquals(upperLeftY - heightInPixel * pixelSize, envelope.getEnvelopeInternal().getMinY(), 0.001);
        assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
        assertEquals("REAL_64BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());

        gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, dataType, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize);
        envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
        assertEquals(upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
        assertEquals(upperLeftY - heightInPixel * pixelSize, envelope.getEnvelopeInternal().getMinY(), 0.001);
        assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
        assertEquals("SIGNED_32BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());

        assertEquals("POLYGON ((0 -4, 0 0, 2 0, 2 -4, 0 -4))", envelope.toString());
        double expectedWidthInDegree = pixelSize * widthInPixel;
        double expectedHeightInDegree = pixelSize * heightInPixel;

        assertEquals(expectedWidthInDegree * expectedHeightInDegree, envelope.getArea(), 0.001);
        assertEquals(heightInPixel, gridCoverage2D.getRenderedImage().getTileHeight());
        assertEquals(widthInPixel, gridCoverage2D.getRenderedImage().getTileWidth());
        assertEquals(0d, gridCoverage2D.getRenderedImage().getData().getPixel(0, 0, (double[])null)[0], 0.001);
        assertEquals(1, gridCoverage2D.getNumSampleDimensions());

        gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize, -pixelSize - 1, 0, 0, 0);
        envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
        assertEquals(upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
        assertEquals(upperLeftY - heightInPixel * (pixelSize + 1), envelope.getEnvelopeInternal().getMinY(), 0.001);
        assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
        assertEquals("REAL_64BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());

        gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, dataType, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize, -pixelSize - 1, 0, 0, 0);
        envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
        assertEquals(upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
        assertEquals(upperLeftY - heightInPixel * (pixelSize + 1), envelope.getEnvelopeInternal().getMinY(), 0.001);
        assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
        assertEquals("SIGNED_32BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());
    }

    @Test
    public void testInDbTileWithoutPadding() {
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
        RasterConstructors.Tile[] tiles = RasterConstructors.generateTiles(raster, null, 10, 10, false, Double.NaN);
        assertTilesSameWithGridCoverage(tiles, raster, null, 10, 10, Double.NaN);
    }

    @Test
    public void testInDbTileWithoutPadding2() {
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
        RasterConstructors.Tile[] tiles = RasterConstructors.generateTiles(raster, null, 9, 9, false, Double.NaN);
        assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, Double.NaN);
    }

    @Test
    public void testInDbTileWithPadding() {
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 2, "EPSG:3857");
        RasterConstructors.Tile[] tiles = RasterConstructors.generateTiles(raster, null, 9, 9, true, 100);
        assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, 100);
    }

    @Test
    public void testInDbTileWithBandSelector() {
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 2, "EPSG:3857");
        int[] bandIndices = {2};
        RasterConstructors.Tile[] tiles = RasterConstructors.generateTiles(raster, bandIndices, 9, 9, true, 100);
        assertTilesSameWithGridCoverage(tiles, raster, bandIndices, 9, 9, 100);
    }

    @Test
    public void testInDbTileWithBandSelector2() {
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 4, "EPSG:3857");
        int[] bandIndices = {3, 1};
        RasterConstructors.Tile[] tiles = RasterConstructors.generateTiles(raster, bandIndices, 8, 7, true, 100);
        assertTilesSameWithGridCoverage(tiles, raster, bandIndices, 8, 7, 100);
    }

    @Test
    public void testInDbTileInheritSourceNoDataValue() {
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
        raster = MapAlgebra.addBandFromArray(raster, MapAlgebra.bandAsArray(raster, 1), 1, 13.0);
        RasterConstructors.Tile[] tiles = RasterConstructors.generateTiles(raster, null, 9, 9, true, Double.NaN);
        assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, 13);
    }

    @Test
    public void testInDbTileOverrideSourceNoDataValue() {
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
        raster = MapAlgebra.addBandFromArray(raster, MapAlgebra.bandAsArray(raster, 1), 1, 13.0);
        RasterConstructors.Tile[] tiles = RasterConstructors.generateTiles(raster, null, 9, 9, true, 42);
        assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, 42);
    }

    @Test
    public void testOutDbTile() throws IOException {
        String[] paths = {
                resourceFolder + "/raster/test1.tiff",
                resourceFolder + "/raster/test2.tiff",
                resourceFolder + "/raster/test3.tif",
                resourceFolder + "/raster/raster_with_no_data/test5.tiff",
                resourceFolder + "/raster_geotiff_color/FAA_UTM18N_NAD83.tif"
        };
        for (String path : paths) {
            GridCoverage2D raster = OutDbGridCoverage2D.create("test", new Path(path), new Configuration());
            RasterConstructors.Tile[] tiles = RasterConstructors.generateTiles(raster, null, 100, 100, false, Double.NaN);
            assertTilesSameWithGridCoverage(tiles, raster, null, 100, 100, Double.NaN);
            raster.dispose(true);
            for (RasterConstructors.Tile tile : tiles) {
                tile.getCoverage().dispose(true);
            }
        }
    }

    private void assertTilesSameWithGridCoverage(RasterConstructors.Tile[] tiles, GridCoverage2D gridCoverage2D,
                                                 int[] bandIndices, int tileWidth, int tileHeight, double noDataValue) {
        RenderedImage image = gridCoverage2D.getRenderedImage();
        int width = image.getWidth();
        int height = image.getHeight();
        int numTilesX = (int) Math.ceil((double) width / tileWidth);
        int numTilesY = (int) Math.ceil((double) height / tileHeight);
        Assert.assertEquals(numTilesX * numTilesY, tiles.length);

        // For each tile, select a few random points, and do the following checks
        // 1. The pixel at the point is the same as the corresponding pixel in the grid coverage
        // 2. The pixel at the point translates to the same world coordinate as the corresponding pixel in the grid
        //    coverage
        Set<Pair<Integer, Integer>> visitedTiles = new HashSet<>();
        for (RasterConstructors.Tile tile : tiles) {
            int tileX = tile.getTileX();
            int tileY = tile.getTileY();
            Pair<Integer, Integer> tilePosition = Pair.of(tileX, tileY);
            Assert.assertFalse(visitedTiles.contains(tilePosition));
            visitedTiles.add(tilePosition);

            int offsetX = tileX * tileWidth;
            int offsetY = tileY * tileHeight;
            for (int i = 0; i < 10; i++) {
                GridCoverage2D tileRaster = tile.getCoverage();
                RenderedImage tileImage = tileRaster.getRenderedImage();
                int currentTileWidth = tileImage.getWidth();
                int currentTileHeight = tileImage.getHeight();
                Assert.assertTrue(currentTileWidth <= tileWidth);
                Assert.assertTrue(currentTileHeight <= tileHeight);
                if (currentTileWidth < tileWidth || currentTileHeight < tileHeight) {
                    Assert.assertTrue((tileX == numTilesX - 1) || (tileY == numTilesY - 1));
                }

                int x = (int) (Math.random() * currentTileWidth);
                int y = (int) (Math.random() * currentTileHeight);

                // Check that the pixel at the point is the same as the corresponding pixel in the grid coverage
                GridCoordinates2D tileGridCoord = new GridCoordinates2D(x, y);
                GridCoordinates2D gridCoord = new GridCoordinates2D(offsetX + x, offsetY + y);
                float[] values = tileRaster.evaluate(tileGridCoord, (float[]) null);
                if (offsetX + x < width && offsetY + y < height) {
                    float[] expectedValues = gridCoverage2D.evaluate(gridCoord, (float[]) null);
                    if (bandIndices == null) {
                        Assert.assertArrayEquals(expectedValues, values, 1e-6f);
                    } else {
                        Assert.assertEquals(bandIndices.length, values.length);
                        for (int j = 0; j < bandIndices.length; j++) {
                            Assert.assertEquals(expectedValues[bandIndices[j] - 1], values[j], 1e-6f);
                        }
                    }

                    // Check that the pixel at the point translates to the same world coordinate as the corresponding
                    // pixel in the grid coverage
                    try {
                        DirectPosition actualWorldCoord = tileRaster.getGridGeometry().gridToWorld(tileGridCoord);
                        DirectPosition expectedWorldCoord = gridCoverage2D.getGridGeometry().gridToWorld(gridCoord);
                        Assert.assertEquals(expectedWorldCoord, actualWorldCoord);
                    } catch (TransformException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    // Padded pixel
                    for (int k = 0; k < values.length; k++) {
                        double tileNoDataValue = RasterUtils.getNoDataValue(tileRaster.getSampleDimension(k));
                        Assert.assertEquals(noDataValue, tileNoDataValue, 1e-6f);
                        Assert.assertEquals(noDataValue, values[k], 1e-6f);
                    }
                }
            }
        }
    }
}
