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
package org.apache.sedona.common.raster.workarounds.geotools.geotiff;

import it.geosolutions.imageio.core.BasicAuthURI;
import it.geosolutions.imageio.maskband.DatasetLayout;
import it.geosolutions.imageio.pam.PAMDataset;
import it.geosolutions.imageio.plugins.tiff.TIFFImageReadParam;
import it.geosolutions.imageio.utilities.ImageIOUtilities;
import it.geosolutions.imageioimpl.plugins.cog.CogImageInputStreamSpi;
import it.geosolutions.imageioimpl.plugins.cog.CogSourceSPIProvider;
import it.geosolutions.imageioimpl.plugins.tiff.TIFFImageReaderSpi;
import it.geosolutions.imageioimpl.plugins.tiff.TiffDatasetLayoutImpl;
import it.geosolutions.jaiext.range.NoDataContainer;
import java.awt.Color;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.awt.image.renderable.ParameterBlock;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.ImageInputStreamSpi;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;
import javax.media.jai.ImageLayout;
import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;
import javax.media.jai.ROI;
import javax.media.jai.RenderedOp;
import org.apache.sedona.common.raster.workarounds.imageioext.tiff.SedonaTIFFImageReaderSpi;
import org.geotools.api.coverage.ColorInterpretation;
import org.geotools.api.coverage.grid.Format;
import org.geotools.api.coverage.grid.GridCoverage;
import org.geotools.api.coverage.grid.GridEnvelope;
import org.geotools.api.data.DataSourceException;
import org.geotools.api.data.FileGroupProvider.FileGroup;
import org.geotools.api.geometry.Bounds;
import org.geotools.api.parameter.GeneralParameterValue;
import org.geotools.api.parameter.ParameterValue;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.ReferenceIdentifier;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.coverage.Category;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GroundControlPoints;
import org.geotools.coverage.grid.io.OverviewPolicy;
import org.geotools.coverage.grid.io.imageio.MaskOverviewProvider;
import org.geotools.coverage.grid.io.imageio.MaskOverviewProvider.MaskInfo;
import org.geotools.coverage.grid.io.imageio.geotiff.GeoTiffIIOMetadataDecoder;
import org.geotools.coverage.grid.io.imageio.geotiff.GeoTiffMetadata2CRSAdapter;
import org.geotools.coverage.grid.io.imageio.geotiff.TiePoint;
import org.geotools.coverage.util.CoverageUtilities;
import org.geotools.data.MapInfoFileReader;
import org.geotools.data.PrjFileReader;
import org.geotools.data.WorldFileReader;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.geometry.GeneralBounds;
import org.geotools.image.ImageWorker;
import org.geotools.image.io.ImageIOExt;
import org.geotools.image.util.ImageUtilities;
import org.geotools.metadata.i18n.Vocabulary;
import org.geotools.metadata.i18n.VocabularyKeys;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.matrix.XAffineTransform;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.util.NumberRange;
import org.geotools.util.URLs;
import org.geotools.util.Utilities;
import org.geotools.util.factory.Hints;

/**
 * this class is responsible for exposing the data and the Georeferencing metadata available to the
 * Geotools library. This reader is heavily based on the capabilities provided by the ImageIO tools
 * and JAI libraries.
 *
 * @author Bryce Nordgren, USDA Forest Service
 * @author Simone Giannecchini
 * @since 2.1
 */
public class SedonaGeoTiffReader extends AbstractGridCoverage2DReader
    implements GridCoverage2DReader {

  private static final String DEFAULT_COVERAGE_NAME = "geotiff_coverage";

  /** Logger for the {@link org.geotools.gce.geotiff.GeoTiffReader} class. */
  private Logger LOGGER =
      org.geotools.util.logging.Logging.getLogger(org.geotools.gce.geotiff.GeoTiffReader.class);

  /**
   * With this java switch I can control whether or not an external PRJ files takes precedence over
   * the internal CRS definition
   */
  public static final String OVERRIDE_CRS_SWITCH = "org.geotools.gce.geotiff.override.crs";

  /**
   * With this java switch I can control whether or not an external PRJ files takes precedence over
   * the internal CRS definition
   */
  static boolean OVERRIDE_INNER_CRS =
      Boolean.valueOf(
          System.getProperty(org.geotools.gce.geotiff.GeoTiffReader.OVERRIDE_CRS_SWITCH, "True"));

  /**
   * SPI for creating tiff readers in ImageIO tools when not using COG
   *
   * <p>HACK: This is where the modification took place
   */
  static final TIFFImageReaderSpi TIFF_READER_SPI = new SedonaTIFFImageReaderSpi();

  private ImageReaderSpi readerSpi;

  /** Adapter for the GeoTiff crs. */
  private GeoTiffMetadata2CRSAdapter gtcs;

  private double noData = Double.NaN;

  /** File containing image overviews */
  private File ovrSource;

  /** {@link ImageInputStreamSpi} for the file containing external overviews */
  private ImageInputStreamSpi ovrInStreamSPI = null;

  /** Image index of the overviews file */
  private int extOvrImgChoice = -1;

  /** {@link MaskOverviewProvider} instance used for handling internal/external Overviews */
  private MaskOverviewProvider maskOvrProvider;

  /** Boolean indicating if {@link MaskOverviewProvider} is present */
  private boolean hasMaskOvrProvider;

  /** The ground control points, populated if there is no grid to world transformation */
  private GroundControlPoints gcps;

  /** The band statistics provided by GDAL, either via sidecar file or custom TIFF tag */
  private PAMDataset pamDataset;

  /**
   * Creates a new instance of GeoTiffReader
   *
   * @param input the GeoTiff file
   * @param uHints user-supplied hints TODO currently are unused
   */
  @SuppressWarnings({
    "PMD.UseTryWithResources" // closing is conditional
  })
  public SedonaGeoTiffReader(Object input, Hints uHints) throws DataSourceException {
    super(input, uHints);

    // /////////////////////////////////////////////////////////////////////
    //
    // Set the source being careful in case it is an URL pointing to a file
    //
    // /////////////////////////////////////////////////////////////////////
    try {
      readerSpi = TIFF_READER_SPI;
      // setting source
      if (input instanceof URL) {
        final URL sourceURL = (URL) input;
        source = URLs.urlToFile(sourceURL);
      }

      closeMe = true;

      // /////////////////////////////////////////////////////////////////////
      //
      // Get a stream in order to read from it for getting the basic
      // information for this coverage
      //
      // /////////////////////////////////////////////////////////////////////
      if ((source instanceof InputStream) || (source instanceof ImageInputStream)) closeMe = false;
      if (source instanceof CogSourceSPIProvider) {
        CogSourceSPIProvider readerInputObject = (CogSourceSPIProvider) input;
        readerSpi = readerInputObject.getReaderSpi();
        inStreamSPI = readerInputObject.getStreamSpi();
        inStream = readerInputObject.getStream();
      } else if (source instanceof ImageInputStream) inStream = (ImageInputStream) source;
      else {

        inStreamSPI = ImageIOExt.getImageInputStreamSPI(source);
        if (inStreamSPI == null)
          throw new IllegalArgumentException("No input stream for the provided source");
        inStream =
            inStreamSPI.createInputStreamInstance(
                source, ImageIO.getUseCache(), ImageIO.getCacheDirectory());
      }
      if (inStream == null) {
        // Try to figure out what went wrong and provide some info to the user.
        if (source instanceof File) {
          File f = (File) source;
          if (!f.exists()) {
            throw new FileNotFoundException("File " + f.getAbsolutePath() + " does not exist.");
          } else if (f.isDirectory()) {
            throw new IOException("File " + f.getAbsolutePath() + " is a directory.");
          } else if (!f.canRead()) {
            throw new IOException("File " + f.getAbsolutePath() + " can not be read.");
          }
        }

        // If we can't give anything more specific, throw the generic error.
        throw new IllegalArgumentException("No input stream for the provided source");
      }

      // /////////////////////////////////////////////////////////////////////
      //
      // Information about multiple levels and such
      //
      // /////////////////////////////////////////////////////////////////////
      getHRInfo(this.hints);

      // /////////////////////////////////////////////////////////////////////
      //
      // Coverage name
      //
      // /////////////////////////////////////////////////////////////////////

      coverageName = extractCoverageName();
      final int dotIndex = coverageName.lastIndexOf('.');
      if (dotIndex != -1 && dotIndex != coverageName.length())
        coverageName = coverageName.substring(0, dotIndex);

    } catch (IOException e) {
      throw new DataSourceException(e);
    } finally {
      // /////////////////////////////////////////////////////////////////////
      //
      // Freeing streams
      //
      // /////////////////////////////////////////////////////////////////////
      if (closeMe && inStream != null) //
      try {
          inStream.close();
        } catch (Throwable t) {
        }
    }
  }

  private String extractCoverageName() {
    if (source instanceof File) {
      return ((File) source).getName();
    } else if (source instanceof CogSourceSPIProvider) {
      BasicAuthURI uri = ((CogSourceSPIProvider) source).getCogUri();
      String path = uri.getUri().getPath();
      int indexOf = path.lastIndexOf("/");
      path = path.substring(indexOf + 1);
      int extensionIndex = path.lastIndexOf(".");
      String name = extensionIndex > 0 ? path.substring(0, extensionIndex) : path;
      return name;
    }
    return DEFAULT_COVERAGE_NAME;
  }

  /** Collect georeferencing information about this geotiff. */
  // stream is reset, not closed. The self assignment has a cast, may change the value
  @SuppressWarnings({"PMD.UseTryWithResources", "SelfAssignment"})
  private void getHRInfo(Hints hints) throws DataSourceException {
    ImageReader reader = null;
    ImageReader ovrReader = null;
    ImageInputStream ovrStream = null;
    try {
      // //
      //
      // Get a reader for this format
      //
      // //
      reader = readerSpi.createReaderInstance();

      // //
      //
      // get the METADATA
      //
      // //
      inStream.mark();
      reader.setInput(inStream);
      final IIOMetadata iioMetadata = reader.getImageMetadata(0);
      final GeoTiffIIOMetadataDecoder metadata = new GeoTiffIIOMetadataDecoder(iioMetadata);
      gtcs = new GeoTiffMetadata2CRSAdapter(hints);

      // //
      //
      // get the CRS INFO
      //
      // //
      final Object tempCRS = this.hints.get(Hints.DEFAULT_COORDINATE_REFERENCE_SYSTEM);
      if (tempCRS != null) {
        this.crs = (CoordinateReferenceSystem) tempCRS;
        if (LOGGER.isLoggable(Level.FINE))
          LOGGER.log(Level.FINE, "Using forced coordinate reference system");
      } else {

        // check external prj first
        if (!ImageIOUtilities.isSkipExternalFilesLookup()) {
          crs = getCRS(source);
        }

        // now, if we did not want to override the inner CRS or we did not have any external
        // PRJ at hand
        // let's look inside the geotiff
        if (!OVERRIDE_INNER_CRS || crs == null) {
          if (metadata.hasGeoKey() && gtcs != null) {
            crs = gtcs.createCoordinateSystem(metadata);
          }
        }
      }

      //
      // No data
      //
      if (metadata.hasNoData()) {
        noData = metadata.getNoData();
        SampleModel sampleModel = reader.getImageTypes(0).next().getSampleModel();

        // nodata is stored as double, but pixels are float? need to cast the
        // nodata though float to get a representation that would successfully compare
        // against the pixels
        if (sampleModel.getDataType() == DataBuffer.TYPE_FLOAT) {
          noData = (float) noData;
        }
      }

      // collect scales and offsets is present
      collectScaleOffset(iioMetadata);

      // collect PAM dataset if available
      this.pamDataset = getPamDataset(getSourceAsFile(), iioMetadata);

      //
      // parse and set layout
      //
      setLayout(reader);

      //
      // parse TIFF StreamMetadata
      //
      dtLayout = TiffDatasetLayoutImpl.parseLayout(reader.getStreamMetadata());

      // allows to skip the .ovr files lookup
      boolean skipOverviews =
          (Boolean)
              hints.getOrDefault(
                  Hints.SKIP_EXTERNAL_OVERVIEWS, ImageIOUtilities.isSkipExternalFilesLookup());
      if (skipOverviews) {
        LOGGER.log(Level.FINE, "Skipping GeoTiff overview sidecar files for {0}", source);
        ((TiffDatasetLayoutImpl) dtLayout).setNumExternalOverviews(0);
      }

      // Creating a new OverviewsProvider instance
      File inputFile = null;
      if (source instanceof File) {
        inputFile = (File) source;
      } else if (source instanceof URL && (((URL) source).getProtocol() == "file")) {
        inputFile = URLs.urlToFile((URL) source);
      }
      // assume overviews are also TIFFs, that's the 99% case
      if (inputFile != null) {
        URL url = URLs.fileToUrl(inputFile);
        maskOvrProvider =
            new MaskOverviewProvider(
                dtLayout,
                url,
                new MaskOverviewProvider.SpiHelper(url, TIFF_READER_SPI),
                skipOverviews);
        hasMaskOvrProvider = true;
      } else if (dtLayout != null && dtLayout.getExternalMasks() != null) {
        String path = dtLayout.getExternalMasks().getAbsolutePath();
        File file = new File(path.substring(0, path.length() - 4));
        URL url = URLs.fileToUrl(file);
        maskOvrProvider =
            new MaskOverviewProvider(
                dtLayout,
                url,
                new MaskOverviewProvider.SpiHelper(url, TIFF_READER_SPI),
                skipOverviews);
        hasMaskOvrProvider = true;
      } else if (source instanceof CogSourceSPIProvider) {
        CogSourceSPIProvider cogSourceProvider = (CogSourceSPIProvider) source;
        maskOvrProvider =
            new MaskOverviewProvider(
                null,
                cogSourceProvider.getSourceUrl(),
                new MaskOverviewProvider.SpiHelper(cogSourceProvider),
                skipOverviews);
        hasMaskOvrProvider = true;
      }

      // //
      //
      // get the dimension of the hr image and build the model as well as
      // computing the resolution
      // //
      numOverviews =
          hasMaskOvrProvider
              ? maskOvrProvider.getNumOverviews()
              : dtLayout.getNumInternalOverviews();
      int hrWidth = reader.getWidth(0);
      int hrHeight = reader.getHeight(0);
      final Rectangle actualDim = new Rectangle(0, 0, hrWidth, hrHeight);
      originalGridRange = new GridEnvelope2D(actualDim);

      if (gtcs != null
          && metadata != null
          && (metadata.hasModelTrasformation()
              || (metadata.hasPixelScales() && metadata.hasTiePoints()))) {
        this.raster2Model = GeoTiffMetadata2CRSAdapter.getRasterToModel(metadata);
      } else {
        // world file
        this.raster2Model = parseWorldFile(source);

        // now world file --> mapinfo?
        if (raster2Model == null) {
          MapInfoFileReader mifReader = parseMapInfoFile(source);
          if (mifReader != null) {
            raster2Model = mifReader.getTransform();
            crs = mifReader.getCRS();
          }
        }
      }

      if (crs == null) {
        if (LOGGER.isLoggable(Level.WARNING)) {
          LOGGER.warning("Coordinate Reference System is not available");
        }
        crs = AbstractGridFormat.getDefaultCRS();
      }

      if (this.raster2Model == null) {
        TiePoint[] modelTiePoints = metadata.getModelTiePoints();
        if (modelTiePoints != null && modelTiePoints.length > 1) {
          // use a unit transform and expose the GCPs
          gcps = new GroundControlPoints(Arrays.asList(modelTiePoints), crs);
          raster2Model = ProjectiveTransform.create(new AffineTransform());
          crs = AbstractGridFormat.getDefaultCRS();
        } else {
          throw new DataSourceException(
              "Raster to Model Transformation is not available for: " + getSourceAsFile());
        }
      }

      // create envelope using corner transformation
      final AffineTransform tempTransform = new AffineTransform((AffineTransform) raster2Model);
      tempTransform.concatenate(CoverageUtilities.CENTER_TO_CORNER);
      originalEnvelope =
          CRS.transform(ProjectiveTransform.create(tempTransform), new GeneralBounds(actualDim));
      originalEnvelope.setCoordinateReferenceSystem(crs);

      // ///
      //
      // setting the higher resolution available for this coverage
      //
      // ///
      highestRes = new double[2];
      highestRes[0] = XAffineTransform.getScaleX0(tempTransform);
      highestRes[1] = XAffineTransform.getScaleY0(tempTransform);

      // External Overview management
      if (maskOvrProvider != null) {
        extOvrImgChoice =
            maskOvrProvider.getNumExternalOverviews() > 0
                ? maskOvrProvider.getNumInternalOverviews() + 1
                : -1;
      } else {
        File extOvrFile = dtLayout.getExternalOverviews();
        if (extOvrFile != null && extOvrFile.exists()) {
          // Setting the overview file
          ovrSource = extOvrFile;
          ovrInStreamSPI = ImageIOExt.getImageInputStreamSPI(extOvrFile);

          ovrReader = TIFF_READER_SPI.createReaderInstance();
          ovrStream =
              ovrInStreamSPI.createInputStreamInstance(
                  extOvrFile, ImageIO.getUseCache(), ImageIO.getCacheDirectory());
          ovrReader.setInput(ovrStream);
          // this includes the real image as this is a image index, we need to add one.
          extOvrImgChoice = numOverviews + 1;
          numOverviews = numOverviews + dtLayout.getNumExternalOverviews();
          if (numOverviews < extOvrImgChoice) extOvrImgChoice = -1;
        }
      }

      // //
      //
      // get information for the successive images
      //
      // //
      if (numOverviews >= 1) {
        overViewResolutions = new double[numOverviews][2];
        // Internal overviews start at 1, so lastInternalOverview matches numOverviews if no
        // external.
        int firstExternalOverview = extOvrImgChoice == -1 ? numOverviews : extOvrImgChoice - 1;
        double spanRes0 = highestRes[0] * this.originalGridRange.getSpan(0);
        double spanRes1 = highestRes[1] * this.originalGridRange.getSpan(1);
        if (maskOvrProvider != null) {
          overViewResolutions = maskOvrProvider.getOverviewResolutions(spanRes0, spanRes1);
        } else {

          for (int i = 0; i < firstExternalOverview; i++) {
            // Setting the correct overview index
            int overviewImageIndex = dtLayout.getInternalOverviewImageIndex(i + 1);
            int index = overviewImageIndex >= 0 ? overviewImageIndex : 0;
            overViewResolutions[i][0] = spanRes0 / reader.getWidth(index);
            overViewResolutions[i][1] = spanRes1 / reader.getHeight(index);
          }
          for (int i = firstExternalOverview; i < numOverviews; i++) {
            overViewResolutions[i][0] = spanRes0 / ovrReader.getWidth(i - firstExternalOverview);
            overViewResolutions[i][1] = spanRes1 / ovrReader.getHeight(i - firstExternalOverview);
          }
        }
      } else overViewResolutions = null;
    } catch (Throwable e) {
      throw new DataSourceException(e);
    } finally {
      if (reader != null)
        try {
          reader.dispose();
        } catch (Throwable t) {
        }

      if (ovrReader != null)
        try {
          ovrReader.dispose();
        } catch (Throwable t) {
        }

      if (ovrStream != null)
        try {
          ovrStream.close();
        } catch (Throwable t) {
        }

      if (inStream != null)
        try {
          inStream.reset();
        } catch (Throwable t) {
        }
    }
  }

  /**
   * @see org.opengis.coverage.grid.GridCoverageReader#getFormat()
   */
  @Override
  public Format getFormat() {
    return new GeoTiffFormat();
  }

  /**
   * This method reads in the TIFF image, constructs an appropriate CRS, determines the math
   * transform from raster to the CRS model, and constructs a GridCoverage.
   *
   * @param params currently ignored, potentially may be used for hints.
   * @return grid coverage represented by the image
   * @throws IOException on any IO related troubles
   */
  @Override
  public GridCoverage2D read(GeneralParameterValue[] params) throws IOException {
    GeneralBounds requestedEnvelope = null;
    Rectangle dim = null;
    Color inputTransparentColor = null;
    OverviewPolicy overviewPolicy = null;
    int[] suggestedTileSize = null;
    int[] bands = null;
    boolean rescalePixels = AbstractGridFormat.RESCALE_PIXELS.getDefaultValue();

    //
    // Checking params
    //
    if (params != null) {
      for (GeneralParameterValue generalParameterValue : params) {
        final ParameterValue param = (ParameterValue) generalParameterValue;
        final ReferenceIdentifier name = param.getDescriptor().getName();
        if (name.equals(AbstractGridFormat.READ_GRIDGEOMETRY2D.getName())) {
          final GridGeometry2D gg = (GridGeometry2D) param.getValue();
          requestedEnvelope = new GeneralBounds((Bounds) gg.getEnvelope2D());
          dim = gg.getGridRange2D().getBounds();
          continue;
        }
        if (name.equals(AbstractGridFormat.OVERVIEW_POLICY.getName())) {
          overviewPolicy = (OverviewPolicy) param.getValue();
          continue;
        }
        if (name.equals(AbstractGridFormat.INPUT_TRANSPARENT_COLOR.getName())) {
          inputTransparentColor = (Color) param.getValue();
          continue;
        }
        if (name.equals(AbstractGridFormat.SUGGESTED_TILE_SIZE.getName())) {
          String suggestedTileSize_ = (String) param.getValue();
          if (suggestedTileSize_ != null && suggestedTileSize_.length() > 0) {
            suggestedTileSize_ = suggestedTileSize_.trim();
            int commaPosition = suggestedTileSize_.indexOf(",");
            if (commaPosition < 0) {
              int tileDim = Integer.parseInt(suggestedTileSize_);
              suggestedTileSize = new int[] {tileDim, tileDim};
            } else {
              int tileW = Integer.parseInt(suggestedTileSize_.substring(0, commaPosition));
              int tileH = Integer.parseInt(suggestedTileSize_.substring(commaPosition + 1));
              suggestedTileSize = new int[] {tileW, tileH};
            }
          }
          continue;
        }
        if (name.equals(AbstractGridFormat.RESCALE_PIXELS.getName())) {
          rescalePixels = Boolean.TRUE.equals(param.getValue());
        }
        if (name.equals(AbstractGridFormat.BANDS.getName())) {
          bands = (int[]) param.getValue();
        }
      }
    }

    //
    // set params
    //
    Integer imageChoice = Integer.valueOf(0);
    final TIFFImageReadParam readP = new TIFFImageReadParam();
    try {
      imageChoice = setReadParams(overviewPolicy, readP, requestedEnvelope, dim);
    } catch (TransformException e) {
      throw new DataSourceException(e);
    }
    if (bands != null) {
      // the destination type is set to support ImageReadOp, as it cannot recognize
      // the bands parameter (which is otherwise needed to support repeated band numbers)
      readP.setBands(bands);
      readP.setDestinationType(
          ImageIOUtilities.getBandSelectedType(
              bands.length, getImageLayout().getSampleModel(null)));
    }

    //
    // IMAGE READ OPERATION
    //
    Hints newHints = null;
    if (suggestedTileSize != null) {
      newHints = hints.clone();
      final ImageLayout layout = new ImageLayout();
      layout.setTileGridXOffset(0);
      layout.setTileGridYOffset(0);
      layout.setTileHeight(suggestedTileSize[1]);
      layout.setTileWidth(suggestedTileSize[0]);
      newHints.add(new RenderingHints(JAI.KEY_IMAGE_LAYOUT, layout));
    }
    final ParameterBlock pbjRead = new ParameterBlock();
    // Image Index used for the Overview management
    if (maskOvrProvider != null) {
      if (maskOvrProvider.isExternalOverview(imageChoice)) {
        pbjRead.add(
            maskOvrProvider
                .getSourceSpiProvider()
                .getCompatibleSourceProvider(maskOvrProvider.getOvrURL())
                .getStream());
      } else {
        pbjRead.add(maskOvrProvider.getSourceSpiProvider().getStream());
      }
      pbjRead.add(maskOvrProvider.getOverviewIndex(imageChoice));
    } else {
      if (extOvrImgChoice >= 0 && imageChoice >= extOvrImgChoice) {
        pbjRead.add(
            ovrInStreamSPI.createInputStreamInstance(
                ovrSource, ImageIO.getUseCache(), ImageIO.getCacheDirectory()));
        pbjRead.add(imageChoice - extOvrImgChoice);
      } else {
        pbjRead.add(getImageInputStream());
        // Setting correct ImageChoice (taking into account overviews and masks)
        int overviewImageIndex = dtLayout.getInternalOverviewImageIndex(imageChoice);
        int index = overviewImageIndex >= 0 ? overviewImageIndex : 0;
        pbjRead.add(index);
      }
    }
    pbjRead.add(Boolean.FALSE);
    pbjRead.add(Boolean.FALSE);
    pbjRead.add(Boolean.FALSE);
    pbjRead.add(null);
    pbjRead.add(null);
    pbjRead.add(readP);
    pbjRead.add(readerSpi.createReaderInstance());
    PlanarImage coverageRaster =
        JAI.create("ImageRead", pbjRead, newHints != null ? newHints : null);

    // applying rescale if needed
    if (rescalePixels) {
      if (!Double.isNaN(noData)) {
        // Force nodata settings since JAI ImageRead may lost that
        // We have to make sure that noData pixels won't be rescaled
        PlanarImage t = PlanarImage.wrapRenderedImage(coverageRaster);
        t.setProperty(NoDataContainer.GC_NODATA, new NoDataContainer(noData));
        coverageRaster = t;
      }
      Double[] scales = selectElements(this.scales, bands);
      Double[] offsets = selectElements(this.offsets, bands);
      coverageRaster =
          PlanarImage.wrapRenderedImage(
              ImageUtilities.applyRescaling(scales, offsets, coverageRaster, newHints));
    }

    //
    // MASKING INPUT COLOR as indicated
    //
    if (inputTransparentColor != null) {
      coverageRaster =
          new ImageWorker(coverageRaster)
              .setRenderingHints(newHints)
              .makeColorTransparent(inputTransparentColor)
              .getRenderedOperation();
    }

    //
    // External/Internal Masking
    //
    // ROI definition
    ROI roi = null;
    // Using MaskOvrProvider
    if (hasMaskOvrProvider) {
      // Parameter definition
      GridEnvelope ogr = getOriginalGridRange();
      Rectangle sourceRegion;
      if (readP.getSourceRegion() != null) {
        sourceRegion = readP.getSourceRegion();
      } else {
        sourceRegion = new Rectangle(ogr.getSpan(0), ogr.getSpan(1));
      }

      MaskInfo info = maskOvrProvider.getMaskInfo(imageChoice, sourceRegion, readP);
      if (info != null) {
        // Reading Mask
        RenderedOp roiRaster =
            readROIRaster(
                info.streamSpi,
                URLs.fileToUrl(info.file),
                info.index,
                newHints,
                info.readParameters);
        roi = MaskOverviewProvider.scaleROI(roiRaster, coverageRaster.getBounds());
      }
    }

    //
    // BUILDING COVERAGE
    //
    AffineTransform rasterToModel = getRescaledRasterToModel(coverageRaster);
    try {
      return createCoverage(coverageRaster, ProjectiveTransform.create(rasterToModel), roi);
    } catch (Exception e) {
      // dispose and close file
      ImageUtilities.disposePlanarImageChain(coverageRaster);

      // rethrow
      if (e instanceof DataSourceException) {
        throw (DataSourceException) e;
      }
      throw new DataSourceException(e);
    }
  }

  private Double[] selectElements(Double[] source, int[] bands) {
    if (bands == null || source == null) return source;
    Double[] result = new Double[bands.length];
    for (int b = 0; b < bands.length; b++) {
      result[b] = source[bands[b]];
    }
    return result;
  }

  private ImageInputStream getImageInputStream() throws IOException {
    if (inStream instanceof ImageInputStream && !closeMe) {
      return inStream;
    } else if (inStreamSPI == null) {
      return ImageIO.createImageInputStream(source);
    } else if (inStreamSPI instanceof CogImageInputStreamSpi) {
      return ((CogSourceSPIProvider) source).getStream();
    } else {
      return inStreamSPI.createInputStreamInstance(
          source, ImageIO.getUseCache(), ImageIO.getCacheDirectory());
    }
  }

  /**
   * General method for reading an input ROI Mask from a file
   *
   * @return A {@link RenderedOp} representing the Raster ROI
   */
  private RenderedOp readROIRaster(
      ImageInputStreamSpi spi,
      URL inFile,
      int index,
      RenderingHints newHints,
      ImageReadParam readP) {
    // Raster initialization
    RenderedOp raster = null;
    try {
      // ParameterBlock creation
      ParameterBlock pb = new ParameterBlock();
      pb.add(
          spi.createInputStreamInstance(
              inFile, ImageIO.getUseCache(), ImageIO.getCacheDirectory()));
      pb.add(index);
      pb.add(Boolean.FALSE);
      pb.add(Boolean.FALSE);
      pb.add(Boolean.FALSE);
      pb.add(null);
      pb.add(null);
      pb.add(readP);
      pb.add(readerSpi.createReaderInstance());
      raster = JAI.create("ImageRead", pb, newHints != null ? newHints : null);
    } catch (Exception e) {
      LOGGER.severe("Unable to read input Mask Band for coverage: " + coverageName);
    }

    return raster;
  }

  /**
   * Returns the geotiff metadata for this geotiff file.
   *
   * @return the metadata
   */
  @SuppressWarnings({
    "PMD.CloseResource",
    "PMD.UseTryWithResources"
  }) // conditional, might have to close the stream, or not
  public GeoTiffIIOMetadataDecoder getMetadata() {
    GeoTiffIIOMetadataDecoder metadata = null;
    ImageReader reader = null;
    boolean closeMe = true;
    ImageInputStream stream = null;

    try {
      if ((source instanceof InputStream) || (source instanceof ImageInputStream)) {
        closeMe = false;
      }
      if (source instanceof ImageInputStream) {
        stream = (ImageInputStream) source;
      } else {
        inStreamSPI = ImageIOExt.getImageInputStreamSPI(source);
        if (inStreamSPI == null) {
          throw new IllegalArgumentException("No input stream for the provided source");
        }
        stream =
            inStreamSPI.createInputStreamInstance(
                source, ImageIO.getUseCache(), ImageIO.getCacheDirectory());
      }
      if (stream == null) {
        throw new IllegalArgumentException("No input stream for the provided source");
      }
      stream.mark();
      reader = readerSpi.createReaderInstance();
      reader.setInput(stream);
      final IIOMetadata iioMetadata = reader.getImageMetadata(0);
      metadata = new GeoTiffIIOMetadataDecoder(iioMetadata);
    } catch (IOException e) {
      if (LOGGER.isLoggable(Level.SEVERE)) {
        LOGGER.log(Level.SEVERE, e.getMessage(), e);
      }
    } finally {
      if (reader != null)
        try {
          reader.dispose();
        } catch (Throwable t) {
        }

      if (stream != null) {
        try {
          stream.reset();
        } catch (Throwable t) {
        }
        if (closeMe) {
          try {
            stream.close();
          } catch (Throwable t) {
          }
        }
      }
    }
    return metadata;
  }

  /**
   * Creates a {@link GridCoverage} for the provided {@link PlanarImage} using the {@link
   * #raster2Model} that was provided for this coverage.
   *
   * <p>This method is vital when working with coverages that have a raster to model transformation
   * that is not a simple scale and translate.
   *
   * @param image contains the data for the coverage to create.
   * @param raster2Model is the {@link MathTransform} that maps from the raster space to the model
   *     space.
   * @param roi Optional ROI used as Mask
   * @return a {@link GridCoverage}
   */
  protected final GridCoverage2D createCoverage(
      PlanarImage image, MathTransform raster2Model, ROI roi) throws IOException {

    // creating bands
    final SampleModel sm = image.getSampleModel();
    final ColorModel cm = image.getColorModel();
    final int numBands = sm.getNumBands();
    final GridSampleDimension[] bands = new GridSampleDimension[numBands];
    // setting bands names.

    Category noDataCategory = null;
    final Map<String, Object> properties = new HashMap<>();
    if (!Double.isNaN(noData)) {
      noDataCategory =
          new Category(
              Vocabulary.formatInternational(VocabularyKeys.NODATA),
              new Color[] {new Color(0, 0, 0, 0)},
              NumberRange.create(noData, noData));
      CoverageUtilities.setNoDataProperty(properties, Double.valueOf(noData));
      image.setProperty(NoDataContainer.GC_NODATA, new NoDataContainer(noData));
    }
    // Setting ROI Property
    if (roi != null) {
      image.setProperty("ROI", roi);
      CoverageUtilities.setROIProperty(properties, roi);
    }

    Set<String> bandNames = new HashSet<>();
    for (int i = 0; i < numBands; i++) {
      final ColorInterpretation colorInterpretation = TypeMap.getColorInterpretation(cm, i);
      if (colorInterpretation == null) throw new IOException("Unrecognized sample dimension type");
      Category[] categories = null;
      if (noDataCategory != null) {
        categories = new Category[] {noDataCategory};
      }
      String bandName = colorInterpretation.name();
      // make sure we create no duplicate band names
      if (colorInterpretation == ColorInterpretation.UNDEFINED || bandNames.contains(bandName)) {
        bandName = "Band" + (i + 1);
      }
      bandNames.add(bandName);
      bands[i] = new GridSampleDimension(bandName, categories, null);
    }
    if (pamDataset != null) {
      properties.put(GridCoverage2DReader.PAM_DATASET, pamDataset);
    }
    // creating coverage
    if (raster2Model != null) {
      return coverageFactory.create(
          coverageName, image, crs, raster2Model, bands, null, properties);
    }
    return coverageFactory.create(
        coverageName, image, new GeneralBounds(originalEnvelope), bands, null, properties);
  }

  private CoordinateReferenceSystem getCRS(Object source) {
    CoordinateReferenceSystem crs = null;
    if (source instanceof File
        || (source instanceof URL && (((URL) source).getProtocol() == "file"))) {
      // getting name for the prj file
      final String sourceAsString;

      if (source instanceof File) {
        sourceAsString = ((File) source).getAbsolutePath();
      } else {
        String auth = ((URL) source).getAuthority();
        String path = ((URL) source).getPath();
        if (auth != null && !auth.equals("")) {
          sourceAsString = "//" + auth + path;
        } else {
          sourceAsString = path;
        }
      }

      final int index = sourceAsString.lastIndexOf(".");
      final String base =
          index > 0 ? sourceAsString.substring(0, index) + ".prj" : sourceAsString + ".prj";

      // does it exist?
      final File prjFile = new File(base.toString());
      if (prjFile.exists()) {
        // it exists then we have top read it
        try (FileInputStream instream = new FileInputStream(prjFile);
            FileChannel channel = instream.getChannel();
            PrjFileReader projReader = new PrjFileReader(channel)) {
          crs = projReader.getCoordinateReferenceSystem();
        } catch (FactoryException | IOException e) {
          // warn about the error but proceed, it is not fatal
          // we have at least the default crs to use
          LOGGER.log(Level.INFO, e.getLocalizedMessage(), e);
        }
      }
    }
    return crs;
  }

  /**
   * @throws IOException
   */
  static MathTransform parseWorldFile(Object source) throws IOException {
    MathTransform raster2Model = null;

    // TODO: Add support for FileImageInputStreamExt
    // TODO: Check for WorldFile on URL beside the actual connection.
    if (source instanceof File) {
      final File sourceFile = ((File) source);
      String parentPath = sourceFile.getParent();
      String filename = sourceFile.getName();
      final int i = filename.lastIndexOf('.');
      filename = (i == -1) ? filename : filename.substring(0, i);

      // getting name and extension
      final String base =
          (parentPath != null)
              ? new StringBuilder(parentPath).append(File.separator).append(filename).toString()
              : filename;

      // We can now construct the baseURL from this string.
      File file2Parse = new File(new StringBuilder(base).append(".wld").toString());

      if (file2Parse.exists()) {
        final WorldFileReader reader = new WorldFileReader(file2Parse);
        raster2Model = reader.getTransform();
      } else {
        // looking for another extension
        file2Parse = new File(new StringBuilder(base).append(".tfw").toString());

        if (file2Parse.exists()) {
          // parse world file
          final WorldFileReader reader = new WorldFileReader(file2Parse);
          raster2Model = reader.getTransform();
        }
      }
    }
    return raster2Model;
  }

  /**
   * @throws IOException
   */
  static MapInfoFileReader parseMapInfoFile(Object source) throws IOException {
    if (source instanceof File) {
      final File sourceFile = ((File) source);
      File file2Parse = getSibling(sourceFile, ".tab");

      if (file2Parse != null && file2Parse.exists()) {
        final MapInfoFileReader reader = new MapInfoFileReader(file2Parse);
        return reader;
      }
    }
    return null;
  }

  @Override
  protected boolean checkName(
      String coverageName) { // GEOS-6327 - tolerate geotiff_coverage as coverageName
    if ("geotiff_coverage".equalsIgnoreCase(coverageName)) {
      return true;
    } else {
      Utilities.ensureNonNull("coverageName", coverageName);
      return coverageName.equalsIgnoreCase(this.coverageName);
    }
  }

  /**
   * Number of coverages for this reader is 1
   *
   * @return the number of coverages for this reader.
   */
  @Override
  public int getGridCoverageCount() {
    return 1;
  }

  @Override
  public GroundControlPoints getGroundControlPoints() {
    return gcps;
  }

  @Override
  protected List<FileGroup> getFiles() {
    File file = getSourceAsFile();
    if (file == null) {
      return null;
    }

    List<File> files = new ArrayList<>();
    // add all common sidecars
    addAllSiblings(file, files, ".prj", ".tab", ".wld", ".tfw");
    if (hasMaskOvrProvider) {
      DatasetLayout layout = maskOvrProvider.getLayout();
      addSiblings(
          files,
          layout.getExternalMaskOverviews(),
          layout.getExternalOverviews(),
          layout.getExternalMasks());
    }
    return Collections.singletonList(new FileGroup(file, files, null));
  }

  /** Returns the {@link MaskOverviewProvider} used by this reader. For testing purposes. */
  public MaskOverviewProvider getMaskOverviewProvider() {
    // the object is read only once initialized, not dangerous
    return maskOvrProvider;
  }
}
