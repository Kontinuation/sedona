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

import com.sun.media.imageioimpl.common.BogusColorSpace;
import java.awt.RenderingHints;
import java.awt.Transparency;
import java.awt.image.ComponentColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.awt.image.renderable.ParameterBlock;
import javax.media.jai.ImageLayout;
import javax.media.jai.JAI;
import javax.media.jai.RenderedOp;

/** Utility functions for image processing. */
public class ImageUtils {
  private ImageUtils() {}

  /**
   * Crop and translate an image, so that the result image will have origin (0, 0), containing
   * cropped pixels of the original image.
   *
   * @param image the original image
   * @param offsetX the x offset of the crop rect
   * @param offsetY the y offset of the crop rect
   * @param width the width of the crop rect
   * @param height the height of the crop rect
   * @return the cropped and translated image
   */
  public static RenderedImage cropAndTranslateImage(
      RenderedImage image, int offsetX, int offsetY, int width, int height) {
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

  /**
   * Select bands from a rendered image. If the band indices are the same as the original image,
   * return the original image.
   *
   * @param image the original image
   * @param bandIndices the band indices to select (0-based)
   * @return the selected image
   */
  public static RenderedImage selectBands(RenderedImage image, int[] bandIndices) {
    if (isBandIndicesIdentity(image, bandIndices)) {
      return image;
    }

    // Create band selected image
    ParameterBlock bandSelectParams = new ParameterBlock();
    bandSelectParams.addSource(image);
    bandSelectParams.add(bandIndices);
    return JAI.create("bandSelect", bandSelectParams);
  }

  /**
   * Check if the band indices are selecting all bands of the image sequentially.
   *
   * @param image the image
   * @param bandIndices the band indices (0-based)
   * @return true if the band indices are selecting all bands of the image
   */
  private static boolean isBandIndicesIdentity(RenderedImage image, int[] bandIndices) {
    int numBands = image.getSampleModel().getNumBands();
    if (numBands != bandIndices.length) {
      return false;
    }
    for (int k = 0; k < bandIndices.length; k++) {
      if (bandIndices[k] != k) {
        return false;
      }
    }
    return true;
  }

  /**
   * Copy a raster to another raster, with padding if necessary.
   *
   * @param sourceRaster the source raster
   * @param sourceBand the source band
   * @param destRaster the destination raster, which must not be smaller than the source raster
   * @param destBand the destination band
   * @param padValue the padding value, or NaN if no padding is needed
   */
  public static void copyRasterWithPadding(
      Raster sourceRaster,
      int sourceBand,
      WritableRaster destRaster,
      int destBand,
      double padValue) {
    int destWidth = destRaster.getWidth();
    int destHeight = destRaster.getHeight();
    int destMinX = destRaster.getMinX();
    int destMinY = destRaster.getMinY();
    int sourceWidth = sourceRaster.getWidth();
    int sourceHeight = sourceRaster.getHeight();
    int sourceMinX = sourceRaster.getMinX();
    int sourceMinY = sourceRaster.getMinY();
    if (sourceWidth > destWidth || sourceHeight > destHeight) {
      throw new IllegalArgumentException("Source raster is larger than destination raster");
    }

    // Copy the source raster to the destination raster
    double[] samples =
        sourceRaster.getSamples(
            sourceMinX, sourceMinY, sourceWidth, sourceHeight, sourceBand, (double[]) null);
    destRaster.setSamples(destMinX, destMinY, sourceWidth, sourceHeight, destBand, samples);

    // Pad the right edge
    for (int y = destMinY; y < sourceHeight + destMinY; y++) {
      for (int x = sourceWidth + destMinX; x < destWidth + destMinX; x++) {
        destRaster.setSample(x, y, destBand, padValue);
      }
    }
    // Pad the bottom edge
    for (int y = sourceHeight + destMinY; y < destHeight + destMinY; y++) {
      for (int x = destMinX; x < destWidth + destMinX; x++) {
        destRaster.setSample(x, y, destBand, padValue);
      }
    }
  }

  /**
   * Cast the data type of a rendered image.
   *
   * @param image the image
   * @param dataType the data type
   * @return the image with the specified data type
   */
  public static RenderedImage castDataType(RenderedImage image, int dataType) {
    ParameterBlock params = new ParameterBlock();

    // Using the image layout hint to force the output image to have the same number of
    // bands as the input image. This is necessary because the default behavior of the
    // "format" operation will convert the color model and sample model. For instance,
    // the source image may have 1 band and an indexed color model, but the output image
    // will have 3 bands and a component color model.
    ImageLayout il = new ImageLayout();
    int numBands = image.getSampleModel().getNumBands();
    ComponentColorModel colorModel =
        new ComponentColorModel(
            new BogusColorSpace(numBands), false, false, Transparency.OPAQUE, dataType);
    il.setColorModel(colorModel);
    il.setSampleModel(image.getSampleModel());
    RenderingHints hint = new RenderingHints(JAI.KEY_IMAGE_LAYOUT, il);

    params.addSource(image);
    params.add(dataType);
    return JAI.create("format", params, hint);
  }
}
