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
package org.apache.sedona.common.raster.workarounds.imageioext.tiff;

import com.sun.media.imageioimpl.common.PackageUtil;
import it.geosolutions.imageioimpl.plugins.tiff.TIFFImageReaderSpi;
import java.io.IOException;
import java.util.Locale;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

public class SedonaTIFFImageReaderSpi extends TIFFImageReaderSpi {

  public SedonaTIFFImageReaderSpi() {
    super(
        "org.apache.sedona.common.raster.workarounds.imageioext.SedonaTIFFImageReader",
        new String[] {"it.geosolutions.imageioimpl.plugins.tiff.TIFFImageWriterSpi"});
  }

  public String getDescription(Locale locale) {
    String desc = PackageUtil.getSpecificationTitle() + " TIFF Image Reader";
    return desc;
  }

  public boolean canDecodeInput(Object input) throws IOException {
    if (!(input instanceof ImageInputStream)) {
      return false;
    }

    ImageInputStream stream = (ImageInputStream) input;
    byte[] b = new byte[4];
    stream.mark();
    stream.readFully(b);
    stream.reset();

    return (((b[0] == (byte) 0x49
                && b[1] == (byte) 0x49
                && b[2] == (byte) 0x2a
                && b[3] == (byte) 0x00)
            || (b[0] == (byte) 0x4d
                && b[1] == (byte) 0x4d
                && b[2] == (byte) 0x00
                && b[3] == (byte) 0x2a))
        || ((b[0] == (byte) 0x49
                && b[1] == (byte) 0x49
                && b[2] == (byte) 0x2b
                && b[3] == (byte) 0x00)
            || (b[0] == (byte) 0x4d
                && b[1] == (byte) 0x4d
                && b[2] == (byte) 0x00
                && b[3] == (byte) 0x2b)));
  }

  public ImageReader createReaderInstance(Object extension) {
    return new SedonaTIFFImageReader(this);
  }
}
