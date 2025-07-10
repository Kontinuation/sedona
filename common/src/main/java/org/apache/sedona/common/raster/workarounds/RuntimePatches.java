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
package org.apache.sedona.common.raster.workarounds;

import it.geosolutions.jaiext.ConcurrentOperationRegistry;
import it.geosolutions.jaiext.JAIExt;
import javax.media.jai.registry.RenderedRegistryMode;
import org.apache.sedona.common.raster.workarounds.geotools.geotiff.SedonaGeoTiffReader;
import org.apache.sedona.common.raster.workarounds.jaiext.SedonaBandMergeCRIF;
import org.geotools.api.data.DataSourceException;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.util.factory.Hints;

public class RuntimePatches {

  public static AbstractGridCoverage2DReader createGeoTiffReader(Object input, Hints uHints)
      throws DataSourceException {
    // Create a modified version of GeoTiff reader to make use of the patched TIFF reader
    // Please see the following link for more details:
    // https://github.com/geosolutions-it/imageio-ext/pull/308
    return new SedonaGeoTiffReader(input, uHints);
  }

  public static void patchBandMerge() {
    // HACK: Patch a bug of the BandMerge operator in JAI-Ext. Please see the following link for
    // more details:
    // https://github.com/geosolutions-it/jai-ext/issues/299
    ConcurrentOperationRegistry registry = JAIExt.getRegistry();
    Object factory = new SedonaBandMergeCRIF();
    registry.registerFactory(
        RenderedRegistryMode.MODE_NAME, "BandMerge", "it.geosolutions.jaiext", factory);
  }
}
