#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from typing import List, Optional
from abc import ABC, abstractmethod

import rasterio
from rasterio.transform import Affine
from rasterio.io import MemoryFile
from rasterio.io import DatasetReader
from rasterio.vrt import WarpedVRT
from rasterio.enums import Resampling
import numpy as np

from .awt_raster import AWTRaster
from .meta import AffineTransform
from .meta import SampleDimension
from .meta import OutDbMeta


class SedonaRaster(ABC):
    width: int
    height: int
    bands_meta: List[SampleDimension]
    affine_trans: AffineTransform
    crs_wkt: str

    def __init__(self, width: int, height: int, bands_meta: List[SampleDimension],
                 affine_trans: AffineTransform, crs_wkt: str):
        self.width = width
        self.height = height
        self.bands_meta = bands_meta
        self.affine_trans = affine_trans
        self.crs_wkt = crs_wkt

    @abstractmethod
    def as_numpy(self) -> np.array:
        raise NotImplementedError()

    @abstractmethod
    def as_rasterio(self) -> DatasetReader:
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()


class InDbSedonaRaster(SedonaRaster):
    awt_raster: Optional[AWTRaster]
    rasterio_memfile: Optional[MemoryFile]
    rasterio_dataset_reader: Optional[DatasetReader]

    def __init__(self, width: int, height: int, bands_meta: List[SampleDimension],
                 affine_trans: AffineTransform, crs_wkt: str,
                 awt_raster: AWTRaster = None):
        super().__init__(width, height, bands_meta, affine_trans, crs_wkt)
        self.awt_raster = awt_raster
        self.rasterio_memfile = None
        self.rasterio_dataset_reader = None

    def as_numpy(self) -> np.array:
        sm = self.awt_raster.sample_model
        return sm.as_numpy(self.awt_raster.data_buffer)

    def as_rasterio(self) -> DatasetReader:
        if self.rasterio_dataset_reader is not None:
            return self.rasterio_dataset_reader

        # XXX: We have a round trip of writing the raster as GeoTIFF to an
        # in-memory file and then read it back. This is super slow. We'll
        # explore other approaches to make it fast.
        if self.rasterio_memfile is None:
            memfile = MemoryFile(ext='.tif')

            # write to memfile as GeoTIFF. Here dataset is a rasterio.io.DatasetWriter
            driver = "GTiff"
            affine = Affine.from_gdal(
                self.affine_trans.ip_x, self.affine_trans.scale_x, self.affine_trans.skew_x,
                self.affine_trans.ip_y, self.affine_trans.skew_y, self.affine_trans.scale_y)
            num_bands = len(self.bands_meta)
            dataset = memfile.open(
                driver=driver, width=self.width, height=self.height, count=num_bands,
                crs=self.crs_wkt, transform=affine, dtype=rasterio.uint8,
                nodata=None, sharing=False)
            data_array = self.as_numpy()
            dataset.write(data_array)
            dataset.close()

            self.rasterio_memfile = memfile

        # read back. Here dataset is a rasterio.io.DatasetReader
        self.rasterio_dataset_reader = memfile.open(driver=driver)
        return self.rasterio_dataset_reader

    def close(self):
        if self.rasterio_dataset_reader is not None:
           self.rasterio_dataset_reader.close()
           self.rasterio_dataset_reader = None
        if self.rasterio_memfile is not None:
            self.rasterio_memfile.close()
            self.rasterio_memfile = None


class OutDbSedonaRaster(SedonaRaster):
    normalized_path: str
    outdb_meta: Optional[OutDbMeta]
    rasterio_memfile: Optional[MemoryFile]
    rasterio_dataset_reader: Optional[DatasetReader]

    def __init__(self, width: int, height: int, bands_meta: List[SampleDimension],
                 affine_trans: AffineTransform, crs_wkt: str,
                 outdb_meta : OutDbMeta = None):
        super().__init__(width, height, bands_meta, affine_trans, crs_wkt)
        self.outdb_meta = outdb_meta
        self.rasterio_memfile = None
        self.rasterio_dataset_reader = None

        path = self.outdb_meta.path
        if path.startswith("s3a://"):
            path = path.replace("s3a://", "s3://")
        self.normalized_path = path

    def as_numpy(self) -> np.array:
        with rasterio.open(self.normalized_path) as src:
            dst_trans = Affine(
                self.affine_trans.scale_x, self.affine_trans.skew_x, self.affine_trans.ip_x,
                self.affine_trans.skew_y, self.affine_trans.scale_y, self.affine_trans.ip_y)
            with WarpedVRT(src, width=self.width, height=self.height, transform=dst_trans) as vrt:
                band_indices = [b + 1 for b in self.outdb_meta.band_indices]
                arr = vrt.read(band_indices)
                return arr

    def as_rasterio(self) -> DatasetReader:
        if self.rasterio_dataset_reader is not None:
            return self.rasterio_dataset_reader

        if self.rasterio_memfile is None:
            with rasterio.open(self.normalized_path) as src:
                dst_trans = Affine(
                    self.affine_trans.scale_x, self.affine_trans.skew_x, self.affine_trans.ip_x,
                    self.affine_trans.skew_y, self.affine_trans.scale_y, self.affine_trans.ip_y)
                # XXX: WarpedVRT does not support specifying panSrcBands and
                # panDstBands options of GDAL's GDALWarpOptions, so we have to
                # warp all the bands then select the bands we need using
                # vrt.read(band_indices).
                with WarpedVRT(src, width=self.width, height=self.height, transform=dst_trans) as vrt:
                    band_indices = [b + 1 for b in self.outdb_meta.band_indices]
                    arr = vrt.read(band_indices)

                    memfile = MemoryFile(ext='.tif')
                    driver = "GTiff"
                    num_bands = len(band_indices)
                    dataset = memfile.open(
                        driver=driver, width=self.width, height=self.height, count=num_bands,
                        crs=src.crs, transform=dst_trans, dtype=src.dtypes[0],
                        nodata=src.nodata, sharing=False)
                    dataset.write(arr)
                    dataset.close()
                    self.rasterio_memfile = memfile

        self.rasterio_dataset_reader = memfile.open(driver=driver)
        return self.rasterio_dataset_reader

    def close(self):
        if self.rasterio_dataset_reader is not None:
           self.rasterio_dataset_reader.close()
           self.rasterio_dataset_reader = None
        if self.rasterio_memfile is not None:
            self.rasterio_memfile.close()
            self.rasterio_memfile = None
