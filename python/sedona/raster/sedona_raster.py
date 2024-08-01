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

from typing import List, Dict, Optional, Tuple
from abc import ABC, abstractmethod
from xml.etree.ElementTree import Element, SubElement, tostring

import numpy as np
import rasterio                       # type: ignore
import rasterio.env                   # type: ignore
from rasterio.transform import Affine # type: ignore
from rasterio.io import MemoryFile    # type: ignore
from rasterio.io import DatasetReader # type: ignore
from rasterio.windows import Window   # type: ignore

from .awt_raster import AWTRaster
from .data_buffer import DataBuffer
from .meta import AffineTransform, PixelAnchor
from .meta import SampleDimension
from .meta import OutDbMeta
from .gdal_conf import get_rasterio_aws_session


def _rasterio_open(fp, driver=None):
    session = get_rasterio_aws_session(fp)
    if session:
        aws_no_sign_request = 'YES' if session.unsigned else 'NO'
        with rasterio.Env(session=session, AWS_NO_SIGN_REQUEST=aws_no_sign_request):
            return rasterio.open(fp, mode="r", driver=driver)
    else:
        return rasterio.open(fp, mode="r", driver=driver)


def _rasterio_open_memfile(path, memfile: MemoryFile, driver=None, band_indices=None):
    session = get_rasterio_aws_session(path)
    if session:
        aws_no_sign_request = 'YES' if session.unsigned else 'NO'
        with rasterio.Env(session=session, AWS_NO_SIGN_REQUEST=aws_no_sign_request):
            ds = memfile.open(driver=driver)
            arr = None
            # We need to perform a read to initialize the VSI file handles here
            # within current environment. Otherwise VSI file handles may not be
            # correctly created due to GDAL configuration changes.
            if band_indices:
                # Invoked by as_numpy, we need to read band data after loading
                # the DatasetReader
                arr = ds.read(band_indices)
            else:
                if driver == 'VRT':
                    # Read a small portion to initialize all VSI file handles in
                    # current environment
                    ds.read(window=Window(0, 0, 1, 1))
            return ds, arr
    else:
        return memfile.open(driver=driver), None


def _normalize_path(src_path: str) -> str:
    """Normalize an url to conform to GDAL convention

    """
    if src_path.startswith("s3a://"):
        src_path = src_path.replace("s3a://", "s3://")
    elif src_path.startswith("file:"):
        src_path = src_path[5:]
    src_path = src_path.replace("s3://", "/vsis3/")
    return src_path


class SedonaRaster(ABC):
    _width: int
    _height: int
    _bands_meta: List[SampleDimension]
    _affine_trans: AffineTransform
    _crs_wkt: str

    def __init__(self, width: int, height: int, bands_meta: List[SampleDimension],
                 affine_trans: AffineTransform, crs_wkt: str):
        self._width = width
        self._height = height
        self._bands_meta = bands_meta
        self._affine_trans = affine_trans
        self._crs_wkt = crs_wkt

    @property
    def width(self) -> int:
        """Width of the raster in pixel"""
        return self._width

    @property
    def height(self) -> int:
        """Height of the raster in pixel"""
        return self._height

    @property
    def crs_wkt(self) -> str:
        """CRS of the raster as a WKT string"""
        return self._crs_wkt

    @property
    def bands_meta(self) -> List[SampleDimension]:
        """Metadata of bands, including nodata value for each band"""
        return self._bands_meta

    @property
    def affine_trans(self) -> AffineTransform:
        """Geo transform of the raster"""
        return self._affine_trans

    @abstractmethod
    def as_numpy(self) -> np.ndarray:
        """Get the bands data as an numpy array in CHW layout

        """
        raise NotImplementedError()

    def as_numpy_masked(self) -> np.ndarray:
        """Get the bands data as an numpy array in CHW layout, with nodata
        values masked as nan.

        """
        arr = self.as_numpy()
        nodata_values = np.array([bm.nodata for bm in self._bands_meta])
        nodata_values_reshaped = nodata_values[:, None, None]
        mask = arr == nodata_values_reshaped
        masked_arr = np.where(mask, np.nan, arr)
        return masked_arr

    @abstractmethod
    def as_rasterio(self) -> DatasetReader:
        """Retrieve the raster as an rasterio DatasetReader

        """
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        """Release all resources allocated for this sedona raster. The rasterio
        DatasetReader returned by as_rasterio() will also be closed.

        """
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()


class InDbSedonaRaster(SedonaRaster):
    awt_raster: AWTRaster
    rasterio_memfile: Optional[MemoryFile]
    rasterio_dataset_reader: Optional[DatasetReader]

    def __init__(self, width: int, height: int, bands_meta: List[SampleDimension],
                 affine_trans: AffineTransform, crs_wkt: str,
                 awt_raster: AWTRaster):
        super().__init__(width, height, bands_meta, affine_trans, crs_wkt)
        self.awt_raster = awt_raster
        self.rasterio_memfile = None
        self.rasterio_dataset_reader = None

    def as_numpy(self) -> np.ndarray:
        sm = self.awt_raster.sample_model
        return sm.as_numpy(self.awt_raster.data_buffer)

    def as_rasterio(self) -> DatasetReader:
        if self.rasterio_dataset_reader is not None:
            return self.rasterio_dataset_reader

        affine = Affine.from_gdal(
            self._affine_trans.ip_x, self._affine_trans.scale_x, self._affine_trans.skew_x,
            self._affine_trans.ip_y, self._affine_trans.skew_y, self._affine_trans.scale_y)
        num_bands = len(self._bands_meta)

        data_array = np.ascontiguousarray(self.as_numpy())

        dtype = data_array.dtype
        if dtype == np.uint8:
            data_type = 'Byte'
        elif dtype == np.int8:
            data_type = 'Int8'
        elif dtype == np.uint16:
            data_type = 'Uint16'
        elif dtype == np.int16:
            data_type = 'Int16'
        elif dtype == np.uint32:
            data_type = 'UInt32'
        elif dtype == np.int32:
            data_type = 'Int32'
        elif dtype == np.float32:
            data_type = 'Float32'
        elif dtype == np.float64:
            data_type = 'Float64'
        elif dtype == np.int64:
            data_type = 'Int64'
        elif dtype == np.uint64:
            data_type = 'Uint64'
        else:
            raise RuntimeError("unknown dtype: " + str(dtype))

        arr_if = data_array.__array_interface__
        data_pointer = arr_if['data'][0]
        geotransform = (f"{self._affine_trans.ip_x}/{self._affine_trans.scale_x}/{self._affine_trans.skew_x}/" +
                        f"{self._affine_trans.ip_y}/{self._affine_trans.skew_y}/{self._affine_trans.scale_y}")
        # FIXME: GDAL 3.6 shipped with rasterio does not support
        # SPATIALREFERENCE parameter, so we have to workaround this issue in a
        # hacky way. If newer versions of rasterio bundle GDAL 3.7 then this
        # won't be a problem. See https://gdal.org/drivers/raster/mem.html
        desc = (f"MEM:::DATAPOINTER={data_pointer},PIXELS={self._width},LINES={self._height},BANDS={num_bands}," +
                f"DATATYPE={data_type},GEOTRANSFORM={geotransform}")

        # construct a VRT to wrap this MEM dataset, with SRS set up properly
        vrt_xml = OutDbSedonaRaster.generate_vrt_xml(
            desc, data_type, self._width, self._height, geotransform.replace('/', ','), self._crs_wkt,
            0, 0, list(range(num_bands)))

        # dataset = _rasterio_open(desc, driver="MEM")
        self.rasterio_memfile = MemoryFile(vrt_xml, ext='.vrt')
        dataset = self.rasterio_memfile.open(driver='VRT')

        # XXX: dataset does not copy the data held by data_array, so we set
        # data_array as a property of dataset to make sure that the lifetime of
        # data_array is as long as dataset, otherwise we may see band data
        # corruption.
        dataset.mem_data_array = data_array
        return dataset

    def close(self):
        if self.rasterio_dataset_reader is not None:
           self.rasterio_dataset_reader.close()
           self.rasterio_dataset_reader = None
        if self.rasterio_memfile is not None:
            self.rasterio_memfile.close()
            self.rasterio_memfile = None


class OutDbSedonaRasterBase(SedonaRaster):
    _outdb_meta: OutDbMeta

    @property
    def outdb_meta(self) -> OutDbMeta:
        return self._outdb_meta

    @property
    def path(self) -> str:
        return self.outdb_meta.path

    def set_path(self, path: str):
        self.outdb_meta.path = path

    def as_numpy(self) -> np.ndarray:
        ds = self.as_rasterio()
        band_indices = [b + 1 for b in self._outdb_meta.band_indices]
        arr = ds.read(band_indices)
        return arr


class OutDbSedonaRaster(OutDbSedonaRasterBase):
    rasterio_memfile: Optional[MemoryFile]
    rasterio_dataset_reader: Optional[DatasetReader]

    def __init__(self, width: int, height: int, bands_meta: List[SampleDimension],
                 affine_trans: AffineTransform, crs_wkt: str,
                 outdb_meta : OutDbMeta):
        super().__init__(width, height, bands_meta, affine_trans, crs_wkt)
        self._outdb_meta = outdb_meta
        self.rasterio_memfile = None
        self.rasterio_dataset_reader = None

    def as_numpy(self) -> np.ndarray:
        band_indices = [b + 1 for b in self._outdb_meta.band_indices]
        ds, arr = self._as_rasterio(band_indices)
        if arr is None:
            arr = ds.read(band_indices)
        return arr

    def as_rasterio(self) -> DatasetReader:
        return self._as_rasterio(None)[0]

    def _as_rasterio(self, load_bands: Optional[List[int]]) -> Tuple[DatasetReader, Optional[np.ndarray]]:
        if self.rasterio_dataset_reader is not None:
            return self.rasterio_dataset_reader, None

        src_path = _normalize_path(self._outdb_meta.path)
        src = _rasterio_open(src_path)
        ip_x, scale_x, skew_x, ip_y, skew_y, scale_y = src.get_transform()
        crs_wkt = src.crs.wkt if src.crs is not None else None
        if not self.is_vrt_needed(src):
            # Fast path: the geo-reference of the out-db raster is the same as the original raster,
            # the original raster is not clipped, no need to construct VRT.
            self.rasterio_dataset_reader = src
            return src, None
        else:
            src.close()
            if self.rasterio_memfile is None:
                # XXX: WarpedVRT does not support specifying panSrcBands and
                # panDstBands options of GDAL's GDALWarpOptions, so we cannot use
                # WarpedVRT directly. As a workaround we construct an in-memory VRT
                # XML file and open it using the VRT driver.
                off_x = round((self._affine_trans.ip_x - ip_x) / scale_x)
                off_y = round((self._affine_trans.ip_y - ip_y) / scale_y)
                width  = self._width
                height = self._height
                band_indices = self.outdb_meta.band_indices
                geo_transform = (f"{self._affine_trans.ip_x}, {self._affine_trans.scale_x}, {self._affine_trans.skew_x}, " +
                                 f"{self._affine_trans.ip_y}, {self._affine_trans.skew_y}, {self._affine_trans.scale_y}")

                dt = self._outdb_meta.data_type
                if dt == DataBuffer.TYPE_BYTE:
                    data_type = 'Byte'
                elif dt == DataBuffer.TYPE_USHORT:
                    data_type = 'UInt16'
                elif dt == DataBuffer.TYPE_SHORT:
                    data_type = 'Int16'
                elif dt == DataBuffer.TYPE_INT:
                    data_type = 'Int32'
                elif dt == DataBuffer.TYPE_FLOAT:
                    data_type = 'Float32'
                elif dt == DataBuffer.TYPE_DOUBLE:
                    data_type = 'Float64'
                else:
                    raise RuntimeError("unknown outdb band data type: " + str(dt))

                # assemble a VRT XML file to describe how we want to retrieve the sub region
                vrt_xml = self.generate_vrt_xml(src_path, data_type, width, height, geo_transform, crs_wkt, off_x, off_y, band_indices)
                self.rasterio_memfile = MemoryFile(vrt_xml, ext='.vrt')

            ds, arr = _rasterio_open_memfile(src_path, self.rasterio_memfile, driver='VRT', band_indices=load_bands)
            self.rasterio_dataset_reader = ds
            return ds, arr

    def is_vrt_needed(self, ds: DatasetReader) -> bool:
        return not (
            self._width == ds.width and
            self._height == ds.height and
            self._outdb_meta.band_indices == [k - 1 for k in ds.indexes]
        )

    def close(self):
        if self.rasterio_dataset_reader is not None:
           self.rasterio_dataset_reader.close()
           self.rasterio_dataset_reader = None
        if self.rasterio_memfile is not None:
            self.rasterio_memfile.close()
            self.rasterio_memfile = None

    @classmethod
    def generate_vrt_xml(cls, src_path, data_type, width, height, geo_transform, crs_wkt, off_x, off_y, band_indices) -> bytes:
        # Create root element
        root = Element('VRTDataset')
        root.set('rasterXSize', str(width))
        root.set('rasterYSize', str(height))

        # Add CRS
        if crs_wkt is not None and crs_wkt != '':
            srs = SubElement(root, 'SRS')
            srs.text = crs_wkt

        # Add GeoTransform
        gt = SubElement(root, 'GeoTransform')
        gt.text = geo_transform

        # Add bands
        for i, band_index in enumerate(band_indices, start=1):
            band = SubElement(root, 'VRTRasterBand')
            band.set('dataType', data_type)
            band.set('band', str(i))

            # Add source
            source = SubElement(band, 'SimpleSource')
            src_prop = SubElement(source, 'SourceFilename')
            src_prop.text = src_path

            # Set source properties
            SubElement(source, 'SourceBand').text = str(band_index + 1)
            SubElement(source, 'SrcRect', {'xOff': str(off_x), 'yOff': str(off_y), 'xSize': str(width), 'ySize': str(height)})
            SubElement(source, 'DstRect', {'xOff': '0', 'yOff': '0', 'xSize': str(width), 'ySize': str(height)})

        # Generate pretty XML
        xml_bytes = tostring(root, encoding='utf-8')
        return xml_bytes


class LazyLoadOutDbSedonaRaster(OutDbSedonaRasterBase):
    _path: str
    params: Optional[Dict[str, str]]
    rasterio_dataset_reader: Optional[DatasetReader]

    def __init__(self, path: str, params: Optional[Dict[str, str]]):
        super().__init__(-1, -1, [], AffineTransform(0, 0, 0, 0, 0, 0, PixelAnchor.UPPER_LEFT), "")
        self._outdb_meta = OutDbMeta(DataBuffer.TYPE_BYTE, [], "", None)
        self._path = path
        self.params = params
        self.rasterio_dataset_reader = None

    def _ensure_loaded(self):
        if self.rasterio_dataset_reader is not None:
            return

        # Load the raster file and extract its metadata
        src_path = _normalize_path(self._path)
        ds = _rasterio_open(src_path)
        ip_x, scale_x, skew_x, ip_y, skew_y, scale_y = ds.get_transform()
        crs_wkt = ds.crs.wkt if ds.crs is not None else None
        affine_trans = AffineTransform(scale_x, skew_y, skew_x, scale_y, ip_x, ip_y, PixelAnchor.UPPER_LEFT)
        width = ds.width
        height = ds.height
        bands_meta = []
        band_indices = []
        nodatavals = ds.nodatavals
        dtype = ds.dtypes[0]

        if dtype in ('uint8', 'int8'):
            data_type = DataBuffer.TYPE_BYTE
        elif dtype == 'uint16':
            data_type = DataBuffer.TYPE_USHORT
        elif dtype == 'int16':
            data_type = DataBuffer.TYPE_SHORT
        elif dtype in ('int32', 'uint32'):
            data_type = DataBuffer.TYPE_INT
        elif dtype == 'float32':
            data_type = DataBuffer.TYPE_FLOAT
        elif dtype == 'float64':
            data_type = DataBuffer.TYPE_DOUBLE
        else:
            raise RuntimeError("unknown rasterio band data type: " + dtype)

        for idx, band_idx in enumerate(ds.indexes):
            sample_dimension = SampleDimension(f"band_{band_idx}", 0.0, 1.0, nodatavals[idx])
            bands_meta.append(sample_dimension)
            band_indices.append(idx)

        # Initialize the internal states with raster metadata
        super().__init__(width, height, bands_meta, affine_trans, crs_wkt)
        self._outdb_meta = OutDbMeta(data_type, band_indices, self._path, self.params)

        # Keep the reference to the rasterio DatasetReader for future usage
        self.rasterio_dataset_reader = ds

    def as_rasterio(self) -> DatasetReader:
        self._ensure_loaded()
        return self.rasterio_dataset_reader

    def close(self):
        if self.rasterio_dataset_reader is not None:
           self.rasterio_dataset_reader.close()
           self.rasterio_dataset_reader = None

    @property
    def path(self) -> str:
        return self._path

    def set_path(self, path: str):
        if self.rasterio_dataset_reader is not None:
            raise RuntimeError("Cannot set the path of an already loaded lazy-loaded out-db raster")
        self._path = path

    @property
    def width(self) -> int:
        self._ensure_loaded()
        return self._width

    @property
    def height(self) -> int:
        self._ensure_loaded()
        return self._height

    @property
    def crs_wkt(self) -> str:
        self._ensure_loaded()
        return self._crs_wkt

    @property
    def bands_meta(self) -> List[SampleDimension]:
        self._ensure_loaded()
        return self._bands_meta

    @property
    def affine_trans(self) -> AffineTransform:
        self._ensure_loaded()
        return self._affine_trans

    @property
    def outdb_meta(self) -> OutDbMeta:
        self._ensure_loaded()
        return self._outdb_meta
