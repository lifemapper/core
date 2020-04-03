"""Layer module
"""
import csv
import glob
from io import StringIO
import os
import subprocess
import zipfile

from osgeo import gdal, gdalconst, ogr, osr

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.attribute_object import LmAttObj
from LmCommon.common.lmconstants import (
    GEOTIFF_INTERFACE, OFTInteger, OFTString, SHAPEFILE_INTERFACE)
from LmCommon.common.time import gmt
from LmCommon.common.verify import compute_hash, verify_hash
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.service_object import ServiceObject
from LmServer.common.lmconstants import (
    GDALDataTypes, OGRDataTypes, LMFormat, LMServiceType, OccurrenceFieldNames,
    UPLOAD_PATH)
from LmServer.common.localconstants import APP_PATH, DEFAULT_EPSG

DEFAULT_OGR = LMFormat.SHAPE
DEFAULT_GDAL = LMFormat.GTIFF


# .............................................................................
class _Layer(LMSpatialObject, ServiceObject):
    """Superclass for all spatial layer objects."""
    META_IS_CATEGORICAL = 'isCategorical'
    META_IS_DISCRETE = 'isDiscrete'

    # .............................
    def __init__(self, name, user_id, epsg_code, lyr_id=None, squid=None,
                 ident=None, verify=None, dlocation=None, metadata=None,
                 data_format=None, gdal_type=None, ogr_type=None,
                 val_units=None, val_attribute=None, nodata_val=None,
                 min_val=None, max_val=None, resolution=None, bbox=None,
                 map_units=None, svc_obj_id=None,
                 service_type=LMServiceType.LAYERS, metadata_url=None,
                 parent_metadata_url=None, mod_time=None):
        """Layer superclass constructor

        Args:
            name: layer name, unique with user_id and epsq
            user_id: user identifier
            epsg_code (int): EPSG code indicating the SRS to use
            lyr_id (int): record identifier for layer
            squid (int): locally unique identifier for taxa-related data
            ident (int): locally unique identifier for taxa-related data
            verify: hash of the data for verification
            dlocation: data location (url, file path, ...)
            metadata: dictionary of metadata key/values; uses class or
                superclass attribute constants META_* as keys
            data_format: ogr or gdal code for spatial data file format
                GDAL Raster Format code at
                    http://www.gdal.org/formats_list.html.
                OGR Vector Format code at http://www.gdal.org/ogr_formats.html
            gdal_type (osgeo.gdalconst): GDAL data_type
                GDALDataType in http://www.gdal.org/gdal_8h.html
            ogr_type (int): OGR geometry type (wkbPoint, ogr.wkbPolygon, etc)
                OGRwkbGeometryType in http://www.gdal.org/ogr/ogr__core_8h.html
            val_units: units of measurement for data
            val_attribute: field containing data values of interest
            nodata_val: value indicating feature/pixel does not contain data
            min_val: smallest value in data
            max_val: largest value in data
            resolution: resolution of the data - pixel size in @map_units
            bbox: spatial extent of data
                sequence in the form (min_x, min_y, max_x, max_y)
                or comma-delimited string in the form
                'min_x, min_y, max_x, max_y'
            map_units: units of measurement for the data.
                These are keywords as used in  mapserver, choice of
                LmCommon.common.lmconstants.LegalMapUnits
                described in
                http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
            svc_obj_id (int): unique db record identifier for an object
                retrievable by REST web services.  May be filled with the
                base layer_id or a unique parameterized id
            service_type: type of data for REST retrieval LMServiceType.LAYERS
            metadata_url: REST URL for API returning this object
            parent_metadata_url: REST URL for API returning this parent object
            mod_time: time of last modification, in MJD
        """
        if svc_obj_id is None:
            svc_obj_id = lyr_id
        LMSpatialObject.__init__(self, epsg_code, bbox, map_units)
        ServiceObject.__init__(
            self, user_id, svc_obj_id, service_type, metadata_url=metadata_url,
            parent_metadata_url=parent_metadata_url, mod_time=mod_time)
#        ogr.UseExceptions()
        self.name = name
        self._layer_user_id = user_id
        self._layer_id = lyr_id
        self.squid = squid
        self.ident = ident
        self.layer_metadata = {}
        self.load_layer_metadata(metadata)
        self._data_format = data_format
        self._gdal_type = gdal_type
        self._ogr_type = ogr_type
        self.val_units = val_units
        self._val_attribute = val_attribute
        self.nodata_val = nodata_val
        self.min_val = min_val
        self.max_val = max_val
        self.resolution = resolution
        self._dlocation = None
        self.set_dlocation(dlocation)
        self._verify = None
        # self.set_verify(verify=verify)
        self._map_filename = None
        self._meta_location = None

    # .............................
    def set_layer_id(self, lyr_id):
        """Sets the database id of the Layer record.

        Args:
            lyr_id: The record id for the database
        """
        self._layer_id = lyr_id

    # .............................
    def get_layer_id(self):
        """Returns the database id of the layer record."""
        return self._layer_id

    # .............................
    @property
    def data_format(self):
        """Gets the data format of the layer"""
        return self._data_format

    # .............................
    @property
    def gdal_type(self):
        """Gets the GDAL data format of the layer"""
        return self._gdal_type

    # .............................
    @property
    def ogr_type(self):
        """Gets the OGR data format of the layer"""
        return self._ogr_type

    # .............................
    @property
    def val_attribute(self):
        """Get the value attribute of the layer"""
        return self._val_attribute

    # .............................
    @staticmethod
    def read_data(dlocation, driver_type):
        """Reads OGR- or GDAL data.

        Saves vector features on the _Layer object

        Args:
            dlocation: file location of the data
            ogr_type: GDAL or OGR-supported data format type code, available at
                http://www.gdal.org/formats_list.html and
                http://www.gdal.org/ogr/ogr_formats.html

        Returns:
            boolean for success/failure

        Raises:
            LMError on call on superclass or failure to read data.
        """
        raise LMError('read_data must be implemented in Subclass')

    # .............................
    def compute_hash(self, dlocation=None, content=None):
        """Computes the sha256sum of the file at dlocation.

        Args:
            dlocation: file location of the data
            content: data stream of the data

        Returns:
            a hash string for data file
        """
        if content is not None:
            value = compute_hash(content=content)
        else:
            if dlocation is None:
                dlocation = self._dlocation
            value = compute_hash(dlocation=dlocation)

        return value

    # .............................
    def verify_hash(self, hashval, dlocation=None, content=None):
        """Compares the sha256sum of the file at dlocation with provided hash

        Args:
            hashval: hash string to compare with data
            dlocation: file location of the data
            content: data stream of the data

        Returns:
            boolean for equal or not equal hashvals
        """
        if content is not None:
            verified = verify_hash(hashval, content=content)
        else:
            if dlocation is None:
                dlocation = self._dlocation
            verified = verify_hash(hashval, dlocation=dlocation)
        return verified

    # .............................
    def set_verify(self, verify=None, dlocation=None, content=None):
        """Sets the sha256sum of the file

        Args:
            verify: hash string to set for the data
            dlocation: file location of the data
            content: data stream of the data
        """
        value = None
        if verify is not None:
            self._verify = verify
        else:
            if content is not None:
                value = self.compute_hash(content=content)
            else:
                if dlocation is None:
                    dlocation = self._dlocation
                if dlocation is not None and os.path.exists(dlocation):
                    value = self.compute_hash(dlocation=dlocation)
            self._verify = value

    # .............................
    def clear_verify(self):
        """Clears the sha256sum of the file"""
        self._verify = None

    # .............................
    @property
    def verify(self):
        """Get the verify hash value"""
        return self._verify

    # .............................
    def get_meta_location(self):
        """Returns the location of metadata file"""
        return self._meta_location

    # .............................
    def get_relative_dlocation(self):
        """Returns the relative filepath for object data

        Note:
            If the object does not have an ID, this returns None
        """
        basename = None
        self.set_dlocation()
        if self._dlocation is not None:
            _, basename = os.path.split(self._dlocation)
        return basename

    # .............................
    def create_local_dlocation(self, extension):
        """Creates an absolute filepath from object attributes

        Args:
            extension: extension for the filename

        Note:
            If the object does not have an ID, this returns None
        """
        dloc = self._earl_jr.create_other_layer_filename(
            self._layer_user_id, self._epsg, self.name, ext=extension)
        return dloc

    # .............................
    def get_dlocation(self):
        """Return the _dlocation attribute

        Note:
            Do not create and populate value by default.
        """
        return self._dlocation

    # .............................
    def clear_dlocation(self):
        """Clears the _dlocation attribute"""
        self._dlocation = None
        self._absolute_path = None
        self._base_filename = None

    # .............................
    def set_dlocation(self, dlocation=None):
        """Set the Layer._dlocation attribute if it is None.

        Args:
            dlocation: optional file location of the data

        Note:
            If argument is None, compute dlocation based on object attributes
            Does NOT override existing dlocation, use clear_dlocation for that

        Todo:
            Fix this, call to create_local_dlocation needs an extension
                argument.  Does this make sense on the base class?
        """
        # Only set DLocation if it is currently None
        if self._dlocation is None:
            if dlocation is None:
                dlocation = self.create_local_dlocation()
            self._dlocation = dlocation
        # Populate absolutePath and baseFilename attributes
        if self._dlocation is not None:
            self._absolute_path, self._base_filename = os.path.split(
                self._dlocation)
        else:
            self._absolute_path, self._base_filename = None, None

    # .............................
    @staticmethod
    def clear_data():
        """Clear layer data"""
        raise LMError('Method must be implemented in subclass')

    # .............................
    @staticmethod
    def copy_data():
        """Copy layer data"""
        raise LMError('Method must be implemented in subclass')

    # .............................
    def get_absolute_path(self):
        """Returns the absolute path, without filename, of the object"""
        if self._absolute_path is None and self._dlocation is not None:
            self._absolute_path, self._base_filename = os.path.split(
                self._dlocation)
        return self._absolute_path

    # .............................
    def get_base_filename(self):
        """Returns the base filename, without path, of the object"""
        if self._base_filename is None and self._dlocation is not None:
            self._absolute_path, self._base_filename = os.path.split(
                self._dlocation)
        return self._base_filename

    # .............................
    def dump_layer_metadata(self):
        """Returns a string of the dictionary of metadata for this object"""
        return super(_Layer, self)._dump_metadata(self.layer_metadata)

    # .............................
    def load_layer_metadata(self, new_metadata):
        """Loads a dictionary or JSON object of metadata into this object"""
        self.layer_metadata = super(_Layer, self)._load_metadata(new_metadata)

    # .............................
    def add_layer_metadata(self, new_metadata_dict):
        """Adds dictionary of metadata to the existing metadata for this object
        """
        self.layer_metadata = super(
            _Layer, self)._add_metadata(
                new_metadata_dict, existing_metadata_dict=self.layer_metadata)

    # .............................
    def update_layer(self, mod_time, metadata=None):
        """Updates mod_time, data verification hash, metadata of the object"""
        self.update_mod_time(mod_time)
        if metadata is not None:
            self.load_layer_metadata(metadata)
        self.set_verify()


# .............................................................................
class _LayerParameters(LMObject):
    """Decorator for _Layer class"""
    # Constants for metadata dictionary keys
    PARAM_FILTER_STRING = 'filterString'
    PARAM_VAL_NAME = 'valName'
    PARAM_VAL_UNITS = 'valUnits'

    # .............................
    def __init__(self, user_id, param_id=None, matrix_index=-1, metadata=None,
                 mod_time=None):
        """Constructor for _LayerParameters decorator class

        Args:
            user_id: user identifier
                if these parameters are not held in a separate db table, this
                value is the same as _Layer._layer_user_id
            param_id: database Id for the parameter values
                if these parameters are not held in a separate db table, this
                value is the same as _Layer._layer_id.
            matrix_index: index of the position in PAM or other matrix.  If
                this Parameterized Layer is not a Matrix input, value is -1.
            metadata: dictionary of metadata keys/values; key constants are
                class attributes.
            mod_time: time/date last modified in MJD
        """
        self._param_user_id = user_id
        self._param_id = param_id
        self.param_metadata = {}
        self.load_param_metadata(metadata)
        self._matrix_index = matrix_index
        self.param_mod_time = mod_time

    # .............................
    def dump_param_metadata(self):
        """Returns a string of the metadata for this object"""
        return super(
            _LayerParameters, self)._dump_metadata(self.param_metadata)

    # .............................
    def load_param_metadata(self, new_metadata):
        """Loads metadata into a dictionary for this object

        Args:
            new_metadata: dictionary or JSON object of metadata
        """
        self.param_metadata = super(
            _LayerParameters, self)._load_metadata(new_metadata)

    # .............................
    def add_param_metadata(self, new_metadata_dict):
        """Adds metadata to the dictionary for this object

        Args:
            new_metadata_dict: dictionary of metadata
        """
        self.param_metadata = super(
            _LayerParameters, self)._add_metadata(
                new_metadata_dict, existing_metadata_dict=self.param_metadata)

    # .............................
    def set_param_id(self, param_id):
        """Sets the database id of the Layer Parameters record

        Args:
            param_id: The db record id
        """
        self._param_id = param_id

    # .............................
    def get_param_id(self):
        """
        @summary: Returns the database id of the Layer Parameters (either
                     PresenceAbsence or AncillaryValues) record, which can be
                     used by multiple Parameterized Layer objects
        """
        return self._param_id

    # .............................
    def set_param_user_id(self, usr):
        """Sets the User id of the Layer Parameters

        Args:
            usr: user id for the parameters
        """
        self._param_user_id = usr

    # .............................
    def get_param_user_id(self):
        """Returns the User id of the Layer Parameters
        """
        return self._param_user_id

    # .............................
    def set_matrix_index(self, matrix_idx):
        """Sets the _matrix_index on the object

        Args:
            matrix_index: position of a layer in a MatrixLayerset

        Note:
            Not relevant for layers not in a MatrixLayerset
        """
        self._matrix_index = matrix_idx

    # .............................
    def get_matrix_index(self):
        """Returns _matrix_index on the layer.

        Note:
            Not relevant for layers not in a MatrixLayerset
        """
        return self._matrix_index

    # .............................
    def set_tree_index(self, tree_idx):
        """Sets the _tree_index on the object

        Args:
            tree_index: position of a layer in a Tree

        Note:
            Not relevant for layers not in a Tree
        """
        self._tree_index = tree_idx

    # .............................
    def get_tree_index(self):
        """Returns the _tree_index on the object

        Note:
            Not relevant for layers not in a Tree
        """
        return self._tree_index

    # .............................
    def update_params(self, mod_time, matrix_index=None, metadata=None):
        """Updates matrix_index, param_metadata, and mod_time on the object.

        Args:
            mod_time: time/date last modified
            matrix_index: position of a layer in a MatrixLayerset; if not in
                a MatrixLayerset, value is -1.
            metadata: dictionary of metadata keys/values; key constants are
                class attributes.

        Note:
            Missing metadata keyword parameters are ignored.
        """
        self.param_mod_time = mod_time
        if metadata is not None:
            self.load_param_metadata(metadata)
        if matrix_index is not None:
            self._matrix_index = matrix_index


# .............................................................................
class Raster(_Layer):
    """Class to hold information about a raster dataset."""

    # .............................
    def __init__(self, name, user_id, epsg_code, lyr_id=None, squid=None,
                 ident=None, verify=None, dlocation=None, metadata=None,
                 data_format=DEFAULT_GDAL.driver, gdal_type=None,
                 val_units=None, nodata_val=None, min_val=None, max_val=None,
                 resolution=None, bbox=None, map_units=None, svc_obj_id=None,
                 service_type=LMServiceType.LAYERS, metadata_url=None,
                 parent_metadata_url=None, mod_time=None):
        """Constructor for Raster superclass, inherits from _Layer

        Args:
            name: layer name, unique with user_id and epsq
            user_id: user identifier
            epsg_code (int): EPSG code indicating the SRS to use
            lyr_id (int): record identifier for layer
            squid (int): locally unique identifier for taxa-related data
            ident (int): locally unique identifier for taxa-related data
            verify: hash of the data for verification
            dlocation: data location (url, file path, ...)
            metadata: dictionary of metadata key/values; uses class or
                superclass attribute constants META_* as keys
            data_format: gdal code for spatial data file format
                GDAL Raster Format code at
                    http://www.gdal.org/formats_list.html.
            gdal_type (osgeo.gdalconst): GDAL data_type
                GDALDataType in http://www.gdal.org/gdal_8h.html
            val_units: units of measurement for data
            nodata_val: value indicating feature/pixel does not contain data
            min_val: smallest value in data
            max_val: largest value in data
            resolution: resolution of the data - pixel size in @map_units
            bbox: spatial extent of data
                sequence in the form (min_x, min_y, max_x, max_y)
                or comma-delimited string in the form
                'min_x, min_y, max_x, max_y'
            map_units: units of measurement for the data. These are keywords as
                used in  mapserver, choice of LegalMapUnits described in
                http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
            svc_obj_id (int): unique db record identifier for an object
                retrievable by REST web services.  May be filled with the base
                layer_id or a unique parameterized id
            service_type: type of data for REST retrieval LMServiceType.LAYERS
            metadata_url: REST URL for API returning this object
            parent_metadata_url: REST URL for API returning this parent object
            mod_time: time of last modification, in MJD
        """
        self.verify_data_description(gdal_type, data_format)
        if all([
                dlocation, os.path.exists(dlocation),
                any([verify is None, gdal_type is None, data_format is None,
                     resolution is None, bbox is None, min_val is None,
                     max_val is None, nodata_val is None])]):
            (dlocation, verify, gdal_type, data_format, bbox, resolution,
             min_val, max_val, nodata_val) = self.populate_stats(
                 dlocation, verify, gdal_type, data_format, bbox, resolution,
                 min_val, max_val, nodata_val)
        _Layer.__init__(
            self, name, user_id, epsg_code, lyr_id=lyr_id, squid=squid,
            ident=ident, verify=verify, dlocation=dlocation, metadata=metadata,
            data_format=data_format, gdal_type=gdal_type, val_units=val_units,
            val_attribute='pixel', nodata_val=nodata_val, min_val=min_val,
            max_val=max_val, map_units=map_units, resolution=resolution,
            bbox=bbox, svc_obj_id=svc_obj_id, service_type=service_type,
            metadata_url=metadata_url, parent_metadata_url=parent_metadata_url,
            mod_time=mod_time)

    # .............................
    def _set_is_discrete_data(self, is_discrete_data, is_categorical):
        if is_discrete_data is None:
            is_discrete_data = bool(is_categorical)
        self._is_discrete_data = is_discrete_data

    # .............................
    def create_local_dlocation(self, ext=None):
        """Create local filename for this layer.

        Args:
            ext: File extension for filename

        Note:
            Data files which are not default User data files (stored locally
                and using this method)
                (in /UserData/<user_id>/<epsg_code>/Layers directory) should be
                created in the appropriate Subclass (EnvironmentalLayer,
                OccurrenceSet, SDMProjections)
        """
        if ext is None:
            if self._data_format is None:
                ext = LMFormat.TMP.ext
            else:
                ext = LMFormat.get_extension_by_driver(self._data_format)
                if ext is None:
                    raise LMError(
                        'Failed to find data_format/driver {}'.format(
                            self._data_format))
        return super(Raster, self).create_local_dlocation(ext)

    # .............................
    @staticmethod
    def test_raster(dlocation, band_num=1):
        """Test a raster file"""
        success = True
        try:
            try:
                dataset = gdal.Open(str(dlocation), gdalconst.GA_ReadOnly)
            except Exception as err:
                raise LMError(
                    'Unable to open dataset {} with GDAL ({})'.format(
                        dlocation, str(err)), err)
            try:
                band = dataset.GetRasterBand(band_num)
            except Exception as err:
                raise LMError(
                    'No band {} in dataset {} ({})'.format(
                        band, dlocation, str(err)), err)
        except Exception:
            success = False
        return success

    # .............................
    @staticmethod
    def verify_data_description(gdal_type, gdal_format):
        """Verify the data description

        Verifies that the data_type and format are either LM-supported GDAL
        types or None.

        Raises:
            LMError: Thrown when gdal_format is missing or either gdal_format
                or gdal_type is not legal for a Lifemapper Raster.
        """
        # GDAL data_format is required (may be a placeholder and changed later)
        if gdal_format not in LMFormat.gdal_drivers():
            raise LMError(['Unsupported Raster GDAL data_format', gdal_format])
        if gdal_type is not None and gdal_type not in GDALDataTypes:
            raise LMError(['Unsupported Raster GDAL type', gdal_type])

    # .............................
    def open_with_gdal(self, dlocation=None, band_num=1):
        """Open the raster with GDAL"""
        if dlocation is None:
            dlocation = self._dlocation
        try:
            dataset = gdal.Open(str(dlocation), gdalconst.GA_ReadOnly)
            band = dataset.GetRasterBand(band_num)
        except Exception as err:
            raise LMError(
                'Unable to open dataset or band {} with GDAL ({})'.format(
                    dlocation, str(err)), err)
        return dataset, band

    # .............................
    def get_data_url(self, interface=GEOTIFF_INTERFACE):
        """Get the data URL for the layer

        Note:
            The ServiceObject._db_id may contain a join id or _layer_id
                depending on the type of Layer being requested
        """
        return self._earl_jr.construct_lm_data_url(
            self.service_type, self.get_id(), interface)

    # .............................
    def get_histogram(self, band_num=1):
        """Get a histogram of layer data values.

        Note:
            - This returns a list, not a true histogram.
            - This only works on 8-bit data.

        Returns:
            list - A list of data values present in the dataset.
        """
        vals = []
        _, band = self.open_with_gdal(band_num=band_num)

        # Get histogram only for 8bit data (projections)
        if band.DataType == gdalconst.GDT_Byte:
            hist = band.GetHistogram()
            for i, hist_val in enumerate(hist):
                if i > 0 and i != self.nodata_val and hist_val > 0:
                    vals.append(i)
        else:
            print('Histogram calculated only for 8-bit data')
        return vals

    # .............................
    def get_is_discrete_data(self):
        """Get the is_discrete_data value."""
        return self._is_discrete_data

    # .............................
    def get_size(self, band_num=1):
        """Return a tuple of xsize and ysize (in pixels).

        Returns:
            tuple of (number of columns, number of rows) in the raster.
        """
        dataset, _ = self.open_with_gdal(band_num=band_num)
        return (dataset.RasterXSize, dataset.RasterYSize)

    # .............................
    def populate_stats(self, dlocation, verify, gdal_type, data_format, bbox,
                       resolution, min_val, max_val, nodata_val, band_num=1):
        """Updates or fills layer parameters by reading the data."""
        msgs = []
        # msgs.append('File does not exist: {}'.format(dlocation))
        dataset, band = self.open_with_gdal(
            dlocation=dlocation, band_num=band_num)
        # srs = dataset.GetProjection()
        # size = (dataset.RasterXSize, dataset.RasterYSize)
        geo_transform = dataset.GetGeoTransform()
        ulx = geo_transform[0]
        x_pixel_size = geo_transform[1]
        uly = geo_transform[3]
        y_pixel_size = geo_transform[5]

        drv = dataset.GetDriver()
        gdal_format = drv.GetDescription()
        if data_format is None:
            data_format = gdal_format
        elif data_format != gdal_format:
            msgs.append(
                'Incorrect gdal_format {}, changing to {} for layer {}'.format(
                    data_format, gdal_format, dlocation))
            data_format = gdal_format
        # Rename with correct extension if incorrect
        head, ext = os.path.splitext(dlocation)
        correct_ext = LMFormat.get_extension_by_driver(data_format)
        if correct_ext is None:
            raise LMError(
                'Failed to find data_format/driver {}'.format(data_format))
        # correct_ext = GDALFormatCodes[data_format]['FILE_EXT']
        if ext != correct_ext:
            msgs.append(
                'Invalid extension {}, renaming to {} for layer {}'.format(
                    ext, correct_ext, dlocation))
            old_dl = dlocation
            dlocation = head + correct_ext
            os.rename(old_dl, dlocation)

        # Assumes square pixels
        if resolution is None:
            resolution = x_pixel_size
        if bbox is None:
            lrx = ulx + x_pixel_size * dataset.RasterXSize
            lry = uly + y_pixel_size * dataset.RasterYSize
            bbox = [ulx, lry, lrx, uly]
        if gdal_type is None:
            gdal_type = band.DataType
        elif gdal_type != band.DataType:
            msgs.append(
                'Incorrect datatype {}, changing to {} for layer {}'.format(
                    gdal_type, band.DataType, dlocation))
            gdal_type = band.DataType
        bmin, bmax, _, _ = band.GetStatistics(False, True)
        if min_val is None:
            min_val = bmin
        if max_val is None:
            max_val = bmax
        if nodata_val is None:
            nodata_val = band.GetNoDataValue()
        # Print all warnings
        if msgs:
            print('Layer.populate_stats Warning: \n{}'.format('\n'.join(msgs)))

        return (
            dlocation, verify, gdal_type, data_format, bbox, resolution,
            min_val, max_val, nodata_val)

    # .............................
    def read_from_uploaded_data(self, data_content,
                                extension=DEFAULT_GDAL.ext):
        """Read from uploaded data via temporary file

        Raises:
            LMError: on failure to write data or read temporary files.
        """
        self.clear_dlocation()
        # Create temp location and write layer to it
        out_location = os.path.join(UPLOAD_PATH, self.name + extension)
        self.write_layer(
            src_data=data_content, out_file=out_location, overwrite=True)
        self.set_dlocation(dlocation=out_location)

    # .............................
    def write_layer(self, src_data=None, src_file=None, out_file=None,
                    overwrite=False):
        """Writes raster data to file.

        Args:
            data: A stream, string, or file of valid raster data
            overwrite: True/False directing whether to overwrite existing file
                or not

        Raises:
            LMError:
                On 1) failure to write file
                   2) attempt to overwrite existing file with overwrite=False
                   3) _dlocation is None
        """
        if out_file is None:
            out_file = self.get_dlocation()
        if out_file is not None:
            self.ready_filename(out_file, overwrite=overwrite)

            # Copy from input file using GDAL (no test necessary later)
            if src_file is not None:
                self.copy_data(src_file, target_dlocation=out_file)

            # Copy from input stream
            elif src_data is not None:
                try:
                    with open(out_file, 'w') as out_file_2:
                        out_file_2.write(src_data)
                except Exception as err:
                    raise LMError(
                        'Error writing data to raster {} ({})'.format(
                            out_file, err), err)
                else:
                    self.set_dlocation(dlocation=out_file)
                # Test input with GDAL
                try:
                    self.populate_stats()
                except Exception as err:
                    success, msg = self.delete_file(out_file)
                    raise LMError(
                        ('Invalid data written to {} ({}); Deleted '
                         '(success={}, {})').format(
                             out_file, str(err), str(success), msg), err)
            else:
                raise LMError(
                    ('Source data or source filename required for '
                     'write to {}').format(self._dlocation))
        else:
            raise LMError('Must set_dlocation before writing file')

    # .............................
    @staticmethod
    def _copy_gdal_data(band_num, in_fname, out_fname, format_='GTiff',
                        kwargs=None):
        """Copy the dataset into a new file.

        Args:
            band_num: The band number to read.
            out_fname: Filename to write this dataset to.
            format: GDAL-writeable raster format to use for new dataset.
                http://www.gdal.org/formats_list.html
        """
        options = []
        if format_ == 'AAIGrid':
            options = ['FORCE_CELLSIZE=True']
            # kwargs['FORCE_CELLSIZE'] = True
            # kwargs['DECIMAL_PRECISION'] = 4
        driver = gdal.GetDriverByName(format_)
        metadata = driver.GetMetadata()
        if not (gdal.DCAP_CREATECOPY in metadata
                and metadata[gdal.DCAP_CREATECOPY] == 'YES'):
            raise LMError(
                'Driver {} does not support CreateCopy() method.'.format(
                    format_))
        in_dataset = gdal.Open(in_fname)
        try:
            out_dataset = driver.CreateCopy(out_fname, in_dataset, 0, options)
        except Exception as err:
            raise LMError(
                'Creation failed for {} from band {} of {} ({})'.format(
                    out_fname, band_num, in_fname, str(err)), err)
        if out_dataset is None:
            raise LMError(
                'Creation failed for {} from band {} of {})'.format(
                    out_fname, band_num, in_fname))
        # Close new dataset to flush to disk
        out_dataset = None
        in_dataset = None

    # .............................
    def copy_data(self, source_dlocation, target_dlocation=None,
                  format_='GTiff'):
        """Copy data from one file to another
        """
        if format_ not in LMFormat.gdal_drivers():
            raise LMError('Unsupported raster format {}'.format(format_))
        if source_dlocation is not None and os.path.exists(source_dlocation):
            if target_dlocation is not None:
                dlocation = target_dlocation
            elif self._dlocation is not None:
                dlocation = self._dlocation
            else:
                raise LMError('Target location is None')
        else:
            raise LMError(
                'Source location {} is invalid'.format(source_dlocation))

        correct_ext = LMFormat.get_extension_by_driver(format_)
        if not dlocation.endswith(correct_ext):
            dlocation += correct_ext

        self.ready_filename(dlocation)

        try:
            self._copy_gdal_data(
                1, source_dlocation, dlocation, format_=format_)
        except Exception as err:
            raise LMError(
                'Failed to copy data source from {} to {} ({})'.format(
                    source_dlocation, dlocation, str(err)), err)

    # .............................
    def get_srs(self):
        """Get the spatial reference system of the layer"""
        if (self._dlocation is not None and os.path.exists(self._dlocation)):
            dataset = gdal.Open(str(self._dlocation), gdalconst.GA_ReadOnly)
            wkt_srs = dataset.GetProjection()
            if wkt_srs:
                srs = osr.SpatialReference()
                srs.ImportFromWkt(wkt_srs)
            else:
                srs = self.create_srs_from_epsg()
            dataset = None
            return srs

        raise LMError('Input file {} does not exist'.format(self._dlocation))

    # .............................
    def write_srs(self, srs):
        """Writes spatial reference system information to this raster file.

        Args:
            srs: An osgeo.osr.SpatialReference object or a WKT string
                describing the desired spatial reference system.

        Raises:
            LMError: on failure to open dataset or write srs
        """

        if (self._dlocation is not None and os.path.exists(self._dlocation)):
            try:
                dataset = gdal.Open(str(self.dlocation), gdalconst.GA_Update)
                if isinstance(srs, osr.SpatialReference):
                    srs = srs.ExportToWkt()
                dataset.SetProjection(srs)
                dataset.FlushCache()
                dataset = None
            except Exception as err:
                raise LMError(
                    'Unable to write SRS info to file', err, srs=srs,
                    d_location=self.dlocation)

    # .............................
    def copy_srs_from_file(self, fname):
        """Copy the spatial reference system from the provided file.

        Args:
            fname: Filename for dataset from which to copy spatial reference
                system.

        Raises:
            LMError: on failure to open dataset or write srs
        """
        if (fname is not None and os.path.exists(fname)):
            dataset = gdal.Open(fname, gdalconst.GA_ReadOnly)
            wkt_srs = dataset.GetProjection()
            dataset = None
            self.write_srs(wkt_srs)
        else:
            raise LMError(['Unable to read file %s' % fname])

    # .............................
    def is_valid_dataset(self):
        """Checks to see if dataset is a valid raster

        Returns:
            bool - True if raster is a valid GDAL dataset; False if not
        """
        valid = True
        if (self._dlocation is not None and os.path.exists(self._dlocation)):
            try:
                gdal.Open(self._dlocation, gdalconst.GA_ReadOnly)
            except Exception:
                valid = False

        return valid

    # .............................
    def delete_data(self, dlocation=None, is_temp=False):
        """Deletes the local data file(s) on disk

        Note:
            Does NOT clear the dlocation attribute
        """
        success = False
        if dlocation is None:
            dlocation = self._dlocation
        if (dlocation is not None and os.path.isfile(dlocation)):
            drv = gdal.GetDriverByName(self._data_format)
            result = drv.Delete(dlocation)
            if result == 0:
                success = True
        if not is_temp:
            pth, _ = os.path.split(dlocation)
            if os.path.isdir(pth) and len(os.listdir(pth)) == 0:
                try:
                    os.rmdir(pth)
                except IOError:
                    print('Unable to rmdir {}'.format(pth))
        return success

    # .............................
    @staticmethod
    def get_wcs_request(bbox=None, resolution=None):
        """Gets a WCS request to retrieve layer data.

        TODO:
            Consider a "ServiceLayer" class that could include this function
        """
        raise LMError(
            ('get_wcs_request must be implemented in Subclasses also '
             'inheriting from ServiceObject'))


# .............................................................................
class Vector(_Layer):
    """Class to hold information about a vector dataset."""
    _local_id_field_name = OccurrenceFieldNames.LOCAL_ID[0]
    _local_id_field_type = OFTInteger
    _geom_field_name = OccurrenceFieldNames.GEOMETRY_WKT[0]
    _geom_field_type = OFTString

    # .............................
    def __init__(self, name, user_id, epsg_code, lyr_id=None, squid=None,
                 ident=None, verify=None, dlocation=None, metadata=None,
                 data_format=DEFAULT_OGR.driver, ogr_type=None,
                 val_units=None, val_attribute=None, nodata_val=None,
                 min_val=None, max_val=None, resolution=None, bbox=None,
                 map_units=None, svc_obj_id=None,
                 service_type=LMServiceType.LAYERS, metadata_url=None,
                 parent_metadata_url=None, mod_time=None,
                 feature_count=0, feature_attributes=None, features=None,
                 fid_attribute=None):
        """Vector superclass constructor, inherits from _Layer

        Args:
            name: layer name, unique with user_id and epsq
            user_id: user identifier
            epsg_code (int): EPSG code indicating the SRS to use
            lyr_id (int): record identifier for layer
            squid (int): locally unique identifier for taxa-related data
            ident (int): locally unique identifier for taxa-related data
            verify: hash of the data for verification
            dlocation: data location (url, file path, ...)
            metadata: dictionary of metadata key/values; uses class or
                superclass attribute constants META_* as keys
            data_format: ogr code for spatial data file format
                OGR Vector Format code at http://www.gdal.org/ogr_formats.html
            ogr_type (int): OGR geometry type (wkbPoint, ogr.wkbPolygon, etc)
                OGRwkbGeometryType in http://www.gdal.org/ogr/ogr__core_8h.html
            val_units: units of measurement for data
            val_attribute: field containing data values of interest
            nodata_val: value indicating feature/pixel does not contain data
            min_val: smallest value in data
            max_val: largest value in data
            resolution: resolution of the data - pixel size in @map_units
            bbox: spatial extent of data
                sequence in the form (min_x, min_y, max_x, max_y)
                or comma-delimited string in the form
                'min_x, min_y, max_x, max_y'
            map_units: units of measurement for the data.
                These are keywords as used in  mapserver, choice of
                LegalMapUnits described in
                http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
            svc_obj_id (int): unique db record identifier for an object
                retrievable by REST web services.  May be filled with the
                base layer_id or a unique parameterized id
            service_type: type of data for REST retrieval LMServiceType.LAYERS
            metadata_url: REST URL for API returning this object
            parent_metadata_url: REST URL for API returning this parent object
            mod_time: time of last modification, in MJD
            feature_count: number of features in this layer
            feature_attributes: dictionary of feature attributes for this layer
            features: dictionary of features in this layer
                key is the featureid (FID) or localid of the feature
                value is a list of values for the feature.  Values are ordered
                in the same order as in feature_attributes.
            fid_attribute: field name of the attribute holding the featureID
        """
        self._geom_idx = None
        self._geometry = None
        self._convex_hull = None
        self._local_id_idx = None
        self._fid_attribute = fid_attribute
        self._feature_attributes = {}
        self._features = {}
        self._feature_count = 0

        _Layer.__init__(
            self, name, user_id, epsg_code, lyr_id=lyr_id, squid=squid,
            ident=ident, verify=verify, dlocation=dlocation, metadata=metadata,
            data_format=data_format, ogr_type=ogr_type, val_units=val_units,
            val_attribute=val_attribute, nodata_val=nodata_val,
            min_val=min_val, max_val=max_val, map_units=map_units,
            resolution=resolution, bbox=bbox, svc_obj_id=svc_obj_id,
            service_type=service_type, metadata_url=metadata_url,
            parent_metadata_url=parent_metadata_url, mod_time=mod_time)
        self.verify_data_description(ogr_type, data_format)
        # The following may be reset by set_features:
        # features, feature_attributes, feature_count, geom_idx, local_id_idx,
        #    geom, convexHull
        self.set_features(
            features, feature_attributes, feature_count=feature_count)
        # If data exists, check description
        if dlocation is not None and os.path.exists(dlocation):
            # sets features, feature_attributes, and feature_count
            #    (if do_read_data)
            (new_bbox, local_id_idx, geom_idx) = self.read_data(
                dlocation=dlocation, data_format=data_format,
                do_read_data=False)
            # Reset some attributes based on data
            if new_bbox is not None:
                self.bbox = new_bbox
                self._geom_idx = geom_idx
                self._local_id_idx = local_id_idx

    # .............................
    @staticmethod
    def verify_data_description(ogr_type, ogr_format):
        """Sets the data type for the vector

        Args:
            ogr_type: OGR type of the vector, valid choices are in OGRDataTypes
            ogr_format: OGR Vector Format, only a subset (in OGRFormats) are
                valid here

        Raises:
            LMError: Thrown when ogr_format is missing or either ogr_format or
                ogr_type is not legal for a Lifemapper Vector.
        """
        # OGR data_format is required (may be a placeholder and changed later)
        if ogr_format not in LMFormat.ogr_drivers():
            raise LMError('Unsupported Vector OGR data_format', ogr_format)
        if ogr_type is not None and ogr_type not in OGRDataTypes:
            raise LMError('Unsupported Vector ogr_type', ogr_type)

    # .............................
    @property
    def features(self):
        """Converts the dictionary of features into a list of LmAttObjs"""
        return [
            LmAttObj(
                {self._feature_attributes[k2][0]: self._features[k1][k2]
                 for k2 in self._feature_attributes},
                'Feature') for k1 in self._features]

    # .............................
    @property
    def feature_attributes(self):
        """Get the attributes of the features"""
        return self._feature_attributes

    # .............................
    @property
    def fid_attribute(self):
        """Get the field identifier attribute"""
        return self._fid_attribute

    # .............................
    def _get_feature_count(self):
        if self._feature_count is None:
            if self._features:
                self._feature_count = len(self._features)
        return self._feature_count

    # .............................
    def _set_feature_count(self, count):
        """Set the number of features in the layer.

        If Vector._features are present, the length of that list takes
        precedent over the count parameter.
        """
        if self._features:
            self._feature_count = len(self._features)
        else:
            self._feature_count = count

    feature_count = property(_get_feature_count, _set_feature_count)

    # .............................
    def set_features(self, features, feature_attributes, feature_count=0):
        """Set the features for the layer.

        Args:
            features (dict): A dictionary of features, with key the feature_id
                (FID) or localid of the feature, and value a list of values for
                the feature.  Values are ordered in the same order as in
                feature_attributes.
            feature_attributes: a dictionary of feature_attributes, with key
                the index of this attribute in each feature, and value a tuple
                of (field name, field type (OGR))
            feature_count: the number of features in these data
        """
        if feature_attributes:
            self._feature_attributes = feature_attributes
            self._set_geometry_index()
            self._set_local_id_index()
        else:
            self._feature_attributes = {}
            self._geom_idx = None
            self._local_id_idx = None

        if features:
            self._features = features
            self._set_geometry()
            self._feature_count = len(features)
        else:
            self._features = {}
            self._geometry = None
            self._convex_hull = None
            self._feature_count = feature_count

    # .............................
    def get_features(self):
        """Get the features of the layer."""
        return self._features

    # .............................
    def clear_features(self):
        """Clear the features of the layer"""
        del self._feature_attributes
        del self._features
        self.set_features(None, None)

    # .............................
    def add_features(self, features):
        """Adds to Vector._features and updates Vector.feature_count

        Args:
            features: A dictionary of features, with key the featureid (FID) or
                localid of the feature, and value a list of values for the
                feature.  Values are ordered in the same order as in
                feature_attributes.
        """
        if features:
            for fid, vals in features.items():
                self._features[fid] = vals
            self._feature_count = len(self._features)

    # .............................
    def get_feature_attributes(self):
        """Returns the feature attributes dictionary"""
        return self._feature_attributes

    # .............................
    def set_val_attribute(self, val_attribute):
        """Sets the value attribute.

        Args:
            val_attribute: field name for the attribute to map
        """
        self._val_attribute = None
        if self._feature_attributes:
            if val_attribute:
                for idx in list(self._feature_attributes.keys()):
                    fldname, _ = self._feature_attributes[idx]
                    if fldname == val_attribute:
                        self._val_attribute = val_attribute
                        break
                if self._val_attribute is None:
                    raise LMError(
                        'Map attribute {} not present in dataset {}'.format(
                            val_attribute, self._dlocation))
        else:
            self._val_attribute = val_attribute

    # .............................
    def get_val_attribute(self):
        """Returns the value attribute"""
        return self._val_attribute

    # .............................
    def get_data_url(self, interface=SHAPEFILE_INTERFACE):
        """Gets the data url of the vector layer.
        """
        return self._earl_jr.construct_lm_data_url(
            self.service_type, self.get_id(), interface)

    # .............................
    def _set_geometry_index(self):
        if self._geom_idx is None and self._feature_attributes:
            for idx, (colname, _) in self._feature_attributes.items():
                if colname == self._geom_field_name:
                    self._geom_idx = idx
                    break

    # .............................
    def _get_geometry_index(self):
        if self._geom_idx is None:
            self._set_geometry_index()
        return self._geom_idx

    # .............................
    def _set_local_id_index(self):
        if self._local_id_idx is None and self._feature_attributes:
            for idx, (colname, _) in self._feature_attributes.items():
                if colname in OccurrenceFieldNames.LOCAL_ID:
                    self._local_id_idx = idx
                    break

    # .............................
    def get_local_id_index(self):
        """Gets the local id index"""
        if self._local_id_idx is None:
            self._set_local_id_index()
        return self._local_id_idx

    # .............................
    def create_local_dlocation(self, ext=DEFAULT_OGR.ext):
        """Create local filename for this layer.

        Args:
            ext: File extension for filename

        Note:
            Data files which are not default User data files (stored locally
                and using this method)
                (in /UserData/<user_id>/<epsg_code>/Layers directory) should be
                created in the appropriate Subclass (EnvironmentalLayer,
                OccurrenceSet, SDMProjections)
        """
        return Vector.create_local_dlocation(ext)

    # .............................
    def get_shapefiles(self, other_location=None):
        """Get the shapefile files for this layer"""
        shpnames = []
        if other_location is not None:
            dloc = other_location
        else:
            dloc = self._dlocation
        if dloc is not None:
            base, ext = os.path.splitext(dloc)
            fnames = glob.glob(base + '.*')
            for fname in fnames:
                base, ext = os.path.splitext(fname)
                if ext in DEFAULT_OGR.get_extensions():
                    shpnames.append(fname)
        return shpnames

    # .............................
    def zip_shapefiles(self, base_name=None):
        """Returns a wrapper around a tar gzip file stream

        Args:
            base_name: (optional) If provided, this will be the prefix for the
                names of the shape file's files in the zip file.
        """
        fnames = self.get_shapefiles()
        tg_stream = StringIO()
        zipf = zipfile.ZipFile(
            tg_stream, mode='w', compression=zipfile.ZIP_DEFLATED,
            allowZip64=True)
        if base_name is None:
            base_name = os.path.splitext(os.path.split(fnames[0])[1])[0]

        for fname in fnames:
            ext = os.path.splitext(fname)[1]
            zipf.write(fname, "%s%s" % (base_name, ext))
        zipf.close()

        tg_stream.seek(0)
        ret = ''.join(tg_stream.readlines())
        tg_stream.close()
        return ret

    # .............................
    def get_min_features(self):
        """Returns a minimal dictionary of features and geometries.

        Returns:
            Dictionary of {feature id (fid): wktGeometry}
        """
        feats = {}
        if self._features and self._feature_attributes:
            self._set_geometry_index()

            for fid in list(self._features.keys()):
                feats[fid] = self._features[fid][self._geom_idx]

        return feats

    # .............................
    def is_valid_dataset(self, dlocation=None):
        """Check to see if the file is a valid vector readable by OGR.

        Returns:
            True if dataset is a valid OGR dataset; False if not
        """
        valid = False
        if dlocation is None:
            dlocation = self._dlocation
        if (dlocation is not None and
                (os.path.isdir(dlocation) or os.path.isfile(dlocation))):
            try:
                dataset = ogr.Open(dlocation)
                dataset.GetLayer(0)
            except Exception:
                pass
            else:
                valid = True
        return valid

    # .............................
    def delete_data(self, dlocation=None, is_temp=False):
        """Deletes the local data file(s) on disk

        Note:
            - Does NOT clear the dlocation attribute
            - May be extended to delete remote data controlled by us.
        """
        if dlocation is None:
            dlocation = self._dlocation
        delete_dir = False
        if not is_temp:
            self.clear_local_mapfile()
            delete_dir = True
        self.delete_file(dlocation, delete_dir=delete_dir)

    # .............................
    @staticmethod
    def get_xy(wkt):
        """Get the X,Y values from well-known text

        Args:
            wkt: well-known text for a point

        Note:
            this assumes the WKT is for a point
        """
        startidx = wkt.find('(')
        if wkt[:startidx].strip().lower() == 'point':
            tmp = wkt[startidx + 1:]
            endidx = tmp.find(')')
            tmp = tmp[:endidx]
            vals = tmp.split()
            if len(vals) == 2:
                try:
                    x_val = float(vals[0])
                    y_val = float(vals[1])
                    return x_val, y_val
                except TypeError:
                    pass
        return None, None

    # .............................
    @classmethod
    def get_shapefile_row_headers(cls, shapefile_filename):
        """Get row headers for a shapefile.

        Args:
            shapefile_filename: absolute filename for shapefile

        Returns:
            a list of tuples representing points, (FeatureID, x, y)

        TODO:
            rename for clarity
        """
        ogr.RegisterAll()
        drv = ogr.GetDriverByName(DEFAULT_OGR.driver)
        dataset = drv.Open(shapefile_filename)
        lyr = dataset.GetLayer(0)

        row_headers = []

        for j in range(lyr.GetFeatureCount()):
            cur_feat = lyr.GetFeature(j)
            site_idx = cur_feat.GetFID()
            x_coord, y_coord = cur_feat.geometry().Centroid().GetPoint_2D()
            row_headers.append((site_idx, x_coord, y_coord))

        return sorted(row_headers)

    # .............................
    def write_layer(self, src_data=None, src_file=None, out_file=None,
                    overwrite=False):
        """Writes vector data to file and sets dlocation.

        Args:
            src_data: A stream, or string of valid vector data
            src_file: A filename for valid vector data.  Currently only
                supports CSV and ESRI shapefiles.
            overwrite: True/False directing whether to overwrite existing file
                or not

        Raises:
            LMError: on
                1) failure to write file
                2) attempt to overwrite existing file with overwrite=False
                3) _dlocation is None
        """
        if src_file is not None:
            self.read_data(dlocation=src_file)
        if out_file is None:
            out_file = self.get_dlocation()
        if self.features is not None:
            self.write_shapefile(dlocation=out_file, overwrite=overwrite)
        # No file, no features, src_data is iterable, write as CSV
        elif src_data is not None:
            if isinstance(src_data, (list, tuple)):
                if not out_file.endswith(LMFormat.CSV.ext):
                    raise LMError(
                        ('Iterable input vector data can only be written '
                         'to CSV'))

                self.write_csv(dlocation=out_file, overwrite=overwrite)

            raise LMError(
                ('Writing vector is currently supported only for file or '
                 'iterable input data'))
        self.set_dlocation(dlocation=out_file)

    # .............................
    @staticmethod
    def _create_point_shapefile(drv, outpath, sp_ref, lyr_name, lyr_def=None,
                                fld_names=None, id_col=None, x_col=None,
                                y_col=None, overwrite=True):
        name_changes = {}
        dlocation = os.path.join(outpath, lyr_name + '.shp')
        if os.path.isfile(dlocation):
            if overwrite:
                drv.DeleteDataSource(dlocation)
            else:
                raise LMError(
                    'Layer {} exists, creation failed'.format(dlocation))
        new_dataset = drv.CreateDataSource(dlocation)
        if new_dataset is None:
            raise LMError('Dataset creation failed for {}'.format(dlocation))
        new_lyr = new_dataset.CreateLayer(
            lyr_name, geom_type=ogr.wkbPoint, srs=sp_ref)
        if new_lyr is None:
            raise LMError('Layer creation failed for {}'.format(dlocation))

        # If LayerDefinition is provided, create and add each field to new
        #    layer
        if lyr_def is not None:
            for i in range(lyr_def.GetFieldCount()):
                fld_def = lyr_def.GetFieldDefn(i)
#                 fldName = fld_def.GetNameRef()
                return_val = new_lyr.CreateField(fld_def)
                if return_val != 0:
                    raise LMError(
                        'CreateField failed for \'{}\' in {}'.format(
                            fld_def.GetNameRef(), dlocation))
        # If layer fields are not yet defined, create from field_names
        elif (fld_names is not None and id_col is not None and
              x_col is not None and y_col is not None):
            # Create field definitions
            fld_def_list = []
            for fldname in fld_names:
                if fldname in (x_col, y_col):
                    fld_def_list.append(ogr.FieldDefn(fldname, ogr.OFTReal))
                elif fldname == id_col:
                    fld_def_list.append(ogr.FieldDefn(fldname, ogr.OFTInteger))
                else:
                    fdef = ogr.FieldDefn(fldname, ogr.OFTString)
                    fld_def_list.append(fdef)
            # Add field definitions to new layer
            for fld_def in fld_def_list:
                try:
                    return_val = new_lyr.CreateField(fld_def)
                    if return_val != 0:
                        raise LMError(
                            'CreateField failed for \'{}\' in {}'.format(
                                fld_def, dlocation))
                    lyr_def = new_lyr.GetLayerDefn()
                    last_idx = lyr_def.GetFieldCount() - 1
                    new_fld_name = lyr_def.GetFieldDefn(last_idx).GetNameRef()
                    old_fld_name = fld_def.GetNameRef()
                    if new_fld_name != old_fld_name:
                        name_changes[old_fld_name] = new_fld_name

                except Exception as err:
                    print(str(err))
        else:
            raise LMError(
                ('Must provide either LayerDefinition or Fieldnames and Id, '
                 'X, and Y column names'))

        return new_dataset, new_lyr, name_changes

    # .............................
    @staticmethod
    def _finish_shapefile(new_ds):
        wrote = None
        dloc = new_ds.GetName()
        try:
            # Closes and flushes to disk
            new_ds.Destroy()
        except Exception:
            wrote = None
        else:
            print('Closed/wrote dataset {}'.format(dloc))
            wrote = dloc

            try:
                retcode = subprocess.call(['shptree', '{}'.format(dloc)])
                if retcode != 0:
                    print('Unable to create shapetree index on %s' % dloc)
            except Exception as err:
                print('Unable to create shapetree index on {}: {}'.format(
                    dloc, str(err)))
        return wrote

    # .............................
    @staticmethod
    def _get_spatial_ref(srs_epsg_or_wkt, layer=None):
        sp_ref = None
        if layer is not None:
            sp_ref = layer.GetSpatialRef()

        if sp_ref is None:
            sp_ref = osr.SpatialReference()
            try:
                sp_ref.ImportFromEPSG(srs_epsg_or_wkt)
            except Exception:
                try:
                    sp_ref.ImportFromWkt(srs_epsg_or_wkt)
                except Exception as e:
                    raise LMError(
                        ('Unable to get Spatial Reference System from {}; '
                         'Error {}').format(str(srs_epsg_or_wkt), str(e)))
        return sp_ref

    # .............................
    @staticmethod
    def _copy_feature(orig_feature):
        new_feat = None
        try:
            new_feat = orig_feature.Clone()
        except Exception as e:
            print('Failure to create new feature; Error: {}'.format(str(e)))
        return new_feat

    # .............................
    @staticmethod
    def create_point_feature(o_dict, x_col, y_col, lyr_def, new_names):
        """Create a point feature"""
        pt_feat = None
        try:
            pt_geom = ogr.Geometry(ogr.wkbPoint)
            pt_geom.AddPoint(float(o_dict[x_col]), float(o_dict[y_col]))
        except Exception as e:
            print('Failure {}:  Point = {}, {}'.format(
                str(e), str(o_dict[x_col]), str(o_dict[y_col])))
        else:
            # Create feature for combo layer
            pt_feat = ogr.Feature(lyr_def)
            pt_feat.SetGeometryDirectly(pt_geom)
            # set other fields to match original values
            for okey in list(o_dict.keys()):
                if okey in list(new_names.keys()):
                    pt_feat.SetField(new_names[okey], o_dict[okey])
                else:
                    pt_feat.SetField(okey, o_dict[okey])
        return pt_feat

    # .............................
    @staticmethod
    def split_csv_points_to_shapefiles(outpath, dlocation, group_field,
                                       combo_layer_name,
                                       srs_epsg_or_wkt=DEFAULT_EPSG,
                                       delimiter=';', quotechar='\"',
                                       id_col='id', x_col='lon', y_col='lat',
                                       overwrite=False):
        """Split a CSV point file into shapefiles

        Read OGR-accessible data and write to a single shapefile and individual
        shapefiles defined by the value of <group_field>

        Args:
            outpath: Directory for output datasets.
            dlocation: Full path location of the data
            group_field: Field containing attribute to group on.
            combo_layer_name: Write the original combined data using this name.
            srs_epsg_or_wkt: Spatial reference as an integer EPSG code or
                Well-Known-Text
            overwrite: Overwrite or fail if data already exists.

        Raises:
            LMError: on failure to read data.
        """
        ogr.UseExceptions()

        data = {}
        successful_writes = []

        ogr.RegisterAll()
        drv = ogr.GetDriverByName(DEFAULT_OGR.driver)
        sp_ref = Vector._get_spatial_ref(srs_epsg_or_wkt)

        with open(dlocation, 'rb') as in_file:
            pt_reader = csv.DictReader(
                in_file, delimiter=delimiter, quotechar=quotechar)
            ((id_name, _), (x_name, _), (y_name, _)
             ) = Vector._get_id_xy_pos(
                 pt_reader.fieldnames, id_name=id_col, x_name=x_col,
                 y_name=y_col)
            (combo_dataset, combo_lyr, name_changes
             ) = Vector._create_point_shapefile(
                 drv, outpath, sp_ref, combo_layer_name,
                 fld_names=pt_reader.fieldnames, id_col=id_name, x_col=x_name,
                 y_col=y_name, overwrite=overwrite)
            lyr_def = combo_lyr.GetLayerDefn()
            # Iterate through records
            for o_dict in pt_reader:
                # Create and add feature to combo layer
                pt_feat1 = Vector.create_point_feature(
                    o_dict, x_col, y_col, lyr_def, name_changes)
                if pt_feat1 is not None:
                    combo_lyr.CreateFeature(pt_feat1)
                    pt_feat1.Destroy()
                    # Create and save point for individual species layer
                    pt_feat2 = Vector.create_point_feature(
                        o_dict, x_col, y_col, lyr_def, name_changes)
                    this_group = o_dict[group_field]
                    if this_group not in list(data.keys()):
                        data[this_group] = [pt_feat2]
                    else:
                        data[this_group].append(pt_feat2)

            dloc = Vector._finish_shapefile(combo_dataset)
            successful_writes.append(dloc)

        for group, point_features in data.items():
            (ind_dataset, ind_lyr, name_changes
             ) = Vector._create_point_shapefile(
                 drv, outpath, sp_ref, group, lyr_def=lyr_def,
                 overwrite=overwrite)
            for point in point_features:
                ind_lyr.CreateFeature(point)
                point.Destroy()
            dloc = Vector._finish_shapefile(ind_dataset)
            successful_writes.append(dloc)

        ogr.DontUseExceptions()
        return successful_writes

    # .............................
    def write_csv(self, data_records, dlocation=None, overwrite=False,
                  header=None):
        """Writes vector data to a CSV file.

        Args:
            dlocation: Location to write the data
            overwrite: True if overwrite existing outfile, False if not

        Returns:
            bool - Indication of success

        Raises:
            LMError: on failure to write file.

        Note:
            This does NOT set the  self._dlocation attribute
        """
        if dlocation is None:
            dlocation = self._dlocation
        did_write = False
        success = self.ready_filename(dlocation, overwrite=overwrite)
        if success:
            try:
                with open(dlocation, 'wb') as csvfile:
                    spamwriter = csv.writer(csvfile, delimiter='\t')
                    if header:
                        spamwriter.writerow(header)
                    for rec in data_records:
                        try:
                            spamwriter.writerow(rec)
                        except Exception as err:
                            # Report and move on
                            print(('Failed to write record {} ({})'.format(
                                rec, str(err))))
                did_write = True
            except Exception as err:
                print(('Failed to write file {} ({})'.format(
                    dlocation, str(err))))
        return did_write

    # .............................
    def write_shapefile(self, dlocation=None, overwrite=False):
        """Writes vector data in the feature attribute to a shapefile.

        Args:
            dlocation: Location to write the data
            overwrite: True if overwrite existing shapefile, False if not

        Returns:
            bool - Indication of success

        Raises:
            LMError: on failure to write file.
        """
        success = False
        if dlocation is None:
            dlocation = self._dlocation

        if not self._features:
            return success

        if overwrite:
            self.delete_data(dlocation=dlocation)
        elif os.path.isfile(dlocation):
            print(('Dataset exists: %s' % dlocation))
            return success

        self.set_dlocation(dlocation)
        self.ready_filename(self._dlocation)

        try:
            # Create the file object, a layer, and attributes
            target_srs = osr.SpatialReference()
            target_srs.ImportFromEPSG(self.epsg_code)
            drv = ogr.GetDriverByName(DEFAULT_OGR.driver)

            dataset = drv.CreateDataSource(self._dlocation)
            if dataset is None:
                raise LMError(
                    'Dataset creation failed for {}'.format(self._dlocation))

            lyr = dataset.CreateLayer(
                dataset.GetName(), geom_type=self._ogr_type, srs=target_srs)
            if lyr is None:
                raise LMError(
                    'Layer creation failed for {}.'.format(self._dlocation))

            # Define the fields
            for idx in list(self._feature_attributes.keys()):
                fldname, fldtype = self._feature_attributes[idx]
                if fldname != self._geom_field_name:
                    fld_defn = ogr.FieldDefn(fldname, fldtype)
                    # Special case to handle long Canonical, Provider,
                    #    Resource names
                    if (fldname.endswith('name') and fldtype == ogr.OFTString):
                        fld_defn.SetWidth(DEFAULT_OGR.options['MAX_STRLEN'])
                    return_val = lyr.CreateField(fld_defn)
                    if return_val != 0:
                        raise LMError(
                            'CreateField failed for {} in {}'.format(
                                fldname, self._dlocation))

            # For each feature
            for i in list(self._features.keys()):
                f_vals = self._features[i]
                feat = ogr.Feature(lyr.GetLayerDefn())
                try:
                    self._fill_ogr_feature(feat, f_vals)
                except Exception as e:
                    print('Failed to fillOGRFeature, e = {}'.format(e))
                else:
                    # Create new feature, setting FID, in this layer
                    lyr.CreateFeature(feat)
                    feat.Destroy()

            # Closes and flushes to disk
            dataset.Destroy()
            print(('Closed/wrote dataset {}'.format(self._dlocation)))
            success = True
            try:
                retcode = subprocess.call(
                    ['shptree', '{}'.format(self._dlocation)])
                if retcode != 0:
                    print(
                        'Unable to create shapetree index on {}'.format(
                            self._dlocation))
            except Exception as e:
                print('Unable to create shapetree index on {}: {}'.format(
                    self._dlocation, str(e)))
        except Exception as e:
            raise LMError(
                'Failed to create shapefile {}'.format(self._dlocation), e)

        return success

    # .............................
    def read_from_uploaded_data(self, data, uploaded_type='shapefile',
                                overwrite=True):
        """Read vector data from uploaded data via temporary file.

        Raises:
            LMError: on failure to write data or read temporary files.
        """
        # Writes zipped stream to temp file and sets dlocation on layer
        if uploaded_type == 'shapefile':
            self.write_from_zipped_shapefile(
                data, is_temp=True, overwrite=overwrite)
            self._data_format = DEFAULT_OGR.driver
            try:
                # read to make sure it's valid (and populate stats)
                self.read_data()
            except Exception as e:
                raise LMError(
                    'Invalid uploaded data in temp file {} ({})'.format(
                        self._dlocation, str(e)), do_trace=True)
        elif uploaded_type == 'csv':
            self.write_temp_from_csv(data)
            self._data_format = 'CSV'
            try:
                # read to make sure it's valid (and populate stats)
                self.read_data()
            except Exception as e:
                raise LMError(
                    'Invalid uploaded data in temp file {} ({})'.format(
                        self._dlocation, str(e)))

    # .............................
    @staticmethod
    def _get_id_xy_pos(field_names, id_name=None, x_name=None, y_name=None):
        id_pos = x_pos = y_pos = None
        if id_name is not None:
            try:
                id_pos = field_names.index(id_name)
            except Exception:
                id_name = None
        if x_name is not None:
            try:
                x_pos = field_names.index(x_name)
            except Exception:
                x_name = None
        if y_name is not None:
            try:
                y_pos = field_names.index(y_name)
            except Exception:
                y_name = None

        if not (id_name and x_name and y_name):
            for i, field_name in enumerate(field_names):
                fldname = field_name.lower()
                if x_name is None and \
                        fldname in OccurrenceFieldNames.LONGITUDE:
                    x_name = fldname
                    x_pos = i
                if y_name is None and \
                        fldname in OccurrenceFieldNames.LATITUDE:
                    y_name = fldname
                    y_pos = i
                if id_name is None and \
                        fldname in OccurrenceFieldNames.LOCAL_ID:
                    id_name = fldname
                    id_pos = i

        return ((id_name, id_pos), (x_name, x_pos), (y_name, y_pos))

    # .............................
    def write_from_zipped_shapefile(self, zip_data, is_temp=True,
                                    overwrite=False):
        """Write a shapefile from zipped data.

        Write a shapefile from a zipped stream of shapefile files to temporary
        files.  Read vector info into layer attributes, reset dlocation.

        Raises:
            LMError: on failure to write file.
        """
        new_fname_wo_ext = None
        out_stream = StringIO()
        out_stream.write(zip_data)
        out_stream.seek(0)
        zip_in = zipfile.ZipFile(out_stream, allowZip64=True)

        # Get filename, prepare directory, delete if overwrite=True
        if is_temp:
            zfnames = zip_in.namelist()
            for zfname in zfnames:
                if zfname.endswith(LMFormat.SHAPE.ext):
                    pth, basefilename = os.path.split(zfname)
                    pth = UPLOAD_PATH
                    basename, _ = os.path.splitext(basefilename)
                    new_fname_wo_ext = os.path.join(pth, basename)
                    outfname = os.path.join(UPLOAD_PATH, basefilename)
                    ready = self.ready_filename(outfname, overwrite=overwrite)
                    break
            if outfname is None:
                raise LMError(
                    'Invalid shapefile, zipped data does not contain .shp')
        else:
            if self._dlocation is None:
                self.set_dlocation()
            outfname = self._dlocation
            if outfname is None:
                raise LMError('Must set_dlocation prior to writing shapefile')
            pth, basefilename = os.path.split(outfname)
            basename, _ = os.path.splitext(basefilename)
            new_fname_wo_ext = os.path.join(pth, basename)
            ready = self.ready_filename(outfname, overwrite=overwrite)

        if ready:
            # unzip zip file stream
            for zname in zip_in.namelist():
                _, ext = os.path.splitext(zname)
                # Check file extension and only unzip valid files
                if ext in LMFormat.SHAPE.get_extensions():
                    newname = new_fname_wo_ext + ext
                    success, _ = self.delete_file(newname)
                    zip_in.extract(zname, pth)
                    if not is_temp:
                        oldname = os.path.join(pth, zname)
                        os.rename(oldname, newname)
            # Reset dlocation on successful write
            self.clear_dlocation()
            self.set_dlocation(outfname)
        else:
            raise LMError('{} exists, overwrite = False'.format(outfname))

    # .............................
    def write_temp_from_csv(self, csv_data):
        """Write csv data to a tempoarary file.

        Raises:
            LMError: on failure to write file.
        """
        curr_time = str(gmt().mjd)
        pid = str(os.getpid())
        dump_name = os.path.join(
            UPLOAD_PATH, '{}_{}_dump.csv'.format(curr_time, pid))
        with open(dump_name, 'w') as f_1:
            f_1.write(csv_data)

        with open(dump_name, 'rU') as f_1:
            tmp_name = os.path.join(
                UPLOAD_PATH, '{}_{}.csv'.format(curr_time, pid))
            with open(tmp_name, 'w') as f_2:
                try:
                    for line in f_1:
                        f_2.write(line)
                except Exception as err:
                    raise LMError('Unable to parse input CSV data', err)
        self.clear_dlocation()
        self.set_dlocation(dlocation=tmp_name)

    # .............................
    def _set_geometry(self, convex_hull_buffer=None):
        """Set the geometry of the layer

        From osgeo.ogr.py: "The num_quad_segs parameter can be used to control
        how many segments should be used to define a 90 degree curve - a
        quadrant of a circle. A value of 30 is a reasonable default"
        """
        num_quad_segs = 30
        if all([self._geometry is None, self._convex_hull is None,
                self._features]):
            if self._ogr_type == ogr.wkbPoint:
                gtype = ogr.wkbMultiPoint
            elif self._ogr_type == ogr.wkbLineString:
                gtype = ogr.wkbMultiLineString
            elif self._ogr_type == ogr.wkbPolygon:
                gtype = ogr.wkbMultiPolygon
            elif self._ogr_type == ogr.wkbMultiPolygon:
                gtype = ogr.wkbGeometryCollection
            else:
                raise LMError(
                    ('Only osgeo.ogr types wkbPoint, wkbLineString, '
                     'wkbPolygon, and wkbMultiPolygon are currently '
                     'supported'))
            geom = ogr.Geometry(gtype)
            srs = self.create_srs_from_epsg()
            gidx = self._get_geometry_index()

            for f_vals in list(self._features.values()):
                wkt = f_vals[gidx]
                fgeom = ogr.CreateGeometryFromWkt(wkt, srs)
                if fgeom is None:
                    print(('What happened on point {}?'.format(
                        str(f_vals[self.get_local_id_index()]))))
                else:
                    geom.AddGeometryDirectly(fgeom)
            self._geometry = geom

            # Now set convexHull
            tmp_geom = self._geometry.ConvexHull()
            if tmp_geom.GetGeometryType() != ogr.wkbPolygon:
                # If geom is a single point, not a polygon, buffer it
                if convex_hull_buffer is None:
                    convex_hull_buffer = 0.1
                self._convex_hull = tmp_geom.Buffer(
                    convex_hull_buffer, num_quad_segs)
            elif convex_hull_buffer is not None:
                # If requested buffer
                self._convex_hull = tmp_geom.Buffer(
                    convex_hull_buffer, num_quad_segs)
            else:
                # No buffer
                self._convex_hull = tmp_geom

            # Don't reset Bounding Box for artificial geometry of stacked 3d
            #    data
            minx, maxx, miny, maxy = self._convex_hull.GetEnvelope()
            self._set_bbox((minx, miny, maxx, maxy))

    # .............................
    def get_convex_hull_wkt(self, convex_hull_buffer=None):
        """Get the convex hull WKT for this layer's data.

        Note:
            If the geometry type is Point, and a ConvexHull is a single point,
                buffer the point to create a small polygon.
        """
        wkt = None
        # If requesting a buffer, reset geometry and recalc convexHull
        if convex_hull_buffer is not None:
            self._geometry = None
            self._convex_hull = None
        self._set_geometry(convex_hull_buffer=convex_hull_buffer)
        if self._convex_hull is not None:
            wkt = self._convex_hull.ExportToWkt()
        return wkt

    # .............................
    def get_features_wkt(self):
        """Return Well Known Text (wkt) of the data features."""
        wkt = None
        self._set_geometry()
        if self._geometry is not None:
            wkt = self._geometry.ExportToWkt()
        return wkt

    # .............................
    @staticmethod
    def _get_geom_type(lyr, lyr_def):
        # Special case to handle multi-polygon datasets that are identified
        # as polygon, this because of a broken driver
        geomtype = lyr_def.GetGeomType()
        if geomtype == ogr.wkbPolygon:
            feature = lyr.GetNextFeature()
            while feature is not None:
                fgeom = feature.GetGeometryRef()
                geomtype = fgeom.GetGeometryType()
                if geomtype == ogr.wkbMultiPolygon:
                    break
                feature = lyr.GetNextFeature()
        return geomtype

    # .............................
    def copy_data(self, source_dlocation, target_dlocation=None,
                  format_=DEFAULT_GDAL.driver):
        """Copy data from one file to another.

        Copy source_dlocation dataset to target_dlocation or this layer's
        dlocation.
        """
        if source_dlocation is not None and os.path.exists(source_dlocation):
            if target_dlocation is None:
                if self._dlocation is not None:
                    target_dlocation = self._dlocation
                else:
                    raise LMError('Target location is None')
        else:
            raise LMError(
                'Source location {} is invalid'.format(source_dlocation))

        ogr.RegisterAll()
        drv = ogr.GetDriverByName(format_)
        try:
            dataset = drv.Open(source_dlocation)
        except Exception as e:
            raise LMError('Invalid datasource', source_dlocation, str(e))

        try:
            new_ds = drv.CopyDataSource(dataset, target_dlocation)
            new_ds.Destroy()
        except Exception as e:
            raise LMError('Failed to copy data source')

    # .............................
    @staticmethod
    def test_vector(dlocation, driver=DEFAULT_OGR.driver):
        """Test the vector file for validity"""
        good_data = False
        feat_count = 0
        if dlocation is not None and os.path.exists(dlocation):
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(driver)
            try:
                dataset = drv.Open(dlocation)
            except Exception:
                good_data = False
            else:
                try:
                    slyr = dataset.GetLayer(0)
                except Exception:
                    good_data = False
                else:
                    feat_count = slyr.GetFeatureCount()
                    good_data = True

        return good_data, feat_count

    # .............................
    @staticmethod
    def index_shapefile(dlocation):
        """Create a shptree index on the data

        Args:
            dlocation: target location for the shptree index file
        """
        try:
            shp_tree_cmd = os.path.join(APP_PATH, 'shptree')
            retcode = subprocess.call([shp_tree_cmd, '{}'.format(dlocation)])
            if retcode != 0:
                print('Failed to create shptree index on {}'.format(dlocation))
        except Exception as e:
            print('Failed create shptree index on {}: {}'.format(
                dlocation, str(e)))

    # .............................
    def read_csv_points_with_ids(self, dlocation=None, feature_limit=None,
                                 do_read_data=False):
        """Read data and set features and feature_attributes

        Returns:
            localId position, feat_attrs, feature_count, and features and BBox
                (if read data)

        Note:
            - We are saving only latitude, longitude and localid if it exists.
                If localid does not exist, we create one.
            - If column headers are not present, assume columns 0 = id,
                1 = longitude, 2 = latitude

        TODO:
            - Save the rest of the fields using
                Vector.split_csv_points_to_shapefiles
            - Remove feature_limit, read subset_dlocation if there is a limit
        """
        this_bbox = None
        feats = {}
        feat_attrs = self.get_user_point_feature_attributes()
        localid = None
        if dlocation is None:
            dlocation = self._dlocation
        self.clear_features()
        infile = open(dlocation, 'rU')
        reader = csv.reader(infile)

        # Read row with possible field_names
        row = next(reader)
        has_header = True
        ((_, id_pos), (_, x_pos), (_, y_pos)) = Vector._get_id_xy_pos(row)
        if not id_pos:
            # If no id column, create it
            if (x_pos and y_pos):
                localid = 0
            # If no headers, assume the positions
            else:
                has_header = False
                id_pos = 0
                x_pos = 1
                y_pos = 2
        if x_pos is None or y_pos is None:
            raise LMError('Must supply longitude and latitude')

        if not do_read_data:
            feature_count = sum(1 for row in reader)
            if not has_header:
                feature_count += 1
        else:
            eof = False
            try:
                row = next(reader)
            except StopIteration:
                eof = True
            x_vals = []
            y_vals = []
            while not eof:
                try:
                    if localid is None:
                        thisid = row[id_pos]
                    else:
                        localid += 1
                        thisid = localid
                    x_val = float(row[x_pos])
                    y_val = float(row[y_pos])
                    x_vals.append(x_val)
                    y_vals.append(y_val)
                    feats[thisid] = self.get_user_point_feature(
                        thisid, x_val, y_val)
                    if feature_limit is not None and \
                            len(feats) >= feature_limit:
                        break
                except Exception:
                    # Skip point if fails.  This could be a blank row or data
                    #    error
                    pass
                # Read next row
                try:
                    row = next(reader)
                except StopIteration:
                    eof = True

            feature_count = len(feats)
            if feature_count == 0:
                raise LMError('Unable to read points from CSV')
            try:
                min_x = min(x_vals)
                min_y = min(y_vals)
                max_x = max(x_vals)
                max_y = max(y_vals)
                this_bbox = (min_x, min_y, max_x, max_y)
            except Exception as e:
                raise LMError(
                    'Failed to get valid coordinates ({})'.format(str(e)))

        infile.close()
        return (this_bbox, id_pos, feats, feat_attrs, feature_count)

    # .............................
    def read_with_OGR(self, dlocation, ogr_format, feature_limit=None,
                      do_read_data=False):
        """Read the vector file using OGR.

        Read OGR-accessible data and set the features and feature_attributes on
        the Vector object

        Args:
            dlocation: Full path location of the data
            ogr_format: OGR-supported data format code, available at
                http://www.gdal.org/ogr/ogr_formats.html

        Returns:
            bool - Indication of success

        Raises:
            LMError: On failure to read data.

        Note:
            populate_stats calls this

        TODO:
            remove feature_limit, read subset_dlocation if there is a limit
        """
        this_bbox = local_id_idx = geom_idx = feats = feat_attrs = None
        if dlocation is not None and os.path.exists(dlocation):
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(ogr_format)
            try:
                dataset = drv.Open(dlocation)
            except Exception as e:
                raise LMError('Invalid datasource' % dlocation, str(e))

            self.clear_features()
            try:
                slyr = dataset.GetLayer(0)
            except Exception as err:
                raise LMError(
                    '#### Failed to GetLayer from {}'.format(dlocation), err,
                    do_trace=True)

            # Get bounding box
            (min_x, max_x, min_y, max_y) = slyr.GetExtent()
            this_bbox = (min_x, min_y, max_x, max_y)

            # .........................
            # Read field structure (feat_attrs)
            lyr_def = slyr.GetLayerDefn()
            fld_count = lyr_def.GetFieldCount()
            found_local_id = False
#             geomtype = self._get_geom_type(slyr, lyr_def)
            # Read Fields (indexes start at 0)
            feat_attrs = {}
            for i in range(fld_count):
                fld = lyr_def.GetFieldDefn(i)
                fldname = fld.GetNameRef()
                # Provided attribute name takes precedence
                if fldname == self._fid_attribute:
                    local_id_idx = i
                    found_local_id = True
                # Don't reset if already found
                if not found_local_id and \
                        fldname in OccurrenceFieldNames.LOCAL_ID:
                    local_id_idx = i
                    found_local_id = True
                feat_attrs[i] = (fld.GetNameRef(), fld.GetType())

            # .........................
            # Add fields FID (if not present) and geom to feat_attrs
            i = fld_count
            if not found_local_id:
                feat_attrs[i] = (
                    self._local_id_field_name, self._local_id_field_type)
                local_id_idx = i
                i += 1
            feat_attrs[i] = (self._geom_field_name, self._geom_field_type)
            geom_idx = i

            # .........................
            # Read data (features)
            feats = {}
            feat_count = slyr.GetFeatureCount()
            if do_read_data:
                # Limit the number of features to read (for mapping and
                #    modeling)
                if feature_limit is not None and feature_limit < feat_count:
                    feat_count = feature_limit
                try:
                    for j in range(feat_count):
                        curr_feat = slyr.GetFeature(j)
                        if curr_feat is not None:
                            curr_feature_vals = []
                            for k in range(fld_count):
                                val = curr_feat.GetField(k)
                                curr_feature_vals.append(val)
                                if k == local_id_idx:
                                    localid = val
                            # Add values localId (if not present) and geom to
                            #    features
                            if not found_local_id:
                                localid = curr_feat.GetFID()
                                curr_feature_vals.append(localid)
                            curr_feature_vals.append(
                                curr_feat.geometry().ExportToWkt())

                            # Add the feature values with key=localId to the
                            #    dictionary
                            feats[localid] = curr_feature_vals
                except Exception as e:
                    raise LMError(
                        'Failed to read features from {} ({})'.format(
                            dlocation, str(e)), do_trace=True)

            # self.set_features(features, feat_attrs, feature_count=feat_count)
        else:
            raise LMError('dlocation {} does not exist'.format(dlocation))
        return this_bbox, local_id_idx, geom_idx, feats, feat_attrs, feat_count

    # .............................
    def _transform_bbox(self, orig_epsg=None, orig_bbox=None):
        if orig_epsg is None:
            orig_epsg = self._epsg
        if orig_bbox is None:
            orig_bbox = self._bbox
        min_x, min_y, max_x, max_y = orig_bbox

        if orig_epsg != DEFAULT_EPSG:
            src_srs = osr.SpatialReference()
            src_srs.ImportFromEPSG(orig_epsg)
            dst_srs = osr.SpatialReference()
            dst_srs.ImportFromEPSG(DEFAULT_EPSG)

            sp_transform = osr.CoordinateTransformation(src_srs, dst_srs)
            # Allow for return of either (x, y) or (x, y, z)
            retvals = sp_transform.TransformPoint(min_x, min_y)
            new_min_x, new_min_y = retvals[0], retvals[1]
            retvals = sp_transform.TransformPoint(max_x, max_y)
            new_max_x, new_max_y = retvals[0], retvals[1]
            return (new_min_x, new_min_y, new_max_x, new_max_y)

        return orig_bbox

    # .............................
    def read_data(self, dlocation=None, data_format=None, feature_limit=None,
                  do_read_data=False):
        """Reads the file at dlocation and fills the related attributes.

        Reading data fills the feature_count value and feature_attributes
        dictionary.  If do_read_data is True, reads and fills features
        dictionary.

        Args:
            dlocation: file location, overrides object attribute
            data_format: OGR format driver name, overrides object attribute
            feature_limit: limits number of features to be read into _features
            do_read_data (boolean): flag indicating whether to read features
                into ; False to read only the
                 feature_attributes and feature count.

        Returns:
            New bbox string, indices of the localId int and geometry fields

        TODO:
            Remove feature_limit, read subset_dlocation if there is a limit
        """
        new_bbox = local_id_idx = geom_idx = None
        if dlocation is None:
            dlocation = self._dlocation
        if data_format is None:
            data_format = self._data_format
        if dlocation is not None and os.path.exists(dlocation):
            if data_format == DEFAULT_OGR.driver:
                (this_bbox, local_id_idx, geom_idx, features,
                 feature_attributes, feature_count
                 ) = self.read_with_OGR(
                     dlocation, data_format, feature_limit=feature_limit,
                     do_read_data=do_read_data)
            # only for Point data
            elif data_format == 'CSV':
                (this_bbox, local_id_idx, features, feature_attributes,
                 feature_count) = self.read_csv_points_with_ids(
                     dlocation=dlocation, feature_limit=feature_limit,
                     do_read_data=do_read_data)
            self.set_features(
                features, feature_attributes, feature_count=feature_count)
            new_bbox = self._transform_bbox(orig_bbox=this_bbox)
        return (new_bbox, local_id_idx, geom_idx)

    # .............................
    def get_ogr_layer_type_name(self, ogr_wkb_type=None):
        """Get the OGR layer type name
        """
        if ogr_wkb_type is None:
            ogr_wkb_type = self._ogr_type
        # Subset of all ogr layer types
        if ogr_wkb_type == ogr.wkbPoint:
            return 'ogr.wkbPoint'
        if ogr_wkb_type == ogr.wkbLineString:
            return 'ogr.wkbLineString'
        if ogr_wkb_type == ogr.wkbPolygon:
            return 'ogr.wkbPolygon'
        if ogr_wkb_type == ogr.wkbMultiPolygon:
            return 'ogr.wkbMultiPolygon'
        return None

    # .............................
    def get_field_metadata(self):
        """Get field metadata
        """
        if self._feature_attributes:
            fld_metadata = {}
            for idx, feat_attrs in self._feature_attributes.items():
                fld_metadata[idx] = (
                    feat_attrs[0],
                    self._get_ogr_field_type_name(feat_attrs[1]))
        return fld_metadata

    # .............................
    @staticmethod
    def _get_ogr_field_type_name(ogr_oft_type):
        if ogr_oft_type == ogr.OFTBinary:
            return 'ogr.OFTBinary'
        if ogr_oft_type == ogr.OFTDate:
            return 'ogr.OFTDate'
        if ogr_oft_type == ogr.OFTDateTime:
            return 'ogr.OFTDateTime'
        if ogr_oft_type == ogr.OFTInteger:
            return 'ogr.OFTInteger'
        if ogr_oft_type == ogr.OFTReal:
            return 'ogr.OFTReal'
        if ogr_oft_type == ogr.OFTString:
            return 'ogr.OFTString'

        return 'ogr Field Type constant: ' + str(ogr_oft_type)

    # .............................
    def get_feature_val_by_field_name(self, field_name, feature_fid):
        """Get feature value by field name
        """
        field_idx = self.get_field_index(field_name)
        return self.get_feature_val_by_field_index(field_idx, feature_fid)

    # .............................
    def get_feature_val_by_field_index(self, field_idx, feature_fid):
        """Get feature value by field index
        """
        if self._features:
            if feature_fid in self._features:
                return self._features[feature_fid][field_idx]

            raise LMError(
                'Feature ID {} not found in dataset {}'.format(
                    feature_fid, self._dlocation))

        raise LMError('Dataset features are empty.')

    # .............................
    def get_field_index(self, field_name):
        """Get the field index for the provided field name
        """
        if self._feature_attributes:
            field_idx = None
            field_local_id = bool(field_name in OccurrenceFieldNames.LOCAL_ID)
            for fldidx, (fldname, _) in self._feature_attributes.items():

                if fldname == field_name:
                    field_idx = fldidx
                    break
                if field_local_id and \
                        fldname in OccurrenceFieldNames.LOCAL_ID:
                    field_idx = fldidx
                    break

            return field_idx

        raise LMError('Dataset feature_attributes are empty.')

    # .............................
    def get_srs(self):
        """Get the SRS for the layer
        """
        if self._dlocation is not None and os.path.exists(self._dlocation):
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(self._data_format)
            try:
                dataset = drv.Open(self._dlocation)
                vlyr = dataset.GetLayer(0)
                srs = vlyr.GetSpatialRef()
                if srs is None:
                    srs = self.create_srs_from_epsg()
                return srs
            except Exception as err:
                raise LMError('Invalid datasource', self._dlocation, err)

        raise LMError('Input file {} does not exist'.format(self._dlocation))

    # .............................
    def _fill_ogr_feature(self, feat, f_vals):
        # Fill the fields
        for j in list(self._feature_attributes.keys()):
            fldname, _ = self._feature_attributes[j]
            val = f_vals[j]
            if fldname == self._geom_field_name:
                geom = ogr.CreateGeometryFromWkt(val)
                feat.SetGeometryDirectly(geom)
            elif val is not None and val != 'None':
                feat.SetField(fldname, val)
