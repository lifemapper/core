"""Layer module
"""
import glob
from io import StringIO
import os
import subprocess
import zipfile

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.attribute_object import LmAttObj
from LmCommon.common.lmconstants import (
    GEOTIFF_INTERFACE, OFTInteger, OFTString, SHAPEFILE_INTERFACE)
from LmCommon.common.time import gmt
from LmCommon.common.verify import compute_hash, verify_hash
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.service_object import ServiceObject
from LmServer.common.geo_tools import GeoFileInfo
from LmServer.common.lmconstants import (GDALDataTypes, OGRDataTypes, 
    LMFormat, LMServiceType, OccurrenceFieldNames, UPLOAD_PATH)
from LmServer.common.localconstants import APP_PATH, DEFAULT_EPSG
from osgeo import gdal, gdalconst, ogr, osr

DEFAULT_OGR = LMFormat.SHAPE
DEFAULT_GDAL = LMFormat.GTIFF

# .............................................................................
class _Layer(LMSpatialObject, ServiceObject):
    """Superclass for all spatial layer objects.
    """
    META_IS_CATEGORICAL = 'isCategorical'
    META_IS_DISCRETE = 'isDiscrete'

    # .............................
    def __init__(self, name, user_id, epsgcode, lyr_id=None, squid=None,
                 ident=None, verify=None, dlocation=None, metadata={},
                 data_format=None, gdal_type=None, ogr_type=None, val_units=None,
                 val_attribute=None, nodata_val=None, min_val=None, max_val=None,
                 resolution=None, bbox=None, mapunits=None, svc_obj_id=None,
                 service_type=LMServiceType.LAYERS, metadata_url=None,
                 parent_metadata_url=None, mod_time=None):
        """Layer superclass constructor

        Args:
            name: layer name, unique with user_id and epsq
            user_id: user identifier
            epsgcode (int): EPSG code indicating the SRS to use
            lyr_id (int): record identifier for layer 
            squid (int): locally unique identifier for taxa-related data
            ident (int): locally unique identifier for taxa-related data
            verify: hash of the data for verification
            dlocation: data location (url, file path, ...)
            metadata: dictionary of metadata key/values; uses class or 
                              superclass attribute constants META_* as keys
            data_format: ogr or gdal code for spatial data file format
                GDAL Raster Format code at http://www.gdal.org/formats_list.html.
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
            resolution: resolution of the data - pixel size in @mapunits
            bbox: spatial extent of data
                sequence in the form (minX, minY, maxX, maxY)
                or comma-delimited string in the form 'minX, minY, maxX, maxY'
            mapunits: units of measurement for the data. 
                These are keywords as used in  mapserver, choice of 
                LmCommon.common.lmconstants.LegalMapUnits
                described in http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
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
        LMSpatialObject.__init__(self, epsgcode, bbox, mapunits)
        ServiceObject.__init__(
            self, user_id, svc_obj_id, service_type, metadata_url=metadata_url,
            parent_metadata_url=parent_metadata_url, mod_time=mod_time)
#        ogr.UseExceptions()
        self.name = name
        self._layer_user_id = user_id
        self._layer_id = lyr_id
        self.squid = squid
        self.ident = ident
        self.lyr_metadata = {}
        self.load_lyr_metadata(metadata)
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
#         self.set_verify(verify=verify)
        self._map_filename = None

#     # .............................
#     @staticmethod
#     def isGeoSimilar(self, lyr1, lyr2):
#         if lyr1.epsgcode != lyr2.epsgcode:
#             return False
#         if lyr1.bbox != lyr2.bbox:
#             return False
#         if ((lyr1.resolution is not None and lyr2.resolution is not None) and
#                 (lyr1.resolution != lyr2.resolution)):
#             return False
#         return True

    # .............................
    def set_layer_id(self, lyr_id):
        """
        @summary: Sets the database id of the Layer record, which can be used
                     by multiple Parameterized Layer objects
        @param lyr_id: The record id for the database
        """
        self._layer_id = lyr_id

    # .............................
    def get_layer_id(self):
        """Returns the database id of the layer record.
        """
        return self._layer_id

#     # .............................
#     def set_layer_user_id(self, lyruserid):
#         """
#         @summary: Sets the User id of the Layer record, which can be used by
#                      by multiple Parameterized Layer objects
#         @param lyruserid: The user id for the layer
#         """
#         self._layer_user_id = lyruserid
# 
#     # .............................
#     def get_layer_user_id(self):
#         """
#         @summary: Returns the User id of the Layer record, which can be used by
#                      by multiple Parameterized Layer objects
#         """
#         return self._layer_user_id

    # .............................
    @property
    def data_format(self):
        return self._data_format

    # .............................
    @property
    def gdal_type(self):
        return self._gdal_type

    # .............................
    @property
    def ogr_type(self):
        return self._ogr_type

    # .............................
    @property
    def val_attribute(self):
        return self._val_attribute

    # .............................
    def read_data(self, dlocation, driver_type):
        """
        @summary: Read OGR- or GDAL data and save the on the _Layer object
        @param dlocation: Location of the data
        @param ogr_type: GDAL or OGR-supported data format type code, available at
                             http://www.gdal.org/formats_list.html and
                             http://www.gdal.org/ogr/ogr_formats.html
        @return: boolean for success/failure
        @raise LMError: on failure to read data.
        """
        raise LMError('read_data must be implemented in Subclass')

    # .............................
    def compute_hash(self, dlocation=None, content=None):
        """
        @summary: Compute the sha256sum of the file at dlocation.
        @return: hash string for data file
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
        """
        @summary: Compute the sha256sum of the file at dlocation.
        @param hash: hash string to compare with data
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
        self._verify = None

    # .............................
    @property
    def verify(self):
        return self._verify

    # .............................
    def get_meta_location(self):
        return self._metalocation

    # .............................
    def get_relative_dlocation(self):
        """
        @summary: Return the relative filepath for object data
        @note: If the object does not have an ID, this returns None
        @note: This is to be pre-pended with a relative directory name for data
                 used by a single workflow/Makeflow
        """
        basename = None
        self.set_dlocation()
        if self._dlocation is not None:
            _, basename = os.path.split(self._dlocation)
        return basename

    # .............................
    def create_local_dlocation(self, extension):
        """
        @summary: Create an absolute filepath from object attributes
        @note: If the object does not have an ID, this returns None
        """
        dloc = self._earl_jr.create_other_layer_filename(
            self._layer_user_id, self._epsg, self.name, ext=extension)
        return dloc

    # .............................
    def get_dlocation(self):
        """
        @summary: Return the _dlocation attribute
        @note: Do not create and populate value by default.
        """
        return self._dlocation

    # .............................
    def clear_dlocation(self):
        """
        @summary: Clear the _dlocation attribute
        """
        self._dlocation = None
        self._absolute_path = None
        self._base_filename = None

    # .............................
    def set_dlocation(self, dlocation=None):
        """
        @summary: Set the Layer._dlocation attribute if it is None.  Use dlocation
                     if provided, otherwise calculate it.
        @note: Does NOT override existing dlocation, use clear_dlocation for that
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
    def clear_data(self):
        raise LMError('Method must be implemented in subclass')

    # .............................
    def copy_data(self):
        raise LMError('Method must be implemented in subclass')

    # .............................
    def get_absolute_path(self):
        if self._absolute_path is None and self._dlocation is not None:
            self._absolute_path, self._base_filename = os.path.split(
                self._dlocation)
        return self._absolute_path

    # .............................
    def get_base_filename(self):
        if self._base_filename is None and self._dlocation is not None:
            self._absolute_path, self._base_filename = os.path.split(
                self._dlocation)
        return self._base_filename

    # .............................
    def dump_lyr_metadata(self):
        return super(_Layer, self)._dump_metadata(self.lyr_metadata)

    # .............................
    def load_lyr_metadata(self, new_metadata):
        self.lyr_metadata = super(_Layer, self)._load_metadata(new_metadata)

    # .............................
    def add_lyr_metadata(self, new_metadata_dict):
        self.lyr_metadata = super(
            _Layer, self)._add_metadata(
                new_metadata_dict, existing_metadata_dict=self.lyr_metadata)

    # .............................
    def update_layer(self, mod_time, metadata=None):
        """
        @summary: Updates mod_time, data verification hash, metadata.
        @param mod_time: time/date last modified
        @param metadata: Dictionary of metadata keys/values; key constants are
                              class attributes.
        """
        self.update_mod_time(mod_time)
        if metadata is not None:
            self.load_lyr_metadata(metadata)
        self.set_verify()


# .............................................................................
class _LayerParameters(LMObject):
    """Decorator for _Layer class """
    # Constants for metadata dictionary keys
    PARAM_FILTER_STRING = 'filterString'
    PARAM_VAL_NAME = 'valName'
    PARAM_VAL_UNITS = 'valUnits'

    # .............................
    def __init__(self, user_id, param_id=None, matrix_index=-1, metadata={}, 
                 mod_time=None):
        """Constructor for _LayerParameters decorator class
        
        Args:
            user_id: user identifier
                if these parameters are not held in a separate db table, this
                value is the same as _Layer._layer_user_id
            param_id: database Id for the parameter values
                if these parameters are not held in a separate db table, this 
                value is the same as _Layer._layer_id.  
            matrix_index: Index of the position in PAM or other matrix.  If this 
                             Parameterized Layer is not a Matrix input, value is -1.
        @param metadata: Dictionary of metadata keys/values; key constants are 
                              class attributes.
        @param mod_time: time/date last modified
        """
        self._param_user_id = user_id
        self._param_id = param_id
        self.param_metadata = {}
        self.load_param_metadata(metadata)
        self._matrix_index = matrix_index
        self.param_mod_time = mod_time

    # .............................
    def dump_param_metadata(self):
        return super(_LayerParameters, self)._dump_metadata(self.param_metadata)

    # .............................
    def load_param_metadata(self, new_metadata):
        self.param_metadata = super(
            _LayerParameters, self)._load_metadata(new_metadata)

    # .............................
    def add_param_metadata(self, new_metadata_dict):
        self.param_metadata = super(
            _LayerParameters, self)._add_metadata(
                new_metadata_dict, existing_metadata_dict=self.param_metadata)

    # .............................
    def set_param_id(self, param_id):
        """
        @summary: Sets the database id of the Layer Parameters (either
                     PresenceAbsence or AncillaryValues) record, which can be
                     used by multiple Parameterized Layer objects
        @param param_id: The record id for the database
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
        """
        @summary: Sets the User id of the Layer Parameters (either
                     PresenceAbsence or AncillaryValues) record, which can be
                     used by multiple Parameterized Layer objects
        @param usr: The user id for the parameters
        """
        self._param_user_id = usr

    # .............................
    def get_param_user_id(self):
        """
        @summary: Returns the User id of the Layer Parameters (either 
                     PresenceAbsence or AncillaryValues) record, which can be used by 
                     multiple Parameterized Layer objects
        """
        return self._param_user_id

    # .............................
    def set_matrix_index(self, matrix_idx):
        """
        @summary: Sets the _matrix_index on the object.  This identifies 
                     the position of the parameterized layer object in the 
                     appropriate MatrixLayerset (PAM or GRIM)
        @param matrix_index: The matrix_index
        """
        self._matrix_index = matrix_idx

    # .............................
    def get_matrix_index(self):
        """
        @summary: Returns _matrix_index on the layer.  This identifies 
                     the position of the parameterized layer object in a 
                     MatrixLayerset (and the PAM or GRIM)
        """
        return self._matrix_index

    # .............................
    def set_tree_index(self, tree_idx):
        """
        @summary: Sets the _treeIndex on the layer.  This identifies 
                     the position of the layer in a tree
        @param treeIdx: The treeIndex
        """
        self._tree_index = tree_idx

    # .............................
    def get_tree_index(self):
        """
        @summary: Returns _tree_index on the object.  This identifies 
                     the position of the layer in a tree
        """
        return self._tree_index

    # .............................
    def update_params(self, mod_time, matrix_index=None, metadata=None):
        """
        @summary: Updates matrix_index, param_metadata, and mod_time.
        @param mod_time: time/date last modified
        @param matrix_index: Index of the position in PAM or other matrix.  If this 
                             Parameterized Layer is not a Matrix input, or part of 
                             a Global PAM, created dynamically upon query of existing
                             matrix columns, value is -1.
        @param metadata: Dictionary of metadata keys/values; key constants are 
                              class attributes.
        @note: Missing keyword parameters are ignored.
        """
        self.param_mod_time = mod_time
        if metadata is not None:
            self.load_param_metadata(metadata)
        if matrix_index is not None:
            self._matrix_index = matrix_index


# .............................................................................
class Raster(_Layer):
    """Class to hold information about a raster dataset.
    """

    # .............................
    def __init__(self, name, user_id, epsgcode, lyr_id=None,
                     squid=None, ident=None, verify=None, dlocation=None,
                     metadata={}, data_format=DEFAULT_GDAL.driver,
                     gdal_type=None,
                     val_units=None, nodata_val=None, min_val=None, max_val=None,
                     resolution=None, bbox=None, mapunits=None, 
                     svc_obj_id=None, service_type=LMServiceType.LAYERS,
                     metadata_url=None, parent_metadata_url=None, mod_time=None):
        """Constructor for Raster superclass, inherits from _Layer
        
        Args:
            name: layer name, unique with user_id and epsq
            user_id: user identifier
            epsgcode (int): EPSG code indicating the SRS to use
            lyr_id (int): record identifier for layer 
            squid (int): locally unique identifier for taxa-related data
            ident (int): locally unique identifier for taxa-related data
            verify: hash of the data for verification
            dlocation: data location (url, file path, ...)
            metadata: dictionary of metadata key/values; uses class or 
                              superclass attribute constants META_* as keys
            data_format: gdal code for spatial data file format
                GDAL Raster Format code at http://www.gdal.org/formats_list.html.
            gdal_type (osgeo.gdalconst): GDAL data_type
                GDALDataType in http://www.gdal.org/gdal_8h.html
            val_units: units of measurement for data
            nodata_val: value indicating feature/pixel does not contain data
            min_val: smallest value in data
            max_val: largest value in data
            resolution: resolution of the data - pixel size in @mapunits
            bbox: spatial extent of data
                sequence in the form (minX, minY, maxX, maxY)
                or comma-delimited string in the form 'minX, minY, maxX, maxY'
            mapunits: units of measurement for the data. 
                These are keywords as used in  mapserver, choice of 
                LmCommon.common.lmconstants.LegalMapUnits
                described in http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
            svc_obj_id (int): unique db record identifier for an object  
                retrievable by REST web services.  May be filled with the 
                base layer_id or a unique parameterized id
            service_type: type of data for REST retrieval LMServiceType.LAYERS 
            metadata_url: REST URL for API returning this object
            parent_metadata_url: REST URL for API returning this parent object
            mod_time: time of last modification, in MJD
        """
        self.verify_data_description(gdal_type, data_format)
        if (dlocation is not None and
             os.path.exists(dlocation) and
             (verify is None or gdal_type is None or data_format is None or
              resolution is None or bbox is None or
              min_val is None or max_val is None or nodata_val)):

            (dlocation, verify, gdal_type, data_format, bbox, resolution, min_val,
             max_val, nodata_val) = self.populateStats(dlocation, verify,
                                                                  gdal_type, data_format, bbox,
                                                                  resolution, min_val, max_val,
                                                                  nodata_val)
        _Layer.__init__(self, name, user_id, epsgcode, lyr_id=lyr_id,
                     squid=squid, ident=ident, verify=verify, dlocation=dlocation,
                     metadata=metadata, data_format=data_format, gdal_type=gdal_type,
                     val_units=val_units, val_attribute='pixel',
                     nodata_val=nodata_val, min_val=min_val, max_val=max_val,
                     mapunits=mapunits, resolution=resolution, bbox=bbox,
                     svc_obj_id=svc_obj_id, service_type=service_type,
                     metadata_url=metadata_url, parent_metadata_url=parent_metadata_url,
                     mod_time=mod_time)

#     # .............................
#     def getFormatLongName(self):
#         name = ''
#         drv = gdal.GetDriverByName(self._data_format)
#         if drv is not None:
#             name = drv.GetMetadataItem('DMD_LONGNAME')
#         return name

    # .............................
    def _set_is_discrete_data(self, is_discrete_data, is_categorical):
        if is_discrete_data is None:
            if is_categorical:
                is_discrete_data = True
            else:
                is_discrete_data = False
        self._is_discrete_data = is_discrete_data

    # .............................
    def create_local_dlocation(self, ext=None):
        """
        @summary: Create local filename for this layer.  
        @param ext: File extension for filename
        @note: Data files which are not default User data files (stored 
                  locally and using this method)
                 (in /UserData/<user_id>/<epsgcode>/Layers directory) should be 
                 created in the appropriate Subclass (EnvironmentalLayer, OccurrenceSet, 
                 SDMProjections) 
        """
        if ext is None:
            if self._data_format is None:
                ext = LMFormat.TMP.ext
            else:
                ext = LMFormat.get_extension_by_driver(self._data_format)
                if ext is None:
                    raise LMError('Failed to find data_format/driver {}'
                                      .format(self._data_format))
        dloc = super(Raster, self).create_local_dlocation(ext)
        return dloc

    # .............................
    @staticmethod
    def test_raster(dlocation, bandnum=1):
        """
        @return: a GDAL dataset object
        """
        success = True
        try:
            try:
                dataset = gdal.Open(str(dlocation), gdalconst.GA_ReadOnly)
            except Exception as e:
                raise LMError('Unable to open dataset {} with GDAL ({})'
                                    .format(dlocation, str(e)))
            try:
                band = dataset.GetRasterBand(bandnum)
            except Exception as e:
                raise LMError('No band {} in dataset {} ({})'
                                    .format(band, dlocation, str(e)))
        except:
            success = False
        return success

    # .............................
    def verify_data_description(self, gdal_type, gdalFormat):
        """
        @summary Verifies that the data_type and format are either LM-supported 
                    GDAL types or None.
        @raise LMError: Thrown when gdalFormat is missing or 
                 either gdalFormat or gdal_type is not legal for a Lifemapper Raster.  
        """
        # GDAL data_format is required (may be a placeholder and changed later)
        if gdalFormat not in LMFormat.gdal_drivers():
            raise LMError(['Unsupported Raster GDAL data_format', gdalFormat])
        if gdal_type is not None and gdal_type not in GDALDataTypes:
            raise LMError(['Unsupported Raster GDAL type', gdal_type])

    # .............................
    def open_with_gdal(self, dlocation=None, bandnum=1):
        """
        @return: a GDAL dataset object
        """
        if dlocation is None:
            dlocation = self._dlocation
        try:
            dataset = gdal.Open(str(dlocation), gdalconst.GA_ReadOnly)
            band = dataset.GetRasterBand(bandnum)
        except Exception as e:
            raise LMError(['Unable to open dataset or band {} with GDAL ({})'
                                .format(dlocation, str(e))])
        return dataset, band

    # .............................
    def get_data_url(self, interface=GEOTIFF_INTERFACE):
        """
        @note: the ServiceObject._dbId may contain a join id or _layer_id depending 
                 on the type of Layer being requested
        """
        durl = self._earl_jr.construct_lm_data_url(self.service_type, 
                                                   self.get_id(), interface)
        return durl

    # .............................
    def get_histogram(self, bandnum=1):
        """
        @return: a list of data values present in the dataset
        @note: this returns only a list, not a true histogram.  
        @note: this only works on 8-bit data.
        """
        vals = []
        _, band = self.open_with_gdal(bandnum=bandnum)

        # Get histogram only for 8bit data (projections)
        if band.DataType == gdalconst.GDT_Byte:
            hist = band.GetHistogram()
            for i in range(len(hist)):
                if i > 0 and i != self.nodata_val and hist[i] > 0:
                    vals.append(i)
        else:
            print('Histogram calculated only for 8-bit data')
        return vals

    # .............................
    def get_is_discrete_data(self):
        return self._is_discrete_data

    # .............................
    def get_size(self, bandnum=1):
        """
        @summary: Return a tuple of xsize and ysize (in pixels).
        @return: A tuple of size 2, where the first number is the number of 
                    columns and the second number is the number of rows.
        """
        dataset, _ = self.open_with_gdal(bandnum=bandnum)
        size = (dataset.RasterXSize, dataset.RasterYSize)
        return size

    # .............................
    def populate_stats(self, dlocation, verify, gdal_type, data_format, bbox, resolution,
                            min_val, max_val, nodata_val, bandnum=1):
        """
        @summary: Updates or fills layer parameters by reading the data.
        @postcondition: prints warning if file is invalid raster
        @postcondition: prints warning if data format and type differ from GDAL-reported
        @postcondition: renames file with supported extension if it differs
        """
        msgs = []
#         msgs.append('File does not exist: {}'.format(dlocation))
        dataset, band = self.open_with_gdal(dlocation=dlocation, bandnum=bandnum)
#         srs = dataset.GetProjection()
#         size = (dataset.RasterXSize, dataset.RasterYSize)
        geoTransform = dataset.GetGeoTransform()
        ulx = geoTransform[0]
        xPixelSize = geoTransform[1]
        uly = geoTransform[3]
        yPixelSize = geoTransform[5]


        drv = dataset.GetDriver()
        gdalFormat = drv.GetDescription()
        if data_format is None:
            data_format = gdalFormat
        elif data_format != gdalFormat:
            msgs.append('Incorrect gdalFormat {}, changing to {} for layer {}'
                            .format(data_format, gdalFormat, dlocation))
            data_format = gdalFormat
        # Rename with correct extension if incorrect
        head, ext = os.path.splitext(dlocation)
        correctExt = LMFormat.get_extension_by_driver(data_format)
        if correctExt is None:
            raise LMError('Failed to find data_format/driver {}'
                              .format(data_format))
#         correctExt = GDALFormatCodes[data_format]['FILE_EXT']
        if ext != correctExt:
            msgs.append('Invalid extension {}, renaming to {} for layer {}'
                            .format(ext, correctExt, dlocation))
            oldDl = dlocation
            dlocation = head + correctExt
            os.rename(oldDl, dlocation)

        # Assumes square pixels
        if resolution is None:
            resolution = xPixelSize
        if bbox is None:
            lrx = ulx + xPixelSize * dataset.RasterXSize
            lry = uly + yPixelSize * dataset.RasterYSize
            bbox = [ulx, lry, lrx, uly]
        if gdal_type is None:
            gdal_type = band.DataType
        elif gdal_type != band.DataType:
            msgs.append('Incorrect datatype {}, changing to {} for layer {}'
                            .format(gdal_type, band.DataType, dlocation))
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
            print('Layer.populateStats Warning: \n{}'.format('\n'.join(msgs)))

        return (dlocation, verify, gdal_type, data_format, bbox, resolution,
                  min_val, max_val, nodata_val)

    # .............................
    def read_from_uploaded_data(self, datacontent, overwrite=False,
                                     extension=DEFAULT_GDAL.ext):
        """
        @summary: Read from uploaded data by writing to temporary file, saving 
                     temporary filename in dlocation.  
        @raise LMError: on failure to write data or read temporary files.
        """
        self.clear_dlocation()
        # Create temp location and write layer to it
        outLocation = os.path.join(UPLOAD_PATH, self.name + extension)
        self.write_layer(srcData=datacontent, outFile=outLocation, overwrite=True)
        self.set_dlocation(dlocation=outLocation)

    # .............................
    def write_layer(self, srcData=None, srcFile=None, outFile=None, overwrite=False):
        """
        @summary: Writes raster data to file.
        @param data: A stream, string, or file of valid raster data
        @param overwrite: True/False directing whether to overwrite existing 
                     file or not
        @postcondition: The raster file is written to the filesystem.
        @raise LMError: on 1) failure to write file 
                                 2) attempt to overwrite existing file with overwrite=False
                                 3) _dlocation is None  
        """
        if outFile is None:
            outFile = self.get_dlocation()
        if outFile is not None:
            self.ready_filename(outFile, overwrite=overwrite)

            # Copy from input file using GDAL (no test necessary later)
            if srcFile is not None:
                self.copy_data(srcFile, target_dlocation=outFile)

            # Copy from input stream
            elif srcData is not None:
                try:
                    f = open(outFile, "w")
                    f.write(srcData)
                    f.close()
                except Exception as e:
                    raise LMError('Error writing data to raster %s (%s)'
                                      % (outFile, str(e)))
                else:
                    self.set_dlocation(dlocation=outFile)
                # Test input with GDAL
                try:
                    self.populateStats()
                except Exception as e:
                    success, msg = self.deleteFile(outFile)
                    raise LMError('Invalid data written to %s (%s); Deleted (success=%s, %s)'
                                      % (outFile, str(e), str(success), msg))
            else:
                raise LMError('Source data or source filename required for write to %s'
                                  % self._dlocation)
        else:
            raise LMError(['Must set_dlocation before writing file'])

    # .............................
    def _copy_gdal_data(self, bandnum, infname, outfname, format_='GTiff', kwargs={}):
        """
        @summary: Copy the dataset into a new file.
        @param bandnum: The band number to read.
        @param outfname: Filename to write this dataset to.
        @param format: GDAL-writeable raster format to use for new dataset. 
                            http://www.gdal.org/formats_list.html
        @param doFlip: True if data begins at the southern edge of the region
        @param doShift: True if the leftmost edge of the data should be shifted 
                 to the center (and right half shifted around to the beginning) 
        @param nodata: Value used to indicate nodata in the new file.
        @param srs: Spatial reference system to use for the data. This is only 
                        necessary if the dataset does not have an SRS present.  This
                        will NOT project the dataset into a different projection.
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
            raise LMError('Driver %s does not support CreateCopy() method.'
                              % format_)
        inds = gdal.Open(infname)
        try:
            outds = driver.CreateCopy(outfname, inds, 0, options)
        except Exception as e:
            raise LMError('Creation failed for %s from band %d of %s (%s)'
                                          % (outfname, bandnum, infname, str(e)))
        if outds is None:
            raise LMError('Creation failed for %s from band %d of %s)'
                                          % (outfname, bandnum, infname))
        # Close new dataset to flush to disk
        outds = None
        inds = None

    # .............................
    def copy_data(self, source_dlocation, target_dlocation=None,
                 format_='GTiff'):
        
        if not format_ in LMFormat.gdal_drivers():
            raise LMError('Unsupported raster format {}'.format(format_))
        if source_dlocation is not None and os.path.exists(source_dlocation):
            if target_dlocation is not None:
                dlocation = target_dlocation
            elif self._dlocation is not None:
                dlocation = self._dlocation
            else:
                raise LMError('Target location is None')
        else:
            raise LMError('Source location %s is invalid' % str(source_dlocation))

        correctExt = LMFormat.get_extension_by_driver(format_)
        if not dlocation.endswith(correctExt):
            dlocation += correctExt

        self.ready_filename(dlocation)

        try:
            self._copy_gdal_data(1, source_dlocation, dlocation, format_=format_)
        except Exception as e:
            raise LMError('Failed to copy data source from %s to %s (%s)'
                              % (source_dlocation, dlocation, str(e)))

    # .............................
    def get_srs(self):
        if (self._dlocation is not None and os.path.exists(self._dlocation)):
            ds = gdal.Open(str(self._dlocation), gdalconst.GA_ReadOnly)
            wktSRS = ds.GetProjection()
            if wktSRS is not '':
                srs = osr.SpatialReference()
                srs.ImportFromWkt(wktSRS)
            else:
                srs = self.create_srs_from_epsg()
            ds = None
            return srs
        else:
            raise LMError('Input file %s does not exist' % self._dlocation)

    # .............................
    def write_srs(self, srs):
        """
        @summary: Writes spatial reference system information to this raster file.
        @param srs: An osgeo.osr.SpatialReference object or
                        A WKT string describing the desired spatial reference system.  
                          Raster.populateStats will populate the Raster.srs 
                          attribute with a correctly formatted string.  Correctly 
                          formatted strings are also output by:
                             * osgeo.gdal.Dataset.GetProjection
                             * osgeo.osr.SpatialReference.ExportToWKT 
        @postcondition: The raster file is updated with new srs information.
        @raise LMError: on failure to open dataset or write srs
        """

        if (self._dlocation is not None and os.path.exists(self._dlocation)):
            geoFI = GeoFileInfo(self._dlocation, updateable=True)
            if isinstance(srs, osr.SpatialReference):
                srs = srs.ExportToWkt()
            geoFI.writeWktSRS(srs)

    # .............................
    def copy_srs_from_file(self, fname):
        """
        @summary: Writes spatial reference system information from provided file 
                     to this raster file.
        @param fname: Filename for dataset from which to copy spatial reference 
                     system. 
        @postcondition: The raster file is updated with new srs information.
        @raise LMError: on failure to open dataset or write srs
        """
        if (fname is not None and os.path.exists(fname)):
            srs = GeoFileInfo.get_srs_as_wkt(fname)
            self.write_srs(srs)
        else:
            raise LMError(['Unable to read file %s' % fname])

    # .............................
    def is_valid_dataset(self):
        """
        @summary: Checks to see if dataset is a valid raster
        @return: True if raster is a valid GDAL dataset; False if not
        """
        valid = True
        if (self._dlocation is not None and os.path.exists(self._dlocation)):
            try:
                self._dataset = gdal.Open(self._dlocation, gdalconst.GA_ReadOnly)
            except Exception:
                valid = False

        return valid

    # .............................
    def delete_data(self, dlocation=None, isTemp=False):
        """
        @summary: Deletes the local data file(s) on disk
        @note: Does NOT clear the dlocation attribute
        """
        success = False
        if dlocation is None:
            dlocation = self._dlocation
        if (dlocation is not None and os.path.isfile(dlocation)):
            drv = gdal.GetDriverByName(self._data_format)
            result = drv.Delete(dlocation)
            if result == 0:
                success = True
        if not isTemp:
            pth, _ = os.path.split(dlocation)
            if os.path.isdir(pth) and len(os.listdir(pth)) == 0:
                try:
                    os.rmdir(pth)
                except:
                    print('Unable to rmdir %s' % pth)
        return success

    # .............................
    def get_WCS_request(self, bbox=None, resolution=None):
        """
        @note: All implemented _Rasters will also be a Subclass of ServiceObject 
        """
        raise LMError('get_WCS_request must be implemented in Subclasses also inheriting from ServiceObject')


# .............................................................................
class Vector(_Layer):
    """
    Class to hold information about a vector dataset.
    """

    # .............................
    def __init__(self, name, user_id, epsgcode, lyr_id=None, squid=None,
                 ident=None, verify=None, dlocation=None, metadata={},
                 data_format=DEFAULT_OGR.driver, ogr_type=None,
                 val_units=None, val_attribute=None, nodata_val=None, min_val=None,
                 max_val=None, resolution=None, bbox=None, mapunits=None,
                 svc_obj_id=None, service_type=LMServiceType.LAYERS,
                 metadata_url=None, parent_metadata_url=None, mod_time=None,
                 featureCount=0, feature_attributes={}, features={},
                 fidAttribute=None):
        """Vector superclass constructor, inherits from _Layer

        Args:
            name: layer name, unique with user_id and epsq
            user_id: user identifier
            epsgcode (int): EPSG code indicating the SRS to use
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
            resolution: resolution of the data - pixel size in @mapunits
            bbox: spatial extent of data
                sequence in the form (minX, minY, maxX, maxY)
                or comma-delimited string in the form 'minX, minY, maxX, maxY'
            mapunits: units of measurement for the data. 
                These are keywords as used in  mapserver, choice of 
                LmCommon.common.lmconstants.LegalMapUnits
                described in http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
            svc_obj_id (int): unique db record identifier for an object  
                retrievable by REST web services.  May be filled with the 
                base layer_id or a unique parameterized id
            service_type: type of data for REST retrieval LMServiceType.LAYERS 
            metadata_url: REST URL for API returning this object
            parent_metadata_url: REST URL for API returning this parent object
            mod_time: time of last modification, in MJD
            featureCount: number of features in this layer
            feature_attributes: dictionary of feature attributes for this layer
            features: dictionary of features in this layer
                key is the featureid (FID) or localid of the feature
                value is a list of values for the feature.  Values are ordered 
                in the same order as in feature_attributes.
            fidAttribute: field name of the attribute holding the featureID
        """
        self._geom_idx = None
        self._geom_field_name = OccurrenceFieldNames.GEOMETRY_WKT[0]
        self._geom_field_type = OFTString
        self._geometry = None
        self._convex_hull = None
        self._local_id_idx = None
        self._local_id_field_name = OccurrenceFieldNames.LOCAL_ID[0]
        self._local_id_field_type = OFTInteger
        self._fidAttribute = fidAttribute
        self._feature_attributes = {}
        self._features = {}
        self._featureCount = 0

        _Layer.__init__(self, name, user_id, epsgcode, lyr_id=lyr_id,
                     squid=squid, ident=ident, verify=verify, dlocation=dlocation,
                     metadata=metadata, data_format=data_format, ogr_type=ogr_type,
                     val_units=val_units, val_attribute=val_attribute,
                     nodata_val=nodata_val, min_val=min_val, max_val=max_val,
                     mapunits=mapunits, resolution=resolution, bbox=bbox,
                     svc_obj_id=svc_obj_id, service_type=service_type,
                     metadata_url=metadata_url, parent_metadata_url=parent_metadata_url,
                     mod_time=mod_time)
        self.verify_data_description(ogr_type, data_format)
        # The following may be reset by set_features:
        # features, feature_attributes, featureCount, geom_idx, local_id_idx, geom, convexHull
        self.set_features(features, feature_attributes, featureCount=featureCount)
        # If data exists, check description
        if dlocation is not None and os.path.exists(dlocation):
            # sets features, feature_attributes, and featureCount (if do_read_data)
            (new_bbox, local_id_idx, geom_idx) = self.read_data(dlocation=dlocation,
                                                     data_format=data_format, do_read_data=False)
        # Reset some attributes based on data
        if new_bbox is not None:
            self.bbox = new_bbox
            self._geom_idx = geom_idx
            self._local_id_idx = local_id_idx
#         else:
#             print('Warning: Vector {} does not exist'.format(dlocation))

    # .............................
    def verify_data_description(self, ogr_type, ogr_format):
        """
        @summary Sets the data type for the vector
        @param ogr_type: OGR type of the vector, valid choices are in OGRDataTypes
        @param ogr_format: OGR Vector Format, only a subset (in OGRFormats) are 
                                valid here
        @raise LMError: Thrown when ogr_format is missing or 
                 either ogr_format or ogr_type is not legal for a Lifemapper Vector.  
        """
        # OGR data_format is required (may be a placeholder and changed later)
        if ogr_format not in LMFormat.ogr_drivers():
            raise LMError('Unsupported Vector OGR data_format', ogr_format)
        if ogr_type is not None and ogr_type not in OGRDataTypes:
            raise LMError('Unsupported Vector ogr_type', ogr_type)

    # .............................
    @property
    def features(self):
        """
        @summary: Converts the private dictionary of features into a list of 
                         LmAttObjs
        @note: Uses list comprehensions to create a list of LmAttObjs and another
                     to create a list of (key, value) pairs for the attribute 
                     dictionary
        @return: A list of LmAttObjs
        """
        return [LmAttObj(dict([
                            (self._feature_attributes[k2][0], self._features[k1][k2]) \
                            for k2 in self._feature_attributes]),
                              "Feature") for k1 in self._features]

    # .............................
    @property
    def feature_attributes(self):
        return self._feature_attributes

    # .............................
    @property
    def fidAttribute(self):
        return self._fidAttribute

    # .............................
    def getFormatLongName(self):
        return self._data_format

    # .............................
    def _getFeatureCount(self):
        if self._featureCount is None:
            if self._features:
                self._featureCount = len(self._features)
        return self._featureCount

    # .............................
    def _setFeatureCount(self, count):
        """
        If Vector._features are present, the length of that list takes precedent 
        over the count parameter.
        """
        if self._features:
            self._featureCount = len(self._features)
        else:
            self._featureCount = count

    featureCount = property(_getFeatureCount, _setFeatureCount)

    # .............................
    def isFilled(self):
        """
        Has the layer been populated with its features.  An empty dataset is 
        considered 'filled' if feature_attributes are present, even if no features 
        exist.  
        """
        if self._feature_attributes:
            return True
        else:
            return False

    # .............................
    def set_features(self, features, feature_attributes, featureCount=0):
        """
        @summary: Sets Vector attributes: 
                         _features, _feature_attributes and featureCount.  
                     Also sets one or more of:
                         _geom_idx, _local_id_idx, _geometry, _convexHull
        @param features: a dictionary of features, with key the featureid (FID) or
                     localid of the feature, and value a list of values for the 
                     feature.  Values are ordered in the same order as 
                     in feature_attributes.
        @param feature_attributes: a dictionary of feature_attributes, with key the 
                     index of this attribute in each feature, and value a tuple of
                     (field name, field type (OGR))
        @param featureCount: the number of features in these data
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
            self._featureCount = len(features)
        else:
            self._features = {}
            self._geometry = None
            self._convex_hull = None
            self._featureCount = featureCount

    # .............................
    def get_features(self):
        """
        @summary: Gets Vector._features as a dictionary of FeatureIDs (FID) with 
                     a list of values
        """
        return self._features

    # .............................
    def clearFeatures(self):
        """
        @summary: Clears Vector._features, Vector._feature_attributes, and 
                     Vector.featureCount        
        """
        del self._feature_attributes
        del self._features
        self.set_features(None, None)

    # .............................
    def add_features(self, features):
        """
        @summary: Adds to Vector._features and updates Vector.featureCount
        @param features: a dictionary of features, with key the featureid (FID) or
                     localid of the feature, and value a list of values for the 
                     feature.  Values are ordered in the same order as 
                     in feature_attributes.
        """
        if features:
            for fid, vals in features.items():
                self._features[fid] = vals
            self._featureCount = len(self._features)

    # .............................
    def get_feature_attributes(self):
        return self._feature_attributes

    # .............................
    def set_val_attribute(self, val_attribute):
        """
        @summary: Sets Vector._val_attribute.  If the feature_attributes are 
                     present, check to make sure val_attribute exists in the dataset.
        @param val_attribute: field name for the attribute to map 
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
                    raise LMError('Map attribute %s not present in dataset %s'
                                      % (val_attribute, self._dlocation))
        else:
            self._val_attribute = val_attribute

    # .............................
    def get_val_attribute(self):
        return self._val_attribute

    # .............................
    def get_data_url(self, interface=SHAPEFILE_INTERFACE):
        """
        @note: the ServiceObject._dbId may contain a join id or _layer_id depending 
                 on the type of Layer being requested
        """
        durl = self._earl_jr.construct_lm_data_url(self.service_type, 
                                                   self.get_id(), interface)
        return durl

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
        if self._local_id_idx is None:
            self._set_local_id_index()
        return self._local_id_idx

    # .............................
    def create_local_dlocation(self, ext=DEFAULT_OGR.ext):
        """
        @summary: Create local filename for this layer.  
        @param ext: File extension for filename
        @note: Data files which are not default User data files (stored 
                  locally and using this method)
                 (in /UserData/<user_id>/<epsgcode>/Layers directory) should be 
                 created in the appropriate Subclass (EnvironmentalLayer, OccurrenceSet, 
                 SDMProjections) 
        """
        dloc = super(Vector, self).create_local_dlocation(ext)
        return dloc

    # .............................
    def get_shapefiles(self, otherlocation=None):
        shpnames = []
        if otherlocation is not None:
            dloc = otherlocation
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
    def zip_shapefiles(self, baseName=None):
        """
        @summary: Returns a wrapper around a tar gzip file stream
        @param baseName: (optional) If provided, this will be the prefix for the 
                                  names of the shape file's files in the zip file.
        """
        fnames = self.get_shapefiles()
        tgStream = StringIO()
        zipf = zipfile.ZipFile(tgStream, mode="w",
                                      compression=zipfile.ZIP_DEFLATED, allowZip64=True)
        if baseName is None:
            baseName = os.path.splitext(os.path.split(fnames[0])[1])[0]

        for fname in fnames:
            ext = os.path.splitext(fname)[1]
            zipf.write(fname, "%s%s" % (baseName, ext))
        zipf.close()

        tgStream.seek(0)
        ret = ''.join(tgStream.readlines())
        tgStream.close()
        return ret

    # .............................
    def get_min_features(self):
        """
        @summary: Returns a dictionary of all feature identifiers with their 
                     well-known-text geometry.
        @return dictionary of {feature id (fid): wktGeometry} 
        """
        feats = {}
        if self._features and self._feature_attributes:
            self._set_geometry_index()

            for fid in list(self._features.keys()):
                feats[fid] = self._features[fid][self._geom_idx]

        return feats

    # .............................
    def is_valid_dataset(self, dlocation=None):
        """
        @summary: Checks to see if the dataset at self.dlocations is a valid 
                     vector readable by OGR.
        @return: True if dataset is a valid OGR dataset; False if not
        """
        valid = False
        if dlocation is None:
            dlocation = self._dlocation
        if (dlocation is not None
             and (os.path.isdir(dlocation)
                    or os.path.isfile(dlocation))):
            try:
                ds = ogr.Open(dlocation)
                ds.GetLayer(0)
            except Exception:
                pass
            else:
                valid = True
        return valid

    # .............................
    def delete_data(self, dlocation=None, isTemp=False):
        """
        @summary: Deletes the local data file(s) on disk
        @note: Does NOT clear the dlocation attribute
        @note: May be extended to delete remote data controlled by us.
        """
        if dlocation is None:
            dlocation = self._dlocation
        deleteDir = False
        if not isTemp:
            self.clear_local_mapfile()
            deleteDir = True
        self.deleteFile(dlocation, deleteDir=deleteDir)

    # .............................
    @staticmethod
    def get_xy(wkt):
        startidx = wkt.find('(')
        if wkt[:startidx].strip().lower() == 'point':
            tmp = wkt[startidx + 1:]
            endidx = tmp.find(')')
            tmp = tmp[:endidx]
            vals = tmp.split()
            if len(vals) == 2:
                try:
                    x = float(vals[0])
                    y = float(vals[1])
                    return x, y
                except:
                    return None
            else:
                return None
        else:
            return None

    # .............................
    @classmethod
    def get_shapefile_row_headers(cls, shapefileFilename):
        """
        @summary: Get a (sorted by feature id) list of row headers for a shapefile
        @todo: Move this to a common Vector class in LmCommon or LmBackend and 
                 use with LmCompute.  This is a rough copy of what is now used for 
                 rasterIntersect.
        """
        ogr.RegisterAll()
        drv = ogr.GetDriverByName(DEFAULT_OGR.driver)
        ds = drv.Open(shapefileFilename)
        lyr = ds.GetLayer(0)

        row_headers = []

        for j in range(lyr.GetFeatureCount()):
            curFeat = lyr.GetFeature(j)
            siteIdx = curFeat.GetFID()
            x, y = curFeat.geometry().Centroid().GetPoint_2D()
            row_headers.append((siteIdx, x, y))

        return sorted(row_headers)

    # .............................
    def write_layer(self, srcData=None, srcFile=None, outFile=None, overwrite=False):
        """
        @summary: Writes vector data to file and sets dlocation.
        @param srcData: A stream, or string of valid vector data
        @param srcFile: A filename for valid vector data.  Currently only 
                             supports CSV and ESRI shapefiles.
        @param overwrite: True/False directing whether to overwrite existing 
                     file or not
        @postcondition: The raster file is written to the filesystem.
        @raise LMError: on 1) failure to write file 
                                 2) attempt to overwrite existing file with overwrite=False
                                 3) _dlocation is None  
        """
        if srcFile is not None:
            self.read_data(dlocation=srcFile)
        if outFile is None:
            outFile = self.get_dlocation()
        if self.features is not None:
            self.write_shapefile(dlocation=outFile, overwrite=overwrite)
        # No file, no features, srcData is iterable, write as CSV
        elif srcData is not None:
            if isinstance(srcData, (list, tuple)):
                if not outFile.endswith(LMFormat.CSV.ext):
                    raise LMError('Iterable input vector data can only be written to CSV')
                else:
                    self.write_csv(dlocation=outFile, overwrite=overwrite)
            else:
                raise LMError('Writing vector is currently supported only for file or iterable input data')
        self.set_dlocation(dlocation=outFile)

    # .............................
    @staticmethod
    def _create_point_shapefile(drv, outpath, spRef, lyrname, lyrDef=None,
                             fldnames=None, idCol=None, xCol=None, yCol=None,
                             overwrite=True):
        nameChanges = {}
        dlocation = os.path.join(outpath, lyrname + '.shp')
        if os.path.isfile(dlocation):
            if overwrite:
                drv.DeleteDataSource(dlocation)
            else:
                raise LMError('Layer %s exists, creation failed' % dlocation)
        newDs = drv.CreateDataSource(dlocation)
        if newDs is None:
            raise LMError('Dataset creation failed for %s' % dlocation)
        newLyr = newDs.CreateLayer(lyrname, geom_type=ogr.wkbPoint, srs=spRef)
        if newLyr is None:
            raise LMError('Layer creation failed for %s' % dlocation)

        # If LayerDefinition is provided, create and add each field to new layer
        if lyrDef is not None:
            for i in range(lyrDef.GetFieldCount()):
                fldDef = lyrDef.GetFieldDefn(i)
#                 fldName = fldDef.GetNameRef()
                return_val = newLyr.CreateField(fldDef)
                if return_val != 0:
                    raise LMError('CreateField failed for \'%s\' in %s'
                                      % (fldDef.GetNameRef(), dlocation))
        # If layer fields are not yet defined, create from fieldnames
        elif (fldnames is not None and idCol is not None and
                xCol is not None and yCol is not None):
            # Create field definitions
            fldDefList = []
            for fldname in fldnames:
                if fldname in (xCol, yCol):
                    fldDefList.append(ogr.FieldDefn(fldname, ogr.OFTReal))
                elif fldname == idCol:
                    fldDefList.append(ogr.FieldDefn(fldname, ogr.OFTInteger))
                else:
                    fdef = ogr.FieldDefn(fldname, ogr.OFTString)
                    fldDefList.append(fdef)
            # Add field definitions to new layer
            for fldDef in fldDefList:
                try:
                    return_val = newLyr.CreateField(fldDef)
                    if return_val != 0:
                        raise LMError('CreateField failed for \'%s\' in %s'
                                          % (fldname, dlocation))
                    lyrDef = newLyr.GetLayerDefn()
                    lastIdx = lyrDef.GetFieldCount() - 1
                    newFldName = lyrDef.GetFieldDefn(lastIdx).GetNameRef()
                    oldFldName = fldDef.GetNameRef()
                    if newFldName != oldFldName:
                        nameChanges[oldFldName] = newFldName

                except Exception as e:
                    print(str(e))
        else:
            raise LMError('Must provide either LayerDefinition or Fieldnames and Id, X, and Y column names')

        return newDs, newLyr, nameChanges

    # .............................
    @staticmethod
    def _finish_shapefile(newDs):
        wrote = None
        dloc = newDs.GetName()
        try:
            # Closes and flushes to disk
            newDs.Destroy()
        except Exception as e:
            wrote = None
        else:
            print(('Closed/wrote dataset %s' % dloc))
            wrote = dloc

            try:
                retcode = subprocess.call(["shptree", "%s" % dloc])
                if retcode != 0:
                    print('Unable to create shapetree index on %s' % dloc)
            except Exception as e:
                print('Unable to create shapetree index on %s: %s' % (dloc, str(e)))
        return wrote

    # .............................
    @staticmethod
    def _get_spatial_ref(srs_epsg_or_wkt, layer=None):
        spRef = None
        if layer is not None:
            spRef = layer.GetSpatialRef()

        if spRef is None:
            spRef = osr.SpatialReference()
            try:
                spRef.ImportFromEPSG(srs_epsg_or_wkt)
            except:
                try:
                    spRef.ImportFromWkt(srs_epsg_or_wkt)
                except Exception as e:
                    raise LMError('Unable to get Spatial Reference System from %s; Error %s'
                                      % (str(srs_epsg_or_wkt), str(e)))
        return spRef

    # .............................
    @staticmethod
    def _copy_feature(orig_feature):
        new_feat = None
        try:
            new_feat = orig_feature.Clone()
        except Exception as e:
            print('Failure to create new feature; Error: %s' % (str(e)))
        return new_feat

    # .............................
    @staticmethod
    def create_point_feature(oDict, xCol, yCol, lyrDef, newNames):
        ptFeat = None
        try:
            ptgeom = ogr.Geometry(ogr.wkbPoint)
            ptgeom.AddPoint(float(oDict[xCol]), float(oDict[yCol]))
        except Exception as e:
            print('Failure %s:  Point = %s, %s' % (str(e), str(oDict[xCol]),
                                                              str(oDict[yCol])))
        else:
            # Create feature for combo layer
            ptFeat = ogr.Feature(lyrDef)
            ptFeat.SetGeometryDirectly(ptgeom)
            # set other fields to match original values
            for okey in list(oDict.keys()):
                if okey in list(newNames.keys()):
                    ptFeat.SetField(newNames[okey], oDict[okey])
                else:
                    ptFeat.SetField(okey, oDict[okey])
        return ptFeat

    # .............................
    @staticmethod
    def split_csv_points_to_shapefiles(outpath, dlocation, group_field, 
                                       comboLayerName,
                                       srs_epsg_or_wkt=DEFAULT_EPSG,
                                       delimiter=';', quotechar='\"',
                                       idCol='id', xCol='lon', yCol='lat',
                                       overwrite=False):
        """Read OGR-accessible data and write to a single shapefile and 
                     individual shapefiles defined by the value of <group_field>
                     
        Args:
            outpath: Directory for output datasets.
            dlocation: Full path location of the data
            group_field: Field containing attribute to group on.
            comboLayerName: Write the original combined data using this name.
            srs_epsg_or_wkt: Spatial reference as an integer EPSG code or 
                Well-Known-Text
            overwrite: Overwrite or fail if data already exists.
            
        Raises:
            LMError: on failure to read data.
        """
        ogr.UseExceptions()
        import csv

        data = {}
        successfulWrites = []

        ogr.RegisterAll()
        drv = ogr.GetDriverByName(DEFAULT_OGR.driver)
        spRef = Vector._get_spatial_ref(srs_epsg_or_wkt)

        f = open(dlocation, 'rb')
        ptreader = csv.DictReader(f, delimiter=delimiter, quotechar=quotechar)
        ((idName, _), (xName, _), (yName, _)
         ) = Vector._get_id_xy_pos(ptreader.fieldnames, idName=idCol,
                                    xName=xCol, yName=yCol)
        comboDs, comboLyr, nameChanges = Vector._create_point_shapefile(
            drv, outpath, spRef, comboLayerName, fldnames=ptreader.fieldnames,
            idCol=idName, xCol=xName, yCol=yName, overwrite=overwrite)
        lyrDef = comboLyr.GetLayerDefn()
        # Iterate through records
        for oDict in ptreader:
            # Create and add feature to combo layer
            ptFeat1 = Vector.create_point_feature(oDict, xCol, yCol, lyrDef, 
                                                nameChanges)
            if ptFeat1 is not None:
                comboLyr.CreateFeature(ptFeat1)
                ptFeat1.Destroy()
                # Create and save point for individual species layer
                ptFeat2 = Vector.create_point_feature(oDict, xCol, yCol, lyrDef, 
                                                    nameChanges)
                thisGroup = oDict[group_field]
                if thisGroup not in list(data.keys()):
                    data[thisGroup] = [ptFeat2]
                else:
                    data[thisGroup].append(ptFeat2)

        dloc = Vector._finish_shapefile(comboDs)
        successfulWrites.append(dloc)
        f.close()

        for group, pointFeatures in data.items():
            indDs, indLyr, nameChanges = Vector._create_point_shapefile(
                drv, outpath, spRef, group, lyrDef=lyrDef, overwrite=overwrite)
            for pt in pointFeatures:
                indLyr.CreateFeature(pt)
                pt.Destroy()
            dloc = Vector._finish_shapefile(indDs)
            successfulWrites.append(dloc)

        ogr.DontUseExceptions()
        return successfulWrites

    # .............................
    def write_csv(self, dataRecords, dlocation=None, overwrite=False, header=None):
        """
        @summary: Writes vector data to a CSV file.
        @param iterableData: a sequence of vector data records, each record is
                 a sequence  
        @param dlocation: Location to write the data
        @param overwrite: True if overwrite existing outfile, False if not
        @return: boolean for success/failure 
        @postcondition: The vector file is written in CSV format (tab-delimited) 
                             to the filesystem.
        @raise LMError: on failure to write file.
        @note: This does NOT set the  self._dlocation attribute
        """
        import csv
        if dlocation is None:
            dlocation = self._dlocation
        didWrite = False
        success = self.ready_filename(dlocation, overwrite=overwrite)
        if success:
            try:
                with open(dlocation, 'wb') as csvfile:
                    spamwriter = csv.writer(csvfile, delimiter='\t')
                    if header:
                        spamwriter.writerow(header)
                    for rec in dataRecords:
                        try:
                            spamwriter.writerow(rec)
                        except Exception as e:
                            # Report and move on
                            print(('Failed to write record {} ({})'.format(rec, str(e))))
                didWrite = True
            except Exception as e:
                print(('Failed to write file {} ({})'.format(dlocation, str(e))))
        return didWrite

    # .............................
    def write_shapefile(self, dlocation=None, overwrite=False):
        """
        @summary: Writes vector data in the feature attribute to a shapefile.  
        @param dlocation: Location to write the data
        @param overwrite: True if overwrite existing shapefile, False if not
        @return: boolean for success/failure 
        @postcondition: The shapefile files are written to the filesystem.
        @raise LMError: on failure to write file.
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
            tSRS = osr.SpatialReference()
            tSRS.ImportFromEPSG(self.epsgcode)
            drv = ogr.GetDriverByName(DEFAULT_OGR.driver)

            ds = drv.CreateDataSource(self._dlocation)
            if ds is None:
                raise LMError('Dataset creation failed for %s' % self._dlocation)

            lyr = ds.CreateLayer(ds.GetName(), geom_type=self._ogr_type, srs=tSRS)
            if lyr is None:
                raise LMError('Layer creation failed for %s.' % self._dlocation)

            # Define the fields
            for idx in list(self._feature_attributes.keys()):
                fldname, fldtype = self._feature_attributes[idx]
                if fldname != self._geom_field_name:
                    fldDefn = ogr.FieldDefn(fldname, fldtype)
                    # Special case to handle long Canonical, Provider, Resource names
                    if (fldname.endswith('name') and fldtype == ogr.OFTString):
                        fldDefn.SetWidth(DEFAULT_OGR.options['MAX_STRLEN'])
                    return_val = lyr.CreateField(fldDefn)
                    if return_val != 0:
                        raise LMError('CreateField failed for %s in %s'
                                          % (fldname, self._dlocation))

            # For each feature
            for i in list(self._features.keys()):
                fvals = self._features[i]
                feat = ogr.Feature(lyr.GetLayerDefn())
                try:
                    self._fill_ogr_feature(feat, fvals)
                except Exception as e:
                    print('Failed to fillOGRFeature, e = %s' % str(e))
                else:
                    # Create new feature, setting FID, in this layer
                    lyr.CreateFeature(feat)
                    feat.Destroy()

            # Closes and flushes to disk
            ds.Destroy()
            print(('Closed/wrote dataset %s' % self._dlocation))
            success = True
            try:
                retcode = subprocess.call(["shptree", "%s" % self._dlocation])
                if retcode != 0:
                    print('Unable to create shapetree index on %s' % self._dlocation)
            except Exception as e:
                print('Unable to create shapetree index on %s: %s' % (self._dlocation,
                                                                                        str(e)))
        except Exception as e:
            raise LMError('Failed to create shapefile %s' % self._dlocation, e)

        return success

    # .............................
    def read_from_uploaded_data(self, data, uploaded_type='shapefile', overwrite=True):
        """
        @summary: Read from uploaded data by writing to temporary files, saving 
                     temporary filename in dlocation.  
        @raise LMError: on failure to write data or read temporary files.
        """
        # Writes zipped stream to temp file and sets dlocation on layer
        if uploaded_type == 'shapefile':
            self.write_from_zipped_shapefile(data, isTemp=True, overwrite=overwrite)
            self._data_format = DEFAULT_OGR.driver
            try:
                # read to make sure it's valid (and populate stats)
                self.read_data()
            except Exception as e:
                raise LMError('Invalid uploaded data in temp file %s (%s)'
                                  % (self._dlocation, str(e)), do_trace=True)
        elif uploaded_type == 'csv':
            self.write_temp_from_csv(data)
            self._data_format = 'CSV'
            try:
                # read to make sure it's valid (and populate stats)
                self.read_data()
            except Exception as e:
                raise LMError('Invalid uploaded data in temp file %s (%s)'
                                  % (self._dlocation, str(e)))

    # .............................
    @staticmethod
    def _get_id_xy_pos(fieldnames, idName=None, xName=None, yName=None):
        idPos = xPos = yPos = None
        if idName is not None:
            try:
                idPos = fieldnames.index(idName)
            except:
                idName = None
        if xName is not None:
            try:
                xPos = fieldnames.index(xName)
            except:
                xName = None
        if yName is not None:
            try:
                yPos = fieldnames.index(yName)
            except:
                yName = None

        if not (idName and xName and yName):
            for i in range(len(fieldnames)):
                fldname = fieldnames[i].lower()
                if xName is None and fldname in OccurrenceFieldNames.LONGITUDE:
                    xName = fldname
                    xPos = i
                if yName is None and fldname in OccurrenceFieldNames.LATITUDE:
                    yName = fldname
                    yPos = i
                if idName is None and fldname in OccurrenceFieldNames.LOCAL_ID:
                    idName = fldname
                    idPos = i

        return ((idName, idPos), (xName, xPos), (yName, yPos))

    # .............................
    def write_from_zipped_shapefile(self, zipdata, isTemp=True, overwrite=False):
        """
        @summary: Write a shapefile from a zipped stream of shapefile files to
                     temporary files.  Read vector info into layer attributes, 
                     Reset dlocation. 
        @raise LMError: on failure to write file.
        """
        newfnamewoext = None
        outStream = StringIO()
        outStream.write(zipdata)
        outStream.seek(0)
        z = zipfile.ZipFile(outStream, allowZip64=True)

        # Get filename, prepare directory, delete if overwrite=True
        if isTemp:
            zfnames = z.namelist()
            for zfname in zfnames:
                if zfname.endswith(LMFormat.SHAPE.ext):
                    pth, basefilename = os.path.split(zfname)
                    pth = UPLOAD_PATH
                    basename, _ = os.path.splitext(basefilename)
                    newfnamewoext = os.path.join(pth, basename)
                    outfname = os.path.join(UPLOAD_PATH, basefilename)
                    ready = self.ready_filename(outfname, overwrite=overwrite)
                    break
            if outfname is None:
                raise Exception('Invalid shapefile, zipped data does not contain .shp')
        else:
            if self._dlocation is None:
                self.set_dlocation()
            outfname = self._dlocation
            if outfname is None:
                raise LMError('Must set_dlocation prior to writing shapefile')
            pth, basefilename = os.path.split(outfname)
            basename, _ = os.path.splitext(basefilename)
            newfnamewoext = os.path.join(pth, basename)
            ready = self.ready_filename(outfname, overwrite=overwrite)

        if ready:
            # unzip zip file stream
            for zname in z.namelist():
                tmp, ext = os.path.splitext(zname)
                # Check file extension and only unzip valid files
                if ext in LMFormat.SHAPE.get_extensions():
                    newname = newfnamewoext + ext
                    success, _ = self.deleteFile(newname)
                    z.extract(zname, pth)
                    if not isTemp:
                        oldname = os.path.join(pth, zname)
                        os.rename(oldname, newname)
            # Reset dlocation on successful write
            self.clear_dlocation()
            self.set_dlocation(outfname)
        else:
            raise LMError('{} exists, overwrite = False'.format(outfname))

    # .............................
    def write_temp_from_csv(self, csvdata):
        """
        @summary: Write csv from a stream of csv data to temporary file.  
                     Read vector info into layer attributes;
                     DO NOT delete temporary files or reset dlocation 
        @raise LMError: on failure to write file.
        """
        currtime = str(gmt().mjd)
        pid = str(os.getpid())
        dumpname = os.path.join(UPLOAD_PATH, '%s_%s_dump.csv' % (currtime, pid))
        f1 = open(dumpname, 'w')
        f1.write(csvdata)
        f1.close()
        f1 = open(dumpname, 'rU')
        tmpname = os.path.join(UPLOAD_PATH, '%s_%s.csv' % (currtime, pid))
        f2 = open(tmpname, 'w')
        try:
            for line in f1:
                f2.write(line)
        except Exception as e:
            raise LMError('Unable to parse input CSV data', e)
        finally:
            f1.close()
            f2.close()
        self.clear_dlocation()
        self.set_dlocation(dlocation=tmpname)

    # .............................
    def _set_geometry(self, convex_hull_buffer=None):
        """
        From osgeo.ogr.py: "The nQuadSegs parameter can be used
          to control how many segments should be used to define a 90 degree
          curve - a quadrant of a circle. A value of 30 is a reasonable default"
        """
        nQuadSegs = 30
        if self._geometry is None and self._convex_hull is None and self._features:
            if self._ogr_type == ogr.wkbPoint:
                gtype = ogr.wkbMultiPoint
            elif self._ogr_type == ogr.wkbLineString:
                gtype = ogr.wkbMultiLineString
            elif self._ogr_type == ogr.wkbPolygon:
                gtype = ogr.wkbMultiPolygon
            elif self._ogr_type == ogr.wkbMultiPolygon:
                gtype = ogr.wkbGeometryCollection
            else:
                raise LMError('Only osgeo.ogr types wkbPoint, wkbLineString, ' +
                                  'wkbPolygon, and wkbMultiPolygon are currently supported')
            geom = ogr.Geometry(gtype)
            srs = self.create_srs_from_epsg()
            gidx = self._get_geometry_index()

            for fvals in list(self._features.values()):
                wkt = fvals[gidx]
                fgeom = ogr.CreateGeometryFromWkt(wkt, srs)
                if fgeom is None:
                    print(('What happened on point %s?' %
                            (str(fvals[self.get_local_id_index()]))))
                else:
                    geom.AddGeometryDirectly(fgeom)
            self._geometry = geom

            # Now set convexHull
            tmpGeom = self._geometry.ConvexHull()
            if tmpGeom.GetGeometryType() != ogr.wkbPolygon:
                # If geom is a single point, not a polygon, buffer it
                if convex_hull_buffer is None:
                    convex_hull_buffer = 0.1
                self._convex_hull = tmpGeom.Buffer(convex_hull_buffer, nQuadSegs)
            elif convex_hull_buffer is not None:
                # If requested buffer
                self._convex_hull = tmpGeom.Buffer(convex_hull_buffer, nQuadSegs)
            else:
                # No buffer
                self._convex_hull = tmpGeom

            # Don't reset Bounding Box for artificial geometry of stacked 3d data
            minx, maxx, miny, maxy = self._convex_hull.GetEnvelope()
            self._set_bbox((minx, miny, maxx, maxy))

    # .............................
    def get_convex_hull_wkt(self, convex_hull_buffer=None):
        """
        @summary: Return Well Known Text (wkt) of the polygon representing the 
                     convex hull of the data.
        @note: If the geometry type is Point, and a ConvexHull is a single point, 
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
        """
        @summary: Return Well Known Text (wkt) of the data features.
        """
        wkt = None
        self._set_geometry()
        if self._geometry is not None:
            wkt = self._geometry.ExportToWkt()
        return wkt

    # .............................
    def _get_geom_type(self, lyr, lyrDef):
        # Special case to handle multi-polygon datasets that are identified
        # as polygon, this because of a broken driver
        geomtype = lyrDef.GetGeomType()
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
                     format_= DEFAULT_GDAL.driver):
        """
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
            raise LMError('Source location %s is invalid' % str(source_dlocation))

        ogr.RegisterAll()
        drv = ogr.GetDriverByName(format_)
        try:
            ds = drv.Open(source_dlocation)
        except Exception as e:
            raise LMError('Invalid datasource' % source_dlocation, str(e))

        try:
            newds = drv.CopyDataSource(ds, target_dlocation)
            newds.Destroy()
        except Exception as e:
            raise LMError('Failed to copy data source')

    # .............................
    def verifyField(self, dlocation, ogr_format, attrname):
        """
        @summary: Read OGR-accessible data and save the features and 
                     feature_attributes on the Vector object
        @param dlocation: Location of the data
        @param ogr_format: OGR-supported data format code, available at
                             http://www.gdal.org/ogr/ogr_formats.html
        @return: boolean for success/failure 
        @raise LMError: on failure to read data.
        @note: populateStats calls this
        """
        if attrname is None:
            fieldOk = True
        else:
            fieldOk = False
            if dlocation is not None and os.path.exists(dlocation):
                ogr.RegisterAll()
                drv = ogr.GetDriverByName(ogr_format)
                try:
                    ds = drv.Open(dlocation)
                except Exception as e:
                    raise LMError('Invalid datasource' % dlocation, str(e))

                lyrDef = ds.GetLayer(0).GetLayerDefn()
                # Make sure given field exists and is the correct type
                for i in range(lyrDef.GetFieldCount()):
                    fld = lyrDef.GetFieldDefn(i)
                    fldname = fld.GetNameRef()
                    if attrname == fldname:
                        fldtype = fld.GetType()
                        if fldtype in (ogr.OFTInteger, ogr.OFTReal, ogr.OFTBinary):
                            fieldOk = True
                        break
        return fieldOk

    # .............................
    @staticmethod
    def test_vector(dlocation, driver=DEFAULT_OGR.driver):
        goodData = False
        featCount = 0
        if dlocation is not None and os.path.exists(dlocation):
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(driver)
            try:
                ds = drv.Open(dlocation)
            except Exception:
                goodData = False
            else:
                try:
                    slyr = ds.GetLayer(0)
                except Exception:
                    goodData = False
                else:
                    featCount = slyr.GetFeatureCount()
                    goodData = True

        return goodData, featCount

    # .............................
    @staticmethod
    def index_shapefile(dlocation):
        try:
            shpTreeCmd = os.path.join(APP_PATH, 'shptree')
            retcode = subprocess.call([shpTreeCmd, '{}'.format(dlocation)])
            if retcode != 0:
                print('Failed to create shptree index on {}'.format(dlocation))
        except Exception as e:
            print('Failed create shptree index on {}: {}'.format(dlocation, str(e)))

    # .............................
    def read_csv_points_with_ids(self, dlocation=None, feature_limit=None,
                                     do_read_data=False):
        """Read data and set features and feature_attributes
        
        Args:
            dlocation: 
        @return: localId position, featAttrs, featureCount, 
                    and features and BBox (if read data)
        @note: We are saving only latitude, longitude and localid if it exists.  
                 If localid does not exist, we create one.
        @note: If column headers are not present, assume 
                 columns 0 = id, 1 = longitude, 2 = latitude
        @todo: Save the rest of the fields using Vector.split_csv_points_to_shapefiles
        @todo: remove feature_limit, read subset_dlocation if there is a limit 
        """
        import csv
        this_bbox = None
        feats = {}
        featAttrs = self.get_user_point_feature_attributes()
        localid = None
        if dlocation is None:
            dlocation = self._dlocation
        self.clearFeatures()
        infile = open(dlocation, 'rU')
        reader = csv.reader(infile)

        # Read row with possible fieldnames
        row = next(reader)
        hasHeader = True
        ((_, idPos), (_, xPos), (_, yPos)) = Vector._get_id_xy_pos(row)
        if not idPos:
            # If no id column, create it
            if (xPos and yPos):
                localid = 0
            # If no headers, assume the positions
            else:
                hasHeader = False
                idPos = 0
                xPos = 1
                yPos = 2
        if xPos is None or yPos is None:
            raise LMError('Must supply longitude and latitude')

        if not do_read_data:
            featureCount = sum(1 for row in reader)
            if not hasHeader:
                featureCount += 1
        else:
            eof = False
            try:
                row = next(reader)
            except StopIteration as e:
                eof = True
            Xs = []
            Ys = []
            while not eof:
                try:
                    if localid is None:
                        thisid = row[idPos]
                    else:
                        localid += 1
                        thisid = localid
                    x = float(row[xPos])
                    y = float(row[yPos])
                    Xs.append(x)
                    Ys.append(y)
                    feats[thisid] = self.get_user_point_feature(thisid, x, y)
                    if feature_limit is not None and len(feats) >= feature_limit:
                        break
                except Exception as e:
                    # Skip point if fails.  This could be a blank row or data error
                    pass
                # Read next row
                try:
                    row = next(reader)
                except StopIteration as e:
                    eof = True

            featureCount = len(feats)
            if featureCount == 0:
                raise LMError('Unable to read points from CSV')
            try:
                minX = min(Xs)
                minY = min(Ys)
                maxX = max(Xs)
                maxY = max(Ys)
                this_bbox = (minX, minY, maxX, maxY)
            except Exception as e:
                raise LMError('Failed to get valid coordinates ({})'.format(str(e)))

        infile.close()
        return (this_bbox, idPos, feats, featAttrs, featureCount)

    # .............................
    def read_with_OGR(self, dlocation, ogr_format, feature_limit=None, 
                      do_read_data=False):
        """
        @summary: Read OGR-accessible data and set the features and 
                     feature_attributes on the Vector object
        @param dlocation: Full path location of the data
        @param ogr_format: OGR-supported data format code, available at
                             http://www.gdal.org/ogr/ogr_formats.html
        @return: boolean for success/failure 
        @raise LMError: on failure to read data.
        @note: populateStats calls this
        @todo: remove feature_limit, read subset_dlocation if there is a limit 
        """
        this_bbox = local_id_idx = geom_idx = feats = featAttrs = None
        if dlocation is not None and os.path.exists(dlocation):
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(ogr_format)
            try:
                ds = drv.Open(dlocation)
            except Exception as e:
                raise LMError('Invalid datasource' % dlocation, str(e))

            self.clearFeatures()
            try:
                slyr = ds.GetLayer(0)
            except Exception as e:
                raise LMError('#### Failed to GetLayer from %s' % dlocation,
                                  e, do_trace=True)

            # Get bounding box
            (minX, maxX, minY, maxY) = slyr.GetExtent()
            this_bbox = (minX, minY, maxX, maxY)

            # .........................
            # Read field structure (featAttrs)
            lyrDef = slyr.GetLayerDefn()
            fldCount = lyrDef.GetFieldCount()
            foundLocalId = False
#             geomtype = self._get_geom_type(slyr, lyrDef)
            # Read Fields (indexes start at 0)
            featAttrs = {}
            for i in range(fldCount):
                fld = lyrDef.GetFieldDefn(i)
                fldname = fld.GetNameRef()
                # Provided attribute name takes precedence
                if fldname == self._fidAttribute:
                    local_id_idx = i
                    foundLocalId = True
                # Don't reset if already found
                if not foundLocalId and fldname in OccurrenceFieldNames.LOCAL_ID:
                    local_id_idx = i
                    foundLocalId = True
                featAttrs[i] = (fld.GetNameRef(), fld.GetType())

            # .........................
            # Add fields FID (if not present) and geom to featAttrs
            i = fldCount
            if not foundLocalId:
                featAttrs[i] = (self._local_id_field_name, self._local_id_field_type)
                local_id_idx = i
                i += 1
            featAttrs[i] = (self._geom_field_name, self._geom_fieldType)
            geom_idx = i

            # .........................
            # Read data (features)
            feats = {}
            featCount = slyr.GetFeatureCount()
            if do_read_data:
                # Limit the number of features to read (for mapping and modeling)
                if feature_limit is not None and feature_limit < featCount:
                    featCount = feature_limit
                try:
                    for j in range(featCount):
                        currFeat = slyr.GetFeature(j)
                        if currFeat is not None:
                            currFeatureVals = []
                            for k in range(fldCount):
                                val = currFeat.GetField(k)
                                currFeatureVals.append(val)
                                if k == local_id_idx:
                                    localid = val
                            # Add values localId (if not present) and geom to features
                            if not foundLocalId:
                                localid = currFeat.GetFID()
                                currFeatureVals.append(localid)
                            currFeatureVals.append(currFeat.geometry().ExportToWkt())

                            # Add the feature values with key=localId to the dictionary
                            feats[localid] = currFeatureVals
                except Exception as e:
                    raise LMError('Failed to read features from %s (%s)'
                                      % (dlocation, str(e)), do_trace=True)

#             self.set_features(features, featAttrs, featureCount=featCount)
        else:
            raise LMError('dlocation %s does not exist' % str(dlocation))
        return this_bbox, local_id_idx, geom_idx, feats, featAttrs, featCount

    # .............................
    def _transform_bbox(self, origEpsg=None, orig_bbox=None):
        if origEpsg is None:
            origEpsg = self._epsg
        if orig_bbox is None:
            orig_bbox = self._bbox
        minX, minY, maxX, maxY = orig_bbox

        if origEpsg != DEFAULT_EPSG:
            srcSRS = osr.SpatialReference()
            srcSRS.ImportFromEPSG(origEpsg)
            dstSRS = osr.SpatialReference()
            dstSRS.ImportFromEPSG(DEFAULT_EPSG)

            spTransform = osr.CoordinateTransformation(srcSRS, dstSRS)
            # Allow for return of either (x, y) or (x, y, z)
            retvals = spTransform.TransformPoint(minX, minY)
            newMinX, newMinY = retvals[0], retvals[1]
            retvals = spTransform.TransformPoint(maxX, maxY)
            newMaxX, newMaxY = retvals[0], retvals[1]
            return (newMinX, newMinY, newMaxX, newMaxY)
        else:
            return orig_bbox

    # .............................
    def read_data(self, dlocation=None, data_format=None, feature_limit=None,
                     do_read_data=False):
        """Reads the file at dlocation and fills the related attributes.
        
        Reading data fills the featureCount value and feature_attributes 
        dictionary.  If do_read_data is True, reads and fills features dictionary.
        
        Args:
            dlocation: file location, overrides object attribute
            data_format: OGR format driver name, overrides object attribute
            feature_limit: limits number of features to be read into _features
            do_read_data (boolean): flag indicating whether to read features
                into ; False to read only the 
                 feature_attributes and feature count. 
        @return: new bbox string, indices of the localId int and geometry fields
        @todo: remove feature_limit, read subset_dlocation if there is a limit 
        """
        new_bbox = local_id_idx = geom_idx = None
        if dlocation is None:
            dlocation = self._dlocation
        if data_format is None:
            data_format = self._data_format
        if dlocation is not None and os.path.exists(dlocation):
            if data_format == DEFAULT_OGR.driver:
                (this_bbox, local_id_idx, geom_idx, features, feature_attributes,
                 featureCount) = self.read_with_OGR(dlocation, data_format,
                                                    feature_limit=feature_limit,
                                                    do_read_data=do_read_data)
            # only for Point data
            elif data_format == 'CSV':
                (this_bbox, local_id_idx, features, feature_attributes,
                    featureCount) = self.read_csv_points_with_ids(
                        dlocation=dlocation, feature_limit=feature_limit,
                        do_read_data=do_read_data)
            self.set_features(features, feature_attributes, 
                             featureCount=featureCount)
            new_bbox = self._transform_bbox(orig_bbox=this_bbox)
        return (new_bbox, local_id_idx, geom_idx)

    # .............................
    def get_ogr_layer_type_name(self, ogrWKBType=None):
        if ogrWKBType is None:
            ogrWKBType = self._ogrType
        # Subset of all ogr layer types
        if ogrWKBType == ogr.wkbPoint:
            return 'ogr.wkbPoint'
        elif ogrWKBType == ogr.wkbLineString:
            return 'ogr.wkbLineString'
        elif ogrWKBType == ogr.wkbPolygon:
            return 'ogr.wkbPolygon'
        elif ogrWKBType == ogr.wkbMultiPolygon:
            return 'ogr.wkbMultiPolygon'

    # .............................
    def get_field_metadata(self):
        if self._feature_attributes:
            fldMetadata = {}
            for idx, featAttrs in self._feature_attributes.items():
                fldMetadata[idx] = (featAttrs[0],
                                          self._getOGRFieldTypeName(featAttrs[1]))
        return fldMetadata

    # .............................
    def _get_ogr_field_type_name(self, ogrOFTType):
        if ogrOFTType == ogr.OFTBinary:
            return 'ogr.OFTBinary'
        elif ogrOFTType == ogr.OFTDate:
            return 'ogr.OFTDate'
        elif ogrOFTType == ogr.OFTDateTime:
            return 'ogr.OFTDateTime'
        elif ogrOFTType == ogr.OFTInteger:
            return 'ogr.OFTInteger'
        elif ogrOFTType == ogr.OFTReal:
            return 'ogr.OFTReal'
        elif ogrOFTType == ogr.OFTString:
            return 'ogr.OFTString'
        else:
            return 'ogr Field Type constant: ' + str(ogrOFTType)

    # .............................
    def get_feature_val_by_field_name(self, fieldname, feature_FID):
        field_idx = self.get_field_index(fieldname)
        return self.get_feature_val_by_field_index(field_idx, feature_FID)

    # .............................
    def get_feature_val_by_field_index(self, field_idx, feature_FID):
        if self._features:
            if feature_FID in self._features:
                return self._features[feature_FID][field_idx]
            else:
                raise LMError ('Feature ID {} not found in dataset {}'
                               .format(feature_FID, self._dlocation))
        else:
            raise LMError('Dataset features are empty.')

    # .............................
    def get_field_index(self, fieldname):
        if self._feature_attributes:
            field_idx = None
            if fieldname in OccurrenceFieldNames.LOCAL_ID:
                findLocalId = True
            else:
                findLocalId = False
            for fldidx, (fldname, _) in self._feature_attributes.items():

                if fldname == fieldname:
                    field_idx = fldidx
                    break
                elif findLocalId and fldname in OccurrenceFieldNames.LOCAL_ID:
                    field_idx = fldidx
                    break

            return field_idx
        else:
            raise LMError('Dataset feature_attributes are empty.')

    # .............................
    def get_srs(self):
        if self._dlocation is not None and os.path.exists(self._dlocation):
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(self._data_format)
            try:
                ds = drv.Open(self._dlocation)
            except Exception as e:
                raise LMError(['Invalid datasource' % self._dlocation, str(e)])

            vlyr = ds.GetLayer(0)
            srs = vlyr.GetSpatialRef()
            if srs is None:
                srs = self.create_srs_from_epsg()
            return srs
        else:
            raise LMError('Input file %s does not exist' % self._dlocation)

    # .............................
    def _fill_ogr_feature(self, feat, fvals):
        # Fill the fields
        for j in list(self._feature_attributes.keys()):
            fldname, _ = self._feature_attributes[j]
            val = fvals[j]
            if fldname == self._geom_field_name:
                geom = ogr.CreateGeometryFromWkt(val)
                feat.SetGeometryDirectly(geom)
            elif val is not None and val != 'None':
                feat.SetField(fldname, val)

