"""Module containing classes and functions for occurrence sets
"""
import os

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.time import gmt
from LmServer.base.layer import Vector, _LayerParameters
from LmServer.base.service_object import ProcessObject
from LmServer.common.lmconstants import (ID_PLACEHOLDER, LMFileType,
                                         LMServiceType, OccurrenceFieldNames)
from osgeo import ogr


# .............................................................................
class OccurrenceType(_LayerParameters, ProcessObject):
    """
    @todo: Update string formatting when python 2.5 is gone
    The point data class interfaces with the GBIF Cache and 
    creates a point data file that can be read by openModeller.
    """

    def __init__(self, display_name, query_count, mod_time, user_id, occ_layer_id, 
                 metadata={}, sci_name=None, raw_dlocation=None, 
                 process_type=None, status=None, status_mod_time=None):
        """
        @summary Initialize the _Occurrences class instance
        @copydoc LmServer.base.layer._LayerParameters::__init__()
        @copydoc LmServer.base.service_object.ProcessObject::__init__()
        @param display_name: Name to be displayed for this dataset
        @param query_count: Count reported by last update to shapefile.  
                                 Used if there are no features attached to this
                                 OccurrenceSet.
        @param occ_layer_id: The occ_layer id for the database
        @param sci_name: ScientificName object containing further 
                 information about the name associated with these data
        @param raw_dlocation: URL or file location of raw data to be processed
        """
        _LayerParameters.__init__(self, user_id, param_id=occ_layer_id,
                                          matrixIndex=-1, metadata=metadata,
                                          mod_time=mod_time)
        ProcessObject.__init__(
            self, obj_id=occ_layer_id, process_type=process_type,
            status=status, status_mod_time=status_mod_time)
        self.display_name = display_name
        self.query_count = query_count
        self._raw_dlocation = raw_dlocation
        self._big_dlocation = None
        self._scientific_name = sci_name

# ...............................................
    def get_scientific_name_id(self):
        if self._scientific_name is not None:
            return self._scientific_name.get_id()
        else:
            return None

# ...............................................
    def get_scientific_name(self):
        return self._scientific_name

# ...............................................
    def set_scientific_name(self, sci_name):
        self._scientific_name = sci_name

# ...............................................
    def get_raw_dlocation(self):
        return self._raw_dlocation

    def set_raw_dlocation(self, raw_dlocation, mod_time):
        self._raw_dlocation = raw_dlocation
        self.paramModTime = mod_time

    # ...............................................
    def update_status(self, status, mod_time=gmt().mjd,
                     query_count=None):
        """
        @note: Overrides ProcessObject.update_status
        """
        ProcessObject.update_status(self, status, mod_time)
        if query_count is not None:
            self.query_count = query_count
            self.paramModTime = self.status_mod_time

# .............................................................................
# .............................................................................


class OccurrenceLayer(OccurrenceType, Vector):

    def __init__(self, display_name, user_id, epsgcode, query_count, lyr_id=None,
                 squid=None, verify=None, dlocation=None, raw_dlocation=None,
                 raw_meta_dlocation=None, lyr_metadata={},
                 data_format=LMFormat.SHAPE.driver, valUnits=None,
                 val_attribute=None, nodata_val=None, min_val=None, max_val=None,
                 mapunits=None, resolution=None, bbox=None,
                 occ_layer_id=None, serviceType=LMServiceType.OCCURRENCES,
                 metadataUrl=None, parentMetadataUrl=None, featureCount=0,
                 feature_attributes={}, features={}, fidAttribute=None,
                 occMetadata={}, sci_name=None, objId=None, process_type=None,
                 status=None, status_mod_time=None):
        """
        @todo: calculate bbox from points upon population, update as appropriate
        @summary Initialize the OccurrenceSet class instance
        @copydoc LmServer.base.layer.Vector::__init__()
        @copydoc LmServer.legion.occlayer.OccurrenceType::__init__()
        @todo: Remove count?
        @note: Vector.name is constructed in OccurrenceLayer.set_id()
        """
        OccurrenceType.__init__(
            self, display_name, query_count, status_mod_time, user_id,
            occ_layer_id, metadata=occMetadata, sci_name=sci_name,
            raw_dlocation=raw_dlocation, process_type=process_type, status=status,
            status_mod_time=status_mod_time)
        Vector.__init__(
            self, None, user_id, epsgcode, lyr_id=occ_layer_id, squid=squid,
            verify=verify, dlocation=dlocation, metadata=lyr_metadata,
            data_format=data_format, ogrType=ogr.wkbPoint, valUnits=valUnits,
            val_attribute=val_attribute, nodata_val=nodata_val, min_val=min_val,
            max_val=max_val, mapunits=mapunits, resolution=resolution, bbox=bbox,
            svcObjId=occ_layer_id, serviceType=serviceType,
            metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl,
            mod_time=status_mod_time, featureCount=featureCount,
            feature_attributes=feature_attributes, features=features,
            fidAttribute=fidAttribute)
        self.raw_meta_dlocation = raw_meta_dlocation
        self.set_id(occ_layer_id)

# .............................................................................
# Class and Static methods
# .............................................................................

# ...............................................
    @staticmethod
    def get_user_point_feature_attributes():
        feature_attributes = {
            0 : (Vector._local_id_field_name, Vector._local_id_field_type),
            1 : (OccurrenceFieldNames.LONGITUDE[0], ogr.OFTReal),
            2 : (OccurrenceFieldNames.LATITUDE[0], ogr.OFTReal),
            3 : (Vector._geom_field_name, Vector._geom_field_type)
            }
        return feature_attributes

# ...............................................
    @staticmethod
    def get_user_point_feature(id, x, y):
        geomwkt = OccurrenceLayer.get_point_wkt(x, y)
        vals = [id, x, y, geomwkt]
        return vals

# ...............................................
    @staticmethod
    def equal_points(wkt1, wkt2):
        if wkt1 == wkt2:
            return True
        else:
            pt1 = OccurrenceLayer.get_point_from_wkt(wkt1)
            pt2 = OccurrenceLayer.get_point_from_wkt(wkt2)
            if abs(pt1[0] - pt2[0]) > 1e-6:
                return False
            elif abs(pt1[1] - pt2[1]) > 1e-6:
                return False
            else:
                return True

# ...............................................
    @staticmethod
    def get_point_from_wkt(wkt):
        if wkt is None:
            raise LMError('Missing wkt')
        start = wkt.find('(')
        end = wkt.rfind(')')
        if (start != -1 and end != -1):
            x, y = wkt[start + 1:end].split()
            try:
                x = float(x)
                y = float(y)
            except:
                raise LMError ('Invalid point WKT {}'.format(wkt))
            else:
                return (x, y)

    @staticmethod
    def get_point_wkt(x, y):
        """
        @summary: Creates a well-known-text string representing the point
        @note: Rounds the float to 4 decimal points 
        """
        try:
            float(x)
            float(y)
        except:
            raise LMError('Invalid point coordinates; x = {}, y = {}'
                                            .format(x, y))
        else:
            x = round(x, 4)
            y = round(y, 4)
            return 'POINT ( {} {} )'.format(x, y)

# .............................................................................
# Properties, getters, setters
# .............................................................................
    def _get_count(self):
        """
        @summary Returns the number of new-style points (generic vector feature 
                    objects)
        @return The number of points for this dataset
        """
        return self._getFeatureCount()

    count = property(_get_count)

# .............................................................................
# Superclass methods overridden
# # .............................................................................
    def set_id(self, occid):
        """
        @summary: Sets the database id on the object, and sets the 
                     OccurrenceSet._dlocation of the shapefile if it is None.
        @param occid: The database id for the object
        @note: Also sets OccurrenceSet._dlocation, _Layer.map_prefix, and
            Vector.name.  ServiceObject.metadataUrl is constructed using the id
            on first access.
        """
        super(OccurrenceLayer, self).set_id(occid)
        if occid is not None:
            if self.name is None:
                self.name = self._earl_jr.createLayername(occsetId=self.get_id())
            self.set_dlocation()
            self.resetMetadataUrl()
            self.set_local_map_filename()
            self._set_map_prefix()

# ...............................................
    def get_absolute_path(self):
        self.set_dlocation()
        return Vector.get_absolute_path(self)

# # ...............................................
#     @property
#     def makeflowFilename(self):
#         dloc = self.create_local_dlocation(makeflow=True)
#         return dloc

# ...............................................
    def create_local_dlocation(self, raw=False, largeFile=False, makeflow=False):
        """
        @summary: Create filename for this layer.
        @param raw: If true, this indicates a raw dump of occurrences (CSV for
            GBIF dump or User file, a URL for a BISON or iDigBio query).
        @param largeFile: If true, this indicates a too-big file of occurrences
        @param makeflow: If true, this indicates a makeflow document of jobs
            related to this object
        """
        dloc = None
        if self.get_id() is not None:
            if raw:
                ftype = LMFileType.OCCURRENCE_RAW_FILE
            elif makeflow:
                ftype = LMFileType.MF_DOCUMENT
            elif largeFile:
                ftype = LMFileType.OCCURRENCE_LARGE_FILE
            else:
                ftype = LMFileType.OCCURRENCE_FILE
            occid = self.get_id()
            dloc = self._earl_jr.create_filename(
                ftype, occsetId=occid, objCode=occid, usr=self._user_id)
        return dloc

# ...............................................
    # Overrides layer.get_dlocation, allowing optional keyword
    def get_dlocation(self, largeFile=False):
        if largeFile:
            if self._big_dlocation is None:
                self._big_dlocation = self.create_local_dlocation(
                    largeFile=largeFile)
            return self._big_dlocation
        else:
            self.set_dlocation()
        return self._dlocation

# ...............................................
    def is_valid_dataset(self, largeFile=False):
        """
        @summary: Check to see if the dataset at self.dlocations is a valid 
            occurrenceset readable by OGR.  If dlocation is None, fill it in
            first.
        @return: True if dlocation is a valid occurrenceset; False if not
        """
        dlocation = self.get_dlocation(largeFile=largeFile)
        valid = Vector.is_valid_dataset(self, dlocation=dlocation)
        return valid

# ...............................................
# ...............................................
    def _create_map_prefix(self):
        """
        @summary: Construct the endpoint of a Lifemapper WMS URL for 
                     this object.
        @note: Uses the metatadataUrl for this object, plus 'ogc' format, 
                 map=<mapname>, and layers=<layername> key/value pairs.  
        @note: If the object has not yet been inserted into the database, a 
                 placeholder is used until replacement after database insertion.
        """
        occid = self.get_id()
        if occid is None:
            occid = ID_PLACEHOLDER
        lyrname = self._earl_jr.createBasename(
            LMFileType.OCCURRENCE_FILE, objCode=occid, usr=self._user_id,
            epsg=self.epsgcode)
        mapprefix = self._earl_jr.constructMapPrefixNew(
            urlprefix=self.metadataUrl, ftype=LMFileType.SDM_MAP,
            mapname=self.map_name, lyrname=lyrname, usr=self._user_id)
        return mapprefix

    def _set_map_prefix(self):
        mapprefix = self._create_map_prefix()
        self._map_prefix = mapprefix

    @property
    def map_prefix(self):
        return self._map_prefix

# ...............................................
    @property
    def map_layername(self):
        lyrname = None
        if self._db_id is not None:
            lyrname = self._earl_jr.createLayername(occsetId=self._db_id)
        return lyrname

# ...............................................
    def create_local_map_filename(self):
        """
        @summary: Find mapfile containing this layer.  
        """
        occid = self.get_id()
        mapfilename = self._earl_jr.create_filename(
            LMFileType.SDM_MAP, occsetId=occid, objCode=occid, usr=self._user_id)
        return mapfilename

# ...............................................
    def set_local_map_filename(self, mapfname=None):
        """
        @note: Overrides existing _map_filename
        @summary: Find mapfile containing layers for this model's occ_layer.
        @param mapfname: Previously constructed mapfilename
        """
        if self._map_filename is None:
            mapfname = self.create_local_map_filename()
        self._map_filename = mapfname

# ...............................................
    @property
    def map_filename(self):
        self.set_local_map_filename()
        return self._map_filename

# ...............................................
    @property
    def map_name(self):
        if self._map_filename is None:
            self.set_local_map_filename()
        _, fname = os.path.split(self._map_filename)
        mapname, _ = os.path.splitext(fname)
        return mapname

# ...............................................
    @property
    def layer_name(self):
        return self._earl_jr.createLayername(occsetId=self.get_id())

# ...............................................
    def clear_local_mapfile(self):
        """
        @summary: Delete the mapfile containing this layer
        """
        if self._map_filename is None:
            self.set_local_map_filename()
        self.delete_local_mapfile()
        self._map_filename = None

# ...............................................
    def clear_output_files(self):
        self.delete_data()
        self.clearDLocation()

# ...............................................
    def delete_local_mapfile(self):
        """
        @summary: Delete the mapfile containing this layer
        """
        success, _ = self.deleteFile(self._map_filename, deleteDir=True)
        return success
    
# .............................................................................
# Public methods
# .............................................................................

# ...............................................
#     def copyForUser(self, user_id):
#         newOcc = OccurrenceLayer(
#             self.display_name, user_id, self.epsgcode, self.query_count,
#             squid=self.squid, verify=self.verify, valUnits=self.valUnits,
#             val_attribute=self.getValAttribute(), nodata_val=self.nodata_val,
#             min_val=self.min_val, max_val=self.max_val, mapunits=self.mapUnits,
#             resolution=self.resolution, bbox=self.bbox,
#             occMetadata=self.paramMetadata, sci_name=self._scientific_name,
#             status=self.status, status_mod_time=self.status_mod_time)
#         return newOcc
# 
# # # ...............................................
#     def getFeaturesIdLongLat(self):
#         """
#         @summary: Returns a list of feature/point tuples - (FID, x, y)
#         """
#         microVals = []
#         if self._localIdIdx is None:
#             self.getLocalIdIndex()
# 
#         geomIdx = self.getFieldIndex(self._geomFieldName)
#         for featureFID in list(self._features.keys()):
#             fid = self.getFeatureValByFieldIndex(self._localIdIdx, featureFID)
#             wkt = self.getFeatureValByFieldIndex(geomIdx, featureFID)
#             x, y = self.get_point_from_wkt(wkt)
#             # returns values id, longitude(x), latitude(y)
#             microVals.append((fid, x, y))
#         return microVals
# 
# # ..............................................................................
#     def getWkt(self):
#         wkt = None
#         if self._features and self._feature_attributes:
#             pttxtlst = []
#             self._setGeometryIndex()
#             for pt in list(self._features.values()):
#                 wkt = pt[self._geomIdx]
#                 pttxtlst.append(wkt.strip('POINT'))
#             multipointstr = ', '.join(pttxtlst)
#             wkt = 'MULTIPOINT( {} )'.format(multipointstr)
#         return wkt

# ...............................................
    def read_shapefile(self, largeFile=False, dlocation=None):
        """
        @note: calls Vector.readData to create points from features. This
                 will be removed when we switch to only using features
        @param largeFile: Indicates if the largeFile should be retrieved
        @param dlocation: Overrides the object's dlocation (possibly for 
                                    temporary file)
        """
        self.clearFeatures()
        if dlocation is None:
            dlocation = self.get_dlocation(largeFile=largeFile)
        Vector.read_data(self, dlocation=dlocation, doReadData=True)
