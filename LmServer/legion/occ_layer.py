"""Module containing classes and functions for occurrence sets
"""
import os

from osgeo import ogr

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.time import gmt
from LmServer.base.layer import Vector, _LayerParameters
from LmServer.base.service_object import ProcessObject
from LmServer.common.lmconstants import (
    ID_PLACEHOLDER, LMFileType, LMServiceType, OccurrenceFieldNames)


# .............................................................................
class OccurrenceType(_LayerParameters, ProcessObject):
    """Occurrence type class"""

    # ................................
    def __init__(self, display_name, query_count, mod_time, user_id,
                 occ_layer_id, metadata=None, sci_name=None,
                 raw_dlocation=None, process_type=None, status=None,
                 status_mod_time=None):
        """Initialize the _Occurrences class instance

        Args:
            display_name: Name to be displayed for this dataset
            query_count: Count reported by last update to shapefile.  Used if
                there are no features attached to this OccurrenceSet.
            occ_layer_id: The occ_layer id for the database
            sci_name: ScientificName object containing further information
                about the name associated with these data
            raw_dlocation: URL or file location of raw data to be processed
        """
        _LayerParameters.__init__(
            self, user_id, param_id=occ_layer_id, matrix_index=-1,
            metadata=metadata, mod_time=mod_time)
        ProcessObject.__init__(
            self, obj_id=occ_layer_id, process_type=process_type,
            status=status, status_mod_time=status_mod_time)
        self.display_name = display_name
        self.query_count = query_count
        self._raw_dlocation = raw_dlocation
        self._big_dlocation = None
        self._scientific_name = sci_name

    # ................................
    def get_scientific_name_id(self):
        """Return the scientific name identifier."""
        if self._scientific_name is not None:
            return self._scientific_name.get_id()

        return None

    # ................................
    def get_scientific_name(self):
        """Return the scientific name object
        """
        return self._scientific_name

    # ................................
    def set_scientific_name(self, sci_name):
        """Set the scientific name object
        """
        self._scientific_name = sci_name

    # ................................
    def get_raw_dlocation(self):
        """Return the raw data location
        """
        return self._raw_dlocation

    # ................................
    def set_raw_dlocation(self, raw_dlocation, mod_time):
        """Set the raw data location of the occurrence layer."""
        self._raw_dlocation = raw_dlocation
        self.param_mod_time = mod_time

    # ................................
    def update_status(self, status, mod_time=gmt().mjd, query_count=None):
        """Update the object status.
        """
        ProcessObject.update_status(self, status, mod_time)
        if query_count is not None:
            self.query_count = query_count
            self.param_mod_time = self.status_mod_time


# .............................................................................
class OccurrenceLayer(OccurrenceType, Vector):
    """Class for occurrence layers."""

    # ................................
    def __init__(self, display_name, user_id, epsg_code, query_count,
                 lyr_id=None, squid=None, verify=None, dlocation=None,
                 raw_dlocation=None, raw_meta_dlocation=None,
                 layer_metadata=None, data_format=LMFormat.SHAPE.driver,
                 val_units=None, val_attribute=None, nodata_val=None,
                 min_val=None, max_val=None, map_units=None, resolution=None,
                 bbox=None, occ_layer_id=None,
                 service_type=LMServiceType.OCCURRENCES,
                 metadata_url=None, parent_metadata_url=None, feature_count=0,
                 feature_attributes=None, features=None, fid_attribute=None,
                 occ_metadata=None, sci_name=None, obj_id=None,
                 process_type=None, status=None, status_mod_time=None):
        """Occurrence layer constructor

        Todo:
            - calculate bbox from points upon population, update as appropriate
            - Remove obj_id
            - Remove lyr_id
        """
        OccurrenceType.__init__(
            self, display_name, query_count, status_mod_time, user_id,
            occ_layer_id, metadata=occ_metadata, sci_name=sci_name,
            raw_dlocation=raw_dlocation, process_type=process_type,
            status=status, status_mod_time=status_mod_time)
        Vector.__init__(
            self, None, user_id, epsg_code, lyr_id=occ_layer_id, squid=squid,
            verify=verify, dlocation=dlocation, metadata=layer_metadata,
            data_format=data_format, ogr_type=ogr.wkbPoint,
            val_units=val_units, val_attribute=val_attribute,
            nodata_val=nodata_val, min_val=min_val, max_val=max_val,
            map_units=map_units, resolution=resolution, bbox=bbox,
            svc_obj_id=occ_layer_id, service_type=service_type,
            metadata_url=metadata_url, parent_metadata_url=parent_metadata_url,
            mod_time=status_mod_time, feature_count=feature_count,
            feature_attributes=feature_attributes, features=features,
            fid_attribute=fid_attribute)
        self.raw_meta_dlocation = raw_meta_dlocation
        self.set_id(occ_layer_id)

    # ................................
    @staticmethod
    def get_user_point_feature_attributes():
        """Return user point feature attributes
        """
        feature_attributes = {
            0: (Vector._local_id_field_name, Vector._local_id_field_type),
            1: (OccurrenceFieldNames.LONGITUDE[0], ogr.OFTReal),
            2: (OccurrenceFieldNames.LATITUDE[0], ogr.OFTReal),
            3: (Vector._geom_field_name, Vector._geom_field_type)
            }
        return feature_attributes

    # ................................
    @staticmethod
    def get_user_point_feature(id_val, x_val, y_val):
        """Return user point feature
        """
        geom_wkt = OccurrenceLayer.get_point_wkt(x_val, y_val)
        return [id_val, x_val, y_val, geom_wkt]

    # ................................
    @staticmethod
    def equal_points(wkt1, wkt2):
        """Check if two points are the same
        """
        if wkt1 == wkt2:
            return True

        pt1 = OccurrenceLayer.get_point_from_wkt(wkt1)
        pt2 = OccurrenceLayer.get_point_from_wkt(wkt2)
        if abs(pt1[0] - pt2[0]) > 1e-6:
            return False
        if abs(pt1[1] - pt2[1]) > 1e-6:
            return False

        return True

    # ................................
    @staticmethod
    def get_point_from_wkt(wkt):
        """Return a point from well-known text"""
        if wkt is None:
            raise LMError('Missing wkt')
        start = wkt.find('(')
        end = wkt.rfind(')')
        if (start != -1 and end != -1):
            x_val, y_val = wkt[start + 1:end].split()
            try:
                x_val = float(x_val)
                y_val = float(y_val)
            except TypeError:
                raise LMError('Invalid point WKT {}'.format(wkt))

            return (x_val, y_val)
        return (None, None)
    
# ...............................................
    def copy_for_user(self, user_id):
        new_occ = OccurrenceLayer(
            self.display_name, user_id, self.epsg_code, self.query_count, squid=self.squid, 
            verify=self.verify, layer_metadata=self.layer_metadata, val_units=self.val_units, 
            val_attribute=self.val_attribute, nodata_val=self.nodata_val,
            min_val=self.min_val, max_val=self.max_val, map_units=self.map_units, 
            resolution=self.resolution, bbox=self.bbox, fid_attribute=self.fid_attribute,
            occ_metadata=self.param_metadata, sci_name=self.get_scientific_name(), 
            status=self.status, status_mod_time=self.status_mod_time)
        return new_occ


    # .............................
    def get_multipoint_wkt(self, ogr_format, feature_limit=None):
        """Read the occurrence shapefile using OGR.
        
        TODO:
            FIXME
            psycopg2.errors.InvalidParameterValue: Geometry has Z dimension but column does not
            CONTEXT:  SQL statement "UPDATE lm_v3.OccurrenceSet SET geompts = ST_GeomFromText(pointswkt, epsg) 
            WHERE occurrenceSetId = occid"


        Read OGR-accessible data and set the features and feature_attributes on
        the Vector object

        Args:
            dlocation: Full path location of the data
            ogr_format: OGR-supported data format code, available at
                http://www.gdal.org/ogr/ogr_formats.html

        Returns:
            string with the WKT of a MULTIPOINT feature containing all (or the 
            first feature_limit number) points

        Raises:
            LMError: On failure to read data.
        """
        multipoint = multipoint_wkt = None
        if self._dlocation is not None and os.path.exists(self._dlocation):
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(ogr_format)
            try:
                dataset = drv.Open(self._dlocation)
            except Exception as e:
                raise LMError('Invalid datasource {}'.format(
                    self._dlocation, str(e)), do_trace=True)
            try:
                slyr = dataset.GetLayer(0)
            except Exception as err:
                raise LMError(
                    '#### Failed to GetLayer from {}'.format(self._dlocation), err,
                    do_trace=True)
            feat_count = slyr.GetFeatureCount()
            if feat_count == 0:
                return None
            
            multipoint = ogr.Geometry(ogr.wkbMultiPoint)
            # Limit the number of features to read?
            if feature_limit is not None and feature_limit < feat_count:
                feat_count = feature_limit
            try:
                # Add each point to a MULTIPOINT geometry
                for j in range(feat_count):
                    curr_feat = slyr.GetFeature(j)
                    if curr_feat is not None:
                        try:
                            geom = curr_feat.geometry()
                            lon = geom.GetX()
                            lat = geom.GetY()
                            pt = ogr.Geometry(ogr.wkbPoint)
                            pt.AddPoint(lon, lat)
                            multipoint.AddGeometry(pt)
                        except:
                            self._log.error('Failed to read coords for feat {}'
                                            .format(curr_feat.GetFID()))
            except Exception as e:
                raise LMError(
                    'Failed to read features from {} ({})'.format(
                        self._dlocation, str(e)), do_trace=True)
            multipoint_wkt = multipoint.ExportToWkt()
        else:
            raise LMError('dlocation {} does not exist'.format(self._dlocation))
        return multipoint_wkt

    # ................................
    @staticmethod
    def get_point_wkt(x_val, y_val):
        """Creates a well-known-text string representing the point

        Note:
            Rounds the float to 4 decimal points
        """
        try:
            float(x_val)
            float(y_val)
        except TypeError:
            raise LMError(
                'Invalid point coordinates; x = {}, y = {}'.format(
                    x_val, y_val))
        else:
            x_val = round(x_val, 4)
            y_val = round(y_val, 4)
            return 'POINT ( {} {} )'.format(x_val, y_val)

    # ................................
    def _get_count(self):
        """Returns the number of new-style points (generic feature objects)

        Returns:
            int - The number of points for this dataset
        """
        return self._get_feature_count()

    count = property(_get_count)

    # ................................
    def set_id(self, occ_id):
        """Set the database identifier on the object.
        """
        super(OccurrenceLayer, self).set_id(occ_id)
        if occ_id is not None:
            if self.name is None:
                self.name = self._earl_jr.create_layer_name(
                    occ_set_id=self.get_id())
            self.set_dlocation()
            self.reset_metadata_url()
            self.set_local_map_filename()
            self._set_map_prefix()

    # ................................
    def get_absolute_path(self):
        """Return the absolute data path for the occurrence layer
        """
        self.set_dlocation()
        return Vector.get_absolute_path(self)

    # ................................
    def create_local_dlocation(self, raw=False, large_file=False,
                               makeflow=False):
        """Create filename for this layer.

        Args:
            raw: If true, this indicates a raw dump of occurrences (CSV for
                GBIF dump or User file, a URL for a BISON or iDigBio query).
            large_file: If true, this indicates a too-big file of occurrences
            makeflow: If true, this indicates a makeflow document of jobs
                related to this object
        """
        dloc = None
        if self.get_id() is not None:
            if raw:
                f_type = LMFileType.OCCURRENCE_RAW_FILE
            elif makeflow:
                f_type = LMFileType.MF_DOCUMENT
            elif large_file:
                f_type = LMFileType.OCCURRENCE_LARGE_FILE
            else:
                f_type = LMFileType.OCCURRENCE_FILE
            occ_id = self.get_id()
            dloc = self._earl_jr.create_filename(
                f_type, occ_set_id=occ_id, obj_code=occ_id, usr=self._user_id)
        return dloc

    # ................................
    def get_dlocation(self, large_file=False):
        """Return the data location of the occurrence layer
        """
        if large_file:
            if self._big_dlocation is None:
                self._big_dlocation = self.create_local_dlocation(
                    large_file=large_file)
            return self._big_dlocation

        self.set_dlocation()
        return self._dlocation

    # ................................
    def is_valid_dataset(self, large_file=False):
        """Check to see if data is valid using OGR.
        """
        dlocation = self.get_dlocation(large_file=large_file)
        return Vector.is_valid_dataset(self, dlocation=dlocation)

    # ................................
    def _create_map_prefix(self):
        """Construct a Lifemapper map URL endpoint.

        Note:
            - Uses the metatadataUrl for this object, plus 'ogc' format,
                map=<mapname>, and layers=<layername> key/value pairs.
            - If the object has not yet been inserted into the database, a
                placeholder is used until replacement after database insertion.
        """
        occ_id = self.get_id()
        if occ_id is None:
            occ_id = ID_PLACEHOLDER
        lyrname = self._earl_jr.create_basename(
            LMFileType.OCCURRENCE_FILE, obj_code=occ_id, usr=self._user_id,
            epsg=self.epsg_code)
        return self._earl_jr.construct_map_prefix_new(
            url_prefix=self.metadata_url, f_type=LMFileType.SDM_MAP,
            map_name=self.map_name, lyr_name=lyrname, usr=self._user_id)

    # ................................
    def _set_map_prefix(self):
        map_prefix = self._create_map_prefix()
        self._map_prefix = map_prefix

    # ................................
    @property
    def map_prefix(self):
        """Return the occurrence lyaer map prefix
        """
        return self._map_prefix

    # ................................
    @property
    def map_layer_name(self):
        """Return the map layer name
        """
        lyr_name = None
        if self._db_id is not None:
            lyr_name = self._earl_jr.create_layer_name(occ_set_id=self._db_id)
        return lyr_name

    # ................................
    def create_local_map_filename(self):
        """Find mapfile containing this layer.
        """
        occ_id = self.get_id()
        return self._earl_jr.create_filename(
            LMFileType.SDM_MAP, occ_set_id=occ_id, obj_code=occ_id,
            usr=self._user_id)

    # ................................
    def set_local_map_filename(self, map_fname=None):
        """Find mapfile containing layers for this model's occ_layer.

        Args:
            map_fname: Previously constructed mapfilename
        """
        if self._map_filename is None:
            map_fname = self.create_local_map_filename()
        self._map_filename = map_fname

    # ................................
    @property
    def map_filename(self):
        """Return the map filename."""
        self.set_local_map_filename()
        return self._map_filename

    # ................................
    @property
    def map_name(self):
        """Return the map name for the occurrence layer."""
        if self._map_filename is None:
            self.set_local_map_filename()
        _, fname = os.path.split(self._map_filename)
        mapname, _ = os.path.splitext(fname)
        return mapname

    # ................................
    @property
    def layer_name(self):
        """Return the layer name of the occurrence layer."""
        return self._earl_jr.create_layer_name(occ_set_id=self.get_id())

    # ................................
    def clear_local_mapfile(self):
        """Delete the mapfile containing this layer."""
        if self._map_filename is None:
            self.set_local_map_filename()
        self.delete_local_mapfile()
        self._map_filename = None

    # ................................
    def clear_output_files(self):
        """Clear occurrence layer output files."""
        self.delete_data()
        self.clear_dlocation()

    # ................................
    def delete_local_mapfile(self):
        """Delete the mapfile containing this layer."""
        success, _ = self.delete_file(self._map_filename, delete_dir=True)
        return success

    # ................................
    def read_shapefile(self, large_file=False, dlocation=None):
        """Read the occurrence layer shapefile.

        Args:
            large_file: Flag to indicate if the large_file should be retrieved
            dlocation: Overrides the object's dlocation (possibly for temporary
                file)
        """
        self.clear_features()
        if dlocation is None:
            dlocation = self.get_dlocation(large_file=large_file)
        Vector.read_data(self, dlocation=dlocation, do_read_data=True)
