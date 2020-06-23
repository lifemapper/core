"""Module that contains the RADExperiment class
"""
import os
import subprocess

from osgeo import ogr

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (LMFormat, MatrixType)
from LmServer.base.service_object import ServiceObject
from LmServer.common.lmconstants import LMFileType, LMServiceType
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.tree import Tree

# TODO: Move these to localconstants
NUM_RAND_GROUPS = 30
NUM_RAND_PER_GROUP = 2


# .............................................................................
class Gridset(ServiceObject):  # LMMap
    """Gridset class containing information for multispecies experiment."""
    # ................................
    def __init__(self, name=None, metadata=None, shapegrid=None,
                 shapegrid_id=None, tree=None, matrices=None, dlocation=None,
                 gridset_id=None,  # tree_id=None, site_indices_filename=None,
                 user_id=None, epsg_code=None, metadata_url=None,
                 mod_time=None):
        """Constructor for the Gridset class

        Args:
            name: Short identifier for this gridset, unique for userid.
            metadata: dictionary of metadata for gridset
            shapegrid: Vector layer with polygons representing geographic
                sites.
            shapegrid_id: database id for shapegrid
            tree: A Tree with taxa matching those in the PAM
            matrices: list of matrices for this gridset
            dlocation: data location for data file
            gridset_id: db_id  for ServiceObject
            user_id: id for the owner of these data
            epsg_code: The EPSG code of the spatial reference system of data.
            metadata_url: URL for retrieving the metadata
            mod_time: Last modification Time/Date, in MJD format
        """
        self._path = None
        if shapegrid is not None:
            if user_id is None:
                user_id = shapegrid.get_user_id()
            if shapegrid_id is None:
                shapegrid_id = shapegrid.get_id()
            if epsg_code is None:
                epsg_code = shapegrid.epsg_code
            elif epsg_code != shapegrid.epsg_code:
                raise LMError(
                    'Gridset EPSG {} does not match Shapegrid EPSG {}'.format(
                        self._epsg, shapegrid.epsg_code))

        ServiceObject.__init__(
            self, user_id, gridset_id, LMServiceType.GRIDSETS,
            metadata_url=metadata_url, mod_time=mod_time)
        # TODO: Aimee, do you want to move this somewhere else?
        self._dlocation = None
        self._map_filename = None
        self._set_map_prefix()
        self.name = name
        self.grid_metadata = {}
        self.load_grid_metadata(metadata)
        self._shapegrid = shapegrid
        self._shapegrid_id = shapegrid_id
        self._dlocation = None
        if dlocation is not None:
            self.set_dlocation(dlocation=dlocation)
        self._set_epsg(epsg_code)
        self._matrices = []
        self.set_matrices(matrices, do_read=False)
        self._tree = tree

    # ................................
    def _set_epsg(self, epsg=None):
        if epsg is None:
            if self._shapegrid is not None:
                epsg = self._shapegrid.epsg_code
        self._epsg = epsg

    # ................................
    def _get_epsg(self):
        if self._epsg is None:
            self._set_epsg()
        return self._epsg

    epsg_code = property(_get_epsg, _set_epsg)

    # ................................
    @property
    def tree_id(self):
        """Return the gridset's tree identifier."""
        try:
            return self._tree.get_id()
        except Exception:
            return None

    # ................................
    @property
    def tree(self):
        """Return the gridset's tree."""
        return self._tree

    # ................................
    def set_local_map_filename(self, map_fname=None):
        """Set absolute map_filename for this gridset."""
        if map_fname is None:
            map_fname = self._earl_jr.create_filename(
                LMFileType.RAD_MAP, gridset_id=self.get_id(),
                usr=self._user_id)
        self._map_filename = map_fname

    # ................................
    def clear_local_mapfile(self):
        """Delete the mapfile containing this layer."""
        if self.map_filename is None:
            self.set_local_map_filename()
        _ = self.delete_file(self.map_filename)

    # ................................
    def _create_map_prefix(self):
        """Construct the endpoint of a Lifemapper WMS URL for this object.

        Note:
            - Uses the metatadataUrl for this object, plus 'ogc' format,
                map=<mapname>, and key/value pairs.
            - If the object has not yet been inserted into the database, a
                placeholder is used until replacement after database insertion.
        """
        return self._earl_jr.construct_map_prefix_new(
            url_prefix=self.metadata_url, f_type=LMFileType.RAD_MAP,
            map_name=self.map_name, usr=self._user_id)

    # ................................
    def _set_map_prefix(self):
        mapprefix = self._create_map_prefix()
        self._map_prefix = mapprefix

    # ................................
    @property
    def map_prefix(self):
        """Get the map prefix for the gridset."""
        return self._map_prefix

    # ................................
    @property
    def map_filename(self):
        """Return the gridset's map filename."""
        if self._map_filename is None:
            self.set_local_map_filename()
        return self._map_filename

    # ................................
    @property
    def map_name(self):
        """Return the gridset's map name."""
        map_name = None
        if self._map_filename is not None:
            _, map_fname = os.path.split(self._map_filename)
            map_name, _ = os.path.splitext(map_fname)
        return map_name

    # ................................
    def get_shapegrid(self):
        """Return the gridset's shapegrid."""
        return self._shapegrid

    # ................................
    def set_id(self, gridset_id):
        """Set the identifier of the gridset."""
        ServiceObject.set_id(self, gridset_id)
        self.set_path()

    # ................................
    def set_path(self):
        """Set the gridset path."""
        if self._path is None:
            if all([self._user_id, self.get_id(), self.epsg_code]):
                self._path = self._earl_jr.create_data_path(
                    self._user_id, LMFileType.UNSPECIFIED_RAD, epsg=self._epsg,
                    gridset_id=self.get_id())
            else:
                raise LMError(
                    'Gridset must have user id, id, epsg to set path')

    # ................................
    @property
    def path(self):
        """Get the gridset path
        """
        if self._path is None:
            self.set_path()
        return self._path

    # ................................
    def create_local_dlocation(self):
        """Create an absolute filepath from object attributes
        """
        return self._earl_jr.create_filename(
            LMFileType.GRIDSET_PACKAGE, obj_code=self.get_id(),
            gridset_id=self.get_id(), usr=self.get_user_id())

    # ................................
    def get_dlocation(self):
        """Return the _dlocation attribute; create and set it if empty."""
        self.set_dlocation()
        return self._dlocation

    # ................................
    def set_dlocation(self, dlocation=None):
        """Set the data location of the gridset."""
        if self._dlocation is None:
            if dlocation is None:
                dlocation = self.create_local_dlocation()
            self._dlocation = dlocation

    # ................................
    def clear_dlocation(self):
        """Clear the gridset data location."""
        self._dlocation = None

    # ................................
    def get_package_location(self):
        """Return the file path for a gridset package."""
        return os.path.join(
            os.path.dirname(self.get_dlocation()),
            'gs_{}_package{}'.format(self.get_id(), LMFormat.ZIP.ext))

    # ................................
    def set_matrices(self, matrices, do_read=False):
        """Fill a Matrix object from Matrix or existing file."""
        if matrices is not None:
            for mtx in matrices:
                try:
                    self.add_matrix(mtx)
                except Exception as err:
                    raise LMError(
                        'Failed to add matrix {}, ({})'.format(mtx, err))

    # ................................
    def add_tree(self, tree, do_read=False):
        """Fill the Tree object, updating the tree dlocation."""
        if isinstance(tree, Tree):
            tree.set_user_id(self.get_user_id())
            # Make sure to set the parent Id and URL
            if self.get_id() is not None:
                tree.parent_id = self.get_id()
                tree.set_parent_metadata_url(self.metadata_url)
            self._tree = tree

    # ................................
    def add_matrix(self, mtx_file_or_obj, do_read=False):
        """Fill a Matrix object from Matrix or existing file."""
        mtx = None
        if mtx_file_or_obj is not None:
            usr = self.get_user_id()
            if isinstance(mtx_file_or_obj, str) and \
                    os.path.exists(mtx_file_or_obj):
                mtx = LMMatrix.init_from_file(mtx_file_or_obj, user_id=usr)
            elif isinstance(mtx_file_or_obj, LMMatrix):
                mtx = mtx_file_or_obj
                mtx.set_user_id(usr)
            if mtx is not None:
                # Make sure to set the parent Id and URL
                if self.get_id() is not None:
                    mtx.parent_id = self.get_id()
                    mtx.set_parent_metadata_url(self.metadata_url)
                if mtx.get_id() is None:
                    self._matrices.append(mtx)
                else:
                    existing_ids = [m.get_id() for m in self._matrices]
                    if mtx.get_id() not in existing_ids:
                        self._matrices.append(mtx)

    # ................................
    def get_matrices(self):
        """Return the matrices of the gridset."""
        return self._matrices

    # ................................
    def _get_matrix_types(self, mtx_types):
        if isinstance(mtx_types, int):
            mtx_types = [mtx_types]
        mtxs = []
        for mtx in self._matrices:
            if mtx.matrix_type in mtx_types:
                mtxs.append(mtx)
        return mtxs

    # ................................
    def get_all_pams(self):
        """Return all pams in the gridset, including rolling."""
        return self._get_matrix_types([MatrixType.PAM, MatrixType.ROLLING_PAM])

    # ................................
    def get_pams(self):
        """Return the pams in the gridset."""
        return self._get_matrix_types(MatrixType.PAM)

    # ................................
    def get_rolling_pams(self):
        """Return the rolling pams in the gridset."""
        return self._get_matrix_types(MatrixType.ROLLING_PAM)

    # ................................
    def get_grims(self):
        """Return the grims of the gridset."""
        return self._get_matrix_types(MatrixType.GRIM)

    # ................................
    def get_biogeographic_hypotheses(self):
        """Return biogeographic hypotheses matrices."""
        return self._get_matrix_types(MatrixType.BIOGEO_HYPOTHESES)

    # ................................
    def get_grim_for_codes(self, gcm_code, alt_pred_code, date_code):
        """Return GRIMs matching the provided codes."""
        for grim in self.get_grims():
            if all([grim.gcm_code == gcm_code,
                    grim.alt_pred_code == alt_pred_code,
                    grim.date_code == date_code]):
                return grim
        return None

    # ................................
    def get_pam_for_codes(self, gcm_code, alt_pred_code, date_code,
                          algorithm_code):
        """Return PAMs matching provided codes."""
        for pam in self.get_all_pams():
            if all([pam.gcm_code == gcm_code,
                    pam.alt_pred_code == alt_pred_code,
                    pam.date_code == date_code,
                    pam.algorithm_code == algorithm_code]):
                return pam
        return None

    # ................................
    def set_matrix_process_type(self, process_type, matrix_types=None,
                                matrix_id=None):
        """Set matrix process type."""
        if isinstance(matrix_types, int):
            matrix_types = [matrix_types]
        matching = []
        for mtx in self._matrices:
            if matrix_types:
                if mtx.matrix_type in matrix_types:
                    matching.append(mtx)
            elif matrix_id is not None:
                matching.append(mtx)
                break
        for mtx in matching:
            mtx.process_type = process_type

    # ................................
    def create_layer_shapefile_from_matrix(self, shp_filename,
                                           is_presence_absence=True):
        """
        TODO:
            Only partially tested, field creation is not holding.
        """
        if is_presence_absence:
            matrix = self.get_full_pam()
        else:
            matrix = self.get_full_grim()
        if matrix is None or self._shapegrid is None:
            return False

        self._shapegrid.copy_data(
            self._shapegrid.get_dlocation(), target_dlocation=shp_filename,
            format=self._shapegrid.data_format)
        ogr.RegisterAll()
        drv = ogr.GetDriverByName(self._shapegrid.data_format)
        try:
            shp_dataset = drv.Open(shp_filename, True)
        except Exception as err:
            raise LMError('Invalid data source: {}'.format(shp_filename), err)
        shp_lyr = shp_dataset.GetLayer(0)

        mtx_lyr_count = matrix.column_count
        fld_type = matrix.ogr_data_type
        # For each layer present, add a field/column to the shapefile
        for lyr_idx in range(mtx_lyr_count):
            if (not self._layers_present or (
                    self._layers_present and self._layers_present[lyr_idx])):
                # 8 character limit, must save fieldname
                fld_name = 'lyr%s' % str(lyr_idx)
                fld_defn = ogr.FieldDefn(fld_name, fld_type)
                if shp_lyr.CreateField(fld_defn) != 0:
                    raise LMError(
                        'CreateField failed for {} in {}'.format(
                            fld_name, shp_filename))

        # For each site/feature, fill with value from matrix
        curr_feat = shp_lyr.GetNextFeature()
        sites_keys = sorted(self.get_sites_present().keys())
        print("starting feature loop")
        while curr_feat is not None:
            # for lyridx in range(mlyrCount):
            for lyridx, exists in self._layers_present.items():
                if exists:
                    # add field to the layer
                    fldname = 'lyr%s' % str(lyridx)
                    siteidx = curr_feat.GetFieldAsInteger(
                        self._shapegrid.siteId)
                    # sites_keys = sorted(self.get_sites_present().keys())
                    realsiteidx = sites_keys.index(siteidx)
                    currval = matrix.getValue(realsiteidx, lyridx)
                    # debug
                    curr_feat.SetField(fldname, currval)
            # add feature to the layer
            shp_lyr.SetFeature(curr_feat)
            curr_feat.Destroy()
            curr_feat = shp_lyr.GetNextFeature()
        # print 'Last siteidx %d' % siteidx

        # Closes and flushes to disk
        shp_dataset.Destroy()
        print(('Closed/wrote dataset {}'.format(shp_filename)))
        success = True
        try:
            ret_code = subprocess.call(["shptree", "{}".format(shp_filename)])
            if ret_code != 0:
                print('Unable to create shapetree index on {}'.format(
                    shp_filename))
        except Exception as err:
            print('Unable to create shapetree index on {}: {}'.format(
                shp_filename, str(err)))
        return success

    # ................................
    def write_map(self, map_filename):
        pass
        # LMMap._write_map(self, map_filename, shpGrid=self._shapegrid,
        #                    matrices=self._matrices)

    # ................................
    def dump_grid_metadata(self):
        """Dump grid metadata to string."""
        return super(Gridset, self)._dump_metadata(self.grid_metadata)

    # ................................
    def load_grid_metadata(self, new_metadata):
        """Load grid metadata."""
        self.grid_metadata = super(Gridset, self)._load_metadata(new_metadata)

    # ................................
    def add_grid_metadata(self, new_metadata_dict):
        """Add grid metadata."""
        self.grid_metadata = super(Gridset, self)._add_metadata(
            new_metadata_dict, existing_metadata_dict=self.grid_metadata)

    # ................................
    @property
    def shapegrid_id(self):
        """Return the gridset's shapegrid identifier."""
        return self._shapegrid_id
