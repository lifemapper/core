"""Module containing class and functions representing shapegrid objects
"""
import os

from osgeo import ogr, osr

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (LMFormat, ProcessType)
from LmCommon.common.time import gmt
from LmCommon.shapes.build_shapegrid import build_shapegrid
from LmServer.base.layer import _LayerParameters, Vector
from LmServer.base.service_object import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (LMFileType, LMServiceType)


# .............................................................................
class Shapegrid(_LayerParameters, Vector, ProcessObject):
    """Shapegrid class"""
    # .................................
    def __init__(self, name, user_id, epsg_code, cell_sides, cell_size,
                 map_units, bbox, site_id='site_id', site_x='center_x',
                 site_y='center_y', size=None, lyr_id=None, verify=None,
                 dlocation=None, metadata=None, resolution=None,
                 metadata_url=None, parent_metadata_url=None, mod_time=None,
                 feature_count=0, feature_attributes=None, features=None,
                 fid_attribute=None, status=None, status_mod_time=None):
        """Constructor

        Args:
            cell_sides: Number of sides in each cell of a site (i.e. square =4,
                hexagon = 6).
            cell_size: Size in map_units of each cell.  For cellSides = 6
                (hexagon).HEXAGON, this is the measurement between two
                vertices.
            site_id: Attribute identifying the site number for each cell
            site_x: Attribute identifying the center X coordinate for each cell
            site_y: Attribute identifying the center Y coordinate for each cell
            size: Total number of cells in shapegrid
        """
        # site_indices are a dictionary of
        #    {siteIndex: (FID, center_x, center_y)}
        # created from the data file and passed to lmpy.Matrix for headers
        self._site_indices = None
        # field with unique values identifying each site
        self.site_id = site_id
        # field with longitude of the centroid of a site
        self.site_x = site_x
        # field with latitude of the centroid of a site
        self.site_y = site_y

        if metadata is None:
            metadata = {}

        _LayerParameters.__init__(
            self, user_id, param_id=lyr_id, mod_time=mod_time)
        Vector.__init__(
            self, name, user_id, epsg_code, lyr_id=lyr_id, verify=verify,
            dlocation=dlocation, metadata=metadata,
            data_format=LMFormat.SHAPE.driver, ogr_type=ogr.wkbPolygon,
            map_units=map_units, resolution=resolution, bbox=bbox,
            svc_obj_id=lyr_id, service_type=LMServiceType.SHAPEGRIDS,
            metadata_url=metadata_url, parent_metadata_url=parent_metadata_url,
            mod_time=mod_time, feature_count=feature_count,
            feature_attributes=feature_attributes, features=features,
            fid_attribute=fid_attribute)
        ProcessObject.__init__(
            self, obj_id=lyr_id, process_type=ProcessType.RAD_BUILDGRID,
            status=status, status_mod_time=status_mod_time)
        # Don't necessarily need centroids (requires reading shapegrid)
#         self._set_map_prefix()
        self._set_cell_sides(cell_sides)
        self.cell_size = cell_size
        self._size = None
        self._set_cell_measurements(size)

    # .................................
    @classmethod
    def init_from_parts(cls, vector, cell_sides, cell_size, site_id='site_id',
                        site_x='center_x', site_y='center_y', size=None,
                        site_indices_filename=None, status=None,
                        status_mod_time=None):
        """Create a shaepgrid object from its parts."""
        return Shapegrid(
            vector.name, vector.get_user_id(), vector.epsg_code, cell_sides,
            cell_size, vector.map_units, vector.bbox, site_id=site_id,
            site_x=site_x, site_y=site_y, size=size,
            lyr_id=vector.get_layer_id(), verify=vector.verify,
            dlocation=vector.get_dlocation(), metadata=vector.layer_metadata,
            resolution=vector.resolution, metadata_url=vector.metadata_url,
            parent_metadata_url=vector.parent_metadata_url,
            mod_time=vector.mod_time, feature_count=vector.feature_count,
            feature_attributes=vector.feature_attributes,
            features=vector.features, fid_attribute=vector.fid_attribute,
            status=status, status_mod_time=status_mod_time)

    # .................................
    def update_status(self, status, matrix_index=None, metadata=None,
                      mod_time=gmt().mjd):
        """Update the status of the shapegrid."""
        ProcessObject.update_status(self, status, mod_time)
        ServiceObject.update_mod_time(self, mod_time)
        _LayerParameters.update_params(
            self, mod_time, matrix_index=matrix_index, metadata=metadata)

    # .................................
    def _set_cell_sides(self, cell_sides):
        try:
            cell_sides = int(cell_sides)
        except TypeError:
            raise LMError('Number of cell sides must be an integer')

        if cell_sides in (4, 6):
            self._cell_sides = cell_sides
        else:
            raise LMError(
                ('Invalid cell shape.  Only 4 (square) and 6 (hexagon) are '
                 'currently supported'))

    # .................................
    @property
    def cell_sides(self):
        """Return the number of sides for each cell of the grid."""
        return self._cell_sides

    # .................................
    def _set_cell_measurements(self, size=None):
        if size is not None and isinstance(size, int):
            self._size = size
        else:
            self._size = self._get_feature_count()

    # .................................
    @property
    def size(self):
        """Return the size of the shapegrid."""
        return self._size

    # .................................
    def get_site_indices(self):
        """Return site indices for the shapegrid.

        Todo:
            Make sure to add this to base object
        """
        site_indices = {}
        if not(self._features) and self.get_dlocation() is not None:
            self.read_data()
        if self._features:
            for site_idx in list(self._features.keys()):
                if site_idx is None:
                    print('WTF?')
                geom = self._features[site_idx][self._geom_idx]
                site_indices[site_idx] = geom
        return site_indices

    # .................................
    def create_local_dlocation(self):
        """Calculates and returns the local _dlocation.
        """
        return self._earl_jr.create_filename(
            LMFileType.SHAPEGRID, lyr_name=self.name, usr=self._user_id,
            epsg=self._epsg)

    # .................................
    @staticmethod
    def check_bbox(min_x, min_y, max_x, max_y, res):
        """Check the extent envelope to ensure minx is less than maxx etc."""
        if min_x > max_x or min_y > max_y:
            raise LMError(
                'Min x > max x or min y > max y; ({}, {}, {}, {})'.format(
                    min_x, min_y, max_x, max_y))

        if (max_x - min_x) < res or (max_y - min_y) < res:
            raise LMError(
                'Resolution of cell_size is greater than x or y range')

    # .................................
    def cutout(self, cutout_wkt, remove_orig=False, dloc=None):
        """Create a new shapegrid from original using cutout.

        Todo:
            Check this, it may fail on newer versions of OGR -
                old: CreateFeature vs new: SetFeature
        """
        if not remove_orig and dloc is None:
            raise LMError(
                "If not modifying existing shapegrid must provide new dloc")
        ods = ogr.Open(self._dlocation)
        orig_layer = ods.GetLayer(0)
        if not orig_layer:
            raise LMError("Could not open Layer at: %s" % self._dlocation)
        if remove_orig:
            new_dloc = self._dlocation
            for ext in LMFormat.SHAPE.get_extensions():
                _, _ = self.delete_file(self._dlocation.replace('.shp', ext))
        else:
            new_dloc = dloc
            if os.path.exists(dloc):
                raise LMError(
                    'Shapegrid file already exists at: {}'.format(dloc))

            self.ready_filename(dloc, overwrite=False)

        t_srs = osr.SpatialReference()
        t_srs.ImportFromEPSG(self.epsg_code)
        drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)
        dataset = drv.CreateDataSource(new_dloc)
        new_layer = dataset.CreateLayer(
            dataset.GetName(), geom_type=ogr.wkbPolygon, srs=t_srs)
        orig_lyr_defn = orig_layer.GetLayerDefn()
        orig_field_cnt = orig_lyr_defn.GetFieldCount()
        for field_idx in range(orig_field_cnt):
            orig_fld_def = orig_lyr_defn.GetFieldDefn(field_idx)
            new_layer.CreateField(orig_fld_def)
        # create geom from wkt
        selected_poly = ogr.CreateGeometryFromWkt(cutout_wkt)
        min_x, max_x, min_y, max_y = selected_poly.GetEnvelope()
        orig_feature = orig_layer.GetNextFeature()
#         site_idIdx = orig_feature.GetFieldIndex(self.site_id)
        new_site_id = 0
        while orig_feature is not None:
            clone = orig_feature.Clone()
            clone_geom_ref = clone.GetGeometryRef()
            if clone_geom_ref.Intersect(selected_poly):
                # clone.SetField(site_idIdx,new_site_id)
                new_layer.CreateFeature(clone)
                new_site_id += 1
            orig_feature = orig_layer.GetNextFeature()
        dataset.Destroy()
        return min_x, min_y, max_x, max_y

    # .................................
    def build_shape(self, cutout=None, overwrite=False):
        """Build and write the shapegrid"""
        # After build, set_dlocation, write shapefile
        if os.path.exists(self._dlocation) and not overwrite:
            print((
                'Shapegrid file already exists at: {}'.format(
                    self._dlocation)))
            self.read_data(do_read_data=False)
            self._set_cell_measurements()
            return
        self.ready_filename(self._dlocation, overwrite=overwrite)
        cell_count = build_shapegrid(
            self._dlocation, self.min_x, self.min_y, self.max_x, self.max_y,
            self.cell_size, self.epsg_code, self._cell_sides,
            site_id=self.site_id, site_x=self.site_x, site_y=self.site_y,
            cutout_wkt=cutout)
        self._set_cell_measurements(size=cell_count)
        self.set_verify()

    # .................................
    def get_target_files(self, work_dir=None):
        """Return target files for shapegrid data creation"""
        if work_dir is None:
            work_dir = ''
        target_files = []
        target_dir = os.path.join(
            work_dir, os.path.splitext(self.get_relative_dlocation())[0])
        base_name = os.path.splitext(os.path.basename(self.get_dlocation()))[0]

        for ext in ['.shp', '.dbf', '.prj', '.shx']:
            target_files.append(
                os.path.join(target_dir, '{}{}'.format(base_name, ext)))
        return target_files
