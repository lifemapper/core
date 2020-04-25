"""Module containing the projection object class and methods.
"""
import glob

from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.time import gmt
from LmServer.base.layer import Raster, _LayerParameters
from LmServer.base.service_object import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (
    Algorithms, ID_PLACEHOLDER, LMFileType, LMServiceType)


# .............................................................................
class _ProjectionType(_LayerParameters, ProcessObject):
    """
    """
    # ................................
    def __init__(self, occ_layer, algorithm, model_scenario, model_mask,
                 proj_scenario, proj_mask, process_type, proj_metadata,
                 status, status_mod_time, user_id, project_id):
        """Constructor

        Args:
            occ_layer: OccurrenceLayer object for SDM model process
            algorithm: Algorithm object for SDM model process
            model_scenario: Scenario (environmental layer inputs) for SDM
                model process
            model_mask: Mask for SDM model process
            proj_scenario: Scenario (environmental layer inputs) for SDM
                project process
            proj_mask: Mask for SDM project process
            process_type: LmCommon.common.lmconstants.ProcessType for
                computation
            proj_metadata: Metadata for this projection
            status: status of processing
            status_mod_time: last status modification time in MJD format
            user_id: id for the owner of these data
            project_id: Unique identifier for this parameterized object

        Note:
            proj_mask and mdlMask are currently input data layer for the only
                mask method.  This is set in the boom configuration file in the
                `PREPROCESSING SDM_MASK` section, with `CODE` set to
                `hull_region_intersect`, `buffer` to some value in the mapunits
                of the occurrence layer, and `region` with name of a layer
                owned bythe boom user.

        See:
            - LmServer.base.layer._LayerParameters::__init__()
            - LmServer.base.service_object.ProcessObject::__init__()

        Todo:
            - proj_mask and mdlMask should be dictionaries with masking method,
                input data and parameters
        """
        if status is not None and status_mod_time is None:
            status_mod_time = gmt().mjd

        _LayerParameters.__init__(
            self, user_id, param_id=project_id, matrix_index=-1,
            metadata=proj_metadata, mod_time=status_mod_time)
        ProcessObject.__init__(
            self, obj_id=project_id, process_type=process_type, status=status,
            status_mod_time=status_mod_time)
        self._occ_layer = occ_layer
        self._algorithm = algorithm
        self._model_mask = model_mask
        self._model_scenario = model_scenario
        self._proj_mask = proj_mask
        self._proj_scenario = proj_scenario

    # ................................
    def get_occ_layer_id(self):
        """Return the occurrence layer identifier
        """
        return self._occ_layer.get_id()

    # ................................
    def dump_algorithm_parameter_string(self):
        """Dump the algorithm parameters to a string
        """
        return self._algorithm.dump_algorithm_parameters()

    # ................................
    def get_model_mask_id(self):
        """Return the model mask identifier
        """
        try:
            return self._model_mask.get_id()
        except Exception:
            return None

    # ................................
    def get_model_scenario_id(self):
        """Return the identifier of the model scenario
        """
        return self._model_scenario.get_id()

    # ................................
    def get_proj_mask_id(self):
        """Return the projection mask layer id
        """
        try:
            return self._proj_mask.get_id()
        except Exception:
            return None

    # ................................
    def get_proj_scenario_id(self):
        """Return the projection scenario id
        """
        return self._proj_scenario.get_id()

    # ................................
    def is_openModeller(self):
        """Return boolean indication if algorithm is from openModeller
        """
        return Algorithms.is_openModeller(self._algorithm.code)

    # ................................
    def is_att(self):
        """Return boolean indicating if the algorithm is ATT maxent
        """
        return Algorithms.is_att(self._algorithm.code)

    # ................................
    @property
    def display_name(self):
        """Return the display name from the occurrence layer
        """
        return self._occ_layer.display_name

    # ................................
    @property
    def proj_scenario(self):
        """Return the projection's scenario object
        """
        return self._proj_scenario

    # ................................
    @property
    def proj_scenario_code(self):
        """Return the projection scenario's code
        """
        return self._proj_scenario.code

    # ................................
    @property
    def proj_mask(self):
        """Return the projection mask layer
        """
        return self._proj_mask

    # ................................
    def set_proj_mask(self, lyr):
        """Set the projection mask layer
        """
        self._proj_mask = lyr

    # ................................
    @property
    def occ_layer(self):
        """Return the occurrence layer for the projection
        """
        return self._occ_layer

    # ................................
    @property
    def status(self):
        """Return the projection status
        """
        return self._status

    # ................................
    @property
    def status_mod_time(self):
        """Return the time that the object status was last modified
        """
        return self._status_mod_time

    # ................................
    @property
    def species_name(self):
        """Return the species name of the occurrence data
        """
        return self._occ_layer.display_name

    # ................................
    @property
    def algorithm_code(self):
        """Return the code of the algorithm used
        """
        return self._algorithm.code

    # ................................
    @property
    def model_scenario(self):
        """Return the model scenario object
        """
        return self._model_scenario

    # ................................
    @property
    def model_scenario_code(self):
        """Return the model scenario code
        """
        return self._model_scenario.code

    # ................................
    @property
    def model_mask(self):
        """Return the model mask layer
        """
        return self._model_mask

    # ................................
    def set_model_mask(self, lyr):
        """Set the model mask layer
        """
        self._model_mask = lyr

    # ................................
    @property
    def proj_input_layers(self):
        """Return the layers of the projection Scenario
        """
        return self._proj_scenario.layers


# .............................................................................
class SDMProjection(_ProjectionType, Raster):
    """Class containing information for creating models and projections

    Todo:
        Make Models and Projections consistent for data member access
            between public/private members, properties, get/set/update

    Note:
        - Uses layerid for filename, layername construction
        - Uses layerid for _db_id, get_id(), ServiceObject
        - Uses sdmprojectid for objId, ProcessObject
    """

    # ................................
    def __init__(self, occ_layer, algorithm, model_scenario, proj_scenario,
                 process_type=None, model_mask=None, proj_mask=None,
                 proj_metadata=None, status=None, status_mod_time=None,
                 sdm_proj_id=None, name=None, epsgcode=None, lyr_id=None,
                 squid=None, verify=None, dlocation=None, layer_metadata=None,
                 data_format=None, gdal_type=None, val_units=None,
                 nodata_val=None, min_val=None, max_val=None, map_units=None,
                 resolution=None, bbox=None, metadata_url=None,
                 parent_metadata_url=None):
        """Constructor"""
        (user_id, name, squid, process_type, bbox, epsg, map_units, resolution,
         is_discrete_data, data_format, title
         ) = self._get_defaults_from_inputs(
             lyr_id, occ_layer, algorithm, model_scenario, proj_scenario,
             name, squid, process_type, bbox, epsgcode, map_units, resolution,
             data_format)
        _ProjectionType.__init__(
            self, occ_layer, algorithm, model_scenario, model_mask,
            proj_scenario, proj_mask, process_type, proj_metadata, status,
            status_mod_time, user_id, sdm_proj_id)
        if not layer_metadata:
            layer_metadata = self._create_metadata(
                proj_scenario, occ_layer.display_name, algorithm.code,
                title=title, is_discrete_data=is_discrete_data)
        Raster.__init__(
            self, name, user_id, epsg, lyr_id=lyr_id, squid=squid,
            verify=verify, dlocation=dlocation, metadata=layer_metadata,
            data_format=data_format, gdal_type=gdal_type, val_units=val_units,
            nodata_val=nodata_val, min_val=min_val, max_val=max_val,
            map_units=map_units, resolution=resolution, bbox=bbox,
            svc_obj_id=lyr_id, service_type=LMServiceType.PROJECTIONS,
            metadata_url=metadata_url, parent_metadata_url=parent_metadata_url,
            mod_time=status_mod_time)
        # TODO: clean this up.  Do not allow layer to calculate dlocation,
        #         subclass SDMProjection must override
        self.set_id(lyr_id)
        self.set_local_map_filename()
        self._set_map_prefix()

    # ................................
    @classmethod
    def init_from_parts(cls, occ_layer, algorithm, model_scenario,
                        proj_scenario, layer, process_type=None,
                        model_mask=None, proj_mask=None, proj_metadata=None,
                        status=None, status_mod_time=None, sdm_proj_id=None):
        """Create a projection object from its parts."""
        return SDMProjection(
            occ_layer, algorithm, model_scenario, proj_scenario,
            process_type=process_type, model_mask=model_mask,
            proj_mask=proj_mask, proj_metadata=proj_metadata, status=status,
            status_mod_time=status_mod_time, sdm_proj_id=sdm_proj_id,
            name=layer.name, epsgcode=layer.epsg_code, lyr_id=layer.get_id(),
            squid=layer.squid, verify=layer.verify, dlocation=layer._dlocation,
            layer_metadata=layer.layer_metadata, data_format=layer.data_format,
            gdal_type=layer.gdal_type, val_units=layer.val_units,
            nodata_val=layer.nodata_val, min_val=layer.min_val,
            max_val=layer.max_val, map_units=layer.map_units,
            resolution=layer.resolution, bbox=layer.bbox,
            metadata_url=layer.metadata_url,
            parent_metadata_url=layer.parent_metadata_url)

    # ................................
    def set_id(self, lyr_id):
        """Set the identifier of the projection.

        Args:
            lyr_id: The database identifier of the projection layer
        """
        super(SDMProjection, self).set_id(lyr_id)
        if lyr_id is not None:
            self.name = self._earl_jr.create_layer_name(proj_id=lyr_id)
            self.clear_dlocation()
            self.set_dlocation()
            self.title = '{} Projection {}'.format(
                self.species_name, str(lyr_id))
            self._set_map_prefix()

    # ................................
    def create_local_dlocation(self):
        """Create data location."""
        dloc = None
        if self.get_id() is not None:
            dloc = self._earl_jr.create_filename(
                LMFileType.PROJECTION_LAYER, obj_code=self.get_id(),
                occ_set_id=self._occ_layer.get_id(), usr=self._user_id,
                epsg=self._epsg)
        return dloc

    # ................................
    def get_dlocation(self):
        """Return the projection data location."""
        self.set_dlocation()
        return self._dlocation

    # ................................
    def set_dlocation(self, dlocation=None):
        """Set the data location of the projection.

        Note:
            Does NOT override existing dlocation, use clear_dlocation for that
        """
        # Only set DLocation if it is currently None
        if self._dlocation is None:
            if dlocation is None:
                dlocation = self.create_local_dlocation()
            self._dlocation = dlocation

    # ................................
    def get_absolute_path(self):
        """Return the absolute path to the species data
        """
        return self._occ_layer.get_absolute_path()

    # ................................
    @staticmethod
    def _create_metadata(prj_scenario, species_name, algorithm_code,
                         title=None, is_discrete_data=False):
        """Assemble SDMProjection metadata the first time it is created."""
        metadata = {}
        keywords = ['SDM', 'potential habitat', species_name, algorithm_code]
        prj_keywords = prj_scenario.scen_metadata[
            ServiceObject.META_KEYWORDS]
        keywords.extend(prj_keywords)
        # remove duplicates
        keywords = list(set(keywords))

        metadata[ServiceObject.META_KEYWORDS] = keywords
        metadata[ServiceObject.META_DESCRIPTION] = (
            'Modeled habitat for {} projected onto {} datalayers'.format(
                species_name, prj_scenario.name))
        metadata[Raster.META_IS_DISCRETE] = is_discrete_data
        if title is not None:
            metadata[Raster.META_TITLE] = title
        return metadata

    # ................................
    @staticmethod
    def _get_defaults_from_inputs(lyr_id, occ_layer, algorithm,
                                  model_scenario, proj_scenario, name, squid,
                                  process_type, bbox, epsg_code, map_units,
                                  resolution, gdal_format):
        """Assemble attributes from process inputs the first time created."""
        user_id = occ_layer.get_user_id()
        if name is None:
            if lyr_id is None:
                lyr_id = ID_PLACEHOLDER
            name = occ_layer._earl_jr.create_layer_name(proj_id=lyr_id)
        if squid is None:
            squid = occ_layer.squid
        if bbox is None:
            bbox = proj_scenario.bbox
        if epsg_code is None:
            epsg_code = proj_scenario.epsg_code
        if map_units is None:
            map_units = proj_scenario.map_units
        if resolution is None:
            resolution = proj_scenario.resolution
        if process_type is None:
            if Algorithms.is_att(algorithm.code):
                process_type = ProcessType.ATT_PROJECT
            else:
                process_type = ProcessType.OM_PROJECT
        is_discrete_data = Algorithms.returns_discrete_output(algorithm.code)
        title = occ_layer._earl_jr.create_sdm_project_title(
            occ_layer._user_id, occ_layer.display_name, algorithm.code,
            model_scenario.code, proj_scenario.code)
        if gdal_format is None:
            gdal_format = Algorithms.get(algorithm.code).output_format
        return (user_id, name, squid, process_type, bbox, epsg_code, map_units,
                resolution, is_discrete_data, gdal_format, title)

    # ................................
    def update_status(self, status, metadata=None, mod_time=gmt().mjd):
        """Update status, metadata, mod_time on the SDMProjection."""
        ProcessObject.update_status(self, status, mod_time)
        ServiceObject.update_mod_time(self, mod_time)
        _LayerParameters.update_params(self, mod_time, metadata=metadata)

        # If projection will be updated with a successful complete status,
        #     clear the map file so that it can be regenerated
        try:
            if status == JobStatus.COMPLETE:
                self.clear_local_mapfile()
        except Exception:
            pass

    # ................................
    def clear_output_files(self):
        """Clear projection output files."""
        pkg_fname = self.get_proj_package_filename()
        success, _ = self.delete_file(pkg_fname)
        # metadata files
        prj_fnames = glob.glob(self._dlocation + '*')
        for fname in prj_fnames:
            success, _ = self.delete_file(fname)
        self.clear_dlocation()

    # ................................
    def rollback(self, status=JobStatus.GENERAL):
        """Rollback processing."""
        self.update_status(status)
        self.clear_output_files()
        self.clear_local_mapfile()

    # ................................
    def get_proj_package_filename(self):
        """Return the projection package file name."""
        return self._earl_jr.create_filename(
            LMFileType.PROJECTION_PACKAGE, obj_code=self.get_id(),
            occ_set_id=self._occ_layer.get_id(), usr=self._user_id,
            epsg=self._epsg)

    # ................................
    def clear_local_mapfile(self):
        """Delete the mapfile containing this layer."""
        return self._occ_layer.clear_local_mapfile()

    # ................................
    def set_local_map_filename(self):
        """Find mapfile containing layers for this projection's occ_layer."""
        self._occ_layer.set_local_map_filename()

    # ................................
    @property
    def map_filename(self):
        """Return the map file name for the projection."""
        return self._occ_layer.map_filename

    # ................................
    @property
    def map_name(self):
        """Return the map name property of the projection."""
        return self._occ_layer.map_name

    # ................................
    def _create_map_prefix(self):
        """Construct the endpoint of a Lifemapper WMS URL for this object.

        Note:
            - Uses the metatadataUrl for this object, plus 'ogc' format,
                map=<mapname>, and layers=<layername> key/value pairs.
            - If the object has not yet been inserted into the database, a
                placeholder is used until replacement after database insertion.
        """
        # Recompute in case we have a new db ID
        proj_id = self.get_id()
        if proj_id is None:
            proj_id = ID_PLACEHOLDER
        lyr_name = self._earl_jr.create_basename(
            LMFileType.PROJECTION_LAYER, obj_code=proj_id, usr=self._user_id,
            epsg=self.epsg_code)
        map_prefix = self._earl_jr.construct_map_prefix_new(
            f_type=LMFileType.SDM_MAP, map_name=self._occ_layer.map_name,
            lyr_name=lyr_name, usr=self._user_id)
        return map_prefix

    # ................................
    def _set_map_prefix(self):
        """Set the map prefix for the projection."""
        map_prefix = self._create_map_prefix()
        self._map_prefix = map_prefix

    # ................................
    @property
    def map_prefix(self):
        """Return the projection map prefix."""
        self._set_map_prefix()
        return self._map_prefix

    # ................................
    @property
    def map_layername(self):
        """Return the projection layer name property."""
        lyr_name = None
        if self._db_id is not None:
            lyr_name = self._earl_jr.create_layer_name(proj_id=self._db_id)
        return lyr_name
