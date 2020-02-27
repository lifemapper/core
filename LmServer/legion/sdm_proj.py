"""Module containing the projection object class and methods.
"""
import glob
from hashlib import md5
import json
import os

from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.time import gmt
from LmServer.base.layer import Raster, _LayerParameters
from LmServer.base.service_object import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (
    Algorithms, ID_PLACEHOLDER, LMFileType, LMServiceType)


# .........................................................................
class _ProjectionType(_LayerParameters, ProcessObject):
    """
    """

    # .........................................
    def __init__(self, occ_layer, algorithm, model_scenario, model_mask,
                     proj_scenario, proj_mask, process_type, proj_metadata,
                     status, status_mod_time, user_id, projectId):
        """Constructor for the _ProjectionType class

        Args:
            * occ_layer: OccurrenceLayer object for SDM model process
            * algorithm: Algorithm object for SDM model process
            * model_scenario: : Scenario (environmental layer inputs) for SDM
                model process
            * model_mask: Mask for SDM model process
            * proj_scenario: Scenario (environmental layer inputs) for SDM
                project process
            * proj_mask: Mask for SDM project process
            * process_type: LmCommon.common.lmconstants.ProcessType for
                computation
            * proj_metadata: Metadata for this projection 

        Note:
            * proj_mask and mdlMask are currently input data layer for the only
                mask method.  This is set in the boom configuration file in the 
                `PREPROCESSING SDM_MASK` section, with `CODE` set to 
                `hull_region_intersect`, `buffer` to some value in the mapunits
                of the occurrence layer, and `region` with name of a layer
                owned bythe boom user. 

        See:
            * LmServer.base.layer._LayerParameters::__init__()
            * LmServer.base.service_object.ProcessObject::__init__()

        Todo:
            * proj_mask and mdlMask should be dictionaries with masking method,
                input data and parameters
        """
        if status is not None and status_mod_time is None:
            status_mod_time = gmt().mjd

        _LayerParameters.__init__(
            self, user_id, paramId=projectId, matrix_index=-1,
            metadata=proj_metadata, mod_time=status_mod_time)
        ProcessObject.__init__(
            self, obj_id=projectId, process_type=process_type, status=status,
            status_mod_time=status_mod_time)
        self._occ_layer = occ_layer
        self._algorithm = algorithm
        self._model_mask = model_mask
        self._model_scenario = model_scenario
        self._proj_mask = proj_mask
        self._proj_scenario = proj_scenario

# ...............................................
# Projection Input Data Object attributes:
# OccurrenceSet, Algorithm, ModelMask, ModelScenario, ProjMask, ProjScenario
# ...............................................
    def get_occ_layer_id(self):
        return self._occ_layer.get_id()

    def dump_algorithm_parameter_string(self):
        return self._algorithm.dumpAlgParameters()

    def get_model_mask_id(self):
        try:
            return self._model_mask.get_id()
        except:
            return None

    def get_model_scenario_id(self):
        return self._model_scenario.get_id()

    def get_proj_mask_id(self):
        try:
            return self._proj_mask.get_id()
        except:
            return None

    def get_proj_scenario_id(self):
        return self._proj_scenario.get_id()

    def is_openModeller(self):
        return Algorithms.is_openModeller(self._algorithm.code)

    def is_att(self):
        return Algorithms.is_att(self._algorithm.code)

    @property
    def display_name(self):
        return self._occ_layer.display_name

    @property
    def proj_scenario(self):
        return self._proj_scenario

    @property
    def proj_scenario_code(self):
        return self._proj_scenario.code

    @property
    def proj_mask(self):
        return self._proj_mask

    def set_proj_mask(self, lyr):
        self._proj_mask = lyr

    @property
    def occ_layer(self):
        return self._occ_layer

    @property
    def status(self):
        return self._status

    @property
    def status_mod_time(self):
        return self._statusmodtime

    @property
    def species_name(self):
        return self._occ_layer.display_name

    @property
    def algorithm_code(self):
        return self._algorithm.code

    @property
    def model_scenario(self):
        return self._model_scenario

    @property
    def model_scenario_code(self):
        return self._model_scenario.code

    @property
    def model_mask(self):
        return self._model_mask

    def set_model_mask(self, lyr):
        self._model_mask = lyr

    @property
    def proj_input_layers(self):
        """
        @summary Gets the layers of the projection Scenario
        """
        return self._proj_scenario.layers


# .............................................................................
class SDMProjection(_ProjectionType, Raster):
    """Class containing information for creating models and projections

    Todo:
        * make Models and Projections consistent for data member access
            between public/private members, properties, get/set/update

    Note:
        * Uses layerid for filename, layername construction
        * Uses layerid for _dbId, get_id(), ServiceObject
        * Uses sdmprojectid for objId, ProcessObject
    """

# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, occ_layer, algorithm, model_scenario, proj_scenario,
                 process_type=None, model_mask=None, proj_mask=None,
                 proj_metadata={}, status=None, status_mod_time=None,
                 sdm_proj_id=None, name=None, epsgcode=None, lyr_id=None,
                 squid=None, verify=None, dlocation=None, lyrMetadata={},
                 data_format=None, gdal_type=None, valUnits=None, nodata_val=None,
                 min_val=None, max_val=None, mapunits=None, resolution=None,
                 bbox=None, metadata_url=None, parent_metadata_url=None):
        """
        @summary Constructor for the SDMProjection class
        @copydoc LmServer.legion.sdmproj._ProjectionType::__init__()
        @copydoc LmServer.base.layer._Layer::__init__()
        """
        (user_id, name, squid, process_type, bbox, epsg, mapunits, resolution,
         is_discrete_data, data_format, title) = self._get_defaults_from_inputs(
             lyr_id, occ_layer, algorithm, model_scenario, proj_scenario,
             name, squid, process_type, bbox, epsgcode, mapunits, resolution,
             data_format)
        _ProjectionType.__init__(
            self, occ_layer, algorithm, model_scenario, model_mask,
            proj_scenario, proj_mask, process_type, proj_metadata, status,
            status_mod_time, user_id, sdm_proj_id)
        if not lyrMetadata:
            lyrMetadata = self._create_metadata(
                proj_scenario, occ_layer.display_name, algorithm.code,
                title=title, is_discrete_data=is_discrete_data)
        Raster.__init__(
            self, name, user_id, epsg, lyr_id=lyr_id, squid=squid, verify=verify,
            dlocation=dlocation, metadata=lyrMetadata, data_format=data_format,
            gdal_type=gdal_type, valUnits=valUnits, nodata_val=nodata_val,
            min_val=min_val, max_val=max_val, mapunits=mapunits,
            resolution=resolution, bbox=bbox, svc_obj_id=lyr_id,
            service_type=LMServiceType.PROJECTIONS, metadata_url=metadata_url,
            parent_metadata_url=parent_metadata_url, mod_time=status_mod_time)
        # TODO: clean this up.  Do not allow layer to calculate dlocation,
        #         subclass SDMProjection must override
        self.set_id(lyr_id)
        self.set_local_map_filename()
        self._set_map_prefix()

# .............................................................................
# another Constructor
# # .............................................................................
    @classmethod
    def init_from_parts(cls, occ_layer, algorithm, model_scenario,
                      proj_scenario, layer, process_type=None, model_mask=None,
                      proj_mask=None, proj_metadata={}, status=None,
                      status_mod_time=None, sdm_proj_id=None):
        prj = SDMProjection(
            occ_layer, algorithm, model_scenario, proj_scenario,
            process_type=process_type, model_mask=model_mask, proj_mask=proj_mask,
            proj_metadata=proj_metadata, status=status,
            status_mod_time=status_mod_time, sdm_proj_id=sdm_proj_id,
            name=layer.name, epsgcode=layer.epsgcode, lyr_id=layer.get_id(),
            squid=layer.squid, verify=layer.verify, dlocation=layer._dlocation,
            lyrMetadata=layer.lyrMetadata, data_format=layer.data_format,
            gdal_type=layer.gdal_type, valUnits=layer.valUnits,
            nodata_val=layer.nodata_val, min_val=layer.min_val,
            max_val=layer.max_val, mapunits=layer.mapUnits,
            resolution=layer.resolution, bbox=layer.bbox,
            metadata_url=layer.metadata_url,
            parent_metadata_url=layer.parent_metadata_url)
        return prj

# .............................................................................
# Superclass methods overridden
# # .............................................................................
    def set_id(self, lyrid):
        """
        @summary: Sets the database id on the object, and sets the 
                     SDMProjection.map_prefix of the file if it is None.
        @param id: The database id for the object
        """
        super(SDMProjection, self).set_id(lyrid)
        if lyrid is not None:
            self.name = self._earl_jr.create_layername(projId=lyrid)
            self.clear_dlocation()
            self.set_dlocation()
            self.title = '%s Projection %s' % (self.speciesName, str(lyrid))
            self._set_map_prefix()

# ...............................................
    def create_local_dlocation(self):
        """
        @summary: Create data location
        """
        dloc = None
        if self.get_id() is not None:
            dloc = self._earl_jr.create_filename(
                LMFileType.PROJECTION_LAYER, objCode=self.get_id(),
                occsetId=self._occ_layer.get_id(), usr=self._user_id,
                epsg=self._epsg)
        return dloc

# ...............................................
    def get_dlocation(self):
        self.set_dlocation()
        return self._dlocation

# ...............................................
    def set_dlocation(self, dlocation=None):
        """
        @summary: Set the Layer._dlocation attribute if it is None.  Use
            dlocation if provided, otherwise calculate it.
        @note: Does NOT override existing dlocation, use clear_dlocation for that
        """
        # Only set DLocation if it is currently None
        if self._dlocation is None:
            if dlocation is None:
                dlocation = self.create_local_dlocation()
            self._dlocation = dlocation

# ...............................................
    def get_absolute_path(self):
        """
        @summary Gets the absolute path to the species data
        @return Path to species points
        """
        return self._occ_layer.get_absolute_path()

# ...............................................
    def _create_metadata(self, prjScenario, speciesName, algorithm_code,
                              title=None, is_discrete_data=False):
        """
        @summary: Assemble SDMProjection metadata the first time it is created.
        """
        metadata = {}
        keywds = ['SDM', 'potential habitat', speciesName, algorithm_code]
        prjKeywds = prjScenario.scenMetadata[ServiceObject.META_KEYWORDS]
        keywds.extend(prjKeywds)
        # remove duplicates
        keywds = list(set(keywds))

        metadata[ServiceObject.META_KEYWORDS] = keywds
        metadata[ServiceObject.META_DESCRIPTION] = (
            'Modeled habitat for {} projected onto {} datalayers'.format(
                speciesName, prjScenario.name))
        metadata[Raster.META_IS_DISCRETE] = is_discrete_data
        if title is not None:
            metadata[Raster.META_TITLE] = title
        return metadata

# ...............................................
    def _get_defaults_from_inputs(self, lyr_id, occ_layer, algorithm,
                               model_scenario, proj_scenario, name, squid,
                               process_type, bbox, epsgcode, mapunits,
                               resolution, gdal_format):
        """
        @summary: Assemble SDMProjection attributes from process inputs the
            first time it is created.
        """
        user_id = occ_layer.get_user_id()
        if name is None:
            if lyr_id is None:
                lyr_id = ID_PLACEHOLDER
            name = occ_layer._earl_jr.create_layername(projId=lyr_id)
        if squid is None:
            squid = occ_layer.squid
        if bbox is None:
            bbox = proj_scenario.bbox
        if epsgcode is None:
            epsgcode = proj_scenario.epsgcode
        if mapunits is None:
            mapunits = proj_scenario.mapUnits
        if resolution is None:
            resolution = proj_scenario.resolution
        if process_type is None:
            if Algorithms.is_att(algorithm.code):
                process_type = ProcessType.ATT_PROJECT
            else:
                process_type = ProcessType.OM_PROJECT
        is_discrete_data = Algorithms.returns_discrete_output(algorithm.code)
        title = occ_layer._earl_jr.createSDMProjectTitle(
            occ_layer._user_id, occ_layer.display_name, algorithm.code,
            model_scenario.code, proj_scenario.code)
        if gdal_format is None:
            gdal_format = Algorithms.get(algorithm.code).output_format
        return (user_id, name, squid, process_type, bbox, epsgcode, mapunits,
                resolution, is_discrete_data, gdal_format, title)

# .............................................................................
# Public methods
# .............................................................................
    def update_status(self, status, metadata=None,
                     mod_time=gmt().mjd):
        """
        @summary Update status, metadata, mod_time attributes on the
            SDMProjection. 
        @copydoc LmServer.base.service_object.ProcessObject::update_status()
        @copydoc LmServer.base.service_object.ServiceObject::update_mod_time()
        @copydoc LmServer.base.layer._LayerParameters::updateParams()
        """
        ProcessObject.update_status(self, status, mod_time)
        ServiceObject.update_mod_time(self, mod_time)
        _LayerParameters.update_params(self, mod_time, metadata=metadata)

        # If projection will be updated with a successful complete status,
        #     clear the map file so that it can be regenerated
        try:
            if status == JobStatus.COMPLETE:
                self.clear_local_mapfile()
        except:
            pass

# # ...............................................
#     def clearProjectionFiles(self):
#         reqfname = self.getProjRequestFilename()
#         success, _ = self.deleteFile(reqfname)
#         pkgfname = self.get_proj_package_filename()
#         success, _ = self.deleteFile(pkgfname)
#         # metadata files
#         prjfnames = glob.glob(self._dlocation + '*')
#         for fname in prjfnames:
#             success, _ = self.deleteFile(fname)
#         self.clear_dlocation()

# ...............................................
    def clear_output_files(self):
        reqfname = self.getProjRequestFilename()
        success, _ = self.deleteFile(reqfname)
        pkgfname = self.get_proj_package_filename()
        success, _ = self.deleteFile(pkgfname)
        # metadata files
        prjfnames = glob.glob(self._dlocation + '*')
        for fname in prjfnames:
            success, _ = self.deleteFile(fname)
        self.clear_dlocation()

# ...............................................
    def rollback(self, status=JobStatus.GENERAL):
        """
        @summary: Rollback processing
        @todo: remove currtime parameter
        """
        self.update_status(status)
        self.clear_output_files()
        self.clear_local_mapfile()

# ...............................................
    def get_proj_package_filename(self):
        fname = self._earl_jr.create_filename(
            LMFileType.PROJECTION_PACKAGE, objCode=self.get_id(),
            occsetId=self._occ_layer.get_id(), usr=self._user_id,
            epsg=self._epsg)
        return fname

    # ...............................................
    def get_algorithm_parameters_json_filename(self, algorithm):
        """
        @summary: Return a file name for algorithm parameters JSON.  Write if 
                         necessary
        @param algorithm: An algorithm object
        @deprecated: Remove this
        """
        # This is a list of algorithm information that will be used for hashing
        algoInfo = []
        algoInfo.append(algorithm.code)

        algoObj = {
            "algorithm_code" : algorithm.code,
            "parameters" : []
        }

        for param in list(algorithm._parameters.keys()):
            algoObj["parameters"].append(
                {"name" : param,
                 "value" : str(algorithm._parameters[param])})
            algoInfo.append((param, str(algorithm._parameters[param])))

        paramsSet = set(algoInfo)
        paramsHash = md5(str(paramsSet)).hexdigest()

        # TODO: Determine if we should copy this to the workspace or something?
        paramsFname = self._earl_jr.create_filename(
            LMFileType.TMP_JSON, objCode=paramsHash[:16], usr=self.get_user_id())

        # Write if it does not exist
        if not os.path.exists(paramsFname):
            with open(paramsFname, 'w') as paramsOut:
                json.dump(algoObj, paramsOut)

        return paramsFname

# ...............................................
    def clear_local_mapfile(self, scen_code=None):
        """
        @summary: Delete the mapfile containing this layer
        """
        return self._occ_layer.clear_local_mapfile()

# ...............................................
    def set_local_map_filename(self):
        """
        @summary: Find mapfile containing layers for this projection's
            occ_layer.
        """
        self._occ_layer.set_local_map_filename()

# ...............................................
    @property
    def map_filename(self):
        return self._occ_layer.map_filename

    @property
    def map_name(self):
        return self._occ_layer.map_name

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
        # Recompute in case we have a new db ID
        projid = self.get_id()
        if projid is None:
            projid = ID_PLACEHOLDER
        lyrname = self._earl_jr.create_basename(
            LMFileType.PROJECTION_LAYER, objCode=projid, usr=self._user_id,
            epsg=self.epsgcode)
        mapprefix = self._earl_jr.construct_map_prefix_new(
            ftype=LMFileType.SDM_MAP, mapname=self._occ_layer.map_name,
            lyrname=lyrname, usr=self._user_id)
        return mapprefix

# ...............................................
    def _set_map_prefix(self):
        mapprefix = self._create_map_prefix()
        self._map_prefix = mapprefix

# ...............................................
    @property
    def map_prefix(self):
        self._set_map_prefix()
        return self._map_prefix

# ...............................................
    @property
    def map_layername(self):
        lyrname = None
        if self._dbId is not None:
            lyrname = self._earl_jr.create_layername(projId=self._dbId)
        return lyrname
