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
    def __init__(self, occ_layer, algorithm, model_scenario, modelMask,
                     projScenario, projMask, process_type, projMetadata,
                     status, status_mod_time, user_id, projectId):
        """Constructor for the _ProjectionType class

        Args:
            * occ_layer: OccurrenceLayer object for SDM model process
            * algorithm: Algorithm object for SDM model process
            * model_scenario: : Scenario (environmental layer inputs) for SDM
                model process
            * modelMask: Mask for SDM model process
            * projScenario: Scenario (environmental layer inputs) for SDM
                project process
            * projMask: Mask for SDM project process
            * process_type: LmCommon.common.lmconstants.ProcessType for
                computation
            * projMetadata: Metadata for this projection 

        Note:
            * projMask and mdlMask are currently input data layer for the only
                mask method.  This is set in the boom configuration file in the 
                `PREPROCESSING SDM_MASK` section, with `CODE` set to 
                `hull_region_intersect`, `buffer` to some value in the mapunits
                of the occurrence layer, and `region` with name of a layer
                owned bythe boom user. 

        See:
            * LmServer.base.layer._LayerParameters::__init__()
            * LmServer.base.service_object.ProcessObject::__init__()

        Todo:
            * projMask and mdlMask should be dictionaries with masking method,
                input data and parameters
        """
        if status is not None and status_mod_time is None:
            status_mod_time = gmt().mjd

        _LayerParameters.__init__(
            self, user_id, paramId=projectId, matrixIndex=-1,
            metadata=projMetadata, mod_time=status_mod_time)
        ProcessObject.__init__(
            self, obj_id=projectId, process_type=process_type, status=status,
            status_mod_time=status_mod_time)
        self._occ_layer = occ_layer
        self._algorithm = algorithm
        self._modelMask = modelMask
        self._model_scenario = model_scenario
        self._projMask = projMask
        self._projScenario = projScenario

# ...............................................
# Projection Input Data Object attributes:
# OccurrenceSet, Algorithm, ModelMask, ModelScenario, ProjMask, ProjScenario
# ...............................................
    def getOccurrenceSetId(self):
        return self._occ_layer.get_id()

    def dumpAlgorithmParametersAsString(self):
        return self._algorithm.dumpAlgParameters()

    def getModelMaskId(self):
        try:
            return self._modelMask.get_id()
        except:
            return None

    def getModelScenarioId(self):
        return self._model_scenario.get_id()

    def getProjMaskId(self):
        try:
            return self._projMask.get_id()
        except:
            return None

    def getProjScenarioId(self):
        return self._projScenario.get_id()

    def isOpenModeller(self):
        return Algorithms.is_openModeller(self._algorithm.code)

    def isATT(self):
        return Algorithms.is_att(self._algorithm.code)

    @property
    def display_name(self):
        return self._occ_layer.display_name

    @property
    def projScenario(self):
        return self._projScenario

    @property
    def projScenarioCode(self):
        return self._projScenario.code

    @property
    def projMask(self):
        return self._projMask

    def setProjMask(self, lyr):
        self._projMask = lyr

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
    def speciesName(self):
        return self._occ_layer.display_name

    @property
    def algorithm_code(self):
        return self._algorithm.code

    @property
    def model_scenario(self):
        return self._model_scenario

    @property
    def modelScenarioCode(self):
        return self._model_scenario.code

    @property
    def modelMask(self):
        return self._modelMask

    def setModelMask(self, lyr):
        self._modelMask = lyr

    @property
    def projInputLayers(self):
        """
        @summary Gets the layers of the projection Scenario
        """
        return self._projScenario.layers


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
    def __init__(self, occ_layer, algorithm, model_scenario, projScenario,
                 process_type=None, modelMask=None, projMask=None,
                 projMetadata={}, status=None, status_mod_time=None,
                 sdmProjectionId=None, name=None, epsgcode=None, lyr_id=None,
                 squid=None, verify=None, dlocation=None, lyrMetadata={},
                 data_format=None, gdalType=None, valUnits=None, nodata_val=None,
                 min_val=None, max_val=None, mapunits=None, resolution=None,
                 bbox=None, metadataUrl=None, parentMetadataUrl=None):
        """
        @summary Constructor for the SDMProjection class
        @copydoc LmServer.legion.sdmproj._ProjectionType::__init__()
        @copydoc LmServer.base.layer._Layer::__init__()
        """
        (user_id, name, squid, process_type, bbox, epsg, mapunits, resolution,
         isDiscreteData, data_format, title) = self._get_defaults_from_inputs(
             lyr_id, occ_layer, algorithm, model_scenario, projScenario,
             name, squid, process_type, bbox, epsgcode, mapunits, resolution,
             data_format)
        _ProjectionType.__init__(
            self, occ_layer, algorithm, model_scenario, modelMask,
            projScenario, projMask, process_type, projMetadata, status,
            status_mod_time, user_id, sdmProjectionId)
        if not lyrMetadata:
            lyrMetadata = self._create_metadata(
                projScenario, occ_layer.display_name, algorithm.code,
                title=title, isDiscreteData=isDiscreteData)
        Raster.__init__(
            self, name, user_id, epsg, lyr_id=lyr_id, squid=squid, verify=verify,
            dlocation=dlocation, metadata=lyrMetadata, data_format=data_format,
            gdalType=gdalType, valUnits=valUnits, nodata_val=nodata_val,
            min_val=min_val, max_val=max_val, mapunits=mapunits,
            resolution=resolution, bbox=bbox, svcObjId=lyr_id,
            serviceType=LMServiceType.PROJECTIONS, metadataUrl=metadataUrl,
            parentMetadataUrl=parentMetadataUrl, mod_time=status_mod_time)
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
                      projScenario, layer, process_type=None, modelMask=None,
                      projMask=None, projMetadata={}, status=None,
                      status_mod_time=None, sdmProjectionId=None):
        prj = SDMProjection(
            occ_layer, algorithm, model_scenario, projScenario,
            process_type=process_type, modelMask=modelMask, projMask=projMask,
            projMetadata=projMetadata, status=status,
            status_mod_time=status_mod_time, sdmProjectionId=sdmProjectionId,
            name=layer.name, epsgcode=layer.epsgcode, lyr_id=layer.get_id(),
            squid=layer.squid, verify=layer.verify, dlocation=layer._dlocation,
            lyrMetadata=layer.lyrMetadata, data_format=layer.data_format,
            gdalType=layer.gdalType, valUnits=layer.valUnits,
            nodata_val=layer.nodata_val, min_val=layer.min_val,
            max_val=layer.max_val, mapunits=layer.mapUnits,
            resolution=layer.resolution, bbox=layer.bbox,
            metadataUrl=layer.metadataUrl,
            parentMetadataUrl=layer.parentMetadataUrl)
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
            self.name = self._earl_jr.createLayername(projId=lyrid)
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
                              title=None, isDiscreteData=False):
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
        metadata[Raster.META_IS_DISCRETE] = isDiscreteData
        if title is not None:
            metadata[Raster.META_TITLE] = title
        return metadata

# ...............................................
    def _get_defaults_from_inputs(self, lyr_id, occ_layer, algorithm,
                               model_scenario, projScenario, name, squid,
                               process_type, bbox, epsgcode, mapunits,
                               resolution, gdalFormat):
        """
        @summary: Assemble SDMProjection attributes from process inputs the
            first time it is created.
        """
        user_id = occ_layer.getUserId()
        if name is None:
            if lyr_id is None:
                lyr_id = ID_PLACEHOLDER
            name = occ_layer._earl_jr.createLayername(projId=lyr_id)
        if squid is None:
            squid = occ_layer.squid
        if bbox is None:
            bbox = projScenario.bbox
        if epsgcode is None:
            epsgcode = projScenario.epsgcode
        if mapunits is None:
            mapunits = projScenario.mapUnits
        if resolution is None:
            resolution = projScenario.resolution
        if process_type is None:
            if Algorithms.is_att(algorithm.code):
                process_type = ProcessType.ATT_PROJECT
            else:
                process_type = ProcessType.OM_PROJECT
        isDiscreteData = Algorithms.returns_discrete_output(algorithm.code)
        title = occ_layer._earl_jr.createSDMProjectTitle(
            occ_layer._user_id, occ_layer.display_name, algorithm.code,
            model_scenario.code, projScenario.code)
        if gdalFormat is None:
            gdalFormat = Algorithms.get(algorithm.code).outputFormat
        return (user_id, name, squid, process_type, bbox, epsgcode, mapunits,
                resolution, isDiscreteData, gdalFormat, title)

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
            LMFileType.TMP_JSON, objCode=paramsHash[:16], usr=self.getUserId())

        # Write if it does not exist
        if not os.path.exists(paramsFname):
            with open(paramsFname, 'w') as paramsOut:
                json.dump(algoObj, paramsOut)

        return paramsFname

# ...............................................
    def clear_local_mapfile(self, scencode=None):
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
    def mapFilename(self):
        return self._occ_layer.mapFilename

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
        lyrname = self._earl_jr.createBasename(
            LMFileType.PROJECTION_LAYER, objCode=projid, usr=self._user_id,
            epsg=self.epsgcode)
        mapprefix = self._earl_jr.constructMapPrefixNew(
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
    def mapLayername(self):
        lyrname = None
        if self._dbId is not None:
            lyrname = self._earl_jr.createLayername(projId=self._dbId)
        return lyrname
