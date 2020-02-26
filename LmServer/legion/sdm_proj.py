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
    def __init__(self, occurrenceSet, algorithm, modelScenario, modelMask,
                     projScenario, projMask, processType, projMetadata,
                     status, status_mod_time, userId, projectId):
        """Constructor for the _ProjectionType class

        Args:
            * occurrenceSet: OccurrenceLayer object for SDM model process
            * algorithm: Algorithm object for SDM model process
            * modelScenario: : Scenario (environmental layer inputs) for SDM
                model process
            * modelMask: Mask for SDM model process
            * projScenario: Scenario (environmental layer inputs) for SDM
                project process
            * projMask: Mask for SDM project process
            * processType: LmCommon.common.lmconstants.ProcessType for
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
            self, userId, paramId=projectId, matrixIndex=-1,
            metadata=projMetadata, mod_time=status_mod_time)
        ProcessObject.__init__(
            self, objId=projectId, processType=processType, status=status,
            status_mod_time=status_mod_time)
        self._occurrenceSet = occurrenceSet
        self._algorithm = algorithm
        self._modelMask = modelMask
        self._modelScenario = modelScenario
        self._projMask = projMask
        self._projScenario = projScenario

# ...............................................
# Projection Input Data Object attributes:
# OccurrenceSet, Algorithm, ModelMask, ModelScenario, ProjMask, ProjScenario
# ...............................................
    def getOccurrenceSetId(self):
        return self._occurrenceSet.get_id()

    def dumpAlgorithmParametersAsString(self):
        return self._algorithm.dumpAlgParameters()

    def getModelMaskId(self):
        try:
            return self._modelMask.get_id()
        except:
            return None

    def getModelScenarioId(self):
        return self._modelScenario.get_id()

    def getProjMaskId(self):
        try:
            return self._projMask.get_id()
        except:
            return None

    def getProjScenarioId(self):
        return self._projScenario.get_id()

    def isOpenModeller(self):
        return Algorithms.isOpenModeller(self._algorithm.code)

    def isATT(self):
        return Algorithms.isATT(self._algorithm.code)

    @property
    def displayName(self):
        return self._occurrenceSet.displayName

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
    def occurrenceSet(self):
        return self._occurrenceSet

    @property
    def status(self):
        return self._status

    @property
    def status_mod_time(self):
        return self._statusmodtime

    @property
    def speciesName(self):
        return self._occurrenceSet.displayName

    @property
    def algorithmCode(self):
        return self._algorithm.code

    @property
    def modelScenario(self):
        return self._modelScenario

    @property
    def modelScenarioCode(self):
        return self._modelScenario.code

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
    def __init__(self, occurrenceSet, algorithm, modelScenario, projScenario,
                 processType=None, modelMask=None, projMask=None,
                 projMetadata={}, status=None, status_mod_time=None,
                 sdmProjectionId=None, name=None, epsgcode=None, lyrId=None,
                 squid=None, verify=None, dlocation=None, lyrMetadata={},
                 dataFormat=None, gdalType=None, valUnits=None, nodataVal=None,
                 minVal=None, maxVal=None, mapunits=None, resolution=None,
                 bbox=None, metadataUrl=None, parentMetadataUrl=None):
        """
        @summary Constructor for the SDMProjection class
        @copydoc LmServer.legion.sdmproj._ProjectionType::__init__()
        @copydoc LmServer.base.layer._Layer::__init__()
        """
        (userId, name, squid, processType, bbox, epsg, mapunits, resolution,
         isDiscreteData, dataFormat, title) = self._getDefaultsFromInputs(
             lyrId, occurrenceSet, algorithm, modelScenario, projScenario,
             name, squid, processType, bbox, epsgcode, mapunits, resolution,
             dataFormat)
        _ProjectionType.__init__(
            self, occurrenceSet, algorithm, modelScenario, modelMask,
            projScenario, projMask, processType, projMetadata, status,
            status_mod_time, userId, sdmProjectionId)
        if not lyrMetadata:
            lyrMetadata = self._createMetadata(
                projScenario, occurrenceSet.displayName, algorithm.code,
                title=title, isDiscreteData=isDiscreteData)
        Raster.__init__(
            self, name, userId, epsg, lyrId=lyrId, squid=squid, verify=verify,
            dlocation=dlocation, metadata=lyrMetadata, dataFormat=dataFormat,
            gdalType=gdalType, valUnits=valUnits, nodataVal=nodataVal,
            minVal=minVal, maxVal=maxVal, mapunits=mapunits,
            resolution=resolution, bbox=bbox, svcObjId=lyrId,
            serviceType=LMServiceType.PROJECTIONS, metadataUrl=metadataUrl,
            parentMetadataUrl=parentMetadataUrl, mod_time=status_mod_time)
        # TODO: clean this up.  Do not allow layer to calculate dlocation,
        #         subclass SDMProjection must override
        self.set_id(lyrId)
        self.set_local_map_filename()
        self._set_map_prefix()

# .............................................................................
# another Constructor
# # .............................................................................
    @classmethod
    def init_from_parts(cls, occurrenceSet, algorithm, modelScenario,
                      projScenario, layer, processType=None, modelMask=None,
                      projMask=None, projMetadata={}, status=None,
                      status_mod_time=None, sdmProjectionId=None):
        prj = SDMProjection(
            occurrenceSet, algorithm, modelScenario, projScenario,
            processType=processType, modelMask=modelMask, projMask=projMask,
            projMetadata=projMetadata, status=status,
            status_mod_time=status_mod_time, sdmProjectionId=sdmProjectionId,
            name=layer.name, epsgcode=layer.epsgcode, lyrId=layer.get_id(),
            squid=layer.squid, verify=layer.verify, dlocation=layer._dlocation,
            lyrMetadata=layer.lyrMetadata, dataFormat=layer.dataFormat,
            gdalType=layer.gdalType, valUnits=layer.valUnits,
            nodataVal=layer.nodataVal, minVal=layer.minVal,
            maxVal=layer.maxVal, mapunits=layer.mapUnits,
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
            self.clearDLocation()
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
            dloc = self._earl_jr.createFilename(
                LMFileType.PROJECTION_LAYER, objCode=self.get_id(),
                occsetId=self._occurrenceSet.get_id(), usr=self._userId,
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
        @note: Does NOT override existing dlocation, use clearDLocation for that
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
        return self._occurrenceSet.get_absolute_path()

# ...............................................
    def _createMetadata(self, prjScenario, speciesName, algorithmCode,
                              title=None, isDiscreteData=False):
        """
        @summary: Assemble SDMProjection metadata the first time it is created.
        """
        metadata = {}
        keywds = ['SDM', 'potential habitat', speciesName, algorithmCode]
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
    def _getDefaultsFromInputs(self, lyrId, occurrenceSet, algorithm,
                               modelScenario, projScenario, name, squid,
                               processType, bbox, epsgcode, mapunits,
                               resolution, gdalFormat):
        """
        @summary: Assemble SDMProjection attributes from process inputs the
            first time it is created.
        """
        userId = occurrenceSet.getUserId()
        if name is None:
            if lyrId is None:
                lyrId = ID_PLACEHOLDER
            name = occurrenceSet._earl_jr.createLayername(projId=lyrId)
        if squid is None:
            squid = occurrenceSet.squid
        if bbox is None:
            bbox = projScenario.bbox
        if epsgcode is None:
            epsgcode = projScenario.epsgcode
        if mapunits is None:
            mapunits = projScenario.mapUnits
        if resolution is None:
            resolution = projScenario.resolution
        if processType is None:
            if Algorithms.isATT(algorithm.code):
                processType = ProcessType.ATT_PROJECT
            else:
                processType = ProcessType.OM_PROJECT
        isDiscreteData = Algorithms.returnsDiscreteOutput(algorithm.code)
        title = occurrenceSet._earl_jr.createSDMProjectTitle(
            occurrenceSet._userId, occurrenceSet.displayName, algorithm.code,
            modelScenario.code, projScenario.code)
        if gdalFormat is None:
            gdalFormat = Algorithms.get(algorithm.code).outputFormat
        return (userId, name, squid, processType, bbox, epsgcode, mapunits,
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
                self.clearLocalMapfile()
        except:
            pass

# ...............................................
    def clearProjectionFiles(self):
        reqfname = self.getProjRequestFilename()
        success, msg = self.deleteFile(reqfname)
        pkgfname = self.getProjPackageFilename()
        success, msg = self.deleteFile(pkgfname)
        # metadata files
        prjfnames = glob.glob(self._dlocation + '*')
        for fname in prjfnames:
            success, msg = self.deleteFile(fname)
        self.clearDLocation()

# ...............................................
    def clearOutputFiles(self):
        reqfname = self.getProjRequestFilename()
        success, msg = self.deleteFile(reqfname)
        pkgfname = self.getProjPackageFilename()
        success, msg = self.deleteFile(pkgfname)
        # metadata files
        prjfnames = glob.glob(self._dlocation + '*')
        for fname in prjfnames:
            success, msg = self.deleteFile(fname)
        self.clearDLocation()

# ...............................................
    def rollback(self, status=JobStatus.GENERAL):
        """
        @summary: Rollback processing
        @todo: remove currtime parameter
        """
        self.update_status(status)
        self.clearOutputFiles()
        self.clearLocalMapfile()

# ...............................................
    def getProjPackageFilename(self):
        fname = self._earl_jr.createFilename(
            LMFileType.PROJECTION_PACKAGE, objCode=self.get_id(),
            occsetId=self._occurrenceSet.get_id(), usr=self._userId,
            epsg=self._epsg)
        return fname

    # ...............................................
    def getAlgorithmParametersJsonFilename(self, algorithm):
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
            "algorithmCode" : algorithm.code,
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
        paramsFname = self._earl_jr.createFilename(
            LMFileType.TMP_JSON, objCode=paramsHash[:16], usr=self.getUserId())

        # Write if it does not exist
        if not os.path.exists(paramsFname):
            with open(paramsFname, 'w') as paramsOut:
                json.dump(algoObj, paramsOut)

        return paramsFname

# ...............................................
    def clearLocalMapfile(self, scencode=None):
        """
        @summary: Delete the mapfile containing this layer
        """
        return self._occurrenceSet.clearLocalMapfile()

# ...............................................
    def set_local_map_filename(self):
        """
        @summary: Find mapfile containing layers for this projection's
            occurrenceSet.
        """
        self._occurrenceSet.set_local_map_filename()

# ...............................................
    @property
    def mapFilename(self):
        return self._occurrenceSet.mapFilename

    @property
    def mapName(self):
        return self._occurrenceSet.mapName

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
            LMFileType.PROJECTION_LAYER, objCode=projid, usr=self._userId,
            epsg=self.epsgcode)
        mapprefix = self._earl_jr.constructMapPrefixNew(
            ftype=LMFileType.SDM_MAP, mapname=self._occurrenceSet.mapName,
            lyrname=lyrname, usr=self._userId)
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
