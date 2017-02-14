"""
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
import glob 
import mx.DateTime
import os

from LmCommon.common.lmconstants import OutputFormat, JobStatus, ProcessType
from LmCommon.common.verify import computeHash

from LmServer.base.layer2 import Raster, _LayerParameters
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (LMFileType, Algorithms,
            DEFAULT_WMS_FORMAT, ID_PLACEHOLDER, LMServiceType, LMServiceModule)
from LmServer.makeflow.cmd import MfRule

# .........................................................................
class _ProjectionType(_LayerParameters, ProcessObject):
# .............................................................................
   """
   """
# .............................................................................
   def __init__(self, occurrenceSet, algorithm, modelScenario, modelMaskId, 
                projScenario, projMaskId, processType, projMetadata,
                status, statusModTime, userId, projectId):
      """
      @summary Initialize the _ProjectionType class instance
      @copydoc LmServer.base.layer2._LayerParameters::__init__()
      @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
      @param occurrenceSet: OccurrenceLayer object for SDM model process
      @param algorithm: Algorithm object for SDM model process
      @param modelScenario: : Scenario (environmental layer inputs) for 
             SDM model process
      @param modelMaskId: Mask for SDM model process
      @param projScenario: Scenario (environmental layer inputs) for 
             SDM project process
      @param projMaskId: Mask for SDM project process
      @param processType: LmCommon.common.lmconstants.ProcessType for computation
      @param projMetadata: Metadata for this projection 
      """
      if status is not None and statusModTime is None:
         statusModTime = mx.DateTime.utc().mjd
         
      _LayerParameters.__init__(self, userId, paramId=projectId, matrixIndex=-1, 
                                metadata=projMetadata, modTime=statusModTime)
      ProcessObject.__init__(self, objId=projectId, 
                             processType=processType, parentId=None, 
                             status=status, statusModTime=statusModTime)
      self._occurrenceSet = occurrenceSet
      self._algorithm = algorithm
      self._modelMaskId = modelMaskId
      self._modelScenario = modelScenario
      self._projMaskId = projMaskId
      self._projScenario = projScenario
      
# ...............................................
# Projection Input Data Object attributes: 
# OccurrenceSet, Algorithm, ModelMask, ModelScenario, ProjMask, ProjScenario 
# ...............................................
   def getOccurrenceSetId(self):
      return self._occurrenceSet.getId()

   def getDisplayName(self):
      return self._occurrenceSet.displayName

   def dumpAlgorithmParametersAsString(self):
      return self._algorithm.dumpAlgParameters()

   def getAlgorithmCode(self):
      return self._algorithm.code
   
   def getModelMaskId(self):
      return self._modelMaskId

   def getModelScenarioId(self):
      return self._modelScenario.getId()

   def getProjMaskId(self):
      return self._projMaskId

   def getProjScenarioId(self):
      return self._projScenario.getId()
   
   def isOpenModeller(self):
      return Algorithms.isOpenModeller(self._algorithm.code)

   def isATT(self):
      return Algorithms.isATT(self._algorithm.code)

# .............................................................................
class SDMProjection(_ProjectionType, Raster):
   """
   @summary: The SDMProjection class contains all of the information 
   that openModeller or ATT Maxent needs to model and project SDM inputs.
   @todo: make Models and Projections consistent for data member access 
          between public/private members, properties, get/set/update
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, occurrenceSet, algorithm, modelScenario, projScenario, 
                processType=None, modelMaskId=None, projMaskId=None, 
                projMetadata={}, status=None, statusModTime=None, 
                sdmProjectionId=None,
                name=None, epsgcode=None, lyrId=None, squid=None, verify=None, 
                dlocation=None, lyrMetadata={}, dataFormat=None, gdalType=None, 
                valUnits=None, nodataVal=None, minVal=None, maxVal=None, 
                mapunits=None, resolution=None, bbox=None,
                metadataUrl=None, parentMetadataUrl=None):
      """
      @summary Constructor for the SDMProjection class
      @copydoc LmServer.legion.sdmproj._ProjectionType::__init__()
      @copydoc LmServer.base.layer2._Layer::__init__()
      """
      (userId, name, squid, processType, bbox, epsg, mapunits, resolution, 
       isDiscreteData, dataFormat, title) = self._getDefaultsFromInputs(lyrId,
                           occurrenceSet, algorithm, modelScenario, projScenario, 
                           name, squid, processType, bbox, epsgcode, mapunits, 
                           resolution, dataFormat)
      _ProjectionType.__init__(self, occurrenceSet, algorithm, 
                               modelScenario, modelMaskId, 
                               projScenario, projMaskId, processType, 
                               projMetadata,
                               status, statusModTime, userId, sdmProjectionId)
      lyrMetadata = self._createMetadata(lyrMetadata, title=title,
                                         isDiscreteData=isDiscreteData)
      Raster.__init__(self, name, userId, epsg, lyrId=lyrId, 
                squid=squid, verify=verify, dlocation=dlocation, 
                metadata=lyrMetadata, dataFormat=dataFormat, gdalType=gdalType, 
                valUnits=valUnits, nodataVal=nodataVal, minVal=minVal, 
                maxVal=maxVal, mapunits=mapunits, resolution=resolution, 
                bbox=bbox, svcObjId=lyrId, serviceType=LMServiceType.PROJECTIONS, 
                moduleType=LMServiceModule.LM, metadataUrl=metadataUrl, 
                parentMetadataUrl=parentMetadataUrl, modTime=statusModTime)
      self.setId(lyrId)
      self.setLocalMapFilename()
      self._setMapPrefix()
   
# .............................................................................
# another Constructor
## .............................................................................
   @classmethod
   def initFromParts(cls, occurrenceSet, algorithm, modelScenario, projScenario,
                     layer, processType=None, modelMaskId=None, projMaskId=None, 
                     projMetadata={}, status=None, statusModTime=None, 
                     sdmProjectionId=None):
      prj = SDMProjection(occurrenceSet, algorithm, modelScenario, projScenario, 
                          processType=processType, modelMaskId=modelMaskId, 
                          projMaskId=projMaskId, projMetadata=projMetadata, 
                          status=status, statusModTime=statusModTime, 
                          sdmProjectionId=sdmProjectionId,
                          name=layer.name, epsgcode=layer.epsgcode, 
                          lyrId=layer.getId(), squid=layer.squid, 
                          verify=layer.verify, dlocation=layer.getDLocation(), 
                          lyrMetadata=layer.lyrMetadata, 
                          dataFormat=layer.dataFormat, gdalType=layer.gdalType,
                          valUnits=layer.valUnits, nodataVal=layer.nodataVal, 
                          minVal=layer.minVal, maxVal=layer.maxVal, 
                          mapunits=layer.mapUnits, resolution=layer.resolution, 
                          bbox=layer.bbox, metadataUrl=layer.metadataUrl, 
                          parentMetadataUrl=layer.parentMetadataUrl)
      return prj

# .............................................................................
# Superclass methods overridden
## .............................................................................
   def setId(self, lyrid):
      """
      @summary: Sets the database id on the object, and sets the 
                SDMProjection.mapPrefix of the file if it is None.
      @param id: The database id for the object
      """
      super(SDMProjection, self).setId(lyrid)
      if lyrid is not None:
         self.name = self._earlJr.createLayername(projId=lyrid)
         if self._dlocation is None:
            filename = self.createLocalDLocation()
            if os.path.exists(filename):
               self._dlocation = filename
         self.title = '%s Projection %s' % (self.speciesName, str(lyrid))
         self._setMapPrefix()
         
# ...............................................
   def createLocalDLocation(self):
      """
      @summary: Create data location
      """
      dloc = None
      if self.getId() is not None:
         dloc = self._earlJr.createFilename(LMFileType.PROJECTION_LAYER, 
                   projId=self.getId(), pth=self.getAbsolutePath(), 
                   usr=self._userId, epsg=self._epsg)
      return dloc

# ...............................................
   def getAbsolutePath(self):
      """
      @summary Gets the absolute path to the species data
      @return Path to species points
      """
      return self._occurrenceSet.getAbsolutePath()

# ...............................................
   def _createMetadata(self, metadata, title=None, isDiscreteData=False):
      """
      @summary: Assemble SDMProjection metadata the first time it is created.
      """
      try:
         metadata[ServiceObject.META_KEYWORDS]
      except:
         keywds = ['SDM', 'potential habitat', self.speciesName, 
                   self.algorithmCode]
         prjKeywds = self._projScenario.scenMetadata[ServiceObject.META_KEYWORDS]
         keywds.extend(prjKeywds)
         # remove duplicates
         keywds = list(set(keywds))
         metadata[ServiceObject.META_KEYWORDS] = keywds
      try:
         metadata[ServiceObject.META_DESCRIPTION]
      except:
         metadata[ServiceObject.META_DESCRIPTION] = (
                           'Modeled habitat for {} projected onto {} datalayers'
                           .format(self.speciesName, self._projScenario.name))
      try:
         metadata[Raster.META_IS_DISCRETE]
      except:
         metadata[Raster.META_IS_DISCRETE] = isDiscreteData
      try:
         metadata[Raster.META_TITLE]
      except:
         if title is not None:
            metadata[Raster.META_TITLE] = title
      return metadata
   
# ...............................................
   def _getDefaultsFromInputs(self, lyrId, occurrenceSet, algorithm, 
                              modelScenario, projScenario, 
                              name, squid, processType, bbox, epsgcode, 
                              mapunits, resolution, gdalFormat):
      """
      @summary: Assemble SDMProjection attributes from process inputs the first 
                time it is created.
      """
      userId = occurrenceSet.getUserId()
      if name is None:
         if lyrId is None:
            lyrId = ID_PLACEHOLDER
         name = occurrenceSet._earlJr.createLayername(projId=lyrId)
      if squid is None:
         squid = occurrenceSet.squid
      if bbox is None:
         bbox = projScenario.bbox
      if epsgcode is None:
         epsgcode = projScenario.epsgcode
      if mapunits is None:
         mapunits = projScenario.units
      if resolution is None:
         resolution = projScenario.resolution
      if processType is None:
         if Algorithms.isATT(algorithm.code):
#          if algorithm.code == 'ATT_MAXENT':
            processType = ProcessType.ATT_PROJECT
         else:
            processType = ProcessType.OM_PROJECT
#       isDiscreteData = ALGORITHM_DATA[algorithm.code]['isDiscreteOutput']
      isDiscreteData = Algorithms.returnsDiscreteOutput(algorithm.code)
      title = occurrenceSet._earlJr.createSDMProjectTitle(
                        occurrenceSet._userId, occurrenceSet.displayName, 
                        algorithm.code, modelScenario.code, projScenario.code)
      if gdalFormat is None:
#          gdalFormat = ALGORITHM_DATA[algorithm.code]['outputFormat']
         gdalFormat = Algorithms.get(algorithm.code).outputFormat
      return (userId, name, squid, processType, bbox, epsgcode, mapunits, 
              resolution, isDiscreteData, gdalFormat, title)

# .............................................................................
# Public methods
# .............................................................................
   def updateStatus(self, status, metadata=None, modTime=None):
      """
      @summary Update status, metadata, modTime attributes on the SDMProjection. 
      @copydoc LmServer.base.serviceobject2.ProcessObject::updateStatus()
      @copydoc LmServer.base.serviceobject2.ServiceObject::updateModtime()
      @copydoc LmServer.base.layer2._LayerParameters::updateParams()
      """
      ProcessObject.updateStatus(self, status, modTime=modTime)
      ServiceObject.updateModtime(self, modTime=modTime)
      _LayerParameters.updateParams(self, metadata=metadata, modTime=modTime)
         
# ...............................................
   def clearProjectionFiles(self):
      reqfname = self.getProjRequestFilename()
      success, msg = self._deleteFile(reqfname)
      pkgfname = self.getProjPackageFilename()
      success, msg = self._deleteFile(pkgfname)
      # metadata files
      prjfnames = glob.glob(self._dlocation+'*')
      for fname in prjfnames:
         success, msg = self._deleteFile(fname)
      self.clearDLocation()
      
# ...............................................
   def rollback(self, status=JobStatus.GENERAL):
      """
      @summary: Rollback processing
      @todo: remove currtime parameter
      """
      self.updateStatus(status)
      self.clearProjectionFiles()
      self.clearLocalMapfile()

# ...............................................
   def getProjRequestFilename(self):
      """
      @summary Return the request filename including absolute path for the 
               given projection id using the given occurrenceSet id
      """
      fname = self._earlJr.createFilename(LMFileType.PROJECTION_REQUEST, 
                projId=self.getId(), pth=self.getAbsolutePath(), 
                usr=self._userId, epsg=self._epsg)
      return fname

# ...............................................
   def getProjPackageFilename(self):
      LMFileType.MODEL_RESULT, LMFileType.MODEL_ATT_RESULT
      fname = self._earlJr.createFilename(LMFileType.PROJECTION_PACKAGE, 
                projId=self.getId(), pth=self.getAbsolutePath(), 
                usr=self._userId, epsg=self._epsg)
      return fname
   
#          dloc = self.occurrenceSet.createLocalDLocation(makeflow=True)
#       else:
#          if self._algorithm.code == 'ATT_MAXENT':
#             ftype = LMFileType.MODEL_ATT_RESULT
#          else:
#             ftype = LMFileType.MODEL_RESULT
#          dloc = self._earlJr.createFilename(ftype, modelId=self.getId(), 
#                            pth=self.getAbsolutePath(), usr=self._userId, 
#                            epsg=self.occurrenceSet.epsgcode)
#       return dloc


# ...............................................
   def getModelTarget(self):
      """
      @summary: Return unique code for the model's parameters.
      """
      uniqueCombo = (self.getUserId(), self.getOccurrenceSetId(), 
                     self.algorithmCode(), self.dumpAlgorithmParametersAsString(),
                     self.getModelScenarioId(), self.getModelMaskId())
      modelCode = computeHash(content=uniqueCombo)
      return modelCode

# ...............................................
   def getModelFilename(self, isResult=True):
      """
      @summary: Return filename for the model for this projection.
      """
      if isResult is True:
         if self.isATT() == 'ATT_MAXENT':
            ftype = LMFileType.MODEL_ATT_RESULT
         else:
            ftype = LMFileType.MODEL_RESULT
      else:
         ftype = LMFileType.MODEL_REQUEST
      modelCode = self.getModelTarget()
      fname = self._earlJr.createFilename(ftype, modelId=modelCode, 
                                          pth=self.getAbsolutePath(), 
                                          usr=self._userId, 
                                          epsg=self._epsg)
      return fname

# # ...............................................
#    def getProjLayerFilename(self):
#       fname = self._earlJr.createFilename(LMFileType.PROJECTION_LAYER, 
#                 projId=self.getId(), pth=self.getAbsolutePath(), 
#                 usr=self._userId, epsg=self._epsg)
#       return fname

# ...............................................
   def writeProjection(self, rasterData, fname=None, srs=None, epsgcode=None, 
                       fileExtension=OutputFormat.GTIFF):
      """
      @note: Overrides Raster.getAbsolutePath
      @summary Gets the absolute path to the model
      @param rasterData: Stream or string data to be written
      @param srs: SRS from another geospatial file.  This will accept an SRS 
                  copied from ChangeThinking files, EPSG 2163.  GDAL has trouble
                  with this projection, this will sidestep those problems.
      @param epsgcode: Standard EPSG code from which to create an SRS for use
                  in a GDAL or OGR dataset.
      @return Absolute path to the model
      @todo: use or remove fileExtension param
      """
      if fname is None:
         fname = self.createLocalDLocation()
      self.setDLocation(dlocation=fname)
      self.writeLayer(rasterData, overwrite=True)
      if srs is None:
         srs = self.createSRSFromEPSG(epsgcode)
      if srs is not None:
         self.writeSRS(srs)
         
# ...............................................
   def writePackage(self, pkgData, fname=None):
      """
      @note: Overrides Raster.getAbsolutePath
      @summary Writes package data (job results) for the projection
      @param pkgData: Stream or string data to be written
      """
      if fname is None:
         fname = self.getProjPackageFilename()
      self._readyFilename(fname, overwrite=True)
      try:
         with open(fname, 'w+') as f:
            f.write(pkgData)
      except Exception, e:
         raise LMError('Unable to write projection package to %s' % fname)

# ...............................................
   def clearLocalMapfile(self, scencode=None):
      """
      @summary: Delete the mapfile containing this layer
      """
      return self._occurrenceSet.clearLocalMapfile()

# ...............................................
   def setLocalMapFilename(self):
      """
      @summary: Find mapfile containing layers for this projection's occurrenceSet.
      """
      self._occurrenceSet.setLocalMapFilename()


# ...............................................
   @property
   def mapFilename(self):
      return self._occurrenceSet.mapFilename
   
   @property
   def mapName(self):
      return self._occurrenceSet.mapName
   
# ...............................................
   def _createMapPrefix(self):
      """
      @summary: Construct the endpoint of a Lifemapper WMS URL for 
                this object.
      @note: Uses the metatadataUrl for this object, plus 'ogc' format, 
             map=<mapname>, and layers=<layername> key/value pairs.  
      @note: If the object has not yet been inserted into the database, a 
             placeholder is used until replacement after database insertion.
      """
      # Recompute in case we have a new db ID 
      if self.getId() is None:
         projid = ID_PLACEHOLDER
      else:
         projid = self.getId()
      mapprefix = self._earlJr.constructMapPrefix(ftype=LMFileType.SDM_MAP, 
                     mapname=self.mapName, projId=projid, usr=self._userId)
      return mapprefix

# ...............................................
   def _setMapPrefix(self):
      mapprefix = self._createMapPrefix()
      self._mapPrefix = mapprefix
      
# ...............................................
   @property
   def mapPrefix(self):
      self._setMapPrefix()
      return self._mapPrefix

# ...............................................
   @property
   def mapLayername(self):
      lyrname = None
      if self._dbId is not None:
         lyrname = self._earlJr.createLayername(projId=self._dbId)
      return lyrname 

# ...............................................
   def getWMSRequest(self, width, height, bbox, color=None, format=DEFAULT_WMS_FORMAT):
      """
      @summary Return a GET query for the Lifemapper WMS GetMap request
      @param color: color in hex format RRGGBB or predefined palette 
             name. Color is applied only to Occurrences or Projection. Valid 
             palette names: 'gray', 'red', 'green', 'blue', 'safe', 'pretty', 
             'bluered', 'bluegreen', 'greenred'. 
      @param format: (optional) image file format, default is 'image/png'
      """
      wmsUrl = self._earlJr.constructLMMapRequest(self.mapPrefix, width, height, 
                     bbox, color, self.getSRSAsString(), format)
      return wmsUrl   

# ...............................................
   @property
   def projScenario(self):
      return self._projScenario

   @property
   def projScenarioCode(self):
      return self._projScenario.code

   @property
   def projMask(self):
      return self._projMask
   
   @property
   def occurrenceSet(self):
      return self._occurrenceSet

   @property
   def displayName(self):
      return self._occurrenceSet.displayName

   @property
   def status(self):
      return self._status

   @property
   def statusModTime(self):
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
   
   @property
   def projInputLayers(self):
      """
      @summary Gets the layers of the projection Scenario
      """
      return self._projScenario.layers

#    # .............................................................................
#    def _computeModel(self):
#       """
#       @summary: Generate a command to create a SDM model ruleset for this projection
#       """
#       # model depends on occurrenceSet
#       #occRule = self._occurrenceSet.compute()
#       occSetFn = self._occurrenceSet.getDLocation()
#       # model input - XML request for model generation
#       xmlRequestFname = self.getModelFilename(isResult=False)
#       dataPath, fname = os.path.split(xmlRequestFname)
#       
#       if self.isATT() == 'ATT_MAXENT':
#          ptype = ProcessType.ATT_MODEL
#       else:
#          ptype = ProcessType.OM_MODEL
#          
#       name = '{}-{}'.format(ptype, self.getModelTarget())
#       statusTarget = "{}.status".format(name)
# 
#       options = {'-n' : name,
#                  '-o' : dataPath,
#                  '-l' : '{}.log'.format(name),
#                  '-s' : statusTarget }
#       # Join arguments
#       args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])
#    
#       cmdArguments = [os.getenv('PYTHON'), ProcessType.getJobRunner(ptype), 
#                       xmlRequestFname, args]
#       cmd = ' '.join(cmdArguments)
#       rule = MfRule(cmd, [statusTarget], dependencies=[occSetFn])
#       
#       return rule

# # ...............................................
#    def compute(self):
#       """
#       @summary: Generate a command to create a SDM projection
#       """
#       # projection depends on model
#       modelRule = self._computeModel()
#       
#       xmlRequestFname = self.getProjRequestFilename()
#       # projection output
#       dataPath, fname = os.path.split(xmlRequestFname)
#       name = '{}-{}'.format(self.processType, self.getId())
#       statusTarget = "{}.status".format(name)
#       
#       options = {'-n' : name,
#                  '-o' : dataPath,
#                  '-l' : '{}.log'.format(name),
#                  '-s' : statusTarget }   
#       # Join arguments
#       args = ' '.join(['{opt} {val}'.format(opt=o, val=v) for o, v in options.iteritems()])
#       
#       cmdArguments = [os.getenv('PYTHON'), 
#                       ProcessType.getJobRunner(self.processType), 
#                       xmlRequestFname, args]
#       cmd = ' '.join(cmdArguments)
#       rule = MfRule(cmd, [statusTarget], dependencies=[modelRule])
#       
#       return rule

   # .............................................................................
   def _computeMyModel(self):
      """
      @summary: Generate a command to create a SDM model ruleset for this projection
      """
      rules = []
      # Output
      # TODO: Make sure this file is deleted on rollback  
      rulesetFname = self.getModelFilename(isResult=True)
      if not os.path.exists(rulesetFname):         
         if self.isATT() == 'ATT_MAXENT':
            ptype = ProcessType.ATT_MODEL
         else:
            ptype = ProcessType.OM_MODEL
            
         occRules = self._occurrenceSet.computeMe()
         rules.extend(occRules)

         occSetFname = self._occurrenceSet.getDLocation()
         xmlRequestFname = self.getModelFilename(isResult=False)
         outPath, fname = os.path.split(xmlRequestFname)
         name = '{}-{}'.format(ptype, self.getModelTarget())
   
         options = {'-n' : name,
                    '-p' : ptype,
                    '-o' : outPath,
                    '-l' : '{}.log'.format(name) }
         # Join arguments
         args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])
      
         cmdArguments = [os.getenv('PYTHON'), ProcessType.getJobRunner(ptype), 
                         xmlRequestFname, args]
         cmd = ' '.join(cmdArguments)
         rules.append(MfRule(cmd, [rulesetFname], dependencies=[occSetFname]))
      
      return rules

   # ......................................
   def computeMe(self):
      """
      """
      rules = []
      
      if JobStatus.waiting(self.status):
         # ................................
         # Model dependency
         modelRules = self._computeMyModel()
         rules.extend(modelRules)
         modelFname = self.getModelFilename(isResult=True)
         
         # ................................
         # Projection request file dependency
         requestFname = self.getProjRequestFilename()
         outPath, fname = os.path.split(requestFname)
         # Partial projection request file
         # TODO: Write the partial request (all but ruleset)
         partialRequestFname = "{0}.part".format(requestFname)
         self.writePartialProjectionRequest(partialRequestFname)
         # TODO: Add the projection request tool, this points to
         #       makeProjectionRequest.py in LmCompute/tools/single/ 
         ptype = ProcessType.PROJECT_REQUEST
         requestCmdArgs = [os.getenv('PYTHON'),
                          ProcessType.getJobRunner(ptype),
                          partialRequestFname,
                          modelFname,
                          requestFname ]
         requestCmd = ' '.join(requestCmdArgs)
         
         requestRule = MfRule(requestCmd, [requestFname], 
                              dependencies=[modelFname, partialRequestFname])
         rules.append(requestRule)
         
         # ................................
         # Projection rule
         # TODO: We may need to move this to the correct location
         tiffTarget = self.getDLocation()
         name = '{}-{}'.format(self.processType, self.getId())         
         options = {'-n' : name,
                    '-p' : self.processType,
                    '-o' : outPath,
                    '-l' : '{}.log'.format(name) }   
         # Join arguments
         args = ' '.join(['{opt} {val}'.format(opt=o, val=v) for o, v in options.iteritems()])
         
         cmdArguments = [os.getenv('PYTHON'), 
                         ProcessType.getJobRunner(self.processType), 
                         requestFname, args]
         prjCmd = ' '.join(cmdArguments)
         
         prjRule = MfRule(prjCmd, [tiffTarget], dependencies=[requestFname])
         rules.append(prjRule)
      return rules
   