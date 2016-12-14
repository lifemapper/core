"""
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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

from LmCommon.common.lmconstants import OutputFormat, JobStatus

from LmServer.base.layer import Raster, _LayerParameters
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (LMFileType, ALGORITHM_DATA,
               DEFAULT_PROJECTION_FORMAT, DEFAULT_WMS_FORMAT,
               ID_PLACEHOLDER, LMServiceType, LMServiceModule)
# .........................................................................
class _ProjectionType(_LayerParameters):
# .............................................................................
   """
   """
# .............................................................................
   def __init__(self, occurrenceSet, algorithm, modelScenario, modelMask, 
                projScenario, projMask, 
                status, statusModTime, userId, projectionId):
      """
      @summary Initialize the _ProjectionType class instance
      @param algorithm: Algorithm object for SDM model process
      @param modelScenario: : Scenario (environmental layer inputs) for 
             SDM model process
      @param modelMask: Mask for SDM model process
      @param projScenario: Scenario (environmental layer inputs) for 
             SDM project process
      @param projMask: Mask for SDM project process
      @param status: status of computation
      @param statusModTime: Time stamp in MJD for status modification.
      @param userId: Id for the owner of this projection
      @param projectionId: The projectionId for the database.  
      """
      if status is not None and statusModTime is None:
         statusModTime = mx.DateTime.utc().mjd
      _LayerParameters.__init__(self, -1, statusModTime, userId, 
                                projectionId)
      self._occurrenceSet = occurrenceSet
      self._algorithm = algorithm
      self._modelMask = modelMask
      self._modelScenario = modelScenario
      self._projMask = projMask
      self._projScenario = projScenario
      self._status = status
      self._statusmodtime = statusModTime
      
# .............................................................................
class SDMProjection(_ProjectionType, Raster, ProcessObject):
   """
   The Projection class contains all of the information 
            that openModeller needs to project a model onto a scenario.
   @todo: make Models and Projections consistent for data member access 
          between public/private members, properties, get/set/update
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, algorithm, modelScenario, modelMask, projScenario, projMask, 
                processType=None, status=None, statusModTime=None, 
                userId=None, layerId=None, 
                verify=None, squid=None, metadata={}, dlocation=None, 
                bbox=None, epsgcode=None, 
                gdalType=None, gdalFormat=DEFAULT_PROJECTION_FORMAT,
                mapunits=None, resolution=None, isDiscreteData=None,
                metadataUrl=None):
      """
      @todo: remove userId keyword parameter??
      @summary Constructor for the SDMProjection class
      @param algorithm: Algorithm object for SDM model process
      @param modelScenario: : Scenario (environmental layer inputs) for 
             SDM model process
      @param modelMask: Mask for SDM model process
      @param projScenario: Scenario (environmental layer inputs) for 
             SDM project process
      @param projMask: Mask for SDM project process
      @param status: status of computation
      @param statusModTime: Time stamp in MJD for status modification.
      @param userId: Id for the owner of this projection
      @param layerId: The layerId for the projection and layer tables in db.  

      @param dlocation: absolute filename of the projection raster
      @param status: status of the Projection
      @param statusModTime: time of the latest status modification 
                            in modified julian date format
      @param projectionId: database id of the Projection
      """
      _ProjectionType.__init__(self, algorithm, modelScenario, modelMask, 
                               projScenario, projMask, 
                               status, statusModTime, userId, layerId)         
      ProcessObject.__init__(self, objId=layerId, 
                             processType=processType, parentId=None, 
                             status=status, statusModTime=statusModTime)

      (bbox, epsgcode, mapunits, resolution, isDiscreteData, gdalFormat) = \
         self._getDefaultsFromInputs(projScenario, algorithm, bbox, epsgcode, 
                              mapunits, resolution, isDiscreteData, gdalFormat)
      lyrmetadata = self._createMetadata(metadata)
      Raster.__init__(metadata=lyrmetadata, bbox=bbox, dlocation=dlocation, 
                gdalType=gdalType, gdalFormat=gdalFormat, 
                mapunits=mapunits, resolution=resolution, epsgcode=epsgcode,
                isDiscreteData=isDiscreteData,
                svcObjId=layerId, lyrId=layerId, lyrUserId=userId, 
                verify=verify, squid=squid, modTime=statusModTime, 
                metadataUrl=metadataUrl,
                serviceType=LMServiceType.PROJECTIONS, moduleType=LMServiceModule.SDM)
      self.setId(layerId)
      self.setLocalMapFilename()
      self._setMapPrefix()

# .............................................................................
# Superclass methods overridden
## .............................................................................
   def setId(self, lyrid):
      """
      @summary: Sets the database id on the object, and sets the 
                SDMProjection.mapPrefix of the file if it is None.
      @param id: The database id for the object
      """
      ServiceObject.setId(self, lyrid)
      if lyrid is not None:
         self.name = self._earlJr.createLayername(projId=lyrid)
         if self._dlocation is None:
            filename = self.createLocalDLocation()
            if os.path.exists(filename):
               self._dlocation = filename
         
         self.title = '%s Projection %s' % (self.speciesName, str(lyrid))
         self._setMapPrefix()

# ...............................................
   @property
   def makeflowFilename(self):
      dloc = self.createLocalDLocation(makeflow=True)
      return dloc
   
# ...............................................
   def createLocalDLocation(self, makeflow=False):
      """
      @summary: Create data location
      """
      dloc = None
      if makeflow:
         dloc = self._model.createLocalDLocation(makeflow=True)
      elif self.getId() is not None:
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
      return self._model.getAbsolutePath()

# ...............................................
   def _createMetadata(self, metadata):
      try:
         metadata['keywords']
      except:
         keywds = set(['SDM', 'potential habitat', self.speciesName, 
                       self.algorithmCode])
         keywds = keywds.union(self._projScenario.keywords)
         metadata['keywords'] = keywds
      try:
         metadata['description']
      except:
         metadata['description'] = ('Modeled habitat for {} projected onto {} datalayers'
                           .format(self.speciesName, self._projScenario.name))
      return metadata
   
# ...............................................
   def _getDefaultsFromInputs(self, projScenario, algorithm, bbox, epsgcode, 
                              mapunits, resolution, isDiscreteData, gdalFormat):
      if bbox is None:
         bbox = self._projScenario.bbox
      if epsgcode is None:
         epsgcode = self._projScenario.epsgcode
      if mapunits is None:
         mapunits = self._projScenario.units
      if resolution is None:
         resolution = self._projScenario.resolution
      if isDiscreteData is None:
         isDiscreteData = ALGORITHM_DATA[self._algorithm.code]['isDiscreteOutput']
      if gdalFormat is None:
         gdalFormat = ALGORITHM_DATA[self._algorithm.code]['outputFormat']
      return (bbox, epsgcode, mapunits, resolution, isDiscreteData, gdalFormat)

# .............................................................................
# Public methods
# .............................................................................
   def updateStatus(self, status, stattime=mx.DateTime.utc().mjd):
      """
      @summary Updates status objects on the Projection. 
      @param status: The new job status for the projection
      """
      if status is not None:
         self._status = status
         self._statusmodtime = stattime
         self.modTime = self._statusmodtime
         
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
   def getProjRequestFilename(self, fileExtension=OutputFormat.XML):
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
      fname = self._earlJr.createFilename(LMFileType.PROJECTION_PACKAGE, 
                projId=self.getId(), pth=self.getAbsolutePath(), 
                usr=self._userId, epsg=self._epsg)
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
                     bbox, color, self.SRS, format)
      return wmsUrl   

# ...............................................
   @property
   def projScenario(self):
      return self._projScenario

   @property
   def projMask(self):
      return self._projMask
   
   @property
   def occurrenceSet(self):
      return self._occurrenceSet

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
   def projScenarioCode(self):
      return self._projScenario.code
   
   @property
   def projInputLayers(self):
      """
      @summary Gets the layers of the projection Scenario
      """
      return self._projScenario.layers

