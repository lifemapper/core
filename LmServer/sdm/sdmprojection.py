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
   def __init__(self, model, scenario, mask, priority, status, statusModTime, 
                userId, projectionId):
      """
      @summary Initialize the _ProjectionType class instance
      @param model: Model to be projected
      @param scenario: Scenario to project upon
      @param mask: EnvironmentalLayer used as a boolean mask to limit the 
                   geographic area projected onto. 
      @param priority: Run priority of the Projection
      @param modTime: Time stamp for creation or modification.
      @param userId: Id for the owner of this layer type
      @param projectionId: The projectionId for the database.  
      """
      if status is not None and statusModTime is None:
         statusModTime = mx.DateTime.utc().mjd
      _LayerParameters.__init__(self, -1, statusModTime, userId, 
                                projectionId)
      self._model = model
      self._scenario = scenario
      self._status = status
      self._statusmodtime = statusModTime      
      self._mask = mask
#       if mask is None and len(scenario.layers) > 0:
#          self._mask = scenario.layers[0]
      if priority is not None:
         self.priority = priority
      else:
         self.priority = self._model.priority


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
   def __init__(self, model, scenario, mask=None, priority=None, 
                dlocation=None, status=None, statusModTime=None, 
                bbox=None, epsgcode=None, 
                gdalType=None, gdalFormat=DEFAULT_PROJECTION_FORMAT,
                mapunits=None, resolution=None, isDiscreteData=None,
                #TODO: remove userId keyword parameter, not needed
                userId=None, projectionId=None, verify=None, squid=None,
                createTime=None, metadataUrl=None):
      """
      @summary Constructor for the Projection class
      @param model: Model to be projected
      @param scenario: Scenario to project upon
      @param mask: (optional) SDMLayer used as a boolean mask
                           to limit the geographic area projected onto. 
      @param priority: (optional) Run priority of the Projection
      @param dlocation: absolute filename of the projection raster
      @param status: status of the Projection
      @param statusModTime: time of the latest status modification 
                            in modified julian date format
      @param isDiscreteData: Superclass Raster.isDiscreteData defaults to 
                             False.  Projections created from models using 
                             GARP BS: isDiscreteData = False
                             BIOCLIM: isDiscreteData = True
      @param projectionId: database id of the Projection
      """
      usr = model.getUserId()
      _ProjectionType.__init__(self, model, scenario, mask, priority, status, 
                               statusModTime, usr, projectionId)
      if bbox is None:
         bbox = self._scenario.bbox
      if epsgcode is None:
         epsgcode = self._scenario.epsgcode
      if mapunits is None:
         mapunits = self._scenario.units
      if resolution is None:
         resolution = self._scenario.resolution
         
      keywds = self._scenario.keywords.union(['habitat model', self.speciesName, 
                                              self.algorithmCode]) 
                      
      desc = ('Predicted habitat for %s projected onto %s datalayers' % 
              (self.speciesName, self._scenario.name))
      if isDiscreteData is None:
         isDiscreteData = ALGORITHM_DATA[model.algorithmCode]['isDiscreteOutput']
      if gdalFormat is None:
         gdalFormat = ALGORITHM_DATA[model.algorithmCode]['outputFormat']
      ProcessObject.__init__(self, objId=projectionId, parentId=None, 
                             status=status, statusModTime=statusModTime)
      Raster.__init__(self, bbox=bbox, verify=verify, squid=squid,
            startDate=scenario.startDate, endDate=scenario.endDate, 
            mapunits=mapunits, resolution=resolution, epsgcode=epsgcode, 
            dlocation=dlocation, gdalType=gdalType, gdalFormat=gdalFormat, 
            isDiscreteData=isDiscreteData, keywords=keywds, description=desc, 
            svcObjId=projectionId, lyrId=projectionId, lyrUserId=usr, 
            createTime=createTime, modTime=statusModTime, metadataUrl=metadataUrl, 
            serviceType=LMServiceType.PROJECTIONS, moduleType=LMServiceModule.SDM)
      self.setId(projectionId)
      self.setLocalMapFilename()
      self._setMapPrefix()

# .............................................................................
# Superclass methods overridden
## .............................................................................
   def setId(self, id):
      """
      @summary: Sets the database id on the object, and sets the 
                SDMProjection.mapPrefix of the file if it is None.
      @param id: The database id for the object
      """
      ServiceObject.setId(self, id)
      if id is not None:
         self.name = self._earlJr.createLayername(projId=id)
         if self._dlocation is None:
            filename = self.createLocalDLocation()
            if os.path.exists(filename):
               self._dlocation = filename
         
         self.title = '%s Projection %s' % (self.speciesName, str(id))
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

# .............................................................................
# Public methods
# .............................................................................
   def update(self, priority=None, status=None, 
              projectionId=None):
      """
      @summary Updates mutable objects on the Projection. Object attributes to 
               be updated are populated, those which remain the same are None.
      @param priority: The new priority for the projection
      @param status: The new job status for the projection
      @param projectionId: The new projection id
      """
      if priority is not None:
         self.priority = priority
      if status is not None:
         self._status = status
         self._statusmodtime = mx.DateTime.utc().mjd
         self.modTime = self._statusmodtime
      if projectionId is not None:
         self.setId(projectionId)
         
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
   def rollback(self, currtime, priority=None, status=JobStatus.GENERAL):
      """
      @summary: Rollback processing
      @param currtime: Time of status/stage modfication
      @todo: remove currtime parameter
      """
      self.update(priority=priority, status=status)
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
      return self._model.occurrenceSet.clearLocalMapfile()

# ...............................................
   def setLocalMapFilename(self):
      """
      @summary: Find mapfile containing layers for this projection's occurrenceSet.
      """
      self._model.occurrenceSet.setLocalMapFilename()


# ...............................................
   @property
   def mapFilename(self):
      return self._model.occurrenceSet.mapFilename
   
   @property
   def mapName(self):
      return self._model.occurrenceSet.mapName
   
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
   def getScenario(self):
      """
      @summary Gets the scenario to use for the projection
      @return Scenario object
      """
      return self._scenario

# ...............................................
   def getMask(self):
      """
      @summary Returns the mask (SDMLayer) object
      @return Scenario object
      """
      return self._mask

# ...............................................
   def getModel(self):
      """
      @summary Gets the model associated with the projection
      @return Model object
      """
      return self._model
   
# ...............................................
   def getOccurrenceSet(self):
      """
      @summary Gets the species name
      @return The species name used in the projection
      """
      return self._model.occurrenceSet

# .............................................................................
# Private methods
# .............................................................................
   def _getStatus(self):
      """
      @summary Gets the run status of the Model
      @return The run status of the Model
      @note: this attribute is Read-Only
      """
      return self._status

   status = property(_getStatus)

# ...............................................
   def _getStatusModTime(self):
      """
      @summary Gets the last time the status was modified
      @return Status modification time in modified julian date format
      @note: this attribute is Read-Only
      """
      return self._statusmodtime

   statusModTime = property(_getStatusModTime)
      
# ...............................................
   def _getSpeciesName(self):
      """
      @summary Gets the species name
      @return The species name used in the projection
      @note: this attribute is Read-Only
      """
      return self._model.pointsName

   speciesName = property(_getSpeciesName)
   
# ...............................................
   def _getSpeciesQuery(self):
      """
      @summary Gets the species query
      @return The query used to find occurrenceSet of the species
      @note: this attribute is Read-Only
      """
      return self._model.pointsQuery

   speciesQuery = property(_getSpeciesQuery)

# ...............................................
   def _getAlgorithmCode(self):
      """
      @summary Gets the code of the algorithm used
      @return The algorithm code
      @note: this attribute is Read-Only
      """
      return self._model.algorithmCode
   
   algorithmCode = property(_getAlgorithmCode)

# ...............................................
   def _getScenarioCode(self):
      """
      @summary Gets the scenario code
      @return The code for the scenario used for the Projection
      @note: this attribute is Read-Only
      """
      return self._scenario.code
   
   scenarioCode = property(_getScenarioCode)
   
# ...............................................
   def _getLayers(self):
      """
      @summary Gets the layers of the Scenario
      @return A list of layer objects included in the Scenario
      @note: this attribute is Read-Only
      """
      return self._scenario.layers

   layers = property(_getLayers)

