"""
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
import mx.DateTime
import os 
   
from LmCommon.common.lmconstants import JobStatus, DEFAULT_EPSG, ProcessType

from LmServer.base.dbpgsql import DbPostgresql
from LmServer.base.layer import Raster, Vector
from LmServer.base.taxon import ScientificName
from LmServer.base.layerset import MapLayerSet                                  
from LmServer.base.lmobj import LMError
from LmServer.common.computeResource import LMComputeResource
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (ALGORITHM_DATA, LMServiceModule,
                  DEFAULT_PROJECTION_FORMAT, JobFamily, DB_STORE, ReferenceType)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import ARCHIVE_USER
from LmServer.common.notifyJob import NotifyJob
from LmServer.sdm.algorithm import Algorithm
from LmServer.sdm.envlayer import EnvironmentalType, EnvironmentalLayer
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.sdmJob import SDMModelJob, SDMProjectionJob, SDMOccurrenceJob
from LmServer.sdm.scenario import Scenario
from LmServer.sdm.sdmmodel import SDMModel
from LmServer.sdm.sdmprojection import SDMProjection

# .............................................................................
class Borg(DbPostgresql):
   """
   Class to control modifications to the MAL database.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger, dbHost, dbPort, dbUser, dbKey):
      """
      @summary Constructor for MAL class
      @param logger: LmLogger to use for MAL
      @param dbHost: hostname for database machine
      @param dbPort: port for database connection
      """
      DbPostgresql.__init__(self, logger, db=DB_STORE, user=dbUser, 
                            password=dbKey, host=dbHost, port=dbPort)
      earl = EarlJr()
      self._relativeArchivePath = earl.createArchiveDataPath()
      self._webservicePrefix = earl.createWebServicePrefix()()
            
# ...............................................
   def _getRelativePath(self, dlocation=None, url=None):
      relativePath = None
      if dlocation is not None:
         if dlocation.startswith(self._relativeArchivePath):
            relativePath = dlocation[len(self._relativeArchivePath):]
      elif url is not None:
         if url.startswith(self._webservicePrefix):
            relativePath = url[len(self._webservicePrefix)]
      return relativePath

# ...............................................
   def _findOrInsertShapeGridParams(self, shpgrd, cutout):
      """
      @summary: Insert ShapeGrid parameters into the database
      @param shpgrd: The ShapeGrid to insert
      @postcondition: The database contains a new record with shpgrd 
                   attributes.  The shpgrd has _dbId, _dlocation, _parametersId 
                   and metadataUrl populated.
      @note: findShapeGrids should be executed first to ensure that the
             user and shapename combination are unique.
      @raise LMError: on failure to insert or update the database record. 
      """
      shpgrdId = self.executeInsertFunction('lm_insertShapeGrid',
                                             shpgrd.getLayerId(),
                                             shpgrd.cellsides,
                                             shpgrd.cellsize,
                                             shpgrd.size,
                                             shpgrd.siteId,
                                             shpgrd.siteX, shpgrd.siteY,
                                             shpgrd.status,
                                             shpgrd.statusModTime)
      shpgrd.setParametersId(shpgrdId)
      return shpgrd

# ...............................................
   def _findOrInsertBaseLayer(self, lyr):
      min = max = nodata = ltypeid = None
      if isinstance(lyr, EnvironmentalLayer):
         ltypeid = lyr.getParametersId()
      if isinstance(lyr, Raster):
         min = lyr.minVal
         max = lyr.maxVal
         nodata = lyr.nodataVal
      if lyr.epsgcode == DEFAULT_EPSG:
         wkt = lyr.getWkt()
      lyrid = self.executeInsertFunction('lm_insertLayer', 
                                         lyr.verify,
                                         lyr.squid,
                                         lyr.getLayerUserId(),
                                         lyr.name,
                                         lyr.title,
                                         lyr.author,
                                         lyr.description,
                                         self._getRelativePath(
                                             dlocation=lyr.getDLocation()),
                                         self._getRelativePath(
                                             dlocation=lyr.getMetaLocation()),
                                         lyr.ogrType,
                                         lyr.gdalType,
                                         lyr.isCategorical,
                                         lyr.dataFormat,
                                         lyr.epsgcode,
                                         lyr.mapUnits,
                                         lyr.resolution,
                                         lyr.startDate,
                                         lyr.endDate,
                                         lyr.modTime,
                                         lyr.getCSVExtentString(), wkt,
                                         lyr.getValAttribute(),
                                         nodata, min, max,
                                         lyr.valUnits,
                                         ltypeid,
                                         self._getRelativePath(
                                             url=lyr.metadataUrl))
      if lyrid != -1:
         lyr.setLayerId(lyrid)
         lyr.setId(lyrid)
         lyr.resetMetadataUrl()
         updatedLyr = lyr
      else:
         raise LMError(currargs='Error on adding Layer object (Command: %s)' % 
                       str(self.lastCommands))
      return updatedLyr

# .............................................................................
# Public functions
# .............................................................................
# ...............................................
   def insertAlgorithm(self, alg):
      """
      @summary Inserts an Algorithm into the database
      @param alg: The algorithm to add
      @note: lm_insertAlgorithm(varchar, varchar, double) returns an int
      """
      alg.modTime = mx.DateTime.utc().mjd
      success = self.executeInsertFunction('lm_insertAlgorithm', alg.code, 
                                           alg.name, alg.modTime)
      return success

# ...............................................
   def findOrInsertScenario(self, scen):
      """
      @summary Inserts all scenario layers into the database
      @param scen: The scenario to insert
      """
      scen.modTime = mx.DateTime.utc().mjd
      wkt = None
      if scen.epsgcode == DEFAULT_EPSG:
         wkt = scen.getWkt()
      scenid = self.executeInsertFunction('lm_insertScenario', scen.name, 
                                      scen.title, scen.author, scen.description,
                                      self._getRelativePath(url=scen.metadataUrl),
                                      scen.startDate, scen.endDate, 
                                      scen.units, scen.resolution, scen.epsgcode,
                                      scen.getCSVExtentString(), wkt, 
                                      scen.modTime, scen.getUserId())
      scen.setId(scenid)
      for kw in scen.keywords:
         successCode = self.executeInsertFunction('lm_insertScenarioKeyword',
                                              scenid, kw)
         if successCode != 0:
            self.log.error('Failed to insert keyword %s for scenario %d' % 
                           (kw, scenid))
      return scen

# ...............................................
   def getEnvironmentalType(self, typeid, typecode, usrid):
      try:
         if typeid is not None:
            row, idxs = self.executeSelectOneFunction('lm_getLayerType', typeid)
         else:
            row, idxs = self.executeSelectOneFunction('lm_getLayerType', 
                                                      usrid, typecode)
      except:
         envType = None
      else:
         envType = self._createLayerType(row, idxs)
      return envType

# ...............................................
   def findOrInsertEnvironmentalType(self, envtype):
      """
      @summary: Insert or find _EnvironmentalType values. Return the record id.
      @param envtype: An EnvironmentalType or EnvironmentalLayer object
      """
      found = False
      # Find
      updatedET = self.getEnvironmentalType(envtype.getParametersId(), 
                                            envtype.typeCode, envtype.getUserId())
      if updatedET is not None:
         found = True
         if isinstance(envtype, EnvironmentalLayer):
            envtype.setLayerParam(updatedET)
         else:
            envtype = updatedET
            
      # or Insert
      if not found:
         envtype.parametersModTime = mx.DateTime.utc().mjd
         etid = self.executeInsertFunction('lm_insertLayerType',
                                            envtype.getParametersUserId(),
                                            envtype.typeCode,
                                            envtype.typeTitle,
                                            envtype.typeDescription,
                                            envtype.parametersModTime)
         envtype.setParametersId(etid)
         for kw in envtype.typeKeywords:
            success = self.executeInsertFunction('lm_insertLayerTypeKeyword', 
                                                 etid, kw)
      return envtype
                             
# ...............................................
   def insertShapeGrid(self, shpgrd, cutout):
      """
      @summary: Find or insert a ShapeGrid into the database
      @param shpgrd: The ShapeGrid to insert
      @postcondition: The database contains a new or existing records for 
                   shapegrid and layer.  The shpgrd object has _dbId, _dlocation, 
                   and metadataUrl populated.
      @note: findShapeGrids should be executed first to ensure that the
             user and shapename combination are unique.
      @raise LMError: on failure to insert or update the database record. 
      """
      shpgrd.modTime = mx.DateTime.utc().mjd
      sgtmp = self._findOrInsertBaseLayer(shpgrd)
      sg = self._findOrInsertShapeGridParams(sgtmp, cutout)
      return sg

# ...............................................
   def findOrInsertEnvLayer(self, lyr, scenarioId=None):
      """
      @summary Insert or find a layer's metadata in the MAL. 
      @param envLayer: layer to update
      @return: the updated or found EnvironmentalLayer
      @note: layer title and layertype title are the same
      @note: Layer should already have name, filename, and url populated.
      @note: We are setting the layername to the layertype to ensure that they 
             will be unique within a scenario
      @note: lm_insertEnvLayer(...) returns int
      @note lm_insertLayerTypeKeyword(...) returns int
      """
      lyr.modTime = mx.DateTime.utc().mjd
      partialUpdatedLyr = self.findOrInsertEnvironmentalType(envlayer=lyr)
      updatedLyr = self._findOrInsertBaseLayer(partialUpdatedLyr)
      if scenarioId is not None:
         success = self.executeModifyFunction('lm_joinScenarioLayer', 
                                              scenarioId, updatedLyr.getId())
         if not success:
            raise LMError(currargs='Failure joining layer {} to scenario {}'
                          .format(updatedLyr.getId(), scenarioId))
      return lyr


