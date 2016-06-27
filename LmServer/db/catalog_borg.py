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
from LmServer.base.job import _Job
from LmServer.base.layer import Raster, Vector
from LmServer.base.taxon import ScientificName
from LmServer.base.layerset import MapLayerSet                                  
from LmServer.base.lmobj import LMError
from LmServer.common.computeResource import LMComputeResource
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
   def insertScenario(self, scen):
      """
      @summary Inserts all scenario layers into the database
      @param scen: The scenario to insert
      """
      currtime = mx.DateTime.utc().mjd
      mUrlWithPlaceholder = scen.metadataUrl
      wkt = None
      if scen.epsgcode == DEFAULT_EPSG:
         wkt = scen.getWkt()
      scenid = self.executeInsertFunction('lm_insertScenario', scen.name, 
                                      scen.title, scen.author, scen.description,
                                      mUrlWithPlaceholder, 
                                      scen.startDate, scen.endDate, 
                                      scen.units, scen.resolution, 
                                      scen.epsgcode,
                                      scen.getCSVExtentString(), 
                                      wkt, currtime, scen.getUserId())
      scen.setId(scenid)
      for kw in scen.keywords:
         successCode = self.executeInsertFunction('lm_insertScenarioKeyword',
                                              scenid, kw)
         if successCode != 0:
            self.log.error('Failed to insert keyword %s for scenario %d' % 
                           (kw, scenid))
      return scenid

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
   def insertEnvironmentalType(self, envtype=None, envlayer=None):
      """
      @summary: Insert or find _EnvironmentalType values. Return the record id.
      @param envtype: An EnvironmentalType object
      @param envlayer: An EnvironmentalLayer object
      """
      etid = None
      # Find  
      if envtype is None:
         etid = envlayer.getParametersId()
         if etid is None:
            envtype = self.getEnvironmentalType(etid, envlayer.typeCode, 
                                                envlayer.getUserId())
            if envtype is not None:
               etid = envtype.getId()
               envlayer.setLayerParam(envtype)
      # or Insert  
      if etid is None and envtype is not None:
         envtype.parametersModTime = mx.DateTime.utc().mjd
         etid = self.executeInsertFunction('lm_insertLayerType',
                                            envtype.getParametersUserId(),
                                            envtype.typeCode,
                                            envtype.typeTitle,
                                            envtype.typeDescription,
                                            envtype.parametersModTime)
         envtype.setParametersId(etid)
         for kw in envtype.typeKeywords:
            etid = self.executeInsertFunction('lm_insertLayerTypeKeyword', 
                                              etid, kw)
      return etid
                             
# ...............................................

