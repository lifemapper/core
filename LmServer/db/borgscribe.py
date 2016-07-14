# coding=utf-8
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
import socket
from types import StringType, UnicodeType, IntType
import xml.etree.ElementTree as ET 

from LmCommon.common.lmconstants import (ENCODING, JobStatus, RandomizeMethods, 
                                         ProcessType)
from LmServer.base.lmobj import LMError, LMObject
from LmServer.db.catalog_borg import Borg
from LmServer.db.catalog_model import MAL
from LmServer.db.catalog_rad import RAD
from LmServer.db.connect import HL_NAME
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import  DbUser, JobFamily, ReferenceType
from LmServer.common.localconstants import (CONNECTION_PORT, DB_HOSTNAME, 
                                 POINT_COUNT_MIN, POINT_COUNT_MAX, ARCHIVE_USER)
from LmServer.sdm.envlayer import EnvironmentalLayer, EnvironmentalType

# .............................................................................
class BorgScribe(LMObject):
   """
   Class to peruse the Lifemapper catalog
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger, dbUser=DbUser.Pipeline):
      """
      @summary Peruser constructor
      @param logger: logger for info and error reporting 
      @param dbUser: optional database user for connection
      """
      LMObject.__init__(self)
      self.log = logger
      self.hostname = socket.gethostname().lower()
      dbHost = DB_HOSTNAME
         
      if dbUser not in HL_NAME.keys():
         raise LMError('Unknown database user {}'.format(dbUser))
            
      self._borg = Borg(logger, dbHost, CONNECTION_PORT, dbUser, HL_NAME[dbUser])
               
# ............................................................................
   @property
   def isOpen(self):
      bOpen = self._borg.isOpen
      return bOpen

# .............................................................................
# Public functions
# .............................................................................
   def openConnections(self):
      try:
         self._borg.open()
      except Exception, e:
         self.log.error('Failed to open Borg (user={} dbname={} host={} port={}): {}' 
                        .format(self._borg.user, self._borg.db, self._borg.host, 
                           self._borg.port, e.args))
         return False
      return True

# ...............................................
   def closeConnections(self):
      self._borg.close()
      
# ...............................................
   def insertAlgorithm(self, alg):
      """
      @summary Inserts an Algorithm into the database
      @param alg: The algorithm to add
      """
      algo = self._borg.findOrInsertAlgorithm(alg)
      return algo

# ...............................................
   def getLayerTypeCode(self, typecode=None, userid=None, typeid=None):
      etype = self._borg.getEnvironmentalType(typeid, typecode, userid)
      return etype
      
# ...............................................
   def countJobChains(self, status, userIdLst=[None]):
      """
      @summary: Return a count of model and projection jobs at given status.
      @param procTypeLst: list of desired LmCommon.common.lmconstants.ProcessType 
      @param status: list of desired LmCommon.common.lmconstants.JobStatus
      @param userIdLst: list of desired userIds
      """
      total = 0
      if not userIdLst:
         userIdLst = [None]
      for usr in userIdLst:
         total += self._borg.countJobChains(status, usr)
      return total

# ...............................................
   def insertScenarioLayer(self, lyr, scenarioid):
      updatedLyr = None
      if isinstance(lyr, EnvironmentalLayer):
         if lyr.isValidDataset():
            updatedLyr = self._borg.findOrInsertEnvLayer(lyr, scenarioId=scenarioid)
         else:
            raise LMError(currargs='Invalid environmental layer: {}'
                                    .format(lyr.getDLocation()), 
                          lineno=self.getLineno())
      return updatedLyr

# ...............................................
   def insertLayerTypeCode(self, envType):
      if isinstance(envType, EnvironmentalType):
         newOrExistingET = self._borg.findOrInsertEnvironmentalType(envtype=envType)
      else:
         raise LMError(currargs='Invalid object for EnvironmentalType insertion')
      return newOrExistingET

# ...............................................
   def insertScenario(self, scen):
      lyrIds = []
      updatedScen = self._borg.findOrInsertScenario(scen)
      scenId = updatedScen.getId()
      for lyr in scen.layers:
         updatedLyr = self.insertScenarioLayer(lyr, scenId)
         lyrIds.append(updatedLyr.getId())
      return scenId, lyrIds

# ...............................................
   def registerComputeResource(self, compResource, crContact):
      """
      @summary: Insert a compute resource of this Lifemapper system.  
      @param usr: LMComputeResource object to insert
      @return: True on success, False on failure (i.e. IPAddress is not unique)
      """
      borgUser = self.insertUser(crContact)
      borgCR = self._borg.findOrInsertComputeResource(compResource)
      return borgCR
   
# ...............................................
   def insertUser(self, usr):
      """
      @summary: Insert a user of the Lifemapper system.  
      @param usr: LMUser object to insert
      @return: True on success, False on failure (i.e. userid is not unique)
      @note: since inserting the same record in both databases, userid is identical
      """
      borgUser = self._borg.findOrInsertUser(usr)
      return borgUser

# ...............................................
   def insertTaxonomySource(self, taxSourceName, taxSourceUrl):
      taxSource = self._borg.findOrInsertTaxonSource(taxSourceName, 
                                                     taxSourceUrl)
      return taxSource

# ...............................................
   def insertShapeGrid(self, shpgrd, cutout=None):
      updatedShpgrd = self._borg.findOrInsertShapeGrid(shpgrd, cutout)
      return updatedShpgrd
