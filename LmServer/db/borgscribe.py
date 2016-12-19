# coding=utf-8
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
import mx.DateTime
from osgeo.ogr import wkbPoint
import os
import socket
from types import StringType, UnicodeType, IntType
import xml.etree.ElementTree as ET 

from LmCommon.common.lmconstants import (ENCODING, JobStatus, ProcessType)
from LmServer.base.lmobj import LMError, LMObject
from LmServer.db.catalog_borg import Borg
from LmServer.db.connect import HL_NAME
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import  DbUser, ReferenceType
from LmServer.common.localconstants import (CONNECTION_PORT, DB_HOSTNAME, 
                                 POINT_COUNT_MIN, POINT_COUNT_MAX, ARCHIVE_USER)
from LmServer.legion.sdmproj import SDMProjection
from LmServer.sdm.envlayer import EnvironmentalLayer, EnvironmentalType
from LmServer.base.taxon import ScientificName

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
   def insertAlgorithm(self, alg, modtime=None):
      """
      @summary Inserts an Algorithm into the database
      @param alg: The algorithm to add
      """
      algo = self._borg.findOrInsertAlgorithm(alg, modtime)
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
            updatedLyr = self._borg.findOrInsertEnvLayer(lyr, scenarioid)
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
      updatedLayers = []
      updatedScen = self._borg.findOrInsertScenario(scen)
      scenId = updatedScen.getId()
      for lyr in scen.layers:
         updatedLyr = self.insertScenarioLayer(lyr, scenId)
         updatedScen.addLayer(updatedLyr)
      return updatedScen
   
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

# ...............................................
   def getShapeGrid(self, shpgridId=None, lyrId=None, 
                    userId=None, lyrName=None, epsg=None):
      """
      @summary: Get and fill a shapeGrid from its shapegrid or layer id, or 
                user/name/epsgcode.  
      @return: Shapegrid object .
      """
      shpgrid = self._borg.getShapeGrid(shpgridId, lyrId, 
                                        userId, lyrName, epsg)
      return shpgrid

# ...............................................
   def getLayer(self, lyrId=None, lyrVerify=None, userId=None, lyrName=None, 
                epsg=None):
      """
      @summary: Get and fill a Layer from its shapegrid or layer id, or 
                user/name/epsgcode.  
      @return: Shapegrid object .
      """
      lyr = self._borg.getBaseLayer(lyrId, lyrVerify, userId, lyrName, epsg)
      return lyr

# ...............................................
   def findTaxonSource(self, taxonSourceName):
      txSourceId, url, moddate = self._borg.findTaxonSource(taxonSourceName)
      return txSourceId, url, moddate
   
# ...............................................
   def findOrInsertTaxon(self, taxonSourceId=None, taxonKey=None, sciName=None):
      sciname = self._borg.findOrInsertTaxon(taxonSourceId, taxonKey, sciName)
      return sciname

# ...............................................
   def getScenario(self, idOrCode, user=None):
      """
      @summary: Get and fill a scenario from its code or database id.  If 
                matchingLayers is given, ensure that only layers with the same
                type as layers in the matchingLayers are filled, and that the 
                requested scenario layers are in the same order as those in 
                the matchingLayers.
      @param code: The code or scenarioid for the scenario to return
      @param user: The userid for the scenario to return.  Needed if querying by code.
      @return: Scenario object filled with Raster objects.
      """
      if isinstance(idOrCode, IntType):
         scenario = self._borg.getScenario(scenid=idOrCode)
      else:
         scenario = self._borg.getScenario(code=idOrCode, usrid=user)
      return scenario

# ...............................................
   def getOccurrenceSet(self, occid=None, squid=None, userId=None, epsg=None):
      """
      @summary: get a list of occurrencesets for the given squid and User
      @param squid: a Squid (Species Thread) string, tied to a ScientificName
      @param userId: the database primary key of the LMUser
      """
      occset = self._borg.getOccurrenceSet(occid, squid, userId, epsg)
      return occset


# ...............................................
   def getOccurrenceSetsForName(self, scinameStr, userId):
      """
      @summary: get a list of occurrencesets for the given squid and User
      @param squid: a Squid (Species Thread) string, tied to a ScientificName
      @param userId: the database primary key of the LMUser
      """
      sciName = ScientificName(scinameStr, userId=userId)
      updatedSciName = self.findOrInsertTaxon(sciName=sciName)
      occsets = self._borg.getOccurrenceSetsForSquid(updatedSciName.squid,userId)
      return occsets

# ...............................................
   def findOrInsertOccurrenceSet(self, occ):
      """
      @summary: Save a new occurrence set   
      @param occ: New OccurrenceSet to save 
      @note: updates db with count, the actual count on the object (likely zero 
             on initial insertion)
      """
      # Save user points reference to MAL
      if occ.getId() is None :
         newOcc = self._borg.findOrInsertOccurrenceSet(occ)
      else:
         self.log.error('OccurrenceLayer {} already contains a database ID'
                        .format(occ.getId()))
      return newOcc
         
# ...............................................
   def updateOccset(self, occ, polyWkt=None, pointsWkt=None):
      """
      @summary: Update OccurrenceLayer attributes: 
                  verify, displayName, dlocation, rawDlocation, queryCount, 
                  bbox, metadata, status, statusModTime, geometries if valid
      @note: Does not update userid, squid, and epsgcode
      @param occ: OccurrenceLayer to be updated.  
      @param polyWkt: geometry for the minimum polygon around these points
      @param pointsWkt: multipoint geometry for these points
      @return: True/False for successful update.
      """
      success = self._borg.updateOccurrenceSet(occ, polyWkt, pointsWkt)
      return success
   
# # ...............................................
#    def initSDMOcc(self, usr, occ, occJobProcessType, currtime):
#       occJob = self.getJobOfType(JobFamily.SDM, occ)
#       if occJob is not None and JobStatus.failed(occJob.status):
#          self._mal.deleteJob(occJob)
#          occJob = None
#       if occJob is None:
# #          try:
#          if occ.status != JobStatus.INITIALIZE:
#             occ.updateStatus(JobStatus.INITIALIZE, modTime=modtime)
#          occJob = SDMOccurrenceJob(occ, processType=occJobProcessType,
#                                    status=JobStatus.INITIALIZE, 
#                                    statusModTime=modtime, createTime=modtime,
#                                    priority=Priority.NORMAL)
# 
#          success = self.updateOccState(occ)
#          updatedOccJob = self.insertJob(occJob)
#          occJob = updatedOccJob
#       return occJob

# ...............................................
   def initSDMProjections(self, usr, occset, mdlScen, prjScenList, alg,  
                          mdlMask=None, prjMask=None, 
                          modtime=mx.DateTime.gmt().mjd, 
                          email=None, name=None, description=None):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      prjs = []
      if alg.code == 'ATT_MAXENT':
         processType = ProcessType.ATT_PROJECT
      else:
         processType = ProcessType.OM_PROJECT
      for pscen in prjScenList:
         prj = SDMProjection(occset, alg, mdlScen, mdlMask, pscen, prjMask, 
                             processType=processType, 
                             status=JobStatus.GENERAL, statusModTime=modtime)
         self._borg.insertProjection(prj)
         prjs.append(prj)
      return prjs

# ...............................................
   def initSDMChain(self, usr, occ, algList, mdlScen, prjScenList, 
                    mdlMask=None, projMask=None,
                    occJobProcessType=ProcessType.GBIF_TAXA_OCCURRENCE,
                    intersectGrid=None, minPointCount=None):
      """
      @summary: Initialize LMArchive job chain (models, projections, 
                optional intersect) for occurrenceset.
      """
      objs = [occ]
      currtime = mx.DateTime.gmt().mjd
      # ........................
      if minPointCount is None or occ.queryCount >= minPointCount: 
         for alg in algList:
            prjs = self.initSDMProjections(occ, mdlScen, 
                              prjScenList, alg, usr, 
                              modtime=currtime, 
                              mdlMask=mdlMask, prjMask=projMask)
            objs.extend(prjs)
      return objs
   
# ...............................................
   def insertMFChain(self, usr, dlocation, priority=None):
      """
      @summary: Inserts a jobChain into database
      @return: jobChainId
      """
      mfchain = self._borg.insertMFChain(usr, dlocation, JobStatus.INITIALIZE, 
                                            priority)
      return mfchain   

