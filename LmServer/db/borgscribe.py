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
import socket
from types import StringType, UnicodeType, IntType

from LmCommon.common.lmconstants import (JobStatus, ProcessType)
from LmServer.base.lmobj import LMError, LMObject
from LmServer.db.catalog_borg import Borg
from LmServer.db.connect import HL_NAME
from LmServer.common.lmconstants import  DbUser
from LmServer.common.localconstants import (CONNECTION_PORT, DB_HOSTNAME)
from LmServer.legion.mtxcolumn import MatrixRaster
from LmServer.legion.sdmproj import SDMProjection
from LmServer.legion.envlayer import EnvLayer, EnvType
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
      if isinstance(lyr, EnvLayer):
         if lyr.isValidDataset():
            updatedLyr = self._borg.findOrInsertEnvLayer(lyr, scenarioid)
         else:
            raise LMError(currargs='Invalid environmental layer: {}'
                                    .format(lyr.getDLocation()), 
                          lineno=self.getLineno())
      return updatedLyr

# ...............................................
   def insertLayerTypeCode(self, envType):
      if isinstance(envType, EnvType):
         newOrExistingET = self._borg.findOrInsertEnvironmentalType(envtype=envType)
      else:
         raise LMError(currargs='Invalid object for EnvType insertion')
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
   def findOrInsertShapeGrid(self, shpgrd, cutout=None):
      updatedShpgrd = self._borg.findOrInsertShapeGrid(shpgrd, cutout)
      return updatedShpgrd
   
# ...............................................
   def findOrInsertGridset(self, grdset):
      updatedGrdset = self._borg.findOrInsertGridset(grdset)
      return updatedGrdset   

# ...............................................
   def findOrInsertMatrix(self, mtx):
      updatedMtx = self._borg.findOrInsertMatrix(mtx)
      return updatedMtx   

# ...............................................
   def updateShapeGrid(self, shpgrd):
      """
      @summary: Update Shapegrid attributes: 
         verify, dlocation, metadata, modtime, size, status, statusModTime
      @param shpgrd: ShapeGrid to be updated.  
      @return: True/False for successful update.
      """
      success = self._borg.updateShapeGrid(shpgrd)
      return success

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
      newOcc = None
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

# ...............................................
   def updateSDMProject(self, proj):
      """
      @summary Method to update an SDMProjection object in the database with 
               the verify hash, metadata, data extent and values, status/statusmodtime.
      @param proj the SDMProjection object to update
      @return: True/False for successful update.
      """
      success = self._borg.updateSDMProject(proj)
      return success   
   
# ...............................................
   def initOrRollbackIntersect(self, prj, gridset, modtime):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      newOrExistingMtxcol = None
      if gridset is not None:
         mtxrst = MatrixRaster(-1, -1, prj.getUserId(), gridset, prj.name, prj.epsgcode,  
                     lyrId=prj.getId(), squid=prj.squid, verify=prj.verify, 
                     dlocation=prj.getDLocation(), lyrMetadata=prj.lyrMetadata, 
                     dataFormat=prj.dataFormat, gdalType=prj.gdalType, 
                     valUnits=prj.valUnits, nodataVal=prj.nodataVal, 
                     minVal=prj.minVal, maxVal=prj.maxVal, mapunits=prj.mapUnits, 
                     resolution=prj.resolution, bbox=prj.bbox, 
                     metadataUrl=prj.metadataUrl, modTime=prj.statusModTime,
                     processType=ProcessType.RAD_INTERSECT, 
                     mtxcolMetadata={}, intersectParams={}, 
                     status=JobStatus.GENERAL, statusModTime=None)
         # TODO:
         newOrExistingMtxcol = self._borg.findOrInsertMtxcol(mtxrst)
         if JobStatus.finished(newOrExistingMtxcol.status):
            newOrExistingMtxcol.updateStatus(JobStatus.GENERAL, stattime=modtime)
            newOrExistingMtxcol = self.updateMtxcol(newOrExistingMtxcol)
      return newOrExistingMtxcol

# ...............................................
   def initOrRollbackSDMProjects(self, occset, mdlScen, prjScenList, alg,  
                          mdlMask=None, projMask=None, 
                          modtime=mx.DateTime.gmt().mjd, email=None):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      prjs = []
      try:
         mmaskid = mdlMask.getId()
      except:
         mmaskid = None
      try:
         pmaskid = projMask.getId()
      except:
         pmaskid = None
      for prjScen in prjScenList:
         prj = SDMProjection(occset, alg, mdlScen, prjScen, 
                        modelMaskId=mmaskid, projMaskId=pmaskid, 
                        status=JobStatus.GENERAL, statusModTime=modtime)
         newOrExistingPrj = self._borg.findOrInsertSDMProject(prj)
         if JobStatus.finished(newOrExistingPrj.status):
            newOrExistingPrj.updateStatus(JobStatus.GENERAL, stattime=modtime)
            newOrExistingPrj = self.updateSDMProject(newOrExistingPrj)
         
         prjs.append(newOrExistingPrj)
      return prjs

# ...............................................
   def initOrRollbackSDMChain(self, usr, occ, algList, mdlScen, prjScenList, 
                    mdlMask=None, projMask=None,
                    occJobProcessType=ProcessType.GBIF_TAXA_OCCURRENCE,
                    gridset=None, minPointCount=None):
      """
      @summary: Initialize or rollback existing LMArchive SDM chain 
                (SDMProjection, Intersection) dependent on this occurrenceset.
      """
      objs = [occ]
      currtime = mx.DateTime.gmt().mjd
      # ........................
      if (minPointCount is None or occ.queryCount is None or 
          occ.queryCount >= minPointCount): 
         for alg in algList:
            prjs = self.initOrRollbackSDMProjects(occ, mdlScen, prjScenList, alg, 
                              mdlMask=mdlMask, projMask=projMask, 
                              modtime=currtime)
            objs.extend(prjs)
            # Intersect if intersectGrid is provided
            if gridset is not None:
               mtxcols = []
               for prj in prjs:
                  mtxcol = self.initOrRollbackIntersect(prj, gridset, currtime)
                  mtxcols.append(mtxcol)
               objs.extend(mtxcols)
      return objs
   
# ...............................................
   def insertMFChain(self, usr, dlocation, priority=None, metadata={}):
      """
      @summary: Inserts a jobChain into database
      @return: jobChainId
      """
      mfchain = self._borg.insertMFChain(usr, dlocation, priority, metadata, 
                                         JobStatus.INITIALIZE)
      return mfchain   

