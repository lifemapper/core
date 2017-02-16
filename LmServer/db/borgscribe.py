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
from types import IntType

from LmCommon.common.lmconstants import (JobStatus, ProcessType, MatrixType)
from LmServer.base.lmobj import LMError, LMObject
from LmServer.db.catalog_borg import Borg
from LmServer.db.connect import HL_NAME
from LmServer.common.lmconstants import (DbUser, DEFAULT_PROJECTION_FORMAT, 
                                         GDALFormatCodes)
from LmServer.common.localconstants import (CONNECTION_PORT, DB_HOSTNAME)
from LmServer.legion.mtxcolumn import MatrixColumn
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
      @copydoc LmServer.db.catalog_borg.Borg::updateShapeGrid()
      """
      success = self._borg.updateShapeGrid(shpgrd)
      return success

# ...............................................
   def getShapeGrid(self, lyrId=None, userId=None, lyrName=None, epsg=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::getShapeGrid()
      """
      shpgrid = self._borg.getShapeGrid(lyrId, userId, lyrName, epsg)
      return shpgrid

# ...............................................
   def getLayer(self, lyrId=None, lyrVerify=None, userId=None, lyrName=None, 
                epsg=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::getLayer()
      """
      lyr = self._borg.getBaseLayer(lyrId, lyrVerify, userId, lyrName, epsg)
      return lyr

# ...............................................
   def getMatrix(self, mtx=None, mtxId=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::getMatrix()
      """
      fullMtx = self._borg.getMatrix(mtx, mtxId)
      return fullMtx

# ...............................................
   def getGridset(self, gridset, fillMatrices=False):
      """
      @copydoc LmServer.db.catalog_borg.Borg::getGridset()
      """
      existingGridset = self._borg.getGridset(gridset, fillMatrices)
      return existingGridset

# ...............................................
   def findTaxonSource(self, taxonSourceName):
      txSourceId, url, moddate = self._borg.findTaxonSource(taxonSourceName)
      return txSourceId, url, moddate
   
# ...............................................
   def findOrInsertTaxon(self, taxonSourceId=None, taxonKey=None, sciName=None):
      sciname = self._borg.findOrInsertTaxon(taxonSourceId, taxonKey, sciName)
      return sciname

# ...............................................
   def getScenario(self, idOrCode, user=None, fillLayers=False):
      """
      @summary: Get and fill a scenario from its user and code or database id.   
                If  fillLayers is true, populate the layers in the object.
      @param idOrCode: ScenarioId or code for the scenario to be fetched.
      @param user: User id for the scenario to be fetched.
      @param fillLayers: Boolean indicating whether to retrieve and populate 
             layers from to be fetched.
      @return: a LmServer.legion.scenario.Scenario object
      """
      if isinstance(idOrCode, IntType):
         scenario = self._borg.getScenario(scenid=idOrCode, 
                                           fillLayers=fillLayers)
      else:
         scenario = self._borg.getScenario(code=idOrCode, usrid=user, 
                                           fillLayers=fillLayers)
      return scenario

# ...............................................
   def getOccurrenceSet(self, occid=None, squid=None, userId=None, epsg=None):
      """
      @summary: get an occurrenceset for the given id or squid and User
      @param squid: a Squid (Species Thread) string, tied to a ScientificName
      @param userId: the database primary key of the LMUser
      """
      occset = self._borg.getOccurrenceSet(occid, squid, userId, epsg)
      return occset

# ...............................................
   def getOccurrenceSetsForName(self, scinameStr, userId):
      """
      @summary: get a list of occurrencesets for the given squid and User
      @param scinameStr: a string associated with a 
                        LmServer.base.taxon.ScientificName 
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
      @note: Does not update the userid, squid, and epsgcode (unique constraint) 
      @param occ: OccurrenceLayer to be updated.  
      @param polyWkt: geometry for the minimum polygon around these points
      @param pointsWkt: multipoint geometry for these points
      @return: True/False for successful update.
      """
      success = self._borg.updateOccurrenceSet(occ, polyWkt, pointsWkt)
      return success

# ...............................................
   def getSDMProject(self, projid):
      """
      @summary: get a projection for the given id
      @param projid: Database id for the SDMProject
      """
      proj = self._borg.getSDMProject(projid)
      return proj

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
   def updateMatrixColumn(self, mtxcol):
      """
      @copydoc LmServer.db.catalog_borg.Borg::updateMatrixColumn()
      """
      success = self._borg.updateMatrixColumn(mtxcol)
      return success
   
# ...............................................
   def initOrRollbackIntersect(self, lyr, mtx, modtime, overridePath=None):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      newOrExistingMtxcol = None
      if mtx is not None and mtx.getId() is not None:
         if lyr.dataFormat in GDALFormatCodes.keys():
            if mtx.matrixType == MatrixType.PAM:
               ptype = ProcessType.INTERSECT_RASTER
            else:
               ptype = ProcessType.INTERSECT_RASTER_GRIM
         else:
            ptype = ProcessType.INTERSECT_VECTOR
         mtxcol = MatrixColumn(None, mtx.getId(), mtx.getUserId(), 
                layer=lyr, shapegrid=None, intersectParams={}, 
                colDLocation=None, squid=lyr.squid, ident=lyr.ident,
                processType=ptype, metadata={}, matrixColumnId=None, 
                status=JobStatus.GENERAL, statusModTime=modtime,
                overridePath=overridePath)
            
         newOrExistingMtxcol = self._borg.findOrInsertMatrixColumn(mtxcol)
         if JobStatus.finished(newOrExistingMtxcol.status):
            newOrExistingMtxcol.updateStatus(JobStatus.GENERAL, modTime=modtime)
            success = self.updateMatrixColumn(newOrExistingMtxcol)
      return newOrExistingMtxcol

# ...............................................
   def initOrRollbackSDMProjects(self, occ, mdlScen, prjScenList, alg,  
                          mdlMask=None, projMask=None, 
                          modtime=mx.DateTime.gmt().mjd, email=None):
      """
      @summary: Initialize or rollback existing LMArchive SDMProjection
               dependent on this occurrenceset and algorithm.
      @param occ: OccurrenceSet for which to initialize or rollback all 
                  dependent objects
      @param mdlScen: Scenario for SDM model computations
      @param prjScenList: Scenarios for SDM project computations
      @param alg: List of algorithm objects for SDM computations on this 
                      OccurrenceSet
      @param mdlMask: Layer mask for SDM model computations
      @param projMask: Layer mask for SDM project computations
      @param modtime: timestamp of modification, in MJD format 
      @param email: email address for notifications 
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
         prj = SDMProjection(occ, alg, mdlScen, prjScen, 
                        modelMaskId=mmaskid, projMaskId=pmaskid, 
                        dataFormat=DEFAULT_PROJECTION_FORMAT,
                        status=JobStatus.GENERAL, statusModTime=modtime)
         newOrExistingPrj = self._borg.findOrInsertSDMProject(prj)
         if JobStatus.finished(newOrExistingPrj.status):
            newOrExistingPrj.updateStatus(JobStatus.GENERAL, modTime=modtime)
            newOrExistingPrj = self.updateSDMProject(newOrExistingPrj)
         
         prjs.append(newOrExistingPrj)
      return prjs

# ...............................................
   def initOrRollbackSDMChain(self, occ, algList, mdlScen, prjScenList, 
                    mdlMask=None, projMask=None,
                    occJobProcessType=ProcessType.GBIF_TAXA_OCCURRENCE,
                    gridset=None, minPointCount=None):
      """
      @summary: Initialize or rollback existing LMArchive SDM chain 
                (SDMProjection, Intersection) dependent on this occurrenceset.
      @param occ: OccurrenceSet for which to initialize or rollback all 
                  dependent objects
      @param algList: List of algorithm objects for SDM computations on this 
                      OccurrenceSet
      @param mdlScen: Scenario for SDM model computations
      @param prjScenList: Scenarios for SDM project computations
      @param mdlMask: Layer mask for SDM model computations
      @param projMask: Layer mask for SDM project computations
      @param occJobProcessType: LmCommon.common.lmconstants.ProcessType for 
                                OccurrenceSet creation 
      @param gridset: Gridset containing Global PAM for output of intersections
      @param minPointCount: Minimum number of points required for SDM 
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
            if gridset is not None and gridset.pam is not None:
               mtxcols = []
               for prj in prjs:
                  overridePath = prj.getAbsolutePath()
                  mtxcol = self.initOrRollbackIntersect(prj, gridset.pam, 
                                          currtime, overridePath=overridePath)
                  mtxcols.append(mtxcol)
               objs.extend(mtxcols)
      return objs
   
# ...............................................
   def insertMFChain(self, mfchain):
      """
      @copydoc LmServer.db.catalog_borg.Borg::insertMFChain()
      """
      mfchain = self._borg.insertMFChain(mfchain)
      return mfchain
   
# ...............................................
   def findMFChains(self, count, userId=None):
      """
      @summary: Retrieves MFChains from database, optionally filtered by status 
                and/or user, updates their status
      @param count: Number of MFChains to pull
      @param userId: If not None, filter by this user 
      """
      mfchainList = self._borg.findMFChains(count, userId, 
                                 JobStatus.INITIALIZE, JobStatus.PULL_REQUESTED)
      return mfchainList

# ...............................................
   def updateMFChain(self, mfchain):
      """
      @copydoc LmServer.db.catalog_borg.Borg::updateMFChain()
      """
      success = self._borg.updateMFChain(mfchain)
      return success

# ...............................................
   def updateObject(self, obj):
      """
      @copydoc LmServer.db.catalog_borg.Borg::updateObject()
      """
      success = self._borg.updateObject(obj)
      return success

# ...............................................
   def deleteObject(self, obj):
      """
      @copydoc LmServer.db.catalog_borg.Borg::deleteObject()
      """
      success = self._borg.deleteObject(obj)
      return success

