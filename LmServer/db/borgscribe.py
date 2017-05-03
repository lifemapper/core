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
from LmServer.common.localconstants import (CONNECTION_PORT, DB_HOSTNAME,
                                            PUBLIC_USER)
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
   def findOrInsertAlgorithm(self, alg, modtime=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::findOrInsertAlgorithm()
      """
      algo = self._borg.findOrInsertAlgorithm(alg, modtime)
      return algo

# ...............................................
   def getLayerTypeCode(self, typeCode=None, userId=None, typeId=None):
      etype = self._borg.getEnvironmentalType(typeId, typeCode, userId)
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
   def findOrInsertEnvLayer(self, lyr, scenarioId=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::findOrInsertEnvLayer()
      @note: This inserts an EnvLayer and optionally joins it to a scenario
      """
      updatedLyr = None
      if isinstance(lyr, EnvLayer):
         if lyr.isValidDataset():
            updatedLyr = self._borg.findOrInsertEnvLayer(lyr, scenarioId)
         else:
            raise LMError(currargs='Invalid environmental layer: {}'
                                    .format(lyr.getDLocation()), 
                          lineno=self.getLineno())
      return updatedLyr

# ...............................................
   def getEnvLayer(self, envlyrId=None, lyrId=None, lyrVerify=None, userId=None, 
                   lyrName=None, epsg=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::getEnvLayer()
      """
      lyr = self._borg.getEnvLayer(envlyrId, lyrId, lyrVerify, userId, lyrName, epsg)
      return lyr

# ...............................................
   def deleteScenarioLayer(self, envlyr, scenarioId):
      """
      @copydoc LmServer.db.catalog_borg.Borg::deleteScenarioLayer()
      @note: This deletes the join only, not the EnvLayer
      """
      success = self._borg.deleteScenarioLayer(envlyr, scenarioId)
      return success

# ...............................................
   def deleteEnvLayer(self, envlyr, scenarioId=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::deleteScenarioLayer()
      @note: This deletes the join only, not the EnvLayer
      """
      if scenarioId is not None:
         scss = self.deleteScenarioLayer(envlyr, scenarioId=scenarioId)
      success = self._borg.deleteEnvLayer(envlyr)
      return success

# .............................................................................
   def countEnvLayers(self, userId=PUBLIC_USER, 
                      envCode=None, gcmcode=None, altpredCode=None, dateCode=None, 
                      afterTime=None, beforeTime=None, epsg=None, 
                      envTypeId=None, scenarioCode=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::countEnvLayers()
      """
      count = self._borg.countEnvLayers(userId, envCode, gcmcode, altpredCode, 
                                        dateCode, afterTime, beforeTime, epsg, 
                                        envTypeId, scenarioCode)
      return count

# .............................................................................
   def listEnvLayers(self, firstRecNum, maxNum, userId=PUBLIC_USER, 
                     envCode=None, gcmcode=None, altpredCode=None, dateCode=None, 
                     afterTime=None, beforeTime=None, epsg=None, 
                     envTypeId=None, scenCode=None, atom=True):
      """
      @copydoc LmServer.db.catalog_borg.Borg::listEnvLayers()
      """
      objs = self._borg.listEnvLayers(firstRecNum, maxNum, userId, envCode, 
                                      gcmcode, altpredCode, dateCode, afterTime, 
                                      beforeTime, epsg, envTypeId, scenCode, 
                                      atom)
      return objs

# ...............................................
   def findOrInsertEnvType(self, envType):
      """
      @copydoc LmServer.db.catalog_borg.Borg::findOrInsertEnvType()
      """
      if isinstance(envType, EnvType):
         newOrExistingET = self._borg.findOrInsertEnvType(envtype=envType)
      else:
         raise LMError(currargs='Invalid object for EnvType insertion')
      return newOrExistingET

# ...............................................
   def findOrInsertScenario(self, scen):
      """
      @copydoc LmServer.db.catalog_borg.Borg::findOrInsertScenario()
      """
      updatedScen = self._borg.findOrInsertScenario(scen)
      scenId = updatedScen.getId()
      for lyr in scen.layers:
         updatedLyr = self.findOrInsertEnvLayer(lyr, scenId)
         updatedScen.addLayer(updatedLyr)
      return updatedScen
   
# ...............................................
   def findOrInsertUser(self, usr):
      """
      @copydoc LmServer.db.catalog_borg.Borg::findOrInsertUser()
      """
      borgUser = self._borg.findOrInsertUser(usr)
      return borgUser

# ...............................................
   def findUser(self, userId=None, email=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::findUser()
      """
      borgUser = self._borg.findUser(userId, email)
      return borgUser

# ...............................................
   def findOrInsertTaxonSource(self, taxSourceName, taxSourceUrl):
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

# .............................................................................
   def countLayers(self, userId=PUBLIC_USER, squid=None, afterTime=None, beforeTime=None, 
                   epsg=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::countLayers()
      """
      count = self._borg.countLayers(userId, squid, afterTime, beforeTime, epsg)
      return count

# .............................................................................
   def listLayers(self, firstRecNum, maxNum, userId=PUBLIC_USER, squid=None, 
                  afterTime=None, beforeTime=None, epsg=None, atom=True):
      """
      @copydoc LmServer.db.catalog_borg.Borg::listLayers()
      """
      objs = self._borg.listLayers(firstRecNum, maxNum, userId, squid, 
                        afterTime, beforeTime, epsg, atom)
      return objs

# .............................................................................
   def countMatrixColumns(self, userId=PUBLIC_USER, squid=None, ident=None, 
                          afterTime=None, beforeTime=None, epsg=None, 
                          afterStatus=None, beforeStatus=None, matrixId=None, 
                          layerId=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::countMatrixColumns()
      """
      count = self._borg.countMatrixColumns(userId, squid, ident, afterTime, 
                                 beforeTime, epsg, afterStatus, beforeStatus, 
                                 matrixId, layerId)
      return count

# .............................................................................
   def listMatrixColumns(self, firstRecNum, maxNum, userId=PUBLIC_USER, 
                         squid=None, ident=None, afterTime=None, beforeTime=None, 
                         epsg=None, afterStatus=None, beforeStatus=None, 
                         matrixId=None, layerId=None, atom=True):
      """
      @copydoc LmServer.db.catalog_borg.Borg::listMatrixColumns()
      """
      objs = self._borg.listMatrixColumns(firstRecNum, maxNum, userId, squid, 
                                 ident, afterTime, beforeTime, epsg, afterStatus, 
                                 beforeStatus, matrixId, layerId, atom)
      return objs

# ...............................................
   def getMatrix(self, mtx=None, mtxId=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::getMatrix()
      """
      fullMtx = self._borg.getMatrix(mtx, mtxId)
      return fullMtx

# .............................................................................
   def countMatrices(self, userId=PUBLIC_USER, matrixType=None, 
                        gcmCode=None, altpredCode=None, dateCode=None,
                        keyword=None, gridsetId=None, 
                        afterTime=None, beforeTime=None, epsg=None, 
                        afterStatus=None, beforeStatus=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::countMatrixColumns()
      """
      count = self._borg.countMatrices(userId, matrixType, gcmCode, 
                        altpredCode, dateCode, keyword, gridsetId, afterTime, 
                        beforeTime, epsg, afterStatus, beforeStatus)
      return count

# .............................................................................
   def listMatrices(self, firstRecNum, maxNum, userId=PUBLIC_USER, 
                    matrixType=None, gcmCode=None, altpredCode=None, 
                    dateCode=None, keyword=None, gridsetId=None, 
                    afterTime=None, beforeTime=None, epsg=None, 
                    afterStatus=None, beforeStatus=None, atom=True):
      """
      @copydoc LmServer.db.catalog_borg.Borg::listMatrixColumns()
      """
      objs = self._borg.listMatrices(firstRecNum, maxNum, userId, 
                        matrixType, gcmCode, altpredCode, dateCode, keyword, 
                        gridsetId, afterTime, beforeTime, epsg, afterStatus, 
                        beforeStatus, atom)
      return objs

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
   def getTaxon(self, taxonSourceId=None, taxonKey=None,
                userId=None, taxonName=None):
      sciname = self._borg.getTaxon(taxonSourceId, taxonKey, userId, taxonName)
      return sciname

# ...............................................
   def getScenario(self, idOrCode, user=None, fillLayers=False):
      """
      @copydoc LmServer.db.catalog_borg.Borg::getScenario()
      """
      if isinstance(idOrCode, IntType):
         scenario = self._borg.getScenario(scenid=idOrCode, 
                                           fillLayers=fillLayers)
      else:
         scenario = self._borg.getScenario(code=idOrCode, usrid=user, 
                                           fillLayers=fillLayers)
      return scenario
   
# .............................................................................
   def countScenarios(self, userId=PUBLIC_USER, beforeTime=None, afterTime=None, 
                      epsg=None, gcmCode=None, altpredCode=None, dateCode=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::countScenarios()
      """
      count = self._borg.countScenarios(userId, beforeTime, afterTime, epsg,
                                            gcmCode, altpredCode, dateCode)
      return count

# .............................................................................
   def listScenarios(self, firstRecNum, maxNum, userId=PUBLIC_USER, 
                     beforeTime=None, afterTime=None, epsg=None, gcmCode=None, 
                     altpredCode=None, dateCode=None, atom=True):
      """
      @copydoc LmServer.db.catalog_borg.Borg::countScenarios()
      """
      count = self._borg.listScenarios(firstRecNum, maxNum, userId, 
                                       beforeTime, afterTime, epsg, gcmCode, 
                                       altpredCode, dateCode, atom)
      return count

# ...............................................
   def getOccurrenceSet(self, occId=None, squid=None, userId=None, epsg=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::getOccurrenceSet()
      """
      occset = self._borg.getOccurrenceSet(occId, squid, userId, epsg)
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
      occsets = self._borg.getOccurrenceSetsForSquid(updatedSciName.squid, userId)
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
         
# .............................................................................
   def countOccurrenceSets(self, userId=PUBLIC_USER, minOccurrenceCount=None, 
               displayName=None, afterTime=None, beforeTime=None, epsg=None, 
               afterStatus=None, beforeStatus=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::countOccurrenceSets()
      """
      count = self._borg.countOccurrenceSets(userId, minOccurrenceCount, 
            displayName, afterTime, beforeTime, epsg, afterStatus, beforeStatus)
      return count

# .............................................................................
   def listOccurrenceSets(self, firstRecNum, maxNum, userId=PUBLIC_USER, 
                          minOccurrenceCount=None, displayName=None, 
                          afterTime=None, beforeTime=None, epsg=None, 
                          afterStatus=None, beforeStatus=None, atom=True):
      """
      @copydoc LmServer.db.catalog_borg.Borg::listOccurrenceSets()
      """
      objs = self._borg.listOccurrenceSets(firstRecNum, maxNum, userId, minOccurrenceCount, 
                                           displayName, afterTime, beforeTime, 
                                           epsg, afterStatus, beforeStatus, atom)
      return objs

# ...............................................
   def updateOccset(self, occ, polyWkt=None, pointsWkt=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::updateOccurrenceSet()
      """
      success = self._borg.updateOccurrenceSet(occ, polyWkt, pointsWkt)
      return success

# ...............................................
   def getSDMProject(self, layerid):
      """
      @copydoc LmServer.db.catalog_borg.Borg::getSDMProject()
      """
      proj = self._borg.getSDMProject(layerid)
      return proj

# ...............................................
   def findOrInsertSDMProject(self, proj):
      """
      @copydoc LmServer.db.catalog_borg.Borg::findOrInsertSDMProject()
      """
      newOrExistingProj = self._borg.findOrInsertSDMProject(proj)
      return newOrExistingProj

# .............................................................................
   def countSDMProjects(self, userId=PUBLIC_USER, displayName=None, 
                        afterTime=None, beforeTime=None, epsg=None, 
                        afterStatus=None, beforeStatus=None, occsetId=None, 
                        algCode=None, mdlscenCode=None, prjscenCode=None):
      """
      @copydoc LmServer.db.catalog_borg.Borg::countSDMProjects()
      """
      count = self._borg.countSDMProjects(userId, displayName, 
                       afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                       occsetId, algCode, mdlscenCode, prjscenCode)
      return count

# .............................................................................
   def listSDMProjects(self, firstRecNum, maxNum, userId=PUBLIC_USER, 
                       displayName=None, afterTime=None, beforeTime=None, 
                       epsg=None, afterStatus=None, beforeStatus=None, 
                       occsetId=None, algCode=None, mdlscenCode=None, 
                       prjscenCode=None, atom=True):
      """
      @copydoc LmServer.db.catalog_borg.Borg::listSDMProjects()
      """
      objs = self._borg.listSDMProjects(firstRecNum, maxNum, userId, displayName, 
                       afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                       occsetId, algCode, mdlscenCode, prjscenCode, atom)
      return objs

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
   def findOrInsertMatrixColumn(self, mtxcol):
      """
      @summary: Find existing OR save a new MatrixColumn
      @param mtxcol: the LmServer.legion.MatrixColumn object to get or insert
      @return new or existing MatrixColumn object
      """
      mtxcol = self._borg.findOrInsertMatrixColumn(mtxcol)
      return mtxcol
      
# ...............................................
   def initOrRollbackIntersect(self, lyr, mtx, intersectParams, modtime):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      newOrExistingMtxcol = None
      if mtx is not None and mtx.getId() is not None:
         # TODO: Save this into the DB??
         if lyr.dataFormat in GDALFormatCodes.keys():
            if mtx.matrixType in (MatrixType.PAM, MatrixType.ROLLING_PAM):
               ptype = ProcessType.INTERSECT_RASTER
            else:
               ptype = ProcessType.INTERSECT_RASTER_GRIM
         else:
            ptype = ProcessType.INTERSECT_VECTOR

         mtxcol = MatrixColumn(None, mtx.getId(), mtx.getUserId(), 
                layer=lyr, shapegrid=None, intersectParams=intersectParams, 
                squid=lyr.squid, ident=lyr.ident,
                processType=ptype, metadata={}, matrixColumnId=None, 
                status=JobStatus.GENERAL, statusModTime=modtime)
         newOrExistingMtxcol = self._borg.findOrInsertMatrixColumn(mtxcol)
         # Reset processType (not in db)
         newOrExistingMtxcol.processType = ptype
         
         if JobStatus.finished(newOrExistingMtxcol.status):
            newOrExistingMtxcol.updateStatus(JobStatus.GENERAL, modTime=modtime)
            success = self.updateMatrixColumn(newOrExistingMtxcol)
      return newOrExistingMtxcol

# ...............................................
   def initOrRollbackSDMProjects(self, occ, mdlScen, projScenList, alg,  
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
      for projScen in projScenList:
         prj = SDMProjection(occ, alg, mdlScen, projScen, 
                        modelMask=mdlMask, projMask=projMask, 
                        dataFormat=DEFAULT_PROJECTION_FORMAT,
                        status=JobStatus.GENERAL, statusModTime=modtime)
         newOrExistingPrj = self._borg.findOrInsertSDMProject(prj)
         # Instead of re-pulling unchanged scenario layers, masks, update 
         # with input arguments
         newOrExistingPrj._modelScenario = mdlScen
         newOrExistingPrj.setModelMask(mdlMask)
         newOrExistingPrj._projScenario = projScen
         newOrExistingPrj.setProjMask(projMask)
         # Rollback if finished
         if JobStatus.finished(newOrExistingPrj.status):
            newOrExistingPrj.updateStatus(JobStatus.GENERAL, modTime=modtime)
            newOrExistingPrj = self.updateSDMProject(newOrExistingPrj)
         
         prjs.append(newOrExistingPrj)
      return prjs

# ...............................................
   def initOrRollbackSDMChain(self, occ, algList, mdlScen, prjScenList, 
                    mdlMask=None, projMask=None,
                    gridset=None, intersectParams=None, minPointCount=None):
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
                  mtxcol = self.initOrRollbackIntersect(prj, gridset.pam, 
                                                        intersectParams, 
                                                        currtime)
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
      if isinstance(obj, OccurrenceLayer):
         prjs = self.listSDMProjects(0, 500, occsetId-obj.getId(), atom=True)
      success = self._borg.deleteObject(obj)
      return success

