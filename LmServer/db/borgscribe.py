# coding=utf-8
"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import (ProcessType, LMFormat)
from LmServer.base.layerset import MapLayerSet
from LmServer.base.taxon import ScientificName
from LmServer.db.catalog_borg import Borg
from LmServer.db.connect import HL_NAME
from LmServer.common.lmconstants import (DbUser, MatrixType, JobStatus, FileFix,
                                         NAME_SEPARATOR, LMFileType)
from LmServer.common.localconstants import (CONNECTION_PORT, DB_HOSTNAME,
                                            PUBLIC_USER)
from LmServer.legion.envlayer import EnvLayer, EnvType
from LmServer.legion.mtxcolumn import MatrixColumn
from LmServer.legion.sdmproj import SDMProjection
from LmServer.common.datalocator import EarlJr

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
    def findOrInsertLayer(self, lyr):
        """
        @copydoc LmServer.db.catalog_borg.Borg::findOrInsertLayer()
        """
        updatedLyr = self._borg.findOrInsertLayer(lyr)
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

# .............................................................................
    def countScenPackages(self, userId=PUBLIC_USER, afterTime=None, 
                                 beforeTime=None, epsg=None, scenId=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countScenPackages()
        """
        count = self._borg.countScenPackages(userId, afterTime, beforeTime, epsg, 
                                                         scenId)
        return count

# .............................................................................
    def listScenPackages(self, firstRecNum, maxNum, userId=PUBLIC_USER, 
                                afterTime=None, beforeTime=None, epsg=None, scenId=None, 
                                atom=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::listScenPackages()
        """
        objs = self._borg.listScenPackages(firstRecNum, maxNum, userId, afterTime, 
                                                      beforeTime, epsg, scenId, atom)
        return objs

# ...............................................
    def findOrInsertScenPackage(self, scenPkg):
        """
        @copydoc LmServer.db.catalog_borg.Borg::findOrInsertScenPackage()
        @note: This returns the updated ScenPackage **without** Scenarios and Layers
        """
        updatedScenPkg = self._borg.findOrInsertScenPackage(scenPkg)
#         # if existing, pull existing scenarios
#         if updatedScenPkg.modTime == scenPkg.modTime:
#             existingScens = self.getScenariosForScenPackage(
#                                                     scenPkgId=updatedScenPkg.getId())
#             updatedScenPkg.setScenarios(existingScens)
#         else:
#             for code, scen in scenPkg.scenarios.iteritems():
#                 updatedScen = self.findOrInsertScenario(scen, updatedScenPkg.getId())
#                 updatedScenPkg.addScenario(updatedScen)
        return updatedScenPkg

# ...............................................
    def getScenPackagesForScenario(self, scen=None, scenId=None, 
                                            userId=None, scenCode=None, fillLayers=False):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getScenPackagesForScenario()
        """
        scenPkgs = self._borg.getScenPackagesForScenario(scen, scenId, userId, 
                                                                         scenCode, fillLayers)
        return scenPkgs

# ...............................................
    def getScenariosForScenPackage(self, scenPkg=None, scenPkgId=None, 
                                            userId=None, scenPkgName=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getScenariosForScenPackage
        """
        scens = self._borg.getScenariosForScenPackage(scenPkg, scenPkgId, userId, 
                                                                     scenPkgName)
        return scens

# ...............................................
    def getScenPackagesForUserCodes(self, usr, scenCodeList, fillLayers=False):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getScenPackagesForScenario()
        @note: This returns all ScenPackages containing this scenario.  All 
                 ScenPackages are filled with Scenarios.
        """
        scenPkgs = []
        if scenCodeList:
            firstCode = scenCodeList[0]
            newlist = scenCodeList[1:]
            firstPkgs = self.getScenPackagesForScenario(userId=usr, 
                                                                      scenCode=firstCode, 
                                                                      fillLayers=fillLayers)
            for pkg in firstPkgs:
                badMatch = False
                for code in newlist:
                    foundScen = pkg.getScenario(code=code)
                    if not foundScen:
                        badMatch = True
                        break
                if not badMatch:
                    scenPkgs.append(pkg)
        return scenPkgs

# ...............................................
    def getScenPackage(self, scenPkg=None, scenPkgId=None, 
                             userId=None, scenPkgName=None, fillLayers=False):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getScenPackage()
        """
        foundScenPkg = self._borg.getScenPackage(scenPkg, scenPkgId, userId, 
                                                              scenPkgName, fillLayers)
        return foundScenPkg

# ...............................................
    def findOrInsertScenario(self, scen, scenPkgId=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::findOrInsertScenario()
        @note: This returns the updated Scenario filled with Layers
        """
        updatedScen = self._borg.findOrInsertScenario(scen, scenPkgId)
        scenId = updatedScen.getId()
        for lyr in scen.layers:
            updatedLyr = self.findOrInsertEnvLayer(lyr, scenId)
            updatedScen.addLayer(updatedLyr)
        return updatedScen

# ...............................................
    def deleteComputedUserData(self, userId):
        """
        @copydoc LmServer.db.catalog_borg.Borg::deleteComputedUserData
        """
        success = self._borg.deleteComputedUserData(userId)
        return success 
    
# ...............................................
    def clearUser(self, userId):
        """
        @copydoc LmServer.db.catalog_borg.Borg::clearUser
        """
        allmtxcolids = []
        grdids = self._borg.findUserGridsets(userId)
        for grdid in grdids:
            mtxcolids = self._borg.deleteGridsetReturnMtxcolids(grdid)
            allmtxcolids.extend(mtxcolids)
            
        success = self._borg.clearUser(userId)
        return success, allmtxcolids
    
# ...............................................
    def findOrInsertUser(self, usr):
        """
        @copydoc LmServer.db.catalog_borg.Borg::findOrInsertUser()
        """
        borgUser = self._borg.findOrInsertUser(usr)
        return borgUser

# ...............................................
    def updateUser(self, usr):
        """
        @copydoc LmServer.db.catalog_borg.Borg::updateUser()
        """
        success = self._borg.updateUser(usr)
        return success

# ...............................................
    def findUser(self, userId=None, email=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::findUser()
        """
        borgUser = self._borg.findUser(userId, email)
        return borgUser
    
# ...............................................
    def findUserForObject(self, layerId=None, scenCode=None, occId=None, 
                                 matrixId=None, gridsetId=None, mfprocessId=None):
        userId = self._borg.findUserForObject(layerId, scenCode, occId, matrixId, 
                                                          gridsetId, mfprocessId)
        return userId
    
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
    def getShapeGrid(self, lyrId=None, userId=None, lyrName=None, epsg=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getShapeGrid()
        """
        shpgrid = self._borg.getShapeGrid(lyrId, userId, lyrName, epsg)
        return shpgrid

# .............................................................................
    def countShapeGrids(self, userId=PUBLIC_USER, cellsides=None, cellsize=None, 
                              afterTime=None, beforeTime=None, epsg=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countShapeGrids()
        """
        count = self._borg.countShapeGrids(userId, cellsides, cellsize, afterTime, 
                                                      beforeTime, epsg)
        return count

# .............................................................................
    def listShapeGrids(self, firstRecNum, maxNum, userId=PUBLIC_USER, 
                             cellsides=None, cellsize=None, 
                             afterTime=None, beforeTime=None, epsg=None, atom=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::listShapeGrids()
        """
        objs = self._borg.listShapeGrids(firstRecNum, maxNum, userId, 
                                cellsides, cellsize, afterTime, beforeTime, epsg, atom)
        return objs


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

# ...............................................
    def getMatrixColumn(self, mtxcol=None, mtxcolId=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getMatrixColumn()
        """
        mtxColumn = self._borg.getMatrixColumn(mtxcol, mtxcolId)
        return mtxColumn
    
# ...............................................
    def getColumnsForMatrix(self, mtxId):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getColumnsForMatrix()
        """
        mtxColumns = self._borg.getColumnsForMatrix(mtxId)
        return mtxColumns

# ...............................................
    def getSDMColumnsForMatrix(self, mtxId, returnColumns=True, 
                               returnProjections=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getSDMColumnsForMatrix()
        """
        colPrjPairs = self._borg.getSDMColumnsForMatrix(mtxId, returnColumns, 
                                                        returnProjections)
        return colPrjPairs

# ...............................................
    def getOccLayersForMatrix(self, mtxId):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getOccLayersForMatrix
        """
        occsets = self._borg.getOccLayersForMatrix(mtxId)
        return occsets
    
# .............................................................................
    def countMatrixColumns(self, userId=None, squid=None, ident=None, 
                           afterTime=None, beforeTime=None, epsg=None, 
                           afterStatus=None, beforeStatus=None, 
                           gridsetId=None, matrixId=None, layerId=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countMatrixColumns()
        """
        count = self._borg.countMatrixColumns(userId, squid, ident, afterTime, 
                        beforeTime, epsg, afterStatus, beforeStatus, 
                        gridsetId, matrixId, layerId)
        return count

# .............................................................................
    def listMatrixColumns(self, firstRecNum, maxNum, userId=None, 
                          squid=None, ident=None, afterTime=None, beforeTime=None, 
                          epsg=None, afterStatus=None, beforeStatus=None, 
                          gridsetId=None, matrixId=None, layerId=None, atom=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::listMatrixColumns()
        """
        objs = self._borg.listMatrixColumns(firstRecNum, maxNum, userId, squid, 
                                            ident, afterTime, beforeTime, 
                                            epsg, afterStatus, beforeStatus, 
                                            gridsetId, matrixId, layerId, atom)
        return objs

# ...............................................
    def getMatrix(self, mtx=None, mtxId=None, gridsetId=None, gridsetName=None, 
                  userId=None, mtxType=None, gcmCode=None, altpredCode=None, 
                  dateCode=None, algCode=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getMatrix()
        @param mtx: A LmServer.legion.LMMatrix object containing the unique 
                        parameters for which to retrieve the existing LMMatrix
        @note: if mtx parameter is present, it overrides individual parameters
        """
        if mtx is not None:
            mtxId = mtx.getId()
            mtxType = mtx.matrixType
            gridsetId = mtx.parentId
            gcmCode = mtx.gcmCode
            altpredCode = mtx.altpredCode
            dateCode = mtx.dateCode
            algCode = mtx.algorithmCode
        fullMtx = self._borg.getMatrix(mtxId, gridsetId, gridsetName, userId, 
                            mtxType, gcmCode, altpredCode, dateCode, algCode)
        return fullMtx

# .............................................................................
    def countMatrices(self, userId=None, matrixType=None, 
                      gcmCode=None, altpredCode=None, dateCode=None, 
                      algCode=None, keyword=None, gridsetId=None, 
                      afterTime=None, beforeTime=None, epsg=None, 
                      afterStatus=None, beforeStatus=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countMatrixColumns()
        """
        count = self._borg.countMatrices(userId, matrixType, gcmCode, 
                                altpredCode, dateCode, algCode, keyword, 
                                gridsetId, afterTime, beforeTime, epsg, 
                                afterStatus, beforeStatus)
        return count

# .............................................................................
    def listMatrices(self, firstRecNum, maxNum, userId=None, 
                     matrixType=None, gcmCode=None, altpredCode=None, 
                     dateCode=None, algCode=None, keyword=None, gridsetId=None, 
                     afterTime=None, beforeTime=None, epsg=None, 
                     afterStatus=None, beforeStatus=None, atom=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::listMatrices()
        """
        objs = self._borg.listMatrices(firstRecNum, maxNum, userId, matrixType, 
                gcmCode, altpredCode, dateCode, algCode, keyword, gridsetId, 
                afterTime, beforeTime, epsg, afterStatus, beforeStatus, atom)
        return objs

# ...............................................
    def findOrInsertTree(self, tree):
        """
        @copydoc LmServer.db.catalog_borg.Borg::findOrInsertTree()
        """
        newTree = self._borg.findOrInsertTree(tree)
        return newTree

# ...............................................
    def getTree(self, tree=None, treeId=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getTree()
        """
        existingTree = self._borg.getTree(tree, treeId)
        # access private attribute to see if it came back from db
        if existingTree is not None and existingTree._dlocation is None:
            existingTree.setDLocation()
        return existingTree

# .............................................................................
    def countTrees(self, userId=PUBLIC_USER, name=None, 
                        isBinary=None, isUltrametric=None, hasBranchLengths=None,
                        metastring=None, afterTime=None, beforeTime=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countTrees()
        """
        count = self._borg.countTrees(userId, name, isBinary, isUltrametric, 
                                    hasBranchLengths, metastring, afterTime, beforeTime)
        return count

# .............................................................................
    def listTrees(self, firstRecNum, maxNum, userId=PUBLIC_USER, name=None, 
                      isBinary=None, isUltrametric=None, hasBranchLengths=None,
                      metastring=None, afterTime=None, beforeTime=None, atom=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::listTrees()
        """
        objs = self._borg.listTrees(firstRecNum, maxNum, userId, 
                                        afterTime, beforeTime, name, metastring,  
                                        isBinary, isUltrametric, hasBranchLengths, atom)
        return objs
# ...............................................
    def getGridset(self, gridset=None, gridsetId=None, userId=None, name=None, 
                        fillMatrices=False):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getGridset()
        @param gridset: LmServer.legion.gridset.Gridset object containing 
                 matching attributes Id, userId, name
        @note: gridset object values override gridsetId, userId, name
        """
        if gridset is not None:
            gridsetId=gridset.getId() 
            userId = gridset.getUserId()
            name = gridset.name
        existingGridset = self._borg.getGridset(gridsetId, userId, name, 
                                                             fillMatrices)
        return existingGridset
    
# .............................................................................
    def countGridsets(self, userId, shpgrdLyrid=None, metastring=None, 
                            afterTime=None, beforeTime=None, epsg=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countGridsets()
        """
        count = self._borg.countGridsets(userId, shpgrdLyrid, metastring, 
                                                    afterTime, beforeTime, epsg)
        return count

# .............................................................................
    def listGridsets(self, firstRecNum, maxNum, userId=PUBLIC_USER, 
                          shpgrdLyrid=None, metastring=None, 
                          afterTime=None, beforeTime=None, epsg=None, atom=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::listGridsets()
        """
        objs = self._borg.listGridsets(firstRecNum, maxNum, userId, shpgrdLyrid, 
                                            metastring, afterTime, beforeTime, epsg, atom)
        return objs

# ...............................................
    def findTaxonSource(self, taxonSourceName):
        """
        @copydoc LmServer.db.catalog_borg.Borg::findTaxonSource()
        """
        txSourceId, url, moddate = self._borg.findTaxonSource(taxonSourceName)
        return txSourceId, url, moddate

# ...............................................
    def getTaxonSource(self, tsId=None, tsName=None, tsUrl=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getTaxonSource()
        """
        ts = self._borg.getTaxonSource(tsId, tsName, tsUrl)
        return ts
    
# ...............................................
    def findOrInsertTaxon(self, taxonSourceId=None, taxonKey=None, sciName=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::findOrInsertTaxon()
        """
        sciname = self._borg.findOrInsertTaxon(taxonSourceId, taxonKey, sciName)
        return sciname

# ...............................................
    def getTaxon(self, squid=None, taxonSourceId=None, taxonKey=None,
                     userId=None, taxonName=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getTaxon()
        """
        sciname = self._borg.getTaxon(squid, taxonSourceId, taxonKey, userId, 
                                                taxonName)
        return sciname

# ...............................................
    def getScenario(self, idOrCode, userId=None, fillLayers=False):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getScenario()
        """
        sid = code = None
        try:
            sid = int(idOrCode)
        except:
            code = idOrCode
        scenario = self._borg.getScenario(scenid=sid, code=code, userId=userId,
                                                     fillLayers=fillLayers)
        return scenario
    
# .............................................................................
    def countScenarios(self, userId=PUBLIC_USER, afterTime=None, beforeTime=None, 
                             epsg=None, gcmCode=None, altpredCode=None, dateCode=None,
                             scenPackageId=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countScenarios()
        """
        count = self._borg.countScenarios(userId, afterTime, beforeTime, epsg,
                                                          gcmCode, altpredCode, dateCode, 
                                                          scenPackageId)
        return count

# .............................................................................
    def listScenarios(self, firstRecNum, maxNum, userId=PUBLIC_USER, 
                            afterTime=None, beforeTime=None, epsg=None, gcmCode=None, 
                            altpredCode=None, dateCode=None, scenPackageId=None, 
                            atom=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countScenarios()
        """
        count = self._borg.listScenarios(firstRecNum, maxNum, userId, 
                                                    afterTime, beforeTime, epsg, gcmCode, 
                                                    altpredCode, dateCode, scenPackageId, 
                                                    atom)
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
        newOcc = self._borg.findOrInsertOccurrenceSet(occ)
        return newOcc
            
# .............................................................................
    def countOccurrenceSets(self, userId=None, squid=None, minOccurrenceCount=None, 
                    displayName=None, afterTime=None, beforeTime=None, epsg=None, 
                    afterStatus=None, beforeStatus=None, gridsetId=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countOccurrenceSets()
        """
        count = self._borg.countOccurrenceSets(userId, squid, minOccurrenceCount, 
                displayName, afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                gridsetId)
        return count

# .............................................................................
    def listOccurrenceSets(self, firstRecNum, maxNum, userId=None, 
                           squid=None, minOccurrenceCount=None, displayName=None, 
                           afterTime=None, beforeTime=None, epsg=None, 
                           afterStatus=None, beforeStatus=None, gridsetId=None,
                           atom=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::listOccurrenceSets()
        """
        objs = self._borg.listOccurrenceSets(firstRecNum, maxNum, userId, 
                                             squid, minOccurrenceCount, 
                                             displayName, afterTime, beforeTime, 
                                             epsg, afterStatus, beforeStatus, 
                                             gridsetId, atom)
        return objs

# ...............................................
    def summarizeOccurrenceSetsForGridset(self, gridsetid):
        """
        @copydoc LmServer.db.catalog_borg.Borg::listOccurrenceSets()
        """
        status_total_pairs = self._borg.summarizeOccurrenceSetsForGridset(gridsetid)
        return status_total_pairs

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
    def countSDMProjects(self, userId=None, squid=None, displayName=None, 
                                afterTime=None, beforeTime=None, epsg=None, 
                                afterStatus=None, beforeStatus=None, occsetId=None, 
                                algCode=None, mdlscenCode=None, prjscenCode=None, 
                                gridsetId=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countSDMProjects()
        """
        if userId is None and gridsetId is None:
            raise LMError('Must provide either userId or gridsetId')
        count = self._borg.countSDMProjects(userId, squid, displayName, 
                              afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                              occsetId, algCode, mdlscenCode, prjscenCode, gridsetId)
        return count

# ...............................................
    def summarizeSDMProjectsForGridset(self, gridsetid):
        """
        @copydoc LmServer.db.catalog_borg.Borg::summarizeSDMProjectsForGridset()
        """
        status_total_pairs = self._borg.summarizeSDMProjectsForGridset(gridsetid)
        return status_total_pairs

# ...............................................
    def summarizeMatricesForGridset(self, gridsetid, mtx_type=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::summarizeMatricesForGridset()
        """
        status_total_pairs = self._borg.summarizeMatricesForGridset(gridsetid, mtx_type)
        return status_total_pairs

# ...............................................
    def summarizeMtxColumnsForGridset(self, gridsetid, mtx_type=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::summarizeMtxColumnsForGridset()
        """
        status_total_pairs = self._borg.summarizeMtxColumnsForGridset(gridsetid, 
                                                                      mtx_type)
        return status_total_pairs


# .............................................................................
    def listSDMProjects(self, firstRecNum, maxNum, userId=None, squid=None, 
                        displayName=None, afterTime=None, beforeTime=None, 
                        epsg=None, afterStatus=None, beforeStatus=None, 
                        occsetId=None, algCode=None, mdlscenCode=None, 
                        prjscenCode=None, gridsetId=None, atom=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::listSDMProjects()
        """
        if userId is None and gridsetId is None:
            raise LMError('Must provide either userId or gridsetId')
        objs = self._borg.listSDMProjects(firstRecNum, maxNum, userId, squid, displayName, 
                              afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                              occsetId, algCode, mdlscenCode, prjscenCode, gridsetId, 
                              atom)
        return objs
    
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
            if lyr.dataFormat in LMFormat.GDALDrivers():
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
        @param modtime: timestamp of modification, in MJD format 
        @param email: email address for notifications 
        """
        prjs = []
        for projScen in projScenList:
            prj = SDMProjection(occ, alg, mdlScen, projScen, 
                                dataFormat=LMFormat.getDefaultGDAL().driver,
                                status=JobStatus.GENERAL, statusModTime=modtime)
            newOrExistingPrj = self._borg.findOrInsertSDMProject(prj)
            # Instead of re-pulling unchanged scenario layers, update 
            # with input arguments
            newOrExistingPrj._modelScenario = mdlScen
            newOrExistingPrj._projScenario = projScen
            # Rollback if finished
            if JobStatus.finished(newOrExistingPrj.status):
                newOrExistingPrj.updateStatus(JobStatus.GENERAL, modTime=modtime)
                newOrExistingPrj = self.updateSDMProject(newOrExistingPrj)
            
            prjs.append(newOrExistingPrj)
        return prjs

# ...............................................
    def initOrRollbackSDMChain(self, occ, algList, mdlScen, prjScenList, 
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
    def insertMFChain(self, mfchain, gridsetId):
        """
        @copydoc LmServer.db.catalog_borg.Borg::insertMFChain()
        """
        mfchain = self._borg.insertMFChain(mfchain, gridsetId)
        return mfchain
    
# ...............................................
    def getMFChain(self, mfprocessid):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getMFChain()
        """
        mfchain = self._borg.getMFChain(mfprocessid)
        return mfchain

# ...............................................
    def findMFChains(self, count, userId=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::findMFChains()
        """
        mfchainList = self._borg.findMFChains(count, userId, 
                                            JobStatus.INITIALIZE, JobStatus.PULL_REQUESTED)
        return mfchainList

# ...............................................
    def deleteMFChainsReturnFilenames(self, gridsetid):
        """
        @copydoc LmServer.db.catalog_borg.Borg::deleteMFChainsReturnFilenames()
        """
        flist = self._borg.deleteMFChainsReturnFilenames(gridsetid)
        return flist
            
# .............................................................................
    def countPriorityMFChains(self, gridsetId):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countPriorityMFChains()
        """
        count = self._borg.countPriorityMFChains(gridsetId)
        return count   
    
# .............................................................................
    def countMFChains(self, userId=None, gridsetId=None, metastring=None,
                      afterStat=None, beforeStat=None, 
                      afterTime=None, beforeTime=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::countMFChains()
        """
        count = self._borg.countMFChains(userId, gridsetId, metastring, 
                                afterStat, beforeStat, afterTime, beforeTime)
        return count

# .............................................................................
    def listMFChains(self, firstRecNum, maxNum, 
                     userId=None, gridsetId=None, metastring=None,
                     afterStat=None, beforeStat=None, 
                     afterTime=None, beforeTime=None, atom=True):
        """
        @copydoc LmServer.db.catalog_borg.Borg::listMFChains()
        """
        objs = self._borg.listMFChains(firstRecNum, maxNum, userId, gridsetId, 
                metastring, afterStat, beforeStat, afterTime, beforeTime, atom)
        return objs

# ...............................................
    def summarizeMFChainsForGridset(self, gridsetid):
        """
        @copydoc LmServer.db.catalog_borg.Borg::summarizeMFChainsForGridset()
        """
        status_total_pairs = self._borg.summarizeMFChainsForGridset(gridsetid)
        return status_total_pairs

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

# ...............................................
    def deleteGridsetReturnFilenamesMtxcolids(self, gridsetId):
        """
        @copydoc LmServer.db.catalog_borg.Borg::deleteGridsetReturnFilenames()
        """
        mtxcolids = self._borg.deleteGridsetReturnMtxcolids(gridsetId)
        fnames = self._borg.deleteGridsetReturnFilenames(gridsetId)
        
        return fnames, mtxcolids

# ...............................................
    def deleteObsoleteUserGridsetsReturnFilenamesMtxcolids(self, userid, obsolete_time):
        """
        @copydoc LmServer.db.catalog_borg.Borg::deleteGridsetReturnFilenames()
        """
        allfilenames = []
        allmtxcolids = []
        grdids = self._borg.findUserGridsets(userid, obsolete_time=obsolete_time)
        for grdid in grdids:
            fnames, mtxcolids = self.deleteGridsetReturnFilenamesMtxcolids(grdid)
            allfilenames.extend(fnames)
            allmtxcolids.extend(mtxcolids)
        return allfilenames, allmtxcolids

    # ...............................................
    def deleteObsoleteSDMDataReturnIds(self, userid, beforetime, max_num=100):
        """
        @copydoc LmServer.db.catalog_borg.Borg::deleteObsoleteSDMDataReturnIds()
        """
        occids = self._borg.deleteObsoleteSDMDataReturnIds(userid, 
                                                           beforetime, max_num)
        mtxcolids = self._borg.deleteObsoleteSDMMtxcolsReturnIds(userid, 
                                                           beforetime, max_num)
        return occids, mtxcolids

# ...............................................
    def getMapServiceForSDMOccurrence(self, occLyrOrId):
        """
        @param mapFilename: absolute path of mapfile
        @return: LmServer.base.layerset.MapLayerSet containing objects for this
                    a map service
        """
        try:
            int(occLyrOrId)
        except:
            occLyrOrId = occLyrOrId.getId()
        occ = self.getOccurrenceSet(occId=occLyrOrId)
        lyrs = self.listSDMProjects(0, 500, userId=occ.getUserId(), 
                                             occsetId=occLyrOrId, atom=False)
        lyrs.append(occ)
        mapname = EarlJr().createBasename(LMFileType.SDM_MAP, objCode=occ.getId(), 
                                                     usr=occ.getUserId())
        mapsvc = MapLayerSet(mapname, layers=lyrs, dbId=occ.getId(), 
                            userId=occ.getUserId(), epsgcode=occ.epsgcode, 
                            bbox=occ.bbox, mapunits=occ.mapUnits, 
                            mapType=LMFileType.SDM_MAP)
        return mapsvc

# ...............................................
    def getMapServiceFromMapFilename(self, mapFilename):
        """
        @param mapFilename: absolute path of mapfile
        @return: LmServer.base.layerset.MapLayerSet containing objects for this
                    a map service
        """
        earl = EarlJr()
        (mapname, ancillary, usr, epsg, occsetId, gridsetId, 
         scencode) = earl.parseMapFilename(mapFilename)
        prefix = mapname.split(NAME_SEPARATOR)[0]
        filetype = FileFix.getMaptypeFromName(prefix=prefix)
        if filetype == LMFileType.SDM_MAP:
            mapsvc = self.getMapServiceForSDMOccurrence(occsetId)
        elif filetype == LMFileType.RAD_MAP:
            self.log.error('Mapping is not yet implemented for RAD_MAP')
        elif filetype == LMFileType.SCENARIO_MAP:
            mapsvc = self.getScenario(scencode, userId=usr, fillLayers=True)
        else:
            self.log.error('Mapping is available for SDM_MAP, SCENARIO_MAP, RAD_MAP')
        return mapsvc
    
# ...............................................
    def getOccLayersForGridset(self, gridsetid):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getOccLayersForGridset()
        """
        pams = self.getMatricesForGridset(gridsetid, mtxType=MatrixType.PAM)
        for pam in pams:
            occs = self._borg.getOccLayersForMatrix(pam.getId())
        return occs

# ...............................................
    def getSDMColumnsForGridset(self, gridsetid, returnColumns=True, 
                                returnProjections=True):
        """
        @summary: Get all existing MatrixColumns and SDMProjections that have  
                     SDMProjections as input layers for a Matrix
        @param mtxId: a database ID for the LmServer.legion.LMMatrix 
                            object to return columns for
        @param returnColumns: option to return MatrixColumn objects
        @param returnProjections: option to return SDMProjection objects
        @return: a list of tuples containing LmServer.legion.MatrixColumn
                    and LmServer.legion.SDMProjection objects.  Either may be None
                    if the option is False  
        """
        allPairs = []
        pams = self.getMatricesForGridset(gridsetid, mtxType=MatrixType.PAM)
        for pam in pams:
            colPrjPairs = self.getSDMColumnsForMatrix(pam.getId(), 
                                                      returnColumns, 
                                                      returnProjections)
            allPairs.extend(colPrjPairs)

        return allPairs

# ...............................................
    def getMatricesForGridset(self, gridsetid, mtxType=None):
        """
        @copydoc LmServer.db.catalog_borg.Borg::getMatricesForGridset()
        """
        mtxs = self._borg.getMatricesForGridset(gridsetid, mtxType)
        return mtxs


"""
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import (CONNECTION_PORT, DB_HOSTNAME,
                                                          PUBLIC_USER)

scribe = BorgScribe(ConsoleLogger())
scribe.openConnections()

usr = scribe.findUser(userId='cshl')
usr.setPassword('jetsons', False)
scribe.updateUser(usr)

usr = scribe.findUser(userId='radicchio')
usr.setPassword('jetsons', False)
scribe.updateUser(usr)

usr = scribe.findUser(userId='madadam')
usr.setPassword('jetsons', False)
scribe.updateUser(usr)

gset = scribe.getGridset(gridsetId=268)
x = scribe.countMFChains(gridsetId=268, afterStat=299, beforeStat=301)

mtx = scribe.getMatrix(mtxId=1410)
mcs = scribe.listMatrixColumns(0,100, userId='x', matrixId=1410)

scribe.closeConnections()
"""