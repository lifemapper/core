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
# .............................................................................
import mx.DateTime as dt
from osgeo.ogr import wkbPoint
import os

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat,
         OutputFormat, SERVER_PIPELINE_HEADING, SERVER_ENV_HEADING, MatrixType) 
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmServer.base.lmobj import LMError, LMObject
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (PUBLIC_ARCHIVE_NAME, LMFileType, 
                                         SPECIES_DATA_PATH)
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.sdmproj import SDMProjection
from LmServer.tools.occwoc import BisonWoC, iDigBioWoC, GBIFWoC, UserWoC

# .............................................................................
class ChristopherWalken(LMObject):
   """
   Class to ChristopherWalken with a species iterator through a sequence of 
   species data creating a Spud for each species.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, userId=None, archiveName=None, jsonFname=None, 
                priority=None, logger=None):
      """
      @summary Constructor for ChristopherWalken class which creates a Spud 
               (Single-species Makeflow chain) for a species.
      """
      super(ChristopherWalken, self).__init__()
      self.priority = priority
      self.name = '{}_{}_{}'.format(userId, self.__class__.__name__.lower(), 
                                    archiveName)      
      # Optionally use parent process logger
      if logger is None:
         logger = ScriptLogger(self.name)
      self.log = logger
      # Database connection
      try:
         self._scribe = BorgScribe(self.log)
         success = self._scribe.openConnections()
      except Exception, e:
         raise LMError(currargs='Exception opening database', prevargs=e.args)
      else:
         if not success:
            raise LMError(currargs='Failed to open database')
         else:
            logger.info('{} opened databases'.format(self.name))
       
      # JSON or ini based configuration
      if jsonFname is not None:
         raise LMError('JSON Walken is not yet implemented')
      else:
         # Get configuration for this pipeline
         (self.userId, self.archiveName, 
          boompath, cfg) = self.getConfig(userId=userId, archiveName=archiveName)
         self.boompath = boompath
         self.cfg = cfg
         
         (self.weaponOfChoice, self.epsg, self.algs, self.mdlScen, self.mdlMask, 
          self.prjScens, self.prjMask, boomGridset, 
          self.intersectParams) = self._getConfiguredObjects(boompath, cfg)
         self.boomGridset = boomGridset

      # Global PAM Matrix for each scenario
      self.globalPAMs = {}
      
      # One Global PAM for each scenario
      for prjscen in self.prjScens:
         self.globalPAMs[prjscen.code] = self.boomGridset.getPAMForCodes(
                        prjscen.gcmCode, prjscen.altpredCode, prjscen.dateCode)

# ...............................................
   def moveToStart(self):
      self.weaponOfChoice.moveToStart()
      
# ...............................................
   def saveNextStart(self, fail=False):
      self.weaponOfChoice.saveNextStart(fail=fail)
   
# ...............................................
   @property
   def currRecnum(self):
      return self.weaponOfChoice.currRecnum

# ...............................................
   @property
   def nextStart(self):
      return self.weaponOfChoice.nextStart
   

# ...............................................
   @property
   def complete(self):
      return self.weaponOfChoice.complete

# .............................................................................
   def _getOccWeaponOfChoice(self, cfg, epsg, boompath):
      # Get datasource and optional taxonomy source
      datasource = cfg.get(SERVER_PIPELINE_HEADING, 'DATASOURCE')
      try:
         taxonSourceName = TAXONOMIC_SOURCE[datasource]['name']
      except:
         taxonSourceName = None
         
      # Minimum number of points required for SDM modeling 
      minPoints = cfg.getint(SERVER_PIPELINE_HEADING, 'POINT_COUNT_MIN')
      # Expiration date for retrieved species data 
      expDate = dt.DateTime(cfg.getint(SERVER_PIPELINE_HEADING, 'SPECIES_EXP_YEAR'), 
                            cfg.getint(SERVER_PIPELINE_HEADING, 'SPECIES_EXP_MONTH'), 
                            cfg.getint(SERVER_PIPELINE_HEADING, 'SPECIES_EXP_DAY')).mjd
      # Get Weapon of Choice depending on type of Occurrence data to parse
      # Bison data
      if datasource == 'BISON':
         bisonTsn = Config().get(SERVER_PIPELINE_HEADING, 'BISON_TSN_FILENAME')
         bisonTsnFile = os.path.join(SPECIES_DATA_PATH, bisonTsn)
         weaponOfChoice = BisonWoC(self._scribe, self.userId, self.archiveName, 
                                   epsg, expDate, minPoints, bisonTsnFile, 
                                   taxonSourceName=taxonSourceName, 
                                   logger=self.log)
      # iDigBio data
      elif datasource == 'IDIGBIO':
         idigTaxonids = Config().get(SERVER_PIPELINE_HEADING, 'IDIG_FILENAME')
         idigTaxonidsFile = os.path.join(SPECIES_DATA_PATH, idigTaxonids)
         weaponOfChoice = iDigBioWoC(self._scribe, self.userId, self.archiveName, 
                                     epsg, expDate, minPoints, idigTaxonidsFile,
                                     taxonSourceName=taxonSourceName, 
                                     logger=self.log)
      # GBIF data
      elif datasource == 'GBIF':
         gbifTax = cfg.get(SERVER_PIPELINE_HEADING, 'GBIF_TAXONOMY_FILENAME')
         gbifTaxFile = os.path.join(SPECIES_DATA_PATH, gbifTax)
         gbifOcc = cfg.get(SERVER_PIPELINE_HEADING, 'GBIF_OCCURRENCE_FILENAME')
         gbifOccFile = os.path.join(SPECIES_DATA_PATH, gbifOcc)
         gbifProv = cfg.get(SERVER_PIPELINE_HEADING, 'GBIF_PROVIDER_FILENAME')
         gbifProvFile = os.path.join(SPECIES_DATA_PATH, gbifProv)
         weaponOfChoice = GBIFWoC(self._scribe, self.userId, self.archiveName, 
                                     epsg, expDate, minPoints, gbifOccFile,
                                     providerFname=gbifProvFile, 
                                     taxonSourceName=taxonSourceName, 
                                     logger=self.log)
      # User data, anything not above
      else:
         userOccData = cfg.get(SERVER_PIPELINE_HEADING, 
                               'USER_OCCURRENCE_DATA')
         userOccDelimiter = cfg.get(SERVER_PIPELINE_HEADING, 
                               'USER_OCCURRENCE_DATA_DELIMITER')
         userOccCSV = os.path.join(boompath, userOccData + OutputFormat.CSV)
         userOccMeta = os.path.join(boompath, userOccData + OutputFormat.METADATA)
         weaponOfChoice = UserWoC(self._scribe, self.userId, self.archiveName, 
                                     epsg, expDate, minPoints, userOccCSV,
                                     userOccMeta, userOccDelimiter, 
                                     logger=self.log)
      return weaponOfChoice

# .............................................................................
   def _getSDMParams(self, cfg, epsg):
      algorithms = []
      prjScens = []
      mdlMask = prjMask = None

      # Get algorithms for SDM modeling
      algCodes = cfg.getlist(SERVER_PIPELINE_HEADING, 'ALGORITHMS')
      for acode in algCodes:
         alg = Algorithm(acode)
         alg.fillWithDefaults()
         algorithms.append(alg)

      # Get environmental data model and projection scenarios
      mdlScenCode = cfg.get(SERVER_PIPELINE_HEADING, 'SCENARIO_PACKAGE_MODEL_SCENARIO')
      prjScenCodes = cfg.getlist(SERVER_PIPELINE_HEADING, 'SCENARIO_PACKAGE_PROJECTION_SCENARIOS')
      mdlScen = self._scribe.getScenario(mdlScenCode, user=self.userId, 
                                         fillLayers=True)
      if mdlScen is not None:
         if mdlScenCode not in prjScenCodes:
            prjScens.append(mdlScen)
         for pcode in prjScenCodes:
            scen = self._scribe.getScenario(pcode, user=self.userId, 
                                            fillLayers=True)
            if scen is not None:
               prjScens.append(scen)
            else:
               raise LMError('Failed to retrieve scenario {}'.format(pcode))
      else:
         raise LMError('Failed to retrieve scenario {}'.format(mdlScen))

      # Get optional model and project masks
      try:
         mdlMaskName = cfg.get(SERVER_PIPELINE_HEADING, 'MODEL_MASK_NAME')
         mdlMask = self._scribe.getLayer(userId=self.userId, 
                                         lyrName=mdlMaskName, epsg=self.epsg)
      except:
         pass
      try:
         prjMaskName = cfg.get(SERVER_PIPELINE_HEADING, 'PROJECTION_MASK_NAME')
         prjMask = self._scribe.getLayer(userId=self.userId, 
                                         lyrName=prjMaskName, epsg=self.epsg)
      except:
         pass

      return (algorithms, mdlScen, mdlMask, prjScens, prjMask)  

# .............................................................................
   def _getGlobalPamObjects(self, cfg, epsg):
      # Get existing intersect grid, gridset and parameters for Global PAM
      gridname = cfg.get(SERVER_PIPELINE_HEADING, 'GRID_NAME')
      intersectGrid = self._scribe.getShapeGrid(userId=self.userId, 
                                 lyrName=gridname, epsg=epsg)
      # Get  for Archive "Global PAM"
      tmpGS = Gridset(name=self.archiveName, shapeGrid=intersectGrid, 
                     epsgcode=epsg, userId=self.userId)
      boomGridset = self._scribe.getGridset(tmpGS, fillMatrices=True)
      boomGridset.setMatrixProcessType(ProcessType.CONCATENATE_MATRICES, 
                                       matrixType=MatrixType.PAM)
      intersectParams = {
         MatrixColumn.INTERSECT_PARAM_FILTER_STRING: 
            Config().get(SERVER_PIPELINE_HEADING, 'INTERSECT_FILTERSTRING'),
         MatrixColumn.INTERSECT_PARAM_VAL_NAME: 
            Config().get(SERVER_PIPELINE_HEADING, 'INTERSECT_VALNAME'),
         MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: 
            Config().getint(SERVER_PIPELINE_HEADING, 'INTERSECT_MINPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: 
            Config().getint(SERVER_PIPELINE_HEADING, 'INTERSECT_MAXPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: 
            Config().getint(SERVER_PIPELINE_HEADING, 'INTERSECT_MINPERCENT')}

      return (boomGridset, intersectParams)  

# .............................................................................
   @classmethod
   def getConfig(cls, userId=None, archiveName=None):
      """
      @summary: Get user, archive, path, and configuration object 
      """
      cfg = boompath = None
      earl = EarlJr()
      # Get user-archive configuration file
      if userId is not None and archiveName is not None:
         boompath = earl.createDataPath(userId, LMFileType.BOOM_CONFIG)
         archiveConfigFile = os.path.join(boompath, '{}{}'
                              .format(archiveName, OutputFormat.CONFIG))
         print 'Config file at {}'.format(archiveConfigFile)
         if os.path.exists(archiveConfigFile):
            cfg = Config(fns=[archiveConfigFile])
      else:
         cfg = Config()

      # Default userId
      if userId is None:
         userId = cfg.get(SERVER_ENV_HEADING, 'PUBLIC_USER')
      # Default archiveName
      if archiveName is None:
         archiveName = PUBLIC_ARCHIVE_NAME
      # Path to configuration and makeflow
      if boompath is None:
         boompath = earl.createDataPath(userId, LMFileType.BOOM_CONFIG)
      return userId, archiveName, boompath, cfg
         
# .............................................................................
   def _getConfiguredObjects(self, boompath, cfg):
      """
      @summary: Get configured string values and any corresponding db objects 
      @TODO: Make all archive/default config keys consistent
      """
      epsg = cfg.getint(SERVER_PIPELINE_HEADING, 'SCENARIO_PACKAGE_EPSG')
      # Species parser/puller
      weaponOfChoice = self._getOccWeaponOfChoice(cfg, epsg, boompath)
      # SDM inputs
      (algorithms, mdlScen, mdlMask, 
       prjScens, prjMask) = self._getSDMParams(cfg, epsg)
      # Global PAM inputs
      (boomGridset, intersectParams) = self._getGlobalPamObjects(cfg, epsg)

      return (weaponOfChoice, epsg, algorithms, 
              mdlScen, mdlMask, prjScens, prjMask, 
              boomGridset, intersectParams)  

   # ...............................
   def _getJSONObjects(self):
      """
      @summary: Get provided values from JSON and any corresponding db objects 
      {
      OccDriver: datasource,
      Occdata: ( filename of taxonids, csv of datablocks, etc each handled differently)
      Algs: [algs/params]
      ScenPkg: mdlscen
      [prjscens]
      """
      pass
   
   # ...............................
   def startWalken(self):
      """
      @summary: Walks a list of Lifemapper objects for computation
      """
      objs = []
      occ = self.weaponOfChoice.getOne()
      objs.append(occ)

      currtime = dt.gmt().mjd
      # Sweep over input options
      for alg in self.algs:
         for prjscen in self.prjScens:
            # Add to Spud - SDM Project and MatrixColumn
            prj = self._createOrResetSDMProject(occ, alg, prjscen, currtime)
            objs.append(prj)
            mtx = self.globalPAMs[prjscen.code]
            mtxcol = self._createOrResetIntersect(prj, mtx, currtime)
            objs.append(mtxcol)

      spudObjs = [o for o in objs if o is not None]
      spud = self._createSpudMakeflow(spudObjs)
      return spud
      
   # ...............................
   def stopWalken(self):
      """
      @summary: Walks a list of Lifemapper objects for computation
      """
      self.saveNextStart()
      self.weaponOfChoice.close()
      
   # ...............................
   def insertMFChain(self, mfchain):
      """
      @summary: Inserts a MFChain (Potato or MasterPotatoHead, for aggregating 
                Spud MFChains)
      """
      updatedMFChain = self._scribe.insertMFChain(mfchain)
      return updatedMFChain
         
# ...............................................
   def _createOrResetIntersect(self, prj, mtx, currtime):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      mtxcol = None
      if prj is not None:
         # TODO: Save processType into the DB??
         if LMFormat.isGDAL(format=prj.dataFormat):
            ptype = ProcessType.INTERSECT_RASTER
         else:
            ptype = ProcessType.INTERSECT_VECTOR
   
         tmpCol = MatrixColumn(None, mtx.getId(), self.userId, 
                layer=prj, shapegrid=self.boomGridset.getShapegrid(), 
                intersectParams=self.intersectParams, 
                squid=prj.squid, ident=prj.ident,
                processType=ptype, metadata={}, matrixColumnId=None, 
                status=JobStatus.GENERAL, statusModTime=currtime)
         mtxcol = self._scribe.findOrInsertMatrixColumn(tmpCol)
         self.log.info('Found or inserted MatrixColumn {}'.format(mtxcol.getId()))
         # Reset processType (not in db)
         mtxcol.processType = ptype
         
         if self._doReset(mtxcol.status, mtxcol.statusModTime):
            self.log.info('Reseting MatrixColumn {}'.format(mtxcol.getId()))
            mtxcol.updateStatus(JobStatus.GENERAL, modTime=currtime)
            success = self._scribe.updateMatrixColumn(mtxcol)
      return mtxcol

# ...............................................
   def _doReset(self, status, statusModTime):
      doReset = False
      if (JobStatus.failed(status) or 
          JobStatus.waiting(status) or 
           # out-of-date
          (status == JobStatus.COMPLETE and statusModTime < self._obsoleteTime)):
         doReset = True
      return doReset

# ...............................................
   def _createOrResetSDMProject(self, occ, alg, prjscen, currtime):
      """
      @summary: Iterates through all input combinations to create or reset
                SDMProjections for the given occurrenceset.
      @param occ: OccurrenceSet for which to initialize or rollback all 
                  dependent projections
      """
      prj = None
      if occ is not None:
         tmpPrj = SDMProjection(occ, alg, self.mdlScen, prjscen, 
                        modelMask=self.mdlMask, projMask=self.prjMask, 
                        dataFormat=LMFormat.GTIFF.driver,
                        status=JobStatus.GENERAL, statusModTime=currtime)
         prj = self._scribe.findOrInsertSDMProject(tmpPrj)
         if prj is not None:
            self.log.info('Found or inserted SDMProject {}'.format(prj.getId()))
            # Instead of re-pulling unchanged scenario layers, masks, update 
            # with input objects
            prj._modelScenario = self.mdlScen
            prj.setModelMask(self.mdlMask)
            prj._projScenario = prjscen
            prj.setProjMask(self.prjMask)
            # Rollback if finished (new is already at initial status)
            if self._doReset(prj.status, prj.statusModTime):
               self.log.info('Reseting SDMProject {}'.format(prj.getId()))
               prj.updateStatus(JobStatus.GENERAL, modTime=currtime)
               success = self._scribe.updateSDMProject(prj)
      return prj

# ...............................................
   def _createSpudMakeflow(self, objs):
      updatedMFChain = None
      if objs:
         meta = {MFChain.META_CREATED_BY: os.path.basename(__file__)}
         newMFC = MFChain(self.userId, priority=self.priority, 
                           metadata=meta, status=JobStatus.GENERAL, 
                           statusModTime=dt.gmt().mjd)
         updatedMFChain = self._scribe.insertMFChain(newMFC)

         for o in objs:
            try:
               rules = o.computeMe()
            except Exception, e:
               self.log.info('Failed on object.compute {}, ({})'.format(type(o), 
                                                                        str(e)))
            else:
               updatedMFChain.addCommands(rules)
         
         updatedMFChain.write()
         self._scribe.updateObject(updatedMFChain)
         
      return updatedMFChain

"""
userId='ryan'
archiveName='Heuchera_archive'

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat,
                     OutputFormat, SERVER_PIPELINE_HEADING, SERVER_ENV_HEADING) 
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmServer.base.lmobj import LMError, LMObject
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (PUBLIC_ARCHIVE_NAME, LMFileType, 
                                         SPECIES_DATA_PATH)
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.sdmproj import SDMProjection
from LmServer.tools.occwoc import BisonWoC, iDigBioWoC, GBIFWoC, UserWoC
from LmServer.tools.cwalken import *

logger = ScriptLogger('-'.join([userId, archiveName]))
chris = ChristopherWalken(userId=userId, archiveName=archiveName, logger=logger)

chris.moveToStart()
chris.startWalken()

"""