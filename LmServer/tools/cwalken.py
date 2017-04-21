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
         OutputFormat, SERVER_BOOM_HEADING, MatrixType) 
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmServer.base.lmobj import LMError, LMObject
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (LMFileType, SPECIES_DATA_PATH)
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
   def __init__(self, configFname, userId=None, archiveName=None, 
                jsonFname=None, priority=None, scribe=None):
      """
      @summary Constructor for ChristopherWalken class which creates a Spud 
               (Single-species Makeflow chain) for a species.
      """
      super(ChristopherWalken, self).__init__()
      self.priority = priority
      basename, ext = os.path.splitext(os.path.basename(configFname))
      self.name = '{}_{}'.format(self.__class__.__name__.lower(), basename)       
      # Config
      if configFname is not None and os.path.exists(configFname):
         self.cfg = Config(siteFn=configFname)
      else:
         raise LMError(currargs='Missing config file {}'.format(configFname))
         
      # JSON or ini based configuration
      if jsonFname is not None:
         raise LMError('JSON Walken is not yet implemented')

      # Optionally use parent process Database connection
      if scribe is not None:
         self.log = scribe.log
         self._scribe = scribe
      else:
         self.log = ScriptLogger(self.name)
         try:
            self._scribe = BorgScribe(self.log)
            success = self._scribe.openConnections()
         except Exception, e:
            raise LMError(currargs='Exception opening database', prevargs=e.args)
         else:
            if not success:
               raise LMError(currargs='Failed to open database')
            else:
               self.log.info('{} opened databases'.format(self.name))

      (self.userId, self.archiveName, self.boompath, self.weaponOfChoice, 
       self.epsg, self.algs, self.mdlScen, self.mdlMask, self.prjScens, self.prjMask, 
       self.boomGridset, self.intersectParams, self.assemblePams) = \
                         self._getConfiguredObjects()

      # Global PAM Matrix for each scenario
      self.globalPAMs = {}
      
      # One Global PAM for each scenario
      if self.assemblePams:
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
   def _getOccWeaponOfChoice(self, userId, archiveName, epsg, boompath):
      # Get datasource and optional taxonomy source
      datasource = self.cfg.get(SERVER_BOOM_HEADING, 'DATASOURCE')
      try:
         taxonSourceName = TAXONOMIC_SOURCE[datasource]['name']
      except:
         taxonSourceName = None
         
      # Minimum number of points required for SDM modeling 
      minPoints = self.cfg.getint(SERVER_BOOM_HEADING, 'POINT_COUNT_MIN')
      # Expiration date for retrieved species data 
      expDate = dt.DateTime(self.cfg.getint(SERVER_BOOM_HEADING, 'SPECIES_EXP_YEAR'), 
                            self.cfg.getint(SERVER_BOOM_HEADING, 'SPECIES_EXP_MONTH'), 
                            self.cfg.getint(SERVER_BOOM_HEADING, 'SPECIES_EXP_DAY')).mjd
      # Get Weapon of Choice depending on type of Occurrence data to parse
      # Bison data
      if datasource == 'BISON':
         bisonTsn = Config().get(SERVER_BOOM_HEADING, 'BISON_TSN_FILENAME')
         bisonTsnFile = os.path.join(SPECIES_DATA_PATH, bisonTsn)
         weaponOfChoice = BisonWoC(self._scribe, userId, archiveName, 
                                   epsg, expDate, minPoints, bisonTsnFile, 
                                   taxonSourceName=taxonSourceName, 
                                   logger=self.log)
      # iDigBio data
      elif datasource == 'IDIGBIO':
         idigTaxonids = Config().get(SERVER_BOOM_HEADING, 'IDIG_FILENAME')
         idigTaxonidsFile = os.path.join(SPECIES_DATA_PATH, idigTaxonids)
         weaponOfChoice = iDigBioWoC(self._scribe, userId, archiveName, 
                                     epsg, expDate, minPoints, idigTaxonidsFile,
                                     taxonSourceName=taxonSourceName, 
                                     logger=self.log)
      # GBIF data
      elif datasource == 'GBIF':
         gbifTax = self.cfg.get(SERVER_BOOM_HEADING, 'GBIF_TAXONOMY_FILENAME')
         gbifTaxFile = os.path.join(SPECIES_DATA_PATH, gbifTax)
         gbifOcc = self.cfg.get(SERVER_BOOM_HEADING, 'GBIF_OCCURRENCE_FILENAME')
         gbifOccFile = os.path.join(SPECIES_DATA_PATH, gbifOcc)
         gbifProv = self.cfg.get(SERVER_BOOM_HEADING, 'GBIF_PROVIDER_FILENAME')
         gbifProvFile = os.path.join(SPECIES_DATA_PATH, gbifProv)
         weaponOfChoice = GBIFWoC(self._scribe, userId, archiveName, 
                                     epsg, expDate, minPoints, gbifOccFile,
                                     providerFname=gbifProvFile, 
                                     taxonSourceName=taxonSourceName, 
                                     logger=self.log)
      # User data, anything not above
      else:
         userOccData = self.cfg.get(SERVER_BOOM_HEADING, 
                               'USER_OCCURRENCE_DATA')
         userOccDelimiter = self.cfg.get(SERVER_BOOM_HEADING, 
                               'USER_OCCURRENCE_DATA_DELIMITER')
         userOccCSV = os.path.join(boompath, userOccData + OutputFormat.CSV)
         userOccMeta = os.path.join(boompath, userOccData + OutputFormat.METADATA)
         weaponOfChoice = UserWoC(self._scribe, userId, archiveName, 
                                     epsg, expDate, minPoints, userOccCSV,
                                     userOccMeta, userOccDelimiter, 
                                     logger=self.log)
      return weaponOfChoice

# .............................................................................
   def _getSDMParams(self, userId, epsg):
      algorithms = []
      prjScens = []
      mdlMask = prjMask = None

      # Get algorithms for SDM modeling
      algCodes = self.cfg.getlist(SERVER_BOOM_HEADING, 'ALGORITHMS')
      for acode in algCodes:
         alg = Algorithm(acode)
         alg.fillWithDefaults()
         algorithms.append(alg)

      # Get environmental data model and projection scenarios
      mdlScenCode = self.cfg.get(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_MODEL_SCENARIO')
      prjScenCodes = self.cfg.getlist(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_PROJECTION_SCENARIOS')
      mdlScen = self._scribe.getScenario(mdlScenCode, user=userId, 
                                         fillLayers=True)
      if mdlScen is not None:
         if mdlScenCode not in prjScenCodes:
            prjScens.append(mdlScen)
         for pcode in prjScenCodes:
            scen = self._scribe.getScenario(pcode, user=userId, fillLayers=True)
            if scen is not None:
               prjScens.append(scen)
            else:
               raise LMError('Failed to retrieve scenario {}'.format(pcode))
      else:
         raise LMError('Failed to retrieve scenario {}'.format(mdlScen))

      # Get optional model and project masks
      try:
         mdlMaskName = self.cfg.get(SERVER_BOOM_HEADING, 'MODEL_MASK_NAME')
         mdlMask = self._scribe.getLayer(userId=userId, 
                                         lyrName=mdlMaskName, epsg=epsg)
      except:
         pass
      try:
         prjMaskName = self.cfg.get(SERVER_BOOM_HEADING, 'PROJECTION_MASK_NAME')
         prjMask = self._scribe.getLayer(userId=userId, 
                                         lyrName=prjMaskName, epsg=epsg)
      except:
         pass

      return (algorithms, mdlScen, mdlMask, prjScens, prjMask)  

# .............................................................................
   def _getGlobalPamObjects(self, userId, archiveName, epsg):
      # Get existing intersect grid, gridset and parameters for Global PAM
      gridname = self.cfg.get(SERVER_BOOM_HEADING, 'GRID_NAME')
      intersectGrid = self._scribe.getShapeGrid(userId=userId, lyrName=gridname, 
                                                epsg=epsg)
      # Get  for Archive "Global PAM"
      tmpGS = Gridset(name=archiveName, shapeGrid=intersectGrid, 
                     epsgcode=epsg, userId=userId)
      boomGridset = self._scribe.getGridset(tmpGS, fillMatrices=True)
      boomGridset.setMatrixProcessType(ProcessType.CONCATENATE_MATRICES, 
                                       matrixType=MatrixType.PAM)
      intersectParams = {
         MatrixColumn.INTERSECT_PARAM_FILTER_STRING: 
            self.cfg.get(SERVER_BOOM_HEADING, 'INTERSECT_FILTERSTRING'),
         MatrixColumn.INTERSECT_PARAM_VAL_NAME: 
            self.cfg.get(SERVER_BOOM_HEADING, 'INTERSECT_VALNAME'),
         MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: 
            self.cfg.getint(SERVER_BOOM_HEADING, 'INTERSECT_MINPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: 
            self.cfg.getint(SERVER_BOOM_HEADING, 'INTERSECT_MAXPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: 
            self.cfg.getint(SERVER_BOOM_HEADING, 'INTERSECT_MINPERCENT')}

      return (boomGridset, intersectParams)  

# .............................................................................
   @classmethod
   def getConfig(cls, configFname):
      """
      @summary: Get user, archive, path, and configuration object 
      """
      if configFname is None or not os.path.exists(configFname):
         raise LMError(currargs='Missing config file {}'.format(configFname))
      config = Config(siteFn=configFname)
      return config
         
# .............................................................................
   def _getConfiguredObjects(self):
      """
      @summary: Get configured string values and any corresponding db objects 
      @TODO: Make all archive/default config keys consistent
      """
      userId = self.cfg.get(SERVER_BOOM_HEADING, 'ARCHIVE_USER')
      archiveName = self.cfg.get(SERVER_BOOM_HEADING, 'ARCHIVE_NAME')
      # Get user-archive configuration file
      if userId is None or archiveName is None:
         raise LMError(currargs='Missing ARCHIVE_USER or ARCHIVE_NAME in {}'
                       .format(self.cfg.configFiles))
      earl = EarlJr()
      boompath = earl.createDataPath(userId, LMFileType.BOOM_CONFIG)
      epsg = self.cfg.getint(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_EPSG')
      # Species parser/puller
      weaponOfChoice = self._getOccWeaponOfChoice(userId, archiveName, epsg, 
                                                  boompath)
      # SDM inputs
      (algorithms, mdlScen, mdlMask, 
       prjScens, prjMask) = self._getSDMParams(userId, epsg)
      # Global PAM inputs
      (boomGridset, intersectParams) = self._getGlobalPamObjects(userId, 
                                                            archiveName, epsg)
      assemblePams = self.cfg.getboolean(SERVER_BOOM_HEADING, 'ASSEMBLE_PAMS')

      return (userId, archiveName, boompath, weaponOfChoice, epsg, algorithms, 
              mdlScen, mdlMask, prjScens, prjMask, 
              boomGridset, intersectParams, assemblePams)  

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
      @return: Single-species MFChain (spud), dictionary of 
               scenarioCode: PAV filename for input into multi-species
               MFChains (potatoInputs)
      """
      objs = []
      currtime = dt.gmt().mjd
      potatoInputs = {}
      
      occ = self.weaponOfChoice.getOne()
      if occ:
         objs.append(occ)   
         # Sweep over input options
         # TODO: This puts all prjScen PAVs with diff algorithms into same matrix.
         #       Change this for BOOM jobs!! 
         for alg in self.algs:
            for prjscen in self.prjScens:
               # Add to Spud - SDM Project and MatrixColumn
               prj = self._createOrResetSDMProject(occ, alg, prjscen, currtime)
               objs.append(prj)
               mtx = self.globalPAMs[prjscen.code]
               mtxcol = self._createOrResetIntersect(prj, mtx, currtime)
               objs.append(mtxcol)
               potatoInputs[prjscen.code] = mtxcol.getTargetFilename()
   
      spudObjs = [o for o in objs if o is not None]
      spud = self._createSpudMakeflow(spudObjs)
      return spud, potatoInputs
      
   # ...............................
   def stopWalken(self):
      """
      @summary: Walks a list of Lifemapper objects for computation
      """
      self.log.info('Saving next start {} ...'.format(self.nextStart))
      self.saveNextStart()
      self.weaponOfChoice.close()
      
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
      rules = []
      if objs:
         for o in objs:
            # Create MFChain with metadata
            if updatedMFChain is None:
               try:
                  # Present on OccurrenceLayer, SDMProjection, MatrixColumn
                  squid = o.squid
                  speciesName = o.displayName
               except:
                  pass
               else:
                  meta = {MFChain.META_CREATED_BY: os.path.basename(__file__),
                          MFChain.META_DESC: 'Spud for User {}, Archive {}, Species {}'
                          .format(self.userId, self.archiveName, speciesName),
                          MFChain.META_SQUID: squid}
                  newMFC = MFChain(self.userId, priority=self.priority, 
                                    metadata=meta, status=JobStatus.GENERAL, 
                                    statusModTime=dt.gmt().mjd)
                  updatedMFChain = self._scribe.insertMFChain(newMFC)
            # Get rules
            try:
               rules.extend(o.computeMe())
            except Exception, e:
               self.log.info('Failed on object.compute {}, ({})'.format(type(o), 
                                                                        str(e)))
         updatedMFChain.addCommands(rules)
         updatedMFChain.write()
         updatedMFChain.updateStatus(JobStatus.INITIALIZE)
         self._scribe.updateObject(updatedMFChain)
         
      return updatedMFChain

"""
userId='ryan'
archiveName='Heuchera_archive'

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat,
                     OutputFormat, SERVER_BOOM_HEADING) 
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