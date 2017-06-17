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

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat,
          SERVER_BOOM_HEADING, MatrixType) 
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (LMFileType, SPECIES_DATA_PATH)
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.sdmproj import SDMProjection
from LmServer.tools.occwoc import BisonWoC, GBIFWoC, UserWoC

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
      self.name = self.__class__.__name__.lower()
      self.priority = priority
      self.configFname = configFname
      baseAbsFilename, ext = os.path.splitext(configFname)
      basename = os.path.basename(baseAbsFilename)
      # Chris writes this file when completed walking through species data
      self.walkedArchiveFname = os.path.join(baseAbsFilename, LMFormat.LOG.ext)
#       basename, ext = os.path.splitext(os.path.basename(configFname))
      self.name = '{}_{}'.format(self.__class__.__name__.lower(), basename)       
      # Config
      if configFname is not None and os.path.exists(configFname):
         self.cfg = Config(siteFn=configFname)
      else:
         raise LMError(currargs='Missing config file {}'.format(configFname))
      
      baseFilename, ext = os.path.splitext(configFname)
         
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
       self.epsg, self.minPoints, self.algs, self.mdlScen, self.mdlMask, 
       self.prjScens, self.prjMask, self.boomGridset, self.intersectParams, 
       self.assemblePams) = self._getConfiguredObjects()

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
      useGBIFTaxonIds = False
      # Get datasource and optional taxonomy source
      datasource = self.cfg.get(SERVER_BOOM_HEADING, 'DATASOURCE')
      try:
         taxonSourceName = TAXONOMIC_SOURCE[datasource]['name']
      except:
         taxonSourceName = None
         
      # Expiration date for retrieved species data 
      expDate = dt.DateTime(self.cfg.getint(SERVER_BOOM_HEADING, 'SPECIES_EXP_YEAR'), 
                            self.cfg.getint(SERVER_BOOM_HEADING, 'SPECIES_EXP_MONTH'), 
                            self.cfg.getint(SERVER_BOOM_HEADING, 'SPECIES_EXP_DAY')).mjd
      # Get Weapon of Choice depending on type of Occurrence data to parse
      # GBIF data
      if datasource == 'GBIF':
#          gbifTax = self.cfg.get(SERVER_BOOM_HEADING, 'GBIF_TAXONOMY_FILENAME')
#          gbifTaxFile = os.path.join(SPECIES_DATA_PATH, gbifTax)
         gbifOcc = self.cfg.get(SERVER_BOOM_HEADING, 'GBIF_OCCURRENCE_FILENAME')
         gbifOccFile = os.path.join(SPECIES_DATA_PATH, gbifOcc)
         gbifProv = self.cfg.get(SERVER_BOOM_HEADING, 'GBIF_PROVIDER_FILENAME')
         gbifProvFile = os.path.join(SPECIES_DATA_PATH, gbifProv)
         weaponOfChoice = GBIFWoC(self._scribe, userId, archiveName, 
                                     epsg, expDate, gbifOccFile,
                                     providerFname=gbifProvFile, 
                                     taxonSourceName=taxonSourceName, 
                                     logger=self.log)
      # Bison data
      elif datasource == 'BISON':
         bisonTsn = Config().get(SERVER_BOOM_HEADING, 'BISON_TSN_FILENAME')
         bisonTsnFile = os.path.join(SPECIES_DATA_PATH, bisonTsn)
         weaponOfChoice = BisonWoC(self._scribe, userId, archiveName, 
                                   epsg, expDate, bisonTsnFile, 
                                   taxonSourceName=taxonSourceName, 
                                   logger=self.log)
      else:
         # iDigBio data
         if datasource == 'IDIGBIO':
            useGBIFTaxonIds = True
            occData = self.cfg.get(SERVER_BOOM_HEADING, 'IDIG_OCCURRENCE_DATA')
            occDelimiter = self.cfg.get(SERVER_BOOM_HEADING, 
                                        'IDIG_OCCURRENCE_DATA_DELIMITER') 
            occCSV = os.path.join(SPECIES_DATA_PATH, occData + LMFormat.CSV.ext)
            occMeta = os.path.join(SPECIES_DATA_PATH, 
                                   occData + LMFormat.METADATA.ext)
         # User data, anything not above
         else:
            useGBIFTaxonIds = False
            occData = self.cfg.get(SERVER_BOOM_HEADING, 'USER_OCCURRENCE_DATA')
            occDelimiter = self.cfg.get(SERVER_BOOM_HEADING, 
                                        'USER_OCCURRENCE_DATA_DELIMITER') 
            occCSV = os.path.join(boompath, occData + LMFormat.CSV.ext)
            occMeta = os.path.join(boompath, occData + LMFormat.METADATA.ext)
            
         weaponOfChoice = UserWoC(self._scribe, userId, archiveName, 
                                  epsg, expDate, occCSV, occMeta, 
                                  occDelimiter, logger=self.log, 
                                  useGBIFTaxonomy=useGBIFTaxonIds)
      return weaponOfChoice

# .............................................................................
   def _getSDMParams(self, userId, epsg):
      algorithms = []
      prjScens = []
      mdlMask = prjMask = None
      
      minPoints = self.cfg.getint(SERVER_BOOM_HEADING, 'POINT_COUNT_MIN')

      # Get algorithms for SDM modeling
      algCodes = self.cfg.getlist(SERVER_BOOM_HEADING, 'ALGORITHMS')
      for acode in algCodes:
         alg = Algorithm(acode)
         alg.fillWithDefaults()
         algorithms.append(alg)

      # Get environmental data model and projection scenarios
      mdlScenCode = self.cfg.get(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_MODEL_SCENARIO')
      prjScenCodes = self.cfg.getlist(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_PROJECTION_SCENARIOS')
      mdlScen = self._scribe.getScenario(mdlScenCode, fillLayers=True)
      if mdlScen is not None:
         if mdlScenCode not in prjScenCodes:
            prjScens.append(mdlScen)
         for pcode in prjScenCodes:
            scen = self._scribe.getScenario(pcode, fillLayers=True)
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

      return (minPoints, algorithms, mdlScen, mdlMask, prjScens, prjMask)  

# .............................................................................
   def _getGlobalPamObjects(self, userId, archiveName, epsg):
      # Get existing intersect grid, gridset and parameters for Global PAM
      gridname = self.cfg.get(SERVER_BOOM_HEADING, 'GRID_NAME')
      intersectGrid = self._scribe.getShapeGrid(userId=userId, lyrName=gridname, 
                                                epsg=epsg)
      # Global PAM and Scenario GRIM for each scenario
      boomGridset = self._scribe.getGridset(name=archiveName, userId=userId, 
                                            fillMatrices=True)
      boomGridset.setMatrixProcessType(ProcessType.CONCATENATE_MATRICES, 
                                       matrixTypes=[MatrixType.PAM, 
                                                    MatrixType.ROLLING_PAM, 
                                                    MatrixType.GRIM])
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
      (minPoints, algorithms, mdlScen, mdlMask, 
       prjScens, prjMask) = self._getSDMParams(userId, epsg)
      # Global PAM inputs
      (boomGridset, intersectParams) = self._getGlobalPamObjects(userId, 
                                                            archiveName, epsg)
      assemblePams = self.cfg.getboolean(SERVER_BOOM_HEADING, 'ASSEMBLE_PAMS')

      return (userId, archiveName, boompath, weaponOfChoice, epsg, 
              minPoints, algorithms, mdlScen, mdlMask, prjScens, prjMask, 
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
      pcount = prcount = icount = ircount = 0
      # WeaponOfChoice resets old or failed Occurrenceset
      occ = self.weaponOfChoice.getOne()
      if self.weaponOfChoice.finishedInput:
         self._writeDoneWalkenFile()
      if occ:
         # Process existing OccurrenceLayer if incomplete, obsolete, or failed
         objs.append(occ)
         # Sweep over input options
         # TODO: This puts all prjScen PAVs with diff algorithms into same matrix.
         #       Change this for BOOM jobs!! 
         if occ.queryCount >= self.minPoints:
            for alg in self.algs:
               for prjscen in self.prjScens:
                  # Add to Spud - SDM Project and MatrixColumn
                  prj, pReset = self._createOrResetSDMProject(occ, alg, prjscen, 
                                                              currtime)
                  if prj is not None:
                     pcount += 1
                     if pReset: prcount += 1 
                     objs.append(prj)
                     mtx = self.globalPAMs[prjscen.code]
                     mtxcol, mReset = self._createOrResetIntersect(prj, mtx, 
                                                                   currtime)
                     if mtxcol is not None:
                        icount += 1
                        if mReset: ircount += 1 
                        objs.append(mtxcol)
                        potatoInputs[prjscen.code] = mtxcol.getTargetFilename()
   
            self.log.info('   Will compute {} projections, {} matrixColumns ( {}, {} reset)'
                          .format(pcount, icount, prcount, ircount))
      spudObjs = [o for o in objs if o is not None]
      spud = self._createSpudMakeflow(spudObjs)
      return spud, potatoInputs
      
   # ...............................
   def stopWalken(self):
      """
      @summary: Walks a list of Lifemapper objects for computation
      """
      if not self.weaponOfChoice.complete:
         self.log.info('Christopher, stop walken')
         self.log.info('Saving next start {} ...'.format(self.nextStart))
         self.saveNextStart()
         self.weaponOfChoice.close()
      else:
         self.log.info('Christopher is done walken')
      
# ...............................................
   def _createOrResetIntersect(self, prj, mtx, currtime):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      mtxcol = None
      reset = False
      if prj is not None:
         # TODO: Save processType into the DB??
         if LMFormat.isGDAL(driver=prj.dataFormat):
            ptype = ProcessType.INTERSECT_RASTER
         else:
            ptype = ProcessType.INTERSECT_VECTOR
   
         tmpCol = MatrixColumn(None, mtx.getId(), self.userId, 
                layer=prj, shapegrid=self.boomGridset.getShapegrid(), 
                intersectParams=self.intersectParams, 
                squid=prj.squid, ident=prj.ident,
                processType=ptype, metadata={}, matrixColumnId=None, 
                postToSolr=self.assemblePams,
                status=JobStatus.GENERAL, statusModTime=currtime)
         mtxcol = self._scribe.findOrInsertMatrixColumn(tmpCol)
         if mtxcol is not None:
            self.log.debug('Found/inserted MatrixColumn {}'.format(mtxcol.getId()))

            # Reset processType (not in db)
            mtxcol.processType = ptype
            
            # Rollback if obsolete or failed
            reset = self._doReset(mtxcol.status, mtxcol.statusModTime)
            if reset:
               self.log.debug('Reset MatrixColumn {}'.format(mtxcol.getId()))
               mtxcol.updateStatus(JobStatus.GENERAL, modTime=currtime)
               success = self._scribe.updateMatrixColumn(mtxcol)
      return mtxcol, reset

# ...............................................
   def _doReset(self, status, statusModTime):
      doReset = False
      if (JobStatus.failed(status) or 
          (status == JobStatus.COMPLETE and 
           statusModTime < self.weaponOfChoice.expirationDate)):
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
            self.log.debug('Found/inserted SDMProject {}'.format(prj.getId()))
            # Fill in projection with input scenario layers, masks
            prj._modelScenario = self.mdlScen
            prj.setModelMask(self.mdlMask)
            prj._projScenario = prjscen
            prj.setProjMask(self.prjMask)
            # Rollback if obsolete or failed
            reset = self._doReset(prj.status, prj.statusModTime)
            if reset:
               self.log.debug('Reset SDMProject {}'.format(prj.getId()))
               prj.updateStatus(JobStatus.GENERAL, modTime=currtime)
               success = self._scribe.updateSDMProject(prj)
      return prj, reset

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
                  meta = {MFChain.META_CREATED_BY: self.name,
                          MFChain.META_DESC: 'Spud for User {}, Archive {}, Species {}'
                          .format(self.userId, self.archiveName, speciesName),
                          MFChain.META_SQUID: squid}
                  newMFC = MFChain(self.userId, priority=self.priority, 
                                    metadata=meta, status=JobStatus.GENERAL, 
                                    statusModTime=dt.gmt().mjd)
                  updatedMFChain = self._scribe.insertMFChain(newMFC)
            # Get rules for objects to be computed
            try:
               rules.extend(o.computeMe(workDir=updatedMFChain.getRelativeDirectory()))
            except Exception, e:
               self.log.info('Failed on object.compute {}, ({})'.format(type(o), 
                                                                        str(e)))
         updatedMFChain.addCommands(rules)
         updatedMFChain.write()
         updatedMFChain.updateStatus(JobStatus.INITIALIZE)
         self._scribe.updateObject(updatedMFChain)
         
      return updatedMFChain

# ...............................................
   def _writeDoneWalkenFile(self):
         try:
            f = open(self.walkedArchiveFname, 'w')
            f.write('# {} Completed walking species input {} in config file {}\n'
                    .format(dt.now(), self.weaponOfChoice.inputFilename, 
                            self.configFname))
            f.close()
         except:
            self.log.error('Failed to write doneWalken file {} for config {}'
                           .format(self.walkedArchiveFname, self.configFname))

"""
userId='kubi'

from LmBackend.common.occparse import OccDataParser
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat,
                     SERVER_BOOM_HEADING) 
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmBackend.common.lmobj import LMError, LMObject
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

logger = ScriptLogger('testchris')
scribe = BorgScribe(logger)
scribe.openConnections()
configFile = '/share/lm/data/archive/kubi/BOOM_Archive.ini'
configFile = '/share/lm/data/archive/biotaphy/biotaphy_boom.ini'
chris = ChristopherWalken(configFile, scribe=scribe)

(chris.userId, chris.archiveName, chris.boompath, chris.weaponOfChoice, 
 chris.epsg, chris.algs, chris.mdlScen, chris.mdlMask, chris.prjScens, chris.prjMask, 
 chris.boomGridset, chris.intersectParams, 
 chris.assemblePams) = chris._getConfiguredObjects()

# Global PAM Matrix for each scenario
chris.globalPAMs = {}

# One Global PAM for each scenario
if chris.assemblePams:
   for prjscen in chris.prjScens:
      chris.globalPAMs[prjscen.code] = chris.boomGridset.getPAMForCodes(
                     prjscen.gcmCode, prjscen.altpredCode, prjscen.dateCode)



chris.moveToStart()
chris.startWalken()

"""