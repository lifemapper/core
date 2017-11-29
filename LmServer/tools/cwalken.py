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
import glob
import mx.DateTime as dt
from osgeo.ogr import wkbPoint
import os
from types import IntType, FloatType

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat,
          SERVER_BOOM_HEADING, SERVER_PIPELINE_HEADING, 
          SERVER_SDM_ALGORITHM_HEADING_PREFIX, SERVER_SDM_MASK_HEADING_PREFIX,
          SERVER_DEFAULT_HEADING_POSTFIX, MatrixType, IDIG_DUMP) 
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE, SpeciesDatasource

from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (LMFileType, SPECIES_DATA_PATH,
                                         Priority)
from LmServer.common.localconstants import PUBLIC_USER, DEFAULT_EPSG
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.sdmproj import SDMProjection
from LmServer.tools.occwoc import BisonWoC, GBIFWoC, UserWoC, ExistingWoC

# .............................................................................
class ChristopherWalken(LMObject):
   """
   Class to ChristopherWalken with a species iterator through a sequence of 
   species data creating a Spud for each species.  Creates and catalogs objects 
   (OccurrenceSets, SMDModels, SDMProjections, and MatrixColumns and MFChains 
    for their calculation) in the database .
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, configFname, jsonFname=None, scribe=None):
      """
      @summary Constructor for ChristopherWalken class which creates a Spud 
               (Single-species Makeflow chain) for a species.
      """
      super(ChristopherWalken, self).__init__()
      self.name = self.__class__.__name__.lower()
      self.configFname = configFname
      baseAbsFilename, ext = os.path.splitext(configFname)
      basename = os.path.basename(baseAbsFilename)
      # Chris writes this file when completed walking through species data
      self.walkedArchiveFname = baseAbsFilename + LMFormat.LOG.ext
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

      # Global PAM Matrix for each scenario
      self.globalPAMs = {}

   # .............................
   def initializeMe(self):
      """
      @summary: Sets objects and parameters for workflow on this object
      """
      self.moreDataToProcess = False
      (self.userId, 
       self.archiveName, 
       self.priority, 
       self.boompath, 
       self.weaponOfChoice, 
       self.epsg, 
       self.minPoints, 
       self.algs, 
       self.mdlScen, 
       self.prjScens, 
       self.sdmMaskInputLayer, 
       self.boomGridset, 
       self.intersectParams, 
       self.assemblePams) = self._getConfiguredObjects()
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

   # ...............................................
   def _getVarValue(self, var):
      try:
         var = int(var)
      except:
         try:
            var = float(var)
         except:
            pass
      return var

   # ...............................................
   def _getBoomOrDefault(self, varname, defaultValue=None, isList=False, isBool=False):
      var = None
      # Get value from BOOM or default config file
      if isBool:
         try:
            var = self.cfg.getboolean(SERVER_BOOM_HEADING, varname)
         except:
            try:
               var = self.cfg.getboolean(SERVER_PIPELINE_HEADING, varname)
            except:
               pass
      else:
         try:
            var = self.cfg.get(SERVER_BOOM_HEADING, varname)
         except:
            try:
               var = self.cfg.get(SERVER_PIPELINE_HEADING, varname)
            except:
               pass
      # Take default if present
      if var is None:
         if defaultValue is not None:
            var = defaultValue
      # or interpret value
      elif not isBool:
         if not isList:
            var = self._getVarValue(var)
         else:
            try:
               tmplist = [v.strip() for v in var.split(',')]
               var = []
            except:
               raise LMError('Failed to split variables on \',\'')
            for v in tmplist:
               v = self._getVarValue(v)
               var.append(v)
      return var

# .............................................................................
   def _getOccWeaponOfChoice(self, userId, archiveName, epsg, boompath):
      useGBIFTaxonIds = False
      # Get datasource and optional taxonomy source
      datasource = self._getBoomOrDefault('DATASOURCE')
      try:
         taxonSourceName = TAXONOMIC_SOURCE[datasource]['name']
      except:
         taxonSourceName = None
         
      # Expiration date for retrieved species data 
      expDate = dt.DateTime(self._getBoomOrDefault('SPECIES_EXP_YEAR'), 
                            self._getBoomOrDefault('SPECIES_EXP_MONTH'), 
                            self._getBoomOrDefault('SPECIES_EXP_DAY')).mjd
      # Get Weapon of Choice depending on type of Occurrence data to parse
      # GBIF data
      if datasource == SpeciesDatasource.GBIF:
#          gbifTax = self._getBoomOrDefault('GBIF_TAXONOMY_FILENAME')
#          gbifTaxFile = os.path.join(SPECIES_DATA_PATH, gbifTax)
         gbifOcc = self._getBoomOrDefault('GBIF_OCCURRENCE_FILENAME')
         # GBIF data may be for user, or public archive (in SPECIES_DATA_PATH)
         gbifOccFile = os.path.join(SPECIES_DATA_PATH, gbifOcc)
         if not os.path.exists(gbifOccFile):
            gbifOccFile = os.path.join(boompath, gbifOcc)
         gbifProv = self._getBoomOrDefault('GBIF_PROVIDER_FILENAME')
         gbifProvFile = os.path.join(SPECIES_DATA_PATH, gbifProv)
         weaponOfChoice = GBIFWoC(self._scribe, userId, archiveName, 
                                     epsg, expDate, gbifOccFile,
                                     providerFname=gbifProvFile, 
                                     taxonSourceName=taxonSourceName, 
                                     logger=self.log)
      # Bison data
      elif datasource == SpeciesDatasource.BISON:
         bisonTsn = self._getBoomOrDefault('BISON_TSN_FILENAME')
         bisonTsnFile = os.path.join(SPECIES_DATA_PATH, bisonTsn)
         weaponOfChoice = BisonWoC(self._scribe, userId, archiveName, 
                                   epsg, expDate, bisonTsnFile, 
                                   taxonSourceName=taxonSourceName, 
                                   logger=self.log)

      # Copy public data to user space
      # TODO: Handle taxonomy, useGBIFTaxonomy=??
      elif datasource == SpeciesDatasource.EXISTING:
         occIdFname = self._getBoomOrDefault('OCCURRENCE_ID_FILENAME')
         weaponOfChoice = ExistingWoC(self._scribe, userId, archiveName, 
                                      epsg, expDate, occIdFname, 
                                      logger=self.log)
      else:
         # iDigBio data
         if datasource == SpeciesDatasource.IDIGBIO:
            useGBIFTaxonIds = True
            occDelimiter = self._getBoomOrDefault('IDIG_OCCURRENCE_DATA_DELIMITER') 
            
            occname = self._getBoomOrDefault('IDIG_OCCURRENCE_DATA')
            occInstalled = os.path.join(SPECIES_DATA_PATH, occname)
            occUser = os.path.join(boompath, occname)
            # Check for data installed into species path 
            occCSV = None
            if os.path.exists(occInstalled + LMFormat.CSV.ext):
               occCSV = occInstalled + LMFormat.CSV.ext
               occMeta = occInstalled + LMFormat.METADATA.ext
            elif os.path.exists(occUser + LMFormat.CSV.ext):
               occCSV = occUser + LMFormat.CSV.ext
               occMeta = IDIG_DUMP.METADATA
               
            # IDIG_OCCURRENCE_DATA may be a directory containing multiple csv files
            if occCSV is None:
               if os.path.exists(occInstalled):
                  pthname = occInstalled
                  occMeta = IDIG_DUMP.METADATA
               elif os.path.exists(occUser):
                  pthname = occUser
                  occMeta = pthname + LMFormat.METADATA.ext
               else:
                  raise LMError('Failed to find file or directory {} in {} or {}'
                                .format(SPECIES_DATA_PATH, boompath))
               fnames = glob.glob(os.path.join(pthname, '*' + LMFormat.CSV.ext))
               if len(fnames) > 0:
                  occCSV = fnames[0]
                  if len(fnames) > 1:
                     self.moreDataToProcess = True
               else:
                  occCSV = None
         # User data, anything not above
         else:
            useGBIFTaxonIds = False
            occData = self._getBoomOrDefault('USER_OCCURRENCE_DATA')
            occDelimiter = self._getBoomOrDefault('USER_OCCURRENCE_DATA_DELIMITER') 
            occCSV = os.path.join(boompath, occData + LMFormat.CSV.ext)
            occMeta = os.path.join(boompath, occData + LMFormat.METADATA.ext)
            
         weaponOfChoice = UserWoC(self._scribe, userId, archiveName, 
                                  epsg, expDate, occCSV, occMeta, 
                                  occDelimiter, logger=self.log, 
                                  processType=ProcessType.USER_TAXA_OCCURRENCE,
                                  useGBIFTaxonomy=useGBIFTaxonIds,
                                  taxonSourceName=taxonSourceName)
         
      weaponOfChoice.initializeMe()
         
      return weaponOfChoice

# .............................................................................
   def _getAlgorithm(self, algHeading):
      """
      @note: Returns configured algorithm
      """
      acode =  self.cfg.get(algHeading, 'CODE')
      alg = Algorithm(acode)
      alg.fillWithDefaults()
      inputs = {}
      # override defaults with any option specified
      algoptions = self.cfg.getoptions(algHeading)
      for name in algoptions:
         pname, ptype = alg.findParamNameType(name)
         if pname is not None:
            if ptype == IntType:
               val = self.cfg.getint(algHeading, pname)
            elif ptype == FloatType:
               val = self.cfg.getfloat(algHeading, pname)
            else:
               val = self.cfg.get(algHeading, pname)
               # Some algorithms(mask) may have a parameter indicating a layer,
               # if so, add name to parameters and object to inputs
               if acode == 'hull_region_intersect' and pname == 'region':
                  inputs[pname] = val
            alg.setParameter(pname, val)
      if inputs:
         alg.setInputs(inputs)
      return alg

# .............................................................................
   def _getAlgorithms(self, sectionPrefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX):
      algs = []
      defaultAlgs = []
      # Get algorithms for SDM modeling
      sections = self.cfg.getsections(sectionPrefix)
      for algHeading in sections:
         alg = self._getAlgorithm(algHeading)
         
         if algHeading.endswith(SERVER_DEFAULT_HEADING_POSTFIX):
            defaultAlgs.append(alg)
         else:
            algs.append(alg)
      if len(algs) == 0:
         algs = defaultAlgs
      return algs

# .............................................................................
   def _getProjParams(self, userId, epsg):
      prjScens = []
      mdlScen = sdmMaskInputLayer = None
      
      # Get environmental data model and projection scenarios
      mdlScenCode = self._getBoomOrDefault('SCENARIO_PACKAGE_MODEL_SCENARIO')
      prjScenCodes = self._getBoomOrDefault('SCENARIO_PACKAGE_PROJECTION_SCENARIOS', 
                                             isList=True)
      if mdlScenCode not in prjScenCodes:
         prjScenCodes.append(mdlScenCode)

      scenPkgs = self._scribe.getScenPackagesForUserCodes(userId, prjScenCodes, 
                                                          fillLayers=True)
      if not scenPkgs:
         scenPkgs = self._scribe.getScenPackagesForUserCodes(PUBLIC_USER, 
                                                prjScenCodes, fillLayers=True)
      if scenPkgs:
         scenPkg = scenPkgs[0]
         mdlScen = scenPkg.getScenario(code=mdlScenCode)
         for pcode in prjScenCodes:
            prjScens.append(scenPkg.getScenario(code=pcode))
      else:
         raise LMError('Failed to retrieve ScenPackage for scenarios {}'
                       .format(prjScenCodes))
      # Should be only one or None
      maskAlgList = self._getAlgorithms(sectionPrefix=SERVER_SDM_MASK_HEADING_PREFIX)
      if len(maskAlgList) == 1:
         sdmMaskAlg = maskAlgList[0]
         # TODO: Handle if there is more than one input layer
         if len(sdmMaskAlg.getInputs()) > 1:
            raise LMError(currargs='Unable to process > 1 input SDM mask layer')
         for inputKey, lyrname in sdmMaskAlg.getInputs().iteritems():
            sdmMaskInputLayer = self._scribe.getLayer(userId=userId, 
                                                      lyrName=lyrname, epsg=epsg) 
            sdmMaskAlg.setInput(inputKey, sdmMaskInputLayer)
            
      return (mdlScen, prjScens, sdmMaskInputLayer)  

# .............................................................................
   def _getGlobalPamObjects(self, userId, archiveName, epsg):
      # Get existing intersect grid, gridset and parameters for Global PAM
      gridname = self._getBoomOrDefault('GRID_NAME')
      if gridname:
         intersectGrid = self._scribe.getShapeGrid(userId=userId, lyrName=gridname, 
                                                   epsg=epsg)
         if intersectGrid is None:
            raise LMError('Failed to retrieve Shapegrid for intersection {}'
                          .format(gridname))

      # Global PAM and Scenario GRIM for each scenario
      boomGridset = self._scribe.getGridset(name=archiveName, userId=userId, 
                                            fillMatrices=True)
      if boomGridset is None:
         raise LMError('Failed to retrieve Gridset for shapegrid {}, user {}'
                       .format(gridname, userId))
      boomGridset.setMatrixProcessType(ProcessType.CONCATENATE_MATRICES, 
                                       matrixTypes=[MatrixType.PAM, 
                                                    MatrixType.ROLLING_PAM, 
                                                    MatrixType.GRIM])
      intersectParams = {
         MatrixColumn.INTERSECT_PARAM_FILTER_STRING: 
            self._getBoomOrDefault('INTERSECT_FILTERSTRING'),
         MatrixColumn.INTERSECT_PARAM_VAL_NAME: 
            self._getBoomOrDefault('INTERSECT_VALNAME'),
         MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: 
            self._getBoomOrDefault('INTERSECT_MINPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: 
            self._getBoomOrDefault('INTERSECT_MAXPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: 
            self._getBoomOrDefault('INTERSECT_MINPERCENT')}

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
      userId = self._getBoomOrDefault('ARCHIVE_USER')
      archiveName = self._getBoomOrDefault('ARCHIVE_NAME')
      archivePriority = self._getBoomOrDefault('ARCHIVE_PRIORITY')
      if archivePriority is None:
         archivePriority = Priority.NORMAL
      # Get user-archive configuration file
      if userId is None or archiveName is None:
         raise LMError(currargs='Missing ARCHIVE_USER or ARCHIVE_NAME in {}'
                       .format(self.cfg.configFiles))
      earl = EarlJr()
      boompath = earl.createDataPath(userId, LMFileType.BOOM_CONFIG)
      epsg = self._getBoomOrDefault('SCENARIO_PACKAGE_EPSG', 
                                    defaultValue=DEFAULT_EPSG)
      # Species parser/puller
      weaponOfChoice = self._getOccWeaponOfChoice(userId, archiveName, epsg, 
                                                  boompath)
      # SDM inputs
      minPoints = self._getBoomOrDefault('POINT_COUNT_MIN')
      algorithms = self._getAlgorithms(sectionPrefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX)

      (mdlScen, prjScens, sdmMaskInputLayer) = self._getProjParams(userId, epsg)
      # Global PAM inputs
      (boomGridset, intersectParams) = self._getGlobalPamObjects(userId, 
                                                            archiveName, epsg)
      assemblePams = self._getBoomOrDefault('ASSEMBLE_PAMS', isBool=True)

      return (userId, archiveName, archivePriority, boompath, weaponOfChoice,  
              epsg, minPoints, algorithms, mdlScen, prjScens, sdmMaskInputLayer, 
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
      currtime = dt.gmt().mjd
      spud = None
      potatoInputs = {}
      pcount = prcount = icount = ircount = 0
      # WeaponOfChoice resets old or failed Occurrenceset
      occ = self.weaponOfChoice.getOne()
      if self.weaponOfChoice.finishedInput:
         self._writeDoneWalkenFile()
      if occ:
         objs = []
         # Process existing OccurrenceLayer (copy if up-to-date and complete,
         # recompute if incomplete, obsolete, or failed)
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
            # DB does not populate with shapegrid on insert
            mtxcol.shapegrid = self.boomGridset.getShapegrid()
            
            # Rollback if obsolete or failed
            reset = self._doReset(mtxcol.status, mtxcol.statusModTime)
            if reset:
               self.log.debug('Reset MatrixColumn {}'.format(mtxcol.getId()))
               mtxcol.updateStatus(JobStatus.GENERAL, modTime=currtime)
               success = self._scribe.updateObject(mtxcol)
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
                        modelMask=self.sdmMaskInputLayer, 
                        projMask=self.sdmMaskInputLayer, 
                        dataFormat=LMFormat.GTIFF.driver,
                        status=JobStatus.GENERAL, statusModTime=currtime)
         prj = self._scribe.findOrInsertSDMProject(tmpPrj)
         if prj is not None:
            self.log.debug('Found/inserted SDMProject {}'.format(prj.getId()))
            # Fill in projection with input scenario layers, masks
            prj._modelScenario = self.mdlScen
            prj.setModelMask(self.sdmMaskInputLayer)
            prj._projScenario = prjscen
            prj.setProjMask(self.sdmMaskInputLayer)
            # Rollback if obsolete or failed
            reset = self._doReset(prj.status, prj.statusModTime)
            if reset:
               self.log.debug('Reset SDMProject {}'.format(prj.getId()))
               prj.updateStatus(JobStatus.GENERAL, modTime=currtime)
               success = self._scribe.updateObject(prj)
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
                          MFChain.META_DESCRIPTION: 'Spud for User {}, Archive {}, Species {}'
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
            f.write('# Completed walking species input {}\n'
                    .format(self.weaponOfChoice.inputFilename))
            f.write('# From config file {}\n'
                    .format(self.configFname))
            f.write('# Full logs in {}\n'
                    .format(self.log.baseFilename))            
            f.close()
         except:
            self.log.error('Failed to write doneWalken file {} for config {}'
                           .format(self.walkedArchiveFname, self.configFname))

