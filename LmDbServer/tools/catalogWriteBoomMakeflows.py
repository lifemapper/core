"""
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research
 
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
import ConfigParser
import json
import mx.DateTime
import os
import types

from LmBackend.command.boom import BoomerCommand
from LmBackend.command.server import (CatalogTaxonomyCommand, EncodeTreeCommand,
                                      EncodeBioGeoHypothesesCommand)
from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (JobStatus, LMFormat, MatrixType, 
      ProcessType, DEFAULT_POST_USER, LM_USER,
      SERVER_BOOM_HEADING, SERVER_SDM_ALGORITHM_HEADING_PREFIX, 
      SERVER_SDM_MASK_HEADING_PREFIX, SERVER_DEFAULT_HEADING_POSTFIX, 
      SERVER_PIPELINE_HEADING)
from LmCommon.common.readyfile import readyFilename

from LmDbServer.common.lmconstants import (SpeciesDatasource, TAXONOMIC_SOURCE)
from LmDbServer.common.localconstants import (GBIF_PROVIDER_FILENAME, 
                                              GBIF_TAXONOMY_FILENAME)
from LmDbServer.tools.catalogScenPkg import SPFiller

from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (ARCHIVE_KEYWORD, GGRIM_KEYWORD,
                           GPAM_KEYWORD, LMFileType, Priority, ENV_DATA_PATH,
                           PUBLIC_ARCHIVE_NAME, DEFAULT_EMAIL_POSTFIX,
                           SPECIES_DATA_PATH)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.tree import Tree
from LmServer.base.utilities import isRootUser

# .............................................................................
class WorkflowCreator(LMObject):
   """
   @summary 
   Class to: 
     1) populate a Lifemapper database with inputs for a BOOM archive
        including: user, scenario package, shapegrid, Tree,
                   Biogeographic Hypotheses, gridset
     2) If named scenario package does not exist for the user, add it.
     2) create default matrices for each scenario, 
        PAMs for SDM projections and GRIMs for Scenario layers
     3) Write a configuration file for computations (BOOM daemon) on the inputs
     4) Write a Makeflow to begin the BOOM daemon
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, paramFname, logname=None):
      """
      @summary Constructor for WorkflowCreator class.
      """
      super(WorkflowCreator, self).__init__()
      scriptname, _ = os.path.splitext(os.path.basename(__file__))
      self.name = scriptname
      # Logfile
      bsname, _ = os.path.splitext(os.path.basename(paramFname))
      if logname is None:
         logname = '{}.{}'.format(self.name, bsname)
      self.logname = logname
      
      self.inParamFname = paramFname
      # Get database
      try:
         self.scribe = self._getDb(self.logname)
      except: 
         raise
      self.open()

   # ...............................................
   def initializeInputs(self):
      """
      @summary Initialize configured and stored inputs for WorkflowCreator class.
      usr, usrPath, userTaxonomyBasename, archiveName, 
              priority, scenPackageName, 
              modelScenCode, prjScenCodeList, doMapBaseline, 
              cellsize, gridname
      """      
      (self.userId, self.userIdPath,
       self.userTaxonomyBasename,
       self.archiveName,
       self.priority,
       self.scenPackageName,
       modelScenCode,
       prjScenCodeList,
       doMapBaseline,
       self.assemblePams,
       self.cellsize,
       self.gridname,
       self.treename,
       self.bgLyrnames) = self.readParamVals()
      earl = EarlJr()
      self.outConfigFilename = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                                   objCode=self.archiveName, 
                                                   usr=self.userId)
       
      # Find existing scenarios or create from user or public ScenPackage metadata
      self.scenPkg = self.findOrAddScenarioPackage()
      (self.modelScenCode,
       self.prjScenCodeList) = self.findMdlProjScenarios(modelScenCode, 
                                 prjScenCodeList, doMapBaseline=doMapBaseline)
      
      earl = EarlJr()
      self.outConfigFilename = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                                   objCode=self.archiveName, 
                                                   usr=self.userId)

   # ...............................................
   def findScenarioPackage(self):
      """
      @summary Find Scenarios from codes 
      @note: Boom parameters must include SCENARIO_PACKAGE, 
                          and optionally, SCENARIO_PACKAGE_MODEL_SCENARIO,
                                          SCENARIO_PACKAGE_PROJECTION_SCENARIOS
      """
      # Make sure Scenario Package exists for this user
      scenPkg = self.scribe.getScenPackage(userId=self.userId, 
                                      scenPkgName=self.scenPackageName, 
                                      fillLayers=True)         
      return scenPkg
                
   # ...............................................
   def findMdlProjScenarios(self, modelScenCode, prjScenCodeList, 
                              doMapBaseline=1):
      """
      @summary Find which Scenario for modeling, which (list) for projecting  
      @note: Boom parameters must include SCENARIO_PACKAGE, 
                              may include SCENARIO_PACKAGE_MODEL_SCENARIO,
                                          SCENARIO_PACKAGE_PROJECTION_SCENARIOS
      """
      # TODO: Put optional masklayer into every Scenario
      masklyr = None 
      
      validScenCodes = self.scenPkg.scenarios.keys()      
      # If model and/or projection Scenarios are not listed, use defaults in 
      # package metadata
      if modelScenCode is None:
         modelScenCode = self._findScenPkgBaseline(self.scenPackageName)
      if not prjScenCodeList:
         prjScenCodeList = validScenCodes      

      if modelScenCode is None or prjScenCodeList is None or len(prjScenCodeList) == 0:
         raise LMError("""SCENARIO_PACKAGE_MODEL_SCENARIO and 
                          SCENARIO_PACKAGE_PROJECTION_SCENARIOS must be 
                          configured in BOOM parameter file or 
                          SCENARIO_PACKAGE metadata file""")
      # Make sure modeling Scenario exists in this package
      if not modelScenCode in validScenCodes:
         raise LMError('Scenario {} must exist in ScenPackage {} for User {}'
                       .format(modelScenCode, self.scenPackageName, self.userId))
      if prjScenCodeList:
         for pcode in prjScenCodeList:
            if not pcode in validScenCodes:
               raise LMError('Scenario {} must exist in ScenPackage {} for User {}'
                             .format(pcode, self.scenPackageName, self.userId))
      if not doMapBaseline:
         prjScenCodeList.remove(modelScenCode)

      # TODO: Need a mask layer for every scenario!!
      self.masklyr = masklyr
                     
      return modelScenCode, prjScenCodeList
                
   # ...............................................
   def open(self):
      success = self.scribe.openConnections()
      if not success: 
         raise LMError('Failed to open database')

      # ...............................................
   def close(self):
      self.scribe.closeConnections()

   # ...............................................
   @property
   def logFilename(self):
      try:
         fname = self.scribe.log.baseFilename
      except:
         fname = None
      return fname

   # ...............................................
   def _getDb(self, logname):
      import logging
      logger = ScriptLogger(logname, level=logging.INFO)
      # DB connection
      scribe = BorgScribe(logger)
      return scribe
   

   # ...............................................
   def _findScenPkgMeta(self, scenpkgName):
      scenpkg_meta_file = os.path.join(ENV_DATA_PATH, scenpkgName + '.py')
      if not os.path.exists(scenpkg_meta_file):
         raise LMError('Missing Scenario Package metadata file {}'.format(scenpkg_meta_file))
         exit(-1)    

      if not os.path.exists(scenpkg_meta_file):
         raise LMError(currargs='Climate metadata {} does not exist'
                       .format(scenpkg_meta_file))
      # TODO: change to importlib on python 2.7 --> 3.3+  
      try:
         import imp
         SPMETA = imp.load_source('currentmetadata', scenpkg_meta_file)
      except Exception, e:
         raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                       .format(scenpkg_meta_file, e))         
      pkgMeta = SPMETA.CLIMATE_PACKAGES[scenpkgName]
      return pkgMeta

   # ...............................................
   def _findScenPkgBaseline(self, scenpkgName):
      pkgMeta = self._findScenPkgMeta(scenpkgName)
      baseCode = pkgMeta['baseline']
      return baseCode

   # ...............................................
   def readParamVals(self):
      if self.inParamFname is None or not os.path.exists(self.inParamFname):
         print('Missing config file {}, using defaults'.format(self.inParamFname))
         paramFname = None
      else:
         paramFname = self.inParamFname
      config = Config(siteFn=paramFname)
   
      # Fill in missing or null variables for archive.config.ini
      usr = self._getBoomOrDefault(config, 'ARCHIVE_USER', defaultValue=PUBLIC_USER)
      earl = EarlJr()
      usrPath = earl.createDataPath(usr, LMFileType.BOOM_CONFIG)
      userTaxonomyBasename = self._getBoomOrDefault(config, 
                           'USER_TAXONOMY_FILENAME', None)
      archiveName = self._getBoomOrDefault(config, 'ARCHIVE_NAME', 
                                           defaultValue=PUBLIC_ARCHIVE_NAME)
      priority = self._getBoomOrDefault(config, 'ARCHIVE_PRIORITY', 
                                        defaultValue=Priority.NORMAL)
                     
      # RAD/PAM params
      cellsize = self._getBoomOrDefault(config, 'GRID_CELLSIZE')
      gridname = '{}-Grid-{}'.format(archiveName, cellsize)

      scenPackageName = self._getBoomOrDefault(config, 'SCENARIO_PACKAGE')
      if scenPackageName is None:
         raise LMError('SCENARIO_PACKAGE must be configured')

      modelScenCode = self._getBoomOrDefault(config, 'SCENARIO_PACKAGE_MODEL_SCENARIO')
      prjScenCodeList = self._getBoomOrDefault(config, 
                  'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', isList=True)
      doMapBaseline = self._getBoomOrDefault(config, 'MAP_BASELINE', defaultValue=1)
      assemblePams = self._getBoomOrDefault(config, 'ASSEMBLE_PAMS', isBool=True)
      treename = self._getBoomOrDefault(config, 'TREE')
      bglyrnames = self._getBoomOrDefault(config, 'biogeo_hypotheses_layers', isList=True)
   
      return (usr, usrPath, userTaxonomyBasename, archiveName, 
              priority, scenPackageName, 
              modelScenCode, prjScenCodeList, doMapBaseline, assemblePams,
              cellsize, gridname, treename, bglyrnames)
      
   
   # ...............................................
   def _getVarValue(self, var):
      # Remove spaces and empty strings
      if var is not None and not bool(var):
         var = var.strip()
         if var == '':
            var = None
      # Convert to number if needed
      try:
         var = int(var)
      except:
         try:
            var = float(var)
         except:
            pass
      return var
   
   # ...............................................
   def _getBoomOrDefault(self, config, varname, defaultValue=None, 
                         isList=False, isBool=False):
      var = None
      # Get value from BOOM or default config file
      if isBool:
         try:
            var = config.getboolean(SERVER_BOOM_HEADING, varname)
         except:
            try:
               var = config.getboolean(SERVER_PIPELINE_HEADING, varname)
            except:
               var = None
      else:
         try:
            var = config.get(SERVER_BOOM_HEADING, varname)
         except:
            try:
               var = config.get(SERVER_PIPELINE_HEADING, varname)
            except:
               var = None
         
      # Take default if present
      if var is None:
         if defaultValue is not None:
            var = defaultValue
      # or interpret value
      else:
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
   

   # ...............................................
   def _getMCProcessType(self, mtxColumn, mtxType):
      """
      @summary Initialize configured and stored inputs for ArchiveFiller class.
      """
      if LMFormat.isOGR(driver=mtxColumn.layer.dataFormat):
         if mtxType == MatrixType.PAM:
            ptype = ProcessType.INTERSECT_VECTOR
         elif mtxType == MatrixType.GRIM:
            raise LMError('Vector GRIM intersection is not implemented')
      else:
         if mtxType == MatrixType.PAM:
            ptype = ProcessType.INTERSECT_RASTER
         elif mtxType == MatrixType.GRIM:
            ptype = ProcessType.INTERSECT_RASTER_GRIM
      return ptype
   


   # ...............................................
   def _getBGMeta(self, bgFname):
      # defaults for no metadata file
      # lower-case dict keys
      bgkeyword = 'biogeographic hypothesis'
      lyrMeta = {MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower(): None,
                 ServiceObject.META_DESCRIPTION.lower(): 
      'Biogeographic hypothesis based on layer {}'.format(bgFname)}
      fpthbasename, _ = os.path.splitext(bgFname)
      metaFname = fpthbasename + LMFormat.JSON.ext
      if os.path.exists(metaFname):
         with open(metaFname) as f:
            meta = json.load(f)
            if type(meta) is dict:
               for k, v in meta.iteritems():
                  lyrMeta[k.lower()] = v
               # Add keyword to metadata
               try:
                  kwdStr = meta[ServiceObject.META_KEYWORDS]
                  keywords = kwdStr.split(',')
                  if bgkeyword not in keywords:
                     keywords.append(bgkeyword)
               except:
                  meta[ServiceObject.META_KEYWORDS] = bgkeyword
            else:
               raise LMError('Metadata must be a dictionary or a JSON-encoded dictionary')
      return lyrMeta
   
   # ...............................................
   def _getBioGeoHypothesesLayerFilenames(self, biogeoName, usrPath):
      bghypFnames = []
      if biogeoName is not None:
         bgpth = os.path.join(usrPath, biogeoName) 
         if os.path.exists(bgpth + LMFormat.SHAPE.ext):
            bghypFnames = [bgpth + LMFormat.SHAPE.ext]
         elif os.path.isdir(bgpth):
            import glob
            pattern = os.path.join(bgpth, '*' + LMFormat.SHAPE.ext)
            bghypFnames = glob.glob(pattern)
         else:
            self.scribe.log.warning('No biogeo shapefiles at {}'.format(bgpth))
      return bghypFnames
    
   # ...............................................
   def getBioGeoHypothesesMatrixAndLayers(self, gridset):
      currtime = mx.DateTime.gmt().mjd
      biogeoLayerNames = []
      bgMtx = None
         
      if len(self.bghypFnames) > 0:
         mtxKeywords = ['biogeographic hypotheses']
         for bgFname in self.bghypFnames:
            if os.path.exists(bgFname):
               lyrMeta = self._getBGMeta(bgFname)
               valAttr = lyrMeta[MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
               try:
                  name = lyrMeta['name']
               except:
                  name, _ = os.path.splitext(os.path.basename(bgFname))
               mtxKeywords.append('Layer {}'.format(name))
               lyr = Vector(name, self.userId, self.scenPkg.epsgcode, dlocation=bgFname, 
                   metadata=lyrMeta, dataFormat=LMFormat.SHAPE.driver, 
                   valAttribute=valAttr, modTime=currtime)
               updatedLyr = self.scribe.findOrInsertLayer(lyr)
               biogeoLayerNames.append(updatedLyr.name)
         self.scribe.log.info('  Added {} layers for biogeo hypotheses matrix'
                       .format(len(biogeoLayerNames)))
         # Add the matrix to contain biogeo hypotheses layer intersections
         meta={ServiceObject.META_DESCRIPTION.lower(): 
               'Biogeographic Hypotheses for archive {}'.format(self.archiveName),
               ServiceObject.META_KEYWORDS.lower(): mtxKeywords}
         tmpMtx = LMMatrix(None, matrixType=MatrixType.BIOGEO_HYPOTHESES, 
                           processType=ProcessType.ENCODE_HYPOTHESES,
                           userId=self.userId, gridset=gridset, metadata=meta,
                           status=JobStatus.INITIALIZE, statusModTime=currtime)
         bgMtx = self.scribe.findOrInsertMatrix(tmpMtx)
         if bgMtx is None:
            self.scribe.log.info('  Failed to add biogeo hypotheses matrix')
      return bgMtx, biogeoLayerNames
    
   # ...............................................
   def addEncodeBioGeoMF(self, gridset):
      """
      @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                and configFile written by WorkflowCreator.initBoom.
      """
      scriptname, _ = os.path.splitext(os.path.basename(__file__))
      meta = {MFChain.META_CREATED_BY: scriptname,
              MFChain.META_DESCRIPTION: 
                        'Encode biogeographic hypotheses task for user {} grid {}'
                        .format(self.userId, gridset.name)}
      newMFC = MFChain(self.userId, priority=Priority.HIGH, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=mx.DateTime.gmt().mjd)
      mfChain = self.scribe.insertMFChain(newMFC, None)
      
      ws_dir = mfChain.getRelativeDirectory()
      baseFilename, _ = os.path.splitext(os.path.basename(self.outConfigFilename))
      bghSuccessFname = os.path.join(ws_dir, baseFilename + '.success')
    
      # Create a rule from the MF 
      bgCmd = EncodeBioGeoHypothesesCommand(self.userId, gridset.name, bghSuccessFname)
    
      mfChain.addCommands([bgCmd.getMakeflowRule(local=True)])
      mfChain.write()
      mfChain.updateStatus(JobStatus.INITIALIZE)
      self.scribe.updateObject(mfChain)
      return mfChain   

   # ...............................................
   def _findGRIM(self, gridset, scen):
      # Create Scenario-GRIM for this archive, scenario
      # GRIM layers are added now
      desc = '{} for Scenario {}'.format(GGRIM_KEYWORD, scen.code)
      grimMeta = {ServiceObject.META_DESCRIPTION: desc,
                 ServiceObject.META_KEYWORDS: [GGRIM_KEYWORD]}
      tmpGrim = LMMatrix(None, matrixType=MatrixType.GRIM, 
                         gcmCode=scen.gcmCode, altpredCode=scen.altpredCode, 
                         dateCode=scen.dateCode, metadata=grimMeta, userId=self.userId, 
                         gridset=gridset, 
                         status=JobStatus.GENERAL, 
                         statusModTime=mx.DateTime.gmt().mjd)
      grim = self.scribe.findOrInsertMatrix(tmpGrim)
      for lyr in scen.layers:
         # Add to GRIM Makeflow ScenarioLayer and MatrixColumn
         mtxcol = self._initGRIMIntersect(lyr, grim)
      return grim


   # .............................
   def _addGrimMF(self, scencode, gridsetId, currtime):
      # Create MFChain for this GPAM
      desc = ('GRIM Makeflow for User {}, Archive {}, Scenario {}'
              .format(self.userId, self.archiveName, scencode))
      meta = {MFChain.META_CREATED_BY: self.name,
              MFChain.META_GRIDSET: gridsetId,
              MFChain.META_DESCRIPTION: desc 
              }
      newMFC = MFChain(self.userId, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=currtime)
      grimChain = self.scribe.insertMFChain(newMFC, gridsetId)
      return grimChain
   
   # .............................
   def addGrimMFs(self, defaultGrims, gridsetId):
      currtime = mx.DateTime.gmt().mjd
      grimChains = []
      
      for code, grim in defaultGrims.iteritems():
         # Create MFChain for this GRIM
         grimChain = self._addGrimMF(code, gridsetId, currtime)
         targetDir = grimChain.getRelativeDirectory()
         mtxcols = self.scribe.getColumnsForMatrix(grim.getId())
         self.scribe.log.info('  {} grim columns for scencode {}'
                       .format(grimChain.objId, code))
         
         colFilenames = []
         for mtxcol in mtxcols:
            mtxcol.postToSolr = False
            mtxcol.processType = self._getMCProcessType(mtxcol, grim.matrixType)
            mtxcol.shapegrid = self.shapegrid
      
            lyrRules = mtxcol.computeMe(workDir=targetDir)
            grimChain.addCommands(lyrRules)
            
            # Keep track of intersection filenames for matrix concatenation
            relDir, _ = os.path.splitext(mtxcol.layer.getRelativeDLocation())
            outFname = os.path.join(targetDir, relDir, mtxcol.getTargetFilename())
            colFilenames.append(outFname)
                        
         # Add concatenate command
         grimRules = grim.getConcatAndStockpileRules(colFilenames, workDir=targetDir)
         
         grimChain.addCommands(grimRules)
         grimChain.write()
         grimChain.updateStatus(JobStatus.INITIALIZE)
         self.scribe.updateObject(grimChain)
         grimChains.append(grimChain)
         self.scribe.log.info('  Wrote GRIM Makeflow {} for scencode {}'
                       .format(grimChain.objId, code))
               
      return grimChains
   
   # ...............................................
   def _getTaxonomyCommand(self):
      """
      @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                and configFile written by WorkflowCreator.initBoom.
      @todo: Define format and enable ingest user taxonomy, commented out below
      """
      cattaxCmd = taxSuccessFname = taxDataFname = None
      config = Config(siteFn=self.inParamFname)
#       # look for User data in user space or GBIF data in species dir
#       taxDataBasename = self._getBoomOrDefault(config, 
#                            'USER_TAXONOMY_FILENAME', None)
#       if taxDataBasename is not None:
#          taxDataFname = os.path.join(self.userIdPath, taxDataBasename)
#          taxSourceName = self.userId
#          taxSourceUrl = None
#       elif self.dataSource in (SpeciesDatasource.GBIF, SpeciesDatasource.IDIGBIO):
      if self.dataSource in (SpeciesDatasource.GBIF, SpeciesDatasource.IDIGBIO):
         taxDataBasename = self._getBoomOrDefault(config, 
                              'GBIF_TAXONOMY_FILENAME', GBIF_TAXONOMY_FILENAME)
         taxDataFname = os.path.join(SPECIES_DATA_PATH, taxDataBasename)
         taxSourceName = TAXONOMIC_SOURCE['GBIF']['name']
         taxSourceUrl = TAXONOMIC_SOURCE['GBIF']['url']
      
      # If there is taxonomy ...
      if taxDataFname and os.path.exists(taxDataFname):
         taxDataBase, _ = os.path.splitext(taxDataFname)
         taxSuccessFname = os.path.join(taxDataBase + '.success')
         if os.path.exists(taxSuccessFname):
            self.scribe.log.info('Taxonomy {} has already been cataloged'
                                 .format(taxDataFname))
         else:         
            # logfile, walkedTaxFname added to outputs in command construction
            cattaxCmd = CatalogTaxonomyCommand(taxSourceName, 
                                               taxDataFname,
                                               taxSuccessFname,
                                               source_url=taxSourceUrl,
                                               delimiter='\t')
      return cattaxCmd, taxSuccessFname
                
   # ...............................................
   def addTaxonomyMF(self, boomGridsetId):
      """
      @summary: Create a Makeflow to initiate taxonomy ingestion.
      """
      meta = {MFChain.META_CREATED_BY: self.name,
              MFChain.META_GRIDSET: boomGridsetId,
              MFChain.META_DESCRIPTION: 'Taxonomy ingest for User {}, Archive {}'
      .format(self.userId, self.archiveName)}
      newMFC = MFChain(self.userId, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=mx.DateTime.gmt().mjd)
      mfChain = self.scribe.insertMFChain(newMFC, boomGridsetId)
      cattaxCmd, taxSuccessFname = self._getTaxonomyCommand()
      mfChain.addCommands([cattaxCmd.getMakeflowRule(local=True)])
      mfChain.write()
      mfChain.updateStatus(JobStatus.INITIALIZE)
      self.scribe.updateObject(mfChain)
      self.scribe.log.info('  Wrote Taxonomy Makeflow {} for gridset {}'
                    .format(mfChain.objId, boomGridsetId))

   # ...............................................
   def addBoomMF(self, boomGridsetId, tree):
      """
      @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                and configFile written by WorkflowCreator.initBoom.
      """
      meta = {MFChain.META_CREATED_BY: self.name,
              MFChain.META_GRIDSET: boomGridsetId,
              MFChain.META_DESCRIPTION: 'Boom start for User {}, Archive {}'
      .format(self.userId, self.archiveName)}
      newMFC = MFChain(self.userId, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=mx.DateTime.gmt().mjd)
      mfChain = self.scribe.insertMFChain(newMFC, boomGridsetId)
      # Workspace directory
      ws_dir = mfChain.getRelativeDirectory()
      baseAbsFilename, _ = os.path.splitext(os.path.basename(self.outConfigFilename))
      # ChristopherWalken writes when finished walking through 
      # species data (initiated by this Makeflow).  
      boomSuccessFname = os.path.join(ws_dir, baseAbsFilename + '.success')
      boomCmd = BoomerCommand(self.outConfigFilename, boomSuccessFname)
                
      # Add taxonomy before Boom, if taxonomy is specified
      cattaxCmd, taxSuccessFname = self._getTaxonomyCommand()
      if cattaxCmd:
         # Add catalog taxonomy command to this Makeflow
         mfChain.addCommands([cattaxCmd.getMakeflowRule(local=True)])
         # Boom requires catalog taxonomy completion
         boomCmd.inputs.append(taxSuccessFname)

      # Encode tree after Boom, if tree exists
      try:
         walkedTreeFname = os.path.join(ws_dir, self.userId+tree.name+'.success')
         treeCmd = EncodeTreeCommand(self.userId, tree.name, walkedTreeFname)
      except:
         pass
      else:
         # Tree requires Boom completion
         treeCmd.inputs.append(boomSuccessFname)
         # Add tree encoding command to this Makeflow
         mfChain.addCommands([treeCmd.getMakeflowRule(local=True)])

      # Add boom command to this Makeflow
      mfChain.addCommands([boomCmd.getMakeflowRule(local=True)])
      mfChain.write()
      mfChain.updateStatus(JobStatus.INITIALIZE)
      self.scribe.updateObject(mfChain)
      self.scribe.log.info('  Wrote BOOM Makeflow {} for gridset {}'
                    .format(mfChain.objId, boomGridsetId))
      return mfChain

   # ...............................................
   def writeMakeflows(self):
      try:
         # Also adds user
         self.initializeInputs()
      
         boomGridset = self.scribe.getGridset(userId=self.userId, 
                                              name=self.archiveName, 
                                              fillMatrices=True)
         if not(self.userId == DEFAULT_POST_USER) and self.assemblePams:
            scenGrims = {}
            for code, scen in self.scenPkg.scenarios.iteritems():
               scenGrim = self._findGRIM(boomGridset, scen)
               scenGrims[code] = scenGrim
         
            # Add GRIM compute Makeflows, independent of Boom completion
            grimMFs = self.addGrimMFs(scenGrims, boomGridset.getId())

         if len(self.bgLyrnames) > 0:
            bgMF = self.addEncodeBioGeoMF(boomGridset)
            
         tree = None
         if self.treename:
            tree = Tree(self.treename, userId=self.userId, gridsetId=boomGridset.getId())
         # This also adds commands for taxonomy insertion before 
         #   and tree encoding after Boom 
         boomMF = self.addBoomMF(boomGridset.getId(), tree)
            
      finally:
         self.close()
         
      # BOOM POST from web requires gridset object to be returned
      return boomMF
   
# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=("""Create makeflows for single- or multi-species 
            computations for the configuration file provided"""))
   parser.add_argument('config_file', default=None,
            help=('Configuration file for the workflow with parameters ' +
                  'to be used for the workflow.'))
   parser.add_argument('--logname', type=str, default=None,
            help=('Basename of the logfile, without extension'))
   args = parser.parse_args()
   configFname = args.param_file
   logname = args.logname
         
   if configFname is not None and not os.path.exists(configFname):
      print ('Missing configuration file {}'.format(configFname))
      exit(-1)
      
   if logname is None:
      import time
      scriptname, _ = os.path.splitext(os.path.basename(__file__))
      secs = time.time()
      timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
      logname = '{}.{}'.format(scriptname, timestamp)

   print('Running catalogWriteBoomMakeflows with configFname = {}'
         .format(configFname))
   
   filler = WorkflowCreator(configFname, logname=logname)
   boomMF = filler.writeMakeflows()
   print('Completed catalogWriteBoomMakeflows creating Master Makeflow: {}'.format(boomMF.getId()))

    
"""

"""