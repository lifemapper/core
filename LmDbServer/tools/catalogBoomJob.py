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
import time
import types

from LmBackend.command.boom import BoomerCommand
from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (JobStatus, LMFormat, MatrixType, 
      ProcessType, DEFAULT_POST_USER,
      SERVER_BOOM_HEADING, SERVER_SDM_ALGORITHM_HEADING_PREFIX, 
      SERVER_SDM_MASK_HEADING_PREFIX, SERVER_DEFAULT_HEADING_POSTFIX, 
      SERVER_PIPELINE_HEADING)
from LmCommon.common.readyfile import readyFilename

from LmDbServer.common.lmconstants import SpeciesDatasource
from LmDbServer.common.localconstants import (GBIF_PROVIDER_FILENAME, 
                                              GBIF_TAXONOMY_FILENAME)

from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (ARCHIVE_KEYWORD, GGRIM_KEYWORD,
                           GPAM_KEYWORD, LMFileType, Priority, ENV_DATA_PATH,
                           PUBLIC_ARCHIVE_NAME, DEFAULT_EMAIL_POSTFIX)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.tree import Tree


CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)

# .............................................................................
class BOOMFiller(LMObject):
   """
   @summary 
   Class to: 
     1) populate a Lifemapper database with inputs for a BOOM archive
     2) create default matrices for each scenario, 
        PAMs for SDM projections and GRIMs for Scenario layers
     3) Write a configuration file for computations (BOOM daemon) on the inputs
     4) Write a Makeflow to begin the BOOM daemon
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, configFname=None):
      """
      @summary Constructor for BOOMFiller class.
      """
      super(BOOMFiller, self).__init__()
      self.name = self.__class__.__name__.lower()
      self.inParamFname = configFname
      # Get database
      try:
         self.scribe = self._getDb()
      except: 
         raise
      self.open()
      
   # ...............................................
   def initializeInputs(self, paramFname=None):
      """
      @summary Initialize configured and stored inputs for BOOMFiller class.
      """      
      # Allow reset configuration
      if paramFname is not None:
         self.inParamFname = paramFname
      (self.userId, self.userIdPath,
       self.userEmail,
       self.archiveName,
       self.priority,
       self.scenPackageName,
       self.modelScenCode,
       self.prjScenCodeList,
       self.dataSource,
       self.occIdFname,
       self.gbifFname,
       self.idigFname,
       self.idigOccSep,
       self.bisonFname,
       self.userOccFname,
       self.userOccSep,   
       self.minpoints,
       self.algorithms,
       self.assemblePams,
       self.gridbbox,
       self.cellsides,
       self.cellsize,
       self.gridname, 
       self.intersectParams, 
       self.maskAlg, 
       self.treeFname, 
       self.bghypFnames,
       self.doComputePAMStats) = self.readParamVals()
       
      # Fill existing scenarios AND SDM mask layer from configured codes 
      # or create from ScenPackage metadata
      config = Config(siteFn=self.inParamFname)
      doMapBaseline = self._getBoomOrDefault(config, 'MAP_BASELINE', defaultValue=1)
      # Checks existence of environmental data for this user and fills
      # self.prjScenCodeList if necessary.      
      self.scenPkg = self.findScenariosFromCodes(doMapBaseline=doMapBaseline)
      # Fill grid bbox with scenario package if it is absent
      if self.gridbbox is None:
         self.gridbbox = self.scenPkg.bbox

      # Created by addArchive
      self.shapegrid = None
      
      # If running as root, new user filespace must have permissions corrected
      self._warnPermissions()

      earl = EarlJr()
      self.outConfigFilename = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                                   objCode=self.archiveName, 
                                                   usr=self.userId)

   # ...............................................
   def findScenariosFromCodes(self, doMapBaseline=1):
      """
      @summary Find Scenarios from codes 
      @note: Boom parameters must include SCENARIO_PACKAGE, 
                          and optionally, SCENARIO_PACKAGE_MODEL_SCENARIO,
                                          SCENARIO_PACKAGE_PROJECTION_SCENARIOS
             If SCENARIO_PACKAGE_PROJECTION_SCENARIOS is not present, SDMs 
             will be projected onto all scenarios
      """
      # TODO: Put optional masklayer into every Scenario
      masklyr = None  

      # Make sure Scenario Package exists for this user
      existingScenPkg = self.scribe.getScenPackage(userId=self.userId, 
                                      scenPkgName=self.scenPackageName, 
                                      fillLayers=False)
      if existingScenPkg is None:
         raise LMError('ScenPackage {} must exist for User {}'
                       .format(self.scenPackageName, self.userId))
      validScenCodes = existingScenPkg.scenarios.keys()
      
      # Make sure modeling Scenario exists in this package
      if not self.modelScenCode in validScenCodes:
         raise LMError('Scenario {} must exist in ScenPackage {} for User {}'
                       .format(self.modelScenCode, self.scenPackageName, self.userId))
      # Make sure (optionally) listed projection Scenarios exist in this package
      if self.prjScenCodeList:
         for pcode in self.prjScenCodeList:
            if not pcode in validScenCodes:
               raise LMError('Scenario {} must exist in ScenPackage {} for User {}'
                             .format(pcode, self.scenPackageName, self.userId))
      # Default to projecting onto all Scenarios in this package
      else:
         self.prjScenCodeList = existingScenPkg.scenarios.keys()

      if not doMapBaseline:
         self.prjScenCodeList.remove(self.modelScenCode)

      # TODO: Need a mask layer for every scenario!!
      self.masklyr = masklyr
                     
      return existingScenPkg
                
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
   def _warnPermissions(self):
      if not isCorrectUser():
         print("""
               When not running this {} as `lmwriter`, make sure to fix
               permissions on the newly created shapegrid {}
               """.format(self.name, self.gridname))
         
   # ...............................................
   def _getDb(self):
      import logging
      loglevel = logging.INFO
      # Logfile
      secs = time.time()
      timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
      logname = '{}.{}'.format(self.name, timestamp)
      logger = ScriptLogger(logname, level=loglevel)
      # DB connection
      scribe = BorgScribe(logger)
      return scribe
   
# .............................................................................
   def _getAlgorithm(self, config, algHeading):
      """
      @note: Returns configured algorithm
      """
      acode =  config.get(algHeading, 'CODE')
      alg = Algorithm(acode)
      alg.fillWithDefaults()
      inputs = {}
      # override defaults with any option specified
      algoptions = config.getoptions(algHeading)
      for name in algoptions:
         pname, ptype = alg.findParamNameType(name)
         if pname is not None:
            if ptype == types.IntType:
               val = config.getint(algHeading, pname)
            elif ptype == types.FloatType:
               val = config.getfloat(algHeading, pname)
            else:
               val = config.get(algHeading, pname)
               # Some algorithms(mask) may have a parameter indicating a layer,
               # if so, add name to parameters and object to inputs
               if acode == 'hull_region_intersect' and pname == 'region':
                  inputs[pname] = val
            alg.setParameter(pname, val)
      if inputs:
         alg.setInputs(inputs)
      return alg
      
# .............................................................................
   def _getAlgorithms(self, config, sectionPrefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX):
      """
      @note: Returns configured algorithms, uses default algorithms only 
             if no others exist
      """
      algs = {}
      defaultAlgs = {}
      # Get algorithms for SDM modeling or SDM mask
      sections = config.getsections(sectionPrefix)
      for algHeading in sections:
         alg = self._getAlgorithm(config, algHeading)
         
         if algHeading.endswith(SERVER_DEFAULT_HEADING_POSTFIX):
            defaultAlgs[algHeading] = alg
         else:
            algs[algHeading] = alg
      if len(algs) == 0:
         algs = defaultAlgs
      return algs

   # ...............................................
   def _findScenPkgMeta(self, scenpkgName):
      scenpkg_meta_file = os.path.join(ENV_DATA_PATH, scenpkgName + '.py')
      if not os.path.exists(scenpkg_meta_file):
         print ('Missing Scenario Package metadata file {}'.format(scenpkg_meta_file))
         exit(-1)    

      if not os.path.exists(self.spMetaFname):
         raise LMError(currargs='Climate metadata {} does not exist'
                       .format(self.spMetaFname))
      # TODO: change to importlib on python 2.7 --> 3.3+  
      try:
         import imp
         SPMETA = imp.load_source('currentmetadata', self.spMetaFname)
      except Exception, e:
         raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                       .format(self.spMetaFname, e))         
      pkgMeta = SPMETA.CLIMATE_PACKAGES[scenpkgName]
      return pkgMeta

   # ...............................................
   def _findScenPkgBaseline(self, scenpkgName):
      pkgMeta = self._findScenPkgMeta(scenpkgName)
      baseCode = pkgMeta['baseline']
      return baseCode

   # ...............................................
   def _findScenPkgPredicted(self, scenpkgName):
      pkgMeta = self._findScenPkgMeta(scenpkgName)
      predCodes = pkgMeta['predicted']
      baseCode = pkgMeta['baseline']
      predCodes.extend(baseCode)
      return predCodes

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
      defaultEmail = '{}{}'.format(usr, DEFAULT_EMAIL_POSTFIX)
      usrEmail = self._getBoomOrDefault(config, 'ARCHIVE_USER_EMAIL', 
                                        defaultValue=defaultEmail)
      archiveName = self._getBoomOrDefault(config, 'ARCHIVE_NAME', 
                                           defaultValue=PUBLIC_ARCHIVE_NAME)
      priority = self._getBoomOrDefault(config, 'ARCHIVE_PRIORITY', 
                                        defaultValue=Priority.NORMAL)
      
      # Species data inputs
      occIdFname = self._getBoomOrDefault(config, 'OCCURRENCE_ID_FILENAME')
      if occIdFname:
         dataSource = SpeciesDatasource.EXISTING
      else:
         dataSource = self._getBoomOrDefault(config, 'DATASOURCE')
         dataSource = dataSource.upper()
      gbifFname = self._getBoomOrDefault(config, 'GBIF_OCCURRENCE_FILENAME')
      idigFname = self._getBoomOrDefault(config, 'IDIG_OCCURRENCE_DATA')
      idigOccSep = self._getBoomOrDefault(config, 'IDIG_OCCURRENCE_DATA_DELIMITER')
      bisonFname = self._getBoomOrDefault(config, 'BISON_TSN_FILENAME') 
      userOccFname = self._getBoomOrDefault(config, 'USER_OCCURRENCE_DATA')
      userOccSep = self._getBoomOrDefault(config, 'USER_OCCURRENCE_DATA_DELIMITER')
      minpoints = self._getBoomOrDefault(config, 'POINT_COUNT_MIN')
      algs = self._getAlgorithms(config, sectionPrefix='ALGORITHM')
      
      # Should be None or one Mask for pre-processing
      maskAlg = None
      maskAlgList = self._getAlgorithms(config, sectionPrefix=SERVER_SDM_MASK_HEADING_PREFIX)
      if len(maskAlgList) == 1:
         maskAlg = maskAlgList.values()[0]
         
      # optional MCPA inputs
      treeFname = self._getBoomOrDefault(config, 'TREE')
      biogeoName = self._getBoomOrDefault(config, 'BIOGEO_HYPOTHESES')
      bghypFnames = self._getBioGeoHypothesesLayerFilenames(biogeoName, usrPath)

      # RAD/PAM params
      doComputePAMStats = self._getBoomOrDefault(config, 'COMPUTE_PAM_STATS', isBool=True)
      assemblePams = self._getBoomOrDefault(config, 'ASSEMBLE_PAMS', isBool=True)
      gridbbox = self._getBoomOrDefault(config, 'GRID_BBOX', isList=True)
      cellsides = self._getBoomOrDefault(config, 'GRID_NUM_SIDES')
      cellsize = self._getBoomOrDefault(config, 'GRID_CELLSIZE')
      gridname = '{}-Grid-{}'.format(archiveName, cellsize)
      # TODO: allow filter
      gridFilter = self._getBoomOrDefault(config, 'INTERSECT_FILTERSTRING')
      gridIntVal = self._getBoomOrDefault(config, 'INTERSECT_VALNAME')
      gridMinPct = self._getBoomOrDefault(config, 'INTERSECT_MINPERCENT')
      gridMinPres = self._getBoomOrDefault(config, 'INTERSECT_MINPRESENCE')
      gridMaxPres = self._getBoomOrDefault(config, 'INTERSECT_MAXPRESENCE')
      intersectParams = {MatrixColumn.INTERSECT_PARAM_FILTER_STRING: gridFilter,
                         MatrixColumn.INTERSECT_PARAM_VAL_NAME: gridIntVal,
                         MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: gridMinPres,
                         MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: gridMaxPres,
                         MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: gridMinPct}

      scenPackageName = self._getBoomOrDefault(config, 'SCENARIO_PACKAGE')
      if scenPackageName is None:
         raise LMError('SCENARIO_PACKAGE must be configured')

      modelScenCode = self._getBoomOrDefault(config, 'SCENARIO_PACKAGE_MODEL_SCENARIO')
      if modelScenCode is None:
         modelScenCode = self._findScenPkgBaseline(scenPackageName)
      if modelScenCode is None:
         raise LMError('SCENARIO_PACKAGE_MODEL_SCENARIO must be configured in '+
                       'configuration file or CLIMATE_PACKAGES[baseline] in '+ 
                       'scenario package metadata file')
      prjScenCodeList = self._getBoomOrDefault(config, 
                  'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', isList=True)
      if not prjScenCodeList:
         prjScenCodeList = self._findScenPkgPredicted(scenPackageName)
      
      return (usr, usrPath, usrEmail, archiveName, priority, scenPackageName, 
              modelScenCode, prjScenCodeList, dataSource, 
              occIdFname, gbifFname, idigFname, idigOccSep, bisonFname, 
              userOccFname, userOccSep, minpoints, algs, 
              assemblePams, gridbbox, cellsides, cellsize, gridname, 
              intersectParams, maskAlg, treeFname, bghypFnames, 
              doComputePAMStats)
      
   # ...............................................
   def writeConfigFile(self, tree=None, biogeoMtx=None, biogeoLayers=[], fname=None):
      config = ConfigParser.SafeConfigParser()
      config.add_section(SERVER_BOOM_HEADING)
      
      # .........................................      
      # SDM Algorithms with all parameters   
      for heading, alg in self.algorithms.iteritems():
         config.add_section(heading)
         config.set(heading, 'CODE', alg.code)
         for name, val in alg.parameters.iteritems():
            config.set(heading, name, str(val))
      
      # SDM Mask input
      if self.maskAlg is not None:
         config.add_section(SERVER_SDM_MASK_HEADING_PREFIX)
         config.set(SERVER_SDM_MASK_HEADING_PREFIX, 'CODE', self.maskAlg.code)
         for name, val in self.maskAlg.parameters.iteritems():
            config.set(SERVER_SDM_MASK_HEADING_PREFIX, name, str(val))
      
      email = self.userEmail
      if email is None:
         email = ''

      config.set(SERVER_BOOM_HEADING, 'ARCHIVE_USER', self.userId)
      config.set(SERVER_BOOM_HEADING, 'ARCHIVE_NAME', self.archiveName)
      config.set(SERVER_BOOM_HEADING, 'ARCHIVE_PRIORITY', str(self.priority))
      config.set(SERVER_BOOM_HEADING, 'TROUBLESHOOTERS', email)
            
      # SDM input environmental data, pulled from SCENARIO_PACKAGE metadata
      pcodes = ','.join(self.prjScenCodeList)
      config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', 
                 pcodes)
      config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_MODEL_SCENARIO', 
                 self.modelScenCode)
      config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_MAPUNITS', self.scenPkg.mapUnits)
      config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_EPSG', str(self.scenPkg.epsgcode))
      config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE', self.scenPkg.name)
      
      # SDM input species source data and type (for processing)
      config.set(SERVER_BOOM_HEADING, 'DATASOURCE', self.dataSource)
      if self.dataSource == SpeciesDatasource.EXISTING:
         config.set(SERVER_BOOM_HEADING, 'OCCURRENCE_ID_FILENAME', 
                    self.occIdFname)
      elif self.dataSource == SpeciesDatasource.GBIF:
         config.set(SERVER_BOOM_HEADING, 'GBIF_OCCURRENCE_FILENAME', self.gbifFname)
         # TODO: allow overwrite of these vars in initboom --> archive config file
         config.set(SERVER_BOOM_HEADING, 'GBIF_TAXONOMY_FILENAME', 
                    GBIF_TAXONOMY_FILENAME)
         config.set(SERVER_BOOM_HEADING, 'GBIF_PROVIDER_FILENAME', 
                    GBIF_PROVIDER_FILENAME)
      elif self.dataSource == SpeciesDatasource.BISON:
         config.set(SERVER_BOOM_HEADING, 'BISON_TSN_FILENAME', self.bisonFname)
      elif self.dataSource == SpeciesDatasource.IDIGBIO:
         config.set(SERVER_BOOM_HEADING, 'IDIG_OCCURRENCE_DATA', self.idigFname)
         config.set(SERVER_BOOM_HEADING, 'IDIG_OCCURRENCE_DATA_DELIMITER',
                    self.idigOccSep)
      else:
         config.set(SERVER_BOOM_HEADING, 'USER_OCCURRENCE_DATA', 
                    self.userOccFname)
         config.set(SERVER_BOOM_HEADING, 'USER_OCCURRENCE_DATA_DELIMITER',
                    self.userOccSep)

      # Expiration date triggering re-query and computation
      config.set(SERVER_BOOM_HEADING, 'SPECIES_EXP_YEAR', str(CURRDATE[0]))
      config.set(SERVER_BOOM_HEADING, 'SPECIES_EXP_MONTH', str(CURRDATE[1]))
      config.set(SERVER_BOOM_HEADING, 'SPECIES_EXP_DAY', str(CURRDATE[2]))
      config.set(SERVER_BOOM_HEADING, 'POINT_COUNT_MIN', str(self.minpoints))

      # .........................................      
      # Global PAM vals
      # Intersection grid
      config.set(SERVER_BOOM_HEADING, 'GRID_NUM_SIDES', str(self.cellsides))
      config.set(SERVER_BOOM_HEADING, 'GRID_CELLSIZE', str(self.cellsize))
      config.set(SERVER_BOOM_HEADING, 'GRID_BBOX', 
                 ','.join(str(v) for v in self.gridbbox))
      config.set(SERVER_BOOM_HEADING, 'GRID_NAME', self.gridname)
      # Intersection params
      for k, v in self.intersectParams.iteritems():
         config.set(SERVER_BOOM_HEADING, 'INTERSECT_{}'.format(k.upper()), str(v))
      # TODO: For now, this defaults to True
      # TODO: Change to 0/1
      config.set(SERVER_BOOM_HEADING, 'ASSEMBLE_PAMS', str(True))

      # TODO: Test these new RAD params
      doHypotheses = doStats = 0
      # Name/User is unique constraint, so add name here
      # Only one allowed per gridset (aka archive), so this is 0/1
      if biogeoMtx is not None:
         doHypotheses = 1
      if self.doComputePAMStats:
         doStats = 1
      config.set(SERVER_BOOM_HEADING, 'BIOGEO_HYPOTHESES', str(doHypotheses))
      bioGeoLayerNames = ','.join(biogeoLayers)
      config.set(SERVER_BOOM_HEADING, 'BIOGEO_HYPOTHESES_LAYERS', bioGeoLayerNames)
      config.set(SERVER_BOOM_HEADING, 'COMPUTE_PAM_STATS', str(doStats))
      if tree is not None:
         config.set(SERVER_BOOM_HEADING, 'TREE', tree.name)
            
      readyFilename(self.outConfigFilename, overwrite=True)
      with open(self.outConfigFilename, 'wb') as configfile:
         config.write(configfile)

   
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
   def addUser(self):
      """
      @summary Adds provided userid to the database
      """
      user = LMUser(self.userId, self.userEmail, self.userEmail, 
                    modTime=mx.DateTime.gmt().mjd)
      self.scribe.log.info('  Find or insert user {} ...'.format(self.userId))
      updatedUser = self.scribe.findOrInsertUser(user)
      # If exists, found by unique Id or Email, update values
      self.userId = updatedUser.userid
      self.userEmail = updatedUser.email
   
   # ...............................................
   def _checkOccurrenceSets(self, limit=10):
      legalUsers = [PUBLIC_USER, self.userId]
      missingCount = 0
      wrongUserCount = 0
      nonIntCount = 0
      if not os.path.exists(self.occIdFname):
         raise LMError('Missing OCCURRENCE_ID_FILENAME {}'.format(self.occIdFname))
      else:
         count = 0
         for line in open(self.occIdFname, 'r'):
            count += 1
            try:
               tmp = line.strip()
            except Exception, e:
               self.scribe.log.info('Error reading line {} ({}), stopping'
                               .format(count, str(e)))
               break
            try:
               occid = int(tmp)
            except Exception, e:
               self.scribe.log.info('Unable to get Id from data {} on line {}'
                               .format(tmp, count))
               nonIntCount += 1
            else:
               occ = self.scribe.getOccurrenceSet(occId=occid)
               if occ is None:
                  missingCount += 1
               elif occ.getUserId() not in legalUsers:
                  self.scribe.log.info('Unauthorized user {} for ID {}'
                                  .format(occ.getUserId(), occid))
                  wrongUserCount += 1
            if count >= limit:
               break
      self.scribe.log.info('Errors out of {} read OccurrenceSets (limit {}):'.format(count, limit))
      self.scribe.log.info('  Missing: {} '.format(missingCount))
      self.scribe.log.info('  Unauthorized data: {} '.format(wrongUserCount))
      self.scribe.log.info('  Bad ID: {} '.format(nonIntCount))
    
   # ...............................................
   def _addIntersectGrid(self):
      shp = ShapeGrid(self.gridname, self.userId, self.scenPkg.epsgcode, self.cellsides, 
                      self.cellsize, self.scenPkg.mapUnits, self.gridbbox,
                      status=JobStatus.INITIALIZE, 
                      statusModTime=mx.DateTime.gmt().mjd)
      newshp = self.scribe.findOrInsertShapeGrid(shp)
      validData = False
      if newshp: 
         # check existence
         validData, _ = ShapeGrid.testVector(newshp.getDLocation())
         if not validData:
            try:
               dloc = newshp.getDLocation()
               newshp.buildShape(overwrite=True)
               validData, _ = ShapeGrid.testVector(dloc)
            except Exception, e:
               self.scribe.log.warning('Unable to build Shapegrid ({})'.format(str(e)))
            if not validData:
               raise LMError(currargs='Failed to write Shapegrid {}'.format(dloc))
         if validData and newshp.status != JobStatus.COMPLETE:
            newshp.updateStatus(JobStatus.COMPLETE)
            success = self.scribe.updateObject(newshp)
            if success is False:
               self.scribe.log.warning('Failed to update Shapegrid record')
      else:
         raise LMError(currargs='Failed to find or insert Shapegrid')
      return newshp
      
   # ...............................................
   def _findOrAddPAM(self, gridset, scen):
      # Create Global PAM for this archive, scenario
      # Pam layers are added upon boom processing
      pamType = MatrixType.PAM
      if self.userId == PUBLIC_USER:
         pamType = MatrixType.ROLLING_PAM
      desc = '{} for Scenario {}'.format(GPAM_KEYWORD, scen.code)
      pamMeta = {ServiceObject.META_DESCRIPTION: desc,
                 ServiceObject.META_KEYWORDS: [GPAM_KEYWORD, scen.code]}
      tmpGpam = LMMatrix(None, matrixType=pamType, 
                         gcmCode=scen.gcmCode, altpredCode=scen.altpredCode, 
                         dateCode=scen.dateCode, metadata=pamMeta, userId=self.userId, 
                         gridset=gridset, 
                         status=JobStatus.GENERAL, 
                         statusModTime=mx.DateTime.gmt().mjd)
      gpam = self.scribe.findOrInsertMatrix(tmpGpam)
      return gpam

   # ...............................................
   def _findOrAddGRIM(self, gridset, scen):
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

   # ...............................................
   def addShapeGridGPAMGridset(self):
      """
      @summary: Create a Gridset, Shapegrid, PAMs, GRIMs for this archive, and
                update attributes with new or existing values from DB
      """
      scenGrims = {}
      self.scribe.log.info('  Insert, build shapegrid {} ...'.format(self.gridname))
      shp = self._addIntersectGrid()
      self.shapegrid = shp
      # "BOOM" Archive
      # TODO: change 'parameters' to ServiceObject.META_PARAMS

      meta = {ServiceObject.META_DESCRIPTION: ARCHIVE_KEYWORD,
              ServiceObject.META_KEYWORDS: [ARCHIVE_KEYWORD],
              'parameters': self.inParamFname}
      grdset = Gridset(name=self.archiveName, metadata=meta, shapeGrid=shp, 
                       epsgcode=self.scenPkg.epsgcode, 
                       userId=self.userId, modTime=mx.DateTime.gmt().mjd)
      updatedGrdset = self.scribe.findOrInsertGridset(grdset)
      # "Global" PAM, GRIM (one each per scenario)
      for code, scen in self.scenPkg.scenarios.iteritems():
         gPam = self._findOrAddPAM(updatedGrdset, scen)
         if not(self.userId == DEFAULT_POST_USER) and self.assemblePams:
            scenGrim = self._findOrAddGRIM(updatedGrdset, scen)
            scenGrims[code] = scenGrim
      return scenGrims, updatedGrdset
   
# ...............................................
   def _initGRIMIntersect(self, lyr, mtx):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      currtime = mx.DateTime.gmt().mjd
      mtxcol = None
      intersectParams = {MatrixColumn.INTERSECT_PARAM_WEIGHTED_MEAN: True}

      if lyr is not None:
         # TODO: Save processType into the DB??
         if LMFormat.isGDAL(driver=lyr.dataFormat):
            ptype = ProcessType.INTERSECT_RASTER_GRIM
         else:
            self.scribe.log.debug('Vector intersect not yet implemented for GRIM column {}'
                                  .format(mtxcol.getId()))
   
         # TODO: Change ident to lyr.ident when that is populated
         tmpCol = MatrixColumn(None, mtx.getId(), self.userId, 
                layer=lyr, shapegrid=self.shapegrid, 
                intersectParams=intersectParams, 
                squid=lyr.squid, ident=lyr.name, processType=ptype, 
                status=JobStatus.GENERAL, statusModTime=currtime,
                postToSolr=False)
         mtxcol = self.scribe.findOrInsertMatrixColumn(tmpCol)
         
         # DB does not populate with shapegrid on insert
         mtxcol.shapegrid = self.shapegrid
         
         # TODO: This is a hack, post to solr needs to be retrieved from DB
         mtxcol.postToSolr = False
         if mtxcol is not None:
            self.scribe.log.debug('Found/inserted MatrixColumn {}'.format(mtxcol.getId()))
            # Reset processType (not in db)
            mtxcol.processType = ptype            
      return mtxcol

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
   
   # .............................
   def _createGrimMF(self, scencode, currtime):
      # Create MFChain for this GPAM
      desc = ('GRIM Makeflow for User {}, Archive {}, Scenario {}'
              .format(self.userId, self.archiveName, scencode))
      meta = {MFChain.META_CREATED_BY: self.name,
              MFChain.META_DESCRIPTION: desc}
      newMFC = MFChain(self.userId, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=currtime)
      grimChain = self.scribe.insertMFChain(newMFC)
      return grimChain
   
   # .............................
   def addGRIMChains(self, defaultGrims):
      currtime = mx.DateTime.gmt().mjd
      grimChains = []

      for code, grim in defaultGrims.iteritems():
         # Create MFChain for this GRIM
         grimChain = self._createGrimMF(code, currtime)
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
            relDir = os.path.splitext(mtxcol.layer.getRelativeDLocation())[0]
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
   def addBoomChain(self):
      """
      @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                and configFile written by BOOMFiller.initBoom.
      """
      meta = {MFChain.META_CREATED_BY: self.name,
              MFChain.META_DESCRIPTION: 'Boom start for User {}, Archive {}'
      .format(self.userId, self.archiveName)}
      newMFC = MFChain(self.userId, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=mx.DateTime.gmt().mjd)
      mfChain = self.scribe.insertMFChain(newMFC)

      baseAbsFilename, ext = os.path.splitext(self.outConfigFilename)
      # Boomer.ChristopherWalken writes this file when finished walking through 
      # species data (initiated by this Makeflow).  
      walkedArchiveFname = baseAbsFilename + LMFormat.LOG.ext

      # Create a rule from the MF and Arf file creation
      boomCmd = BoomerCommand(configFile=self.outConfigFilename)
      boomCmd.outputs.append(walkedArchiveFname)

      mfChain.addCommands([boomCmd.getMakeflowRule(local=True)])
      mfChain.write()
      mfChain.updateStatus(JobStatus.INITIALIZE)
      self.scribe.updateObject(mfChain)
      return mfChain

   # ...............................................
   def addTree(self, gridset):
      tree = None
      if self.treeFname is not None:
         currtime = mx.DateTime.gmt().mjd
         name = os.path.splitext(self.treeFname)[0]
         treeFilename = os.path.join(self.userIdPath, self.treeFname) 
         if os.path.exists(treeFilename):
            # TODO: save gridset link to tree???
            baretree = Tree(name, dlocation=treeFilename, userId=self.userId, 
                            gridsetId=gridset.getId(), modTime=currtime)
            tree = self.scribe.findOrInsertTree(baretree)
         else:
            self.scribe.log.warning('No tree at {}'.format(treeFilename))
   
         # Save tree link to gridset
         print "Add tree to grid set"
         gridset.addTree(tree)
         gridset.updateModtime(currtime)
         self.scribe.updateObject(gridset)
      return tree

   # ...............................................
   def _getBGMeta(self, bgFname):
      # defaults for no metadata file
      # lower-case dict keys
      bgkeyword = 'biogeographic hypothesis'
      lyrMeta = {MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower(): None,
                 ServiceObject.META_DESCRIPTION.lower(): 
      'Biogeographic hypothesis based on layer {}'.format(bgFname)}
      fpthbasename = os.path.splitext(bgFname)[0]
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
   def addBioGeoHypothesesMatrixAndLayers(self, gridset):
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
                  name = os.path.splitext(os.path.basename(bgFname))[0]
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
def initBoom(paramFname, walkNow=False):
   """
   @summary: Initialize an empty Lifemapper database and archive
   """
   filler = BOOMFiller(configFname=paramFname)
   filler.initializeInputs()

   # Add/find user for this Boom process (should exist)
   filler.addUser()

   # ...............................................
   # Data for this Boom archive
   # ...............................................
   # Test a subset of OccurrenceLayer Ids for existing or PUBLIC user
   if filler.occIdFname:
      filler._checkOccurrenceSets()
      
   # Add or get ShapeGrid, Global PAM, Gridset for this archive
   # This updates the gridset, shapegrid, default PAMs (rolling, with no 
   #     matrixColumns, default GRIMs with matrixColumns
   # Anonymous and simple SDM booms do not need Scenario GRIMs and return empty dict
   scenGrims, boomGridset = filler.addShapeGridGPAMGridset()
   # If there are Scenario GRIMs, create MFChain for each 
   filler.addGRIMChains(scenGrims)
   # If there is a tree, add and biogeographic hypotheses, create MFChain for each
   tree = filler.addTree(boomGridset)
   # If there are biogeographic hypotheses layers, add them and matrix 
   # TODO: create MFChain 
   biogeoMtx, biogeoLayerNames = filler.addBioGeoHypothesesMatrixAndLayers(boomGridset)
   
   
   # Write config file for this archive
#    filler.writeConfigFile(tree, biogeoMtx, biogeoLayers)
   filler.writeConfigFile(tree=tree, biogeoMtx=biogeoMtx, biogeoLayers=biogeoLayerNames)
   filler.scribe.log.info('')
   filler.scribe.log.info('******')
   filler.scribe.log.info('--config_file={}'.format(filler.outConfigFilename))   
   filler.scribe.log.info('gridset name = {}'.format(boomGridset.name))   
   filler.scribe.log.info('******')
   filler.scribe.log.info('')
         
   if walkNow is True:
      # Create MFChain to run Boomer on these inputs IFF not the initial archive 
      # If this is the initial archive, we will run the boomer as a daemon
      mfChain = filler.addBoomChain()
      
   filler.close()
   return filler.outConfigFilename
   
# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('--param_file', default=None,
            help=('Parameter file for the archive inputs and outputs ' +
                  'to be created from these data.'))
   parser.add_argument('--do_walk', type=bool, default=False,
            help=('Walk these species data to create Makeflow jobs immediately.'))
   args = parser.parse_args()
   paramFname = args.param_file
   doWalk = args.do_walk
         
   if paramFname is not None and not os.path.exists(paramFname):
      print ('Missing configuration file {}'.format(paramFname))
      exit(-1)
      
   print('Running catalogBoomJob with paramFname = {}'
         .format(paramFname))
   outConfigFilename = initBoom(paramFname, walkNow=doWalk)
   print('Completed catalogBoomJob creating boom parameters = {}'
         .format(outConfigFilename))

    
