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
from LmCommon.common.lmconstants import (DEFAULT_EPSG, DEFAULT_MAPUNITS, 
      DEFAULT_POST_USER, JobStatus, LMFormat, MatrixType, ProcessType, 
      SERVER_BOOM_HEADING, SERVER_SDM_ALGORITHM_HEADING_PREFIX, 
      SERVER_SDM_MASK_HEADING_PREFIX, SERVER_DEFAULT_HEADING_POSTFIX, 
      SERVER_PIPELINE_HEADING)
from LmCommon.common.readyfile import readyFilename

from LmDbServer.common.lmconstants import (SpeciesDatasource, TAXONOMIC_SOURCE, 
                                           TNCMetadata)
from LmDbServer.common.localconstants import (GBIF_PROVIDER_FILENAME, 
                                              GBIF_TAXONOMY_FILENAME)

from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, ARCHIVE_KEYWORD, 
                           ENV_DATA_PATH, DEFAULT_EMAIL_POSTFIX, GGRIM_KEYWORD,
                           GPAM_KEYWORD, LMFileType, Priority, 
                           PUBLIC_ARCHIVE_NAME)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector, Raster
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.scenario import Scenario, ScenPackage
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
      (self.usr, self.usrPath,
       self.usrEmail,
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
      self._fillScenarios(doMapBaseline=doMapBaseline)

      # Created by addArchive
      self.shapegrid = None
      
      # If running as root, new user filespace must have permissions corrected
      self._warnPermissions()

      earl = EarlJr()
      self.outConfigFilename = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                                   objCode=self.archiveName, 
                                                   usr=self.usr)
      
   # ...............................................
   def _findScenariosFromCodes(self, doMapBaseline=1):
      """
      @summary Find Scenarios from codes or create from ScenPackage metadata
      """
      existingScenPkg = None
      # TODO: Put optional masklayer into every Scenario
      masklyr = None  
      
      # Check to see if these exist as public or user data first
      if self.scenPackageName is not None:
         existingScenPkg = self.scribe.getScenPackage(userId=PUBLIC_USER, 
                                         scenPkgName=self.scenPackageName, 
                                         fillLayers=False)
         if not existingScenPkg:
            existingScenPkg = self.scribe.getScenPackage(userId=self.usr, 
                                            scenPkgName=self.scenPackageName, 
                                            fillLayers=False)
      # Required vals if not creating new from metadata and layers
      # SCENARIO_PACKAGE_MODEL_SCENARIO
      # SCENARIO_PACKAGE_PROJECTION_SCENARIOS
      if not existingScenPkg and self.modelScenCode is not None:
         codes = [self.modelScenCode]
         for c in self.prjScenCodeList:
            codes.append(c)
         allsps = self.scribe.getScenPackagesForUserCodes(PUBLIC_USER, codes, fillLayers=False)
         if not allsps:
            allsps = self.scribe.getScenPackagesForUserCodes(self.usr, codes, fillLayers=False)
         if len(allsps) > 0:
            existingScenPkg = allsps[0]
            
      return existingScenPkg
                
   # ...............................................
   def _fillScenarios(self, doMapBaseline=1):
      """
      @summary Find Scenarios from codes or create from ScenPackage metadata
      """
      # TODO: Put optional masklayer into every Scenario
      masklyr = None  
      
      # Check to see if these exist as public or user data first
      self.scenPkg = self._findScenariosFromCodes(doMapBaseline=1)
            
      # if SCENARIO_PACKAGE_MODEL_SCENARIO is missing, 
      # identify and construct from provided metadata 
      # Configured codes for existing Scenarios
      if self.scenPkg is not None:
         s = self.scenPkg.getScenario(code=self.modelScenCode)
         if s is None:
            raise LMError('Scenario Package {}: {} contains no scenarios'
                          .format(self.scenPkg.getId(), self.scenPkg.name))
         self.epsg = s.epsgcode
         self.mapunits = s.mapUnits
      else:
         if self.modelScenCode is not None:
            SPMETA, scenPackageMetaFilename, pkgMeta = self._pullClimatePackageMetadata()
#             masklyr = self._createMaskLayer(SPMETA, pkgMeta)
            self.scenPackageMetaFilename = None
            # Fill or reset epsgcode, mapunits, gridbbox
#             self.scenPkg, self.epsg, self.mapunits = self._checkScenarios()
            if not self.prjScenCodeList:
               self.prjScenCodeList = self.scenPkg.scenarios.keys()
         else:
            # If new ScenPackage was provided (not SDM scenario codes), fill codes 
            (self.scenPkg, self.modelScenCode, self.epsg, self.mapunits, 
             self.scenPackageMetaFilename, masklyr) = self._createScenarios()
            self.prjScenCodeList = self.scenPkg.scenarios.keys()

      if not doMapBaseline:
         self.prjScenCodeList.remove(self.modelScenCode)
      # TODO: Need a mask layer for every scenario!!
      self.masklyr = masklyr

      # Fill grid bbox with scenario package if it is absent
      if self.gridbbox is None:
         self.gridbbox = self.scenPkg.bbox

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
      usrEmail = self._getBoomOrDefault(config, 'ARCHIVE_USER_EMAIL')
      archiveName = self._getBoomOrDefault(config, 'ARCHIVE_NAME', 
                                           defaultValue=PUBLIC_ARCHIVE_NAME)
      priority = self._getBoomOrDefault(config, 'ARCHIVE_PRIORITY', 
                                        defaultValue=Priority.NORMAL)
      
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
      
      # Should be only one or None
      maskAlg = None
      maskAlgList = self._getAlgorithms(config, sectionPrefix=SERVER_SDM_MASK_HEADING_PREFIX)
      if len(maskAlgList) == 1:
         maskAlg = maskAlgList.values()[0]
         
      # RAD stats
      treeFname = self._getBoomOrDefault(config, 'TREE')
      biogeoName = self._getBoomOrDefault(config, 'BIOGEO_HYPOTHESES')
      bghypFnames = self._getBioGeoHypothesesLayerFilenames(biogeoName, usrPath)
      doComputePAMStats = self._getBoomOrDefault(config, 'COMPUTE_PAM_STATS', isBool=True)
      
#       mdlMaskName = self._getBoomOrDefault(config, 'MODEL_MASK_NAME')
#       prjMaskName = self._getBoomOrDefault(config, 'PROJECTION_MASK_NAME')
         
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
      # SCENARIO_PACKAGE_MODEL_SCENARIO and
      # SCENARIO_PACKAGE_PROJECTION_SCENARIOS
      # are required iff there is no environmental metadata from which to 
      # construct scenarios (baseline = Model Scen, all scenarios = Proj Scens)
      # Get epsg, mapunits from scenarios
      scenPackageName = self._getBoomOrDefault(config, 'SCENARIO_PACKAGE')
      modelScenCode = self._getBoomOrDefault(config, 'SCENARIO_PACKAGE_MODEL_SCENARIO')
      prjScenCodeList = self._getBoomOrDefault(config, 
                  'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', isList=True)
      
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
      
      email = self.usrEmail
      if email is None:
         email = ''

      config.set(SERVER_BOOM_HEADING, 'ARCHIVE_USER', self.usr)
      config.set(SERVER_BOOM_HEADING, 'ARCHIVE_NAME', self.archiveName)
      config.set(SERVER_BOOM_HEADING, 'ARCHIVE_PRIORITY', str(self.priority))
      config.set(SERVER_BOOM_HEADING, 'TROUBLESHOOTERS', email)
            
      # SDM input environmental data, pulled from SCENARIO_PACKAGE metadata
      pcodes = ','.join(self.prjScenCodeList)
      config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', 
                 pcodes)
      config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_MODEL_SCENARIO', 
                 self.modelScenCode)
      config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_MAPUNITS', self.mapunits)
      config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_EPSG', str(self.epsg))
      config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE', self.scenPackageName)
      
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
   def addUsers(self, isInitial=True):
      """
      @summary Adds PUBLIC_USER, DEFAULT_POST_USER and USER from metadata to the database
      """
      userList = []
      if isInitial:
         userList.append((PUBLIC_USER,'{}{}'.format(PUBLIC_USER, 
                                                    DEFAULT_EMAIL_POSTFIX)))
         userList.append((DEFAULT_POST_USER,'{}{}'.format(DEFAULT_POST_USER, 
                                                          DEFAULT_EMAIL_POSTFIX)))
      if self.usr != PUBLIC_USER:
         email = self.usrEmail
         if email is None:
            email = '{}{}'.format(self.usr, DEFAULT_EMAIL_POSTFIX)
         userList.append((self.usr, email))
   
      for uinfo in userList:
         try:
            user = LMUser(uinfo[0], uinfo[1], uinfo[1], modTime=mx.DateTime.gmt().mjd)
         except:
            pass
         else:
            self.scribe.log.info('  Find or insert user {} ...'.format(uinfo[0]))
            updatedUser = self.scribe.findOrInsertUser(user)
            if updatedUser.userid == self.usr and self.usrEmail is None:
               self.usrEmail = updatedUser.email
   
#    # ...............................................
#    def _checkScenarios(self):
#       epsg = mapunits = None
#       if self.modelScenCode not in self.prjScenCodeList:
#          self.prjScenCodeList.append(self.modelScenCode)
#       scenPkgs = self.scribe.getScenPackagesForUserCodes(self.usr, 
#                                                        self.prjScenCodeList)
#       if not scenPkgs:
#          scenPkgs = self.scribe.getScenPackagesForUserCodes(PUBLIC_USER, 
#                                                           self.prjScenCodeList)
#       if len(scenPkgs) == 0:
#          raise LMError('There are no matching scenPackages!')
#       elif len(scenPkgs) > 1:
#          raise LMError('I cannot handle multiple matching scenPackages!')
#       else:
#          scenPkg = scenPkgs[0]
#          scen = scenPkg.getScenario(code=self.prjScenCodeList[0])
#          epsg = scen.epsgcode
#          mapunits = scen.mapUnits
# 
#       return scenPkg, epsg, mapunits
         
   # ...............................................
   def _createScenarios(self):
      # TODO move these next 2 commands into fillScenarios
      SPMETA, scenPackageMetaFilename, pkgMeta = self._pullClimatePackageMetadata()
      # TODO: Put optional masklayer into every Scenario
#       masklyr = self._createMaskLayer(SPMETA, pkgMeta)
      masklyr = None

      epsg = pkgMeta['epsg']
      mapunits = pkgMeta['mapunits']
      self.scribe.log.info('  Read ScenPackage {} metadata ...'.format(self.scenPackageName))
      scenPkg = ScenPackage(self.scenPackageName, self.usr, 
                            epsgcode=epsg,
                            mapunits=mapunits,
                            modTime=mx.DateTime.gmt().mjd)
      
      # Current
      basescen, staticLayers = self._createBaselineScenario(pkgMeta,  
                                                      SPMETA.LAYERTYPE_META,
                                                      SPMETA.BASELINE_META)
      self.scribe.log.info('     Assembled base scenario {}'.format(basescen.code))
      scenPkg.addScenario(basescen)

      # Predicted Past and Future
      allScens = self._createPredictedScenarios(pkgMeta, 
                                           SPMETA.LAYERTYPE_META, 
                                           staticLayers,
                                           SPMETA.PREDICTED_META)
      self.scribe.log.info('     Assembled predicted scenarios {}'.format(allScens.keys()))
      for scen in allScens.values():
         scenPkg.addScenario(scen)
      
      scenPkg.resetBBox()
      return scenPkg, basescen.code, epsg, mapunits, scenPackageMetaFilename, masklyr
   
   # ...............................................
   def _checkOccurrenceSets(self, limit=10):
      legalUsers = [PUBLIC_USER, self.usr]
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
   def _getbioName(self, code, res, gcm=None, tm=None, altpred=None, 
                   lyrtype=None, suffix=None, isTitle=False):
      sep = '-'
      if isTitle: 
         sep = ', '
      name = code
      if lyrtype is not None:
         name = sep.join((lyrtype, name))
      for descriptor in (gcm, altpred, tm, res, suffix):
         if descriptor is not None:
            name = sep.join((name, descriptor))
      return name
    
   # ...............................................
   def _getOptionalMetadata(self, metaDict, key):
      """
      @summary Assembles layer metadata for mask
      """
      val = None
      try:
         val = metaDict[key]
      except:
         pass
      return val

   # ...............................................
   def _createScenMaskLayer(self, SPMETA, pkgMeta, scenMeta):
      """
      @summary Assembles layer metadata for input to optional 
               pre-processing SDM Mask step identified in scenario package 
               metadata. 
               Currently only the 'hull_region_intersect' method is available.
      """
      # Required keys in SDM_MASK_INPUT: name, bbox, gdaltype, gdalformat, file
      maskMeta = SPMETA.SDM_MASK_INPUT
      
      lyrmeta = {
         Vector.META_IS_CATEGORICAL: self._getOptionalMetadata(maskMeta, 'iscategorical'), 
         ServiceObject.META_TITLE: self._getOptionalMetadata(maskMeta, 'title'), 
         ServiceObject.META_AUTHOR: self._getOptionalMetadata(maskMeta, 'author'), 
         ServiceObject.META_DESCRIPTION: self._getOptionalMetadata(maskMeta, 'description'),
         ServiceObject.META_KEYWORDS: self._getOptionalMetadata(maskMeta, 'keywords'),
         ServiceObject.META_CITATION: self._getOptionalMetadata(maskMeta, 'citation')}
      # required
      try:
         name = maskMeta['name']
         bbox = maskMeta['bbox']
         relfname = maskMeta['file']
         dtype = maskMeta['gdaltype']
         dformat = maskMeta['gdalformat']
      except KeyError:
         raise LMError(currargs='Missing one of: name, bbox, file, gdaltype, '+ 
                       'gdalformat in SDM_MASK_INPUT in scenario package metadata')
      else:   
         dloc = os.path.join(ENV_DATA_PATH, relfname)
         if not os.path.exists(dloc):
            print('Missing local data %s' % dloc)

      masklyr = Raster(name, self.usr, 
                       pkgMeta['epsg'], 
                       mapunits=pkgMeta['mapunits'],  
                       resolution=scenMeta['res'][1], 
                       dlocation=dloc, metadata=lyrmeta, 
                       dataFormat=dformat, 
                       gdalType=dtype, 
                       bbox=bbox,
                       modTime=mx.DateTime.gmt().mjd)
      return masklyr

   
   # ...............................................
   def _getBaselineLayers(self, pkgMeta, baseMeta, lyrtypeMeta):
      """
      @summary Assembles layer metadata for a single layerset
      """
      currtime = mx.DateTime.gmt().mjd
      layers = []
      staticLayers = {}
      dateCode = baseMeta['times'].keys()[0]
      res_name = baseMeta['res'][0]
      res_val = baseMeta['res'][1]
#       resolution = baseMeta['res']
      region = baseMeta['region']
      for envcode in pkgMeta['layertypes']:
         ltmeta = lyrtypeMeta[envcode]
         envKeywords = [k for k in baseMeta['keywords']]
         relfname, isStatic = self._findFileFor(ltmeta, baseMeta['code'], 
                                           gcm=None, tm=None, altPred=None)
         lyrname = self._getbioName(baseMeta['code'], res_name, 
                                    lyrtype=envcode, suffix=pkgMeta['suffix'])
         lyrmeta = {'title': ' '.join((baseMeta['code'], ltmeta['title'])),
                    'description': ' '.join((baseMeta['code'], ltmeta['description']))}
         envmeta = {'title': ltmeta['title'],
                    'description': ltmeta['description'],
                    'keywords': envKeywords.extend(ltmeta['keywords'])}
         dloc = os.path.join(ENV_DATA_PATH, relfname)
         if not os.path.exists(dloc):
            print('Missing local data %s' % dloc)
         envlyr = EnvLayer(lyrname, self.usr, pkgMeta['epsg'], 
                           dlocation=dloc, 
                           lyrMetadata=lyrmeta,
                           dataFormat=pkgMeta['gdalformat'], 
                           gdalType=pkgMeta['gdaltype'],
                           valUnits=ltmeta['valunits'],
                           mapunits=pkgMeta['mapunits'], 
                           resolution=res_val, 
                           bbox=region, 
                           modTime=currtime, 
                           envCode=envcode, 
                           dateCode=dateCode,
                           envMetadata=envmeta,
                           envModTime=currtime)
         layers.append(envlyr)
         if isStatic:
            staticLayers[envcode] = envlyr
      return layers, staticLayers

   # ...............................................
   def _findFileFor(self, ltmeta, basecode, gcm=None, tm=None, altPred=None):
      isStatic = False
      ltfiles = ltmeta['files']
      if len(ltfiles) == 1:
         isStatic = True
         relFname = ltfiles.keys()[0]
         if basecode in ltfiles[relFname]:
            return relFname, isStatic
      else:
         for relFname, kList in ltfiles.iteritems():
            if basecode in kList:
               return relFname, isStatic
            elif (gcm in kList and tm in kList and
                  (altPred is None or altPred in kList)):
               return relFname, isStatic
      print('Failed to find layertype {} for basecode {}, gcm {}, altpred {}, time {}'
            .format(ltmeta['title'], basecode, gcm, altPred, tm))
      return None, None
         

   # ...............................................
   def _getPredictedLayers(self, pkgMeta, lyrtypeMeta, staticLayers,
                           scenMeta, basecode, tm, gcm=None, altpred=None):
      """
      @summary Assembles layer metadata for a single layerset
      """
      currtime = mx.DateTime.gmt().mjd
      res_name = scenMeta['res'][0]
      res_val = scenMeta['res'][1]
      mdlname = tmname = None
      if gcm is not None:
         mdlname = scenMeta['models'][gcm]['name']
         tmname = scenMeta['times'][tm]['name']
      layers = []
      rstType = None
      layertypes = pkgMeta['layertypes']
      for envcode in layertypes:
         keywords = [k for k in scenMeta['keywords']]
         ltmeta = lyrtypeMeta[envcode]
         relfname, isStatic = self._findFileFor(ltmeta, basecode, 
                                           gcm=gcm, tm=tm, altPred=altpred)
         if not isStatic:
            lyrname = self._getbioName(basecode, res_name, gcm=gcm, tm=tm, 
                                  altpred=altpred, lyrtype=envcode, 
                                  suffix=pkgMeta['suffix'], isTitle=False)
            lyrtitle = self._getbioName(basecode, res_name, gcm=gcm, tm=tmname, 
                                   altpred=altpred, lyrtype=envcode, 
                                   suffix=pkgMeta['suffix'], isTitle=True)
            scentitle = scenMeta['name']
#             scentitle = self._getbioName(basecode, res_name, gcm=mdlname, 
#                                     tm=tmname, altpred=altpred, 
#                                     suffix=pkgMeta['suffix'], isTitle=True)
            lyrdesc = '{} for {}'.format(ltmeta['description'], scentitle)
            
            lyrmeta = {'title': lyrtitle, 'description': lyrdesc}
            envmeta = {'title': ltmeta['title'],
                       'description': ltmeta['description'],
                       'keywords': keywords.extend(ltmeta['keywords'])}
            dloc = os.path.join(ENV_DATA_PATH, relfname)
            if not os.path.exists(dloc):
               print('Missing local data %s' % dloc)
               dloc = None
            envlyr = EnvLayer(lyrname, self.usr, pkgMeta['epsg'], 
                              dlocation=dloc, 
                              lyrMetadata=lyrmeta,
                              dataFormat=pkgMeta['gdalformat'], 
                              gdalType=rstType,
                              valUnits=ltmeta['valunits'],
                              mapunits=pkgMeta['mapunits'], 
                              resolution=res_val, 
                              bbox=scenMeta['region'], 
                              modTime=currtime,
                              envCode=envcode, 
                              gcmCode=gcm, altpredCode=altpred, dateCode=tm,
                              envMetadata=envmeta, 
                              envModTime=currtime)
         else:
            # Use the observed data
            envlyr = staticLayers[envcode]
         layers.append(envlyr)
      return layers
   
   
   # ...............................................
   def _createBaselineScenario(self, pkgMeta, lyrtypeMeta, 
                              scenMeta, climKeywords):
      """
      @summary Assemble Worldclim/bioclim scenario
      """
      baseCode = scenMeta['code']
      res_name = scenMeta['res'][0]
      res_val = scenMeta['res'][1]
      region = scenMeta['region']
      basekeywords = [k for k in climKeywords]
      basekeywords.extend(scenMeta['keywords'])
      # there should only be one
      dateCode = scenMeta['times'].keys()[0]
      scencode = self._getbioName(baseCode, res_name, suffix=pkgMeta['suffix'])
      lyrs, staticLayers = self._getBaselineLayers(pkgMeta, scenMeta, 
                                              lyrtypeMeta)
      scenmeta = {ServiceObject.META_TITLE: scenMeta['title'], 
                  ServiceObject.META_AUTHOR: scenMeta['author'], 
                  ServiceObject.META_DESCRIPTION: scenMeta['description'], 
                  ServiceObject.META_KEYWORDS: basekeywords}
      scen = Scenario(scencode, self.usr, pkgMeta['epsg'], 
                      metadata=scenmeta, 
                      units=pkgMeta['mapunits'], 
                      res=res_val, 
                      dateCode=dateCode,
                      bbox=region, 
                      modTime=mx.DateTime.gmt().mjd,  
                      layers=lyrs)
      return scen, staticLayers
   
   # ...............................................
   def _createSimpleScenario(self, code, pkgMeta, lyrtypeMeta, 
                             staticLayers, scenMeta):
      """
      @summary Assemble Worldclim/bioclim scenario
      """
      res_name = scenMeta['res'][0]
      res_val = scenMeta['res'][1]
      # there should only be one
      lyrs = self._getPredictedLayers(pkgMeta, lyrtypeMeta, staticLayers, 
                                      scenMeta, code, 
                                      scenMeta['date'], 
                                      gcm=scenMeta['gcm'], 
                                      altpred=scenMeta['altpred'])

      scenmeta = {ServiceObject.META_TITLE: scenMeta['name'], 
                  ServiceObject.META_AUTHOR: scenMeta['author'], 
                  ServiceObject.META_DESCRIPTION: scenMeta['description'], 
                  ServiceObject.META_KEYWORDS: scenMeta['keywords']}
      scen = Scenario(code, self.usr, pkgMeta['epsg'], 
                      metadata=scenmeta, 
                      units=pkgMeta['mapunits'], 
                      res=res_val, 
                      dateCode=scenMeta['date'],
                      bbox=scenMeta['region'], 
                      modTime=mx.DateTime.gmt().mjd,  
                      layers=lyrs)
      return scen
            
   
   # ...............................................
   def _createPredictedScenarios(self, pkgMeta, lyrtypeMeta, staticLayers,
                                predMeta):
      """
      @summary Assemble predicted future scenarios defined by IPCC report
      """
      predScenarios = {}
      for scode in pkgMeta['predicted']:
         thisScenMeta = predMeta[scode]

      # Assemble one or more simply-defined scenarios
         thisScen = self._createSimpleScenario(scode, pkgMeta, 
                             lyrtypeMeta, staticLayers,
                             thisScenMeta)
         predScenarios[scode] = thisScen
      return predScenarios            
   
   # ...............................................
   def addPackageScenariosLayers(self):
      """
      @summary Add scenPackage, scenario and layer metadata to database, and 
               update the scenPkg attribute with newly inserted objects
      """
      if self.scenPkg.getId() is not None:
         self.scribe.log.info('ScenarioPackage {} is present'
                              .format(self.scenPkg.name))
      else:
         updatedScens = []
         updatedScenPkg = self.scribe.findOrInsertScenPackage(self.scenPkg)
         for scode, scen in self.scenPkg.scenarios.iteritems():
            if scen.getId() is not None:
               self.scribe.log.info('Scenario {} is present'.format(scode))
               updatedScens.append(scen)
            else:
               self.scribe.log.info('Insert scenario {}'.format(scode))
               newscen = self.scribe.findOrInsertScenario(scen, 
                                                   scenPkgId=updatedScenPkg.getId())
               updatedScens.append(newscen)
         self.scenPkg.setScenarios(updatedScens)
   
   # ...............................................
   def addMaskLayer(self):
      if self.masklyr:
         self.scribe.findOrInsertLayer(self.masklyr)

   # ...............................................
   def _findClimatePackageMetadata(self):
      scenPackageMetaFilename = os.path.join(ENV_DATA_PATH, 
                     '{}{}'.format(self.scenPackageName, LMFormat.PYTHON.ext))      
      if not os.path.exists(scenPackageMetaFilename):
         raise LMError(currargs='Climate metadata {} does not exist'
                       .format(scenPackageMetaFilename))
      # TODO: change to importlib on python 2.7 --> 3.3+  
      try:
         import imp
         SPMETA = imp.load_source('currentmetadata', scenPackageMetaFilename)
      except Exception, e:
         raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                       .format(scenPackageMetaFilename, e))
      return SPMETA, scenPackageMetaFilename
   
   # ...............................................
   def _pullClimatePackageMetadata(self):
      """
      All layers in a package must share EPSG. mapunits, GDAL format
      """
      SPMETA, scenPackageMetaFilename = self._findClimatePackageMetadata()
      # Combination of scenario and layer attributes making up these data 
      pkgMeta = SPMETA.CLIMATE_PACKAGES[self.scenPackageName]
      # Spatial and format attributes of data files
      # Resolution and BBox is in each scenario, not global for package
      return SPMETA, scenPackageMetaFilename, pkgMeta

   # ...............................................
   def _addIntersectGrid(self):
      shp = ShapeGrid(self.gridname, self.usr, self.epsg, self.cellsides, 
                      self.cellsize, self.mapunits, self.gridbbox,
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
      if self.usr == PUBLIC_USER:
         pamType = MatrixType.ROLLING_PAM
      desc = '{} for Scenario {}'.format(GPAM_KEYWORD, scen.code)
      pamMeta = {ServiceObject.META_DESCRIPTION: desc,
                 ServiceObject.META_KEYWORDS: [GPAM_KEYWORD, scen.code]}
      tmpGpam = LMMatrix(None, matrixType=pamType, 
                         gcmCode=scen.gcmCode, altpredCode=scen.altpredCode, 
                         dateCode=scen.dateCode, metadata=pamMeta, userId=self.usr, 
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
                         dateCode=scen.dateCode, metadata=grimMeta, userId=self.usr, 
                         gridset=gridset, 
                         status=JobStatus.GENERAL, 
                         statusModTime=mx.DateTime.gmt().mjd)
      grim = self.scribe.findOrInsertMatrix(tmpGrim)
      for lyr in scen.layers:
         # Add to GRIM Makeflow ScenarioLayer and MatrixColumn
         mtxcol = self._initGRIMIntersect(lyr, grim)
      return grim

   # ...............................................
#    def addArchive(self):
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
                       epsgcode=self.epsg, 
                       userId=self.usr, modTime=mx.DateTime.gmt().mjd)
      updatedGrdset = self.scribe.findOrInsertGridset(grdset)
      # "Global" PAM, GRIM (one each per scenario)
      for code, scen in self.scenPkg.scenarios.iteritems():
         gPam = self._findOrAddPAM(updatedGrdset, scen)
         if not(self.usr == DEFAULT_POST_USER) and self.assemblePams:
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
         tmpCol = MatrixColumn(None, mtx.getId(), self.usr, 
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
              .format(self.usr, self.archiveName, scencode))
      meta = {MFChain.META_CREATED_BY: self.name,
              MFChain.META_DESCRIPTION: desc}
      newMFC = MFChain(self.usr, priority=self.priority, 
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

   # .............................
   def addTNCEcoregions(self):
      meta = {Vector.META_IS_CATEGORICAL: TNCMetadata.isCategorical, 
              ServiceObject.META_TITLE: TNCMetadata.title, 
              ServiceObject.META_AUTHOR: TNCMetadata.author, 
              ServiceObject.META_DESCRIPTION: TNCMetadata.description,
              ServiceObject.META_KEYWORDS: TNCMetadata.keywords,
              ServiceObject.META_CITATION: TNCMetadata.citation,
              }
      dloc = os.path.join(ENV_DATA_PATH, 
                          TNCMetadata.filename + LMFormat.getDefaultOGR().ext)
      ecoregions = Vector(TNCMetadata.title, PUBLIC_USER, DEFAULT_EPSG, 
                          ident=None, dlocation=dloc, 
                          metadata=meta, dataFormat=LMFormat.getDefaultOGR().driver, 
                          ogrType=TNCMetadata.ogrType,
                          valAttribute=TNCMetadata.valAttribute, 
                          mapunits=DEFAULT_MAPUNITS, bbox=TNCMetadata.bbox,
                          modTime=mx.DateTime.gmt().mjd)
      updatedEcoregions = self.scribe.findOrInsertLayer(ecoregions)
      return updatedEcoregions

   # ...............................................
   def addAlgorithms(self):
      """
      @summary Adds algorithms to the database from the algorithm dictionary
      """
      algs = []
      for alginfo in Algorithms.implemented():
         meta = {'name': alginfo.name, 
                 'isDiscreteOutput': alginfo.isDiscreteOutput,
                 'outputFormat': alginfo.outputFormat,
                 'acceptsCategoricalMaps': alginfo.acceptsCategoricalMaps}
         alg = Algorithm(alginfo.code, metadata=meta)
         self.scribe.log.info('  Insert algorithm {} ...'.format(alginfo.code))
         algid = self.scribe.findOrInsertAlgorithm(alg)
         algs.append(algid)
   
   # ...............................................
   def addBoomChain(self):
      """
      @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                and configFile written by BOOMFiller.initBoom.
      """
      meta = {MFChain.META_CREATED_BY: self.name,
              MFChain.META_DESCRIPTION: 'Boom start for User {}, Archive {}'
      .format(self.usr, self.archiveName)}
      newMFC = MFChain(self.usr, priority=self.priority, 
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
         treeFilename = os.path.join(self.usrPath, self.treeFname) 
         if os.path.exists(treeFilename):
            # TODO: save gridset link to tree???
            baretree = Tree(name, dlocation=treeFilename, userId=self.usr, 
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
               lyr = Vector(name, self.usr, self.epsg, dlocation=bgFname, 
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
                           userId=self.usr, gridset=gridset, metadata=meta,
                           status=JobStatus.INITIALIZE, statusModTime=currtime)
         bgMtx = self.scribe.findOrInsertMatrix(tmpMtx)
         if bgMtx is None:
            self.scribe.log.info('  Failed to add biogeo hypotheses matrix')
      return bgMtx, biogeoLayerNames

# ...............................................
def initBoom(paramFname, isInitial=True):
   """
   @summary: Initialize an empty Lifemapper database and archive
   """
   filler = BOOMFiller(configFname=paramFname)
   filler.initializeInputs()

   # This user and default users
   # Add param user, PUBLIC_USER, DEFAULT_POST_USER users
   filler.addUsers(isInitial=isInitial)

   if isInitial:
      # Insert all taxonomic sources for now
      filler.scribe.log.info('  Insert taxonomy metadata ...')
      for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
         taxSourceId = filler.scribe.findOrInsertTaxonSource(taxInfo['name'],
                                                             taxInfo['url'])
      filler.addAlgorithms()
      # Insert all taxonomic sources for now
      filler.scribe.log.info('  Insert Algorithms ...')
      filler.addTNCEcoregions()

   # ...............................................
   # Data for this Boom archive
   # ...............................................
   # This updates the scenPkg with db objects for other operations
   filler.addPackageScenariosLayers()
   
   filler.addMaskLayer()
         
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
         
   if not isInitial:
      # Create MFChain to run Boomer on these inputs IFF not the initial archive 
      # If this is the initial archive, we will run the boomer as a daemon
      mfChain = filler.addBoomChain()
      
   filler.close()
   return boomGridset
   
# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('--config_file', default=None,
            help=('Configuration file for the archive, gridset, and grid ' +
                  'to be created from these data.'))
   parser.add_argument('--is_first_run', action='store_true',
            help=('For first, public archive, compute "Global PAM" '))
   args = parser.parse_args()
   paramFname = args.config_file
   isInitial = args.is_first_run
         
   if paramFname is not None and not os.path.exists(paramFname):
      print ('Missing configuration file {}'.format(paramFname))
      exit(-1)
      
   print('Running initBoom with isInitial = {}, configFname = {}'
         .format(isInitial, paramFname))
   initBoom(paramFname, isInitial=isInitial)


    
"""
import mx.DateTime
import os
from osgeo.ogr import wkbPolygon
import time
from types import IntType, FloatType

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, LMFormat, 
   ProcessType, JobStatus, MatrixType, SERVER_PIPELINE_HEADING, 
   SERVER_BOOM_HEADING, SERVER_SDM_MASK_HEADING_PREFIX, DEFAULT_MAPUNITS, DEFAULT_EPSG)
from LmCommon.common.readyfile import readyFilename
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource,
                                           TNCMetadata)
from LmDbServer.common.localconstants import (GBIF_TAXONOMY_FILENAME, 
                                              GBIF_PROVIDER_FILENAME)
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, LMFileType, ENV_DATA_PATH, 
         GPAM_KEYWORD, GGRIM_KEYWORD, ARCHIVE_KEYWORD, PUBLIC_ARCHIVE_NAME, 
         DEFAULT_EMAIL_POSTFIX, Priority, ProcessTool)
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmBackend.common.cmd import MfRule
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.scenario import Scenario, ScenPackage
from LmServer.legion.shapegrid import ShapeGrid
from LmDbServer.boom.initboom import BOOMFiller

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd

cfname='/state/partition1/lmscratch/temp/sax_biotaphy.ini'
cfname='/state/partition1/lmscratch/temp/heuchera_boom_params.ini'
cfname='/state/partition1/lmscratch/temp/taiwan_boom_params.ini'
cfname='/state/partition1/lmscratch/temp/file_85752.ini'

filler = BOOMFiller(configFname=cfname)
filler.initializeInputs()


filler.addPackageScenariosLayers()
scenGrims, boomGridset = filler.addShapeGridGPAMGridset()
tree = filler.addTree(boomGridset)
biogeoMtx, biogeoLayerNames = filler.addBioGeoHypothesesMatrixAndLayers(boomGridset)
filler.writeConfigFile(tree=tree, biogeoMtx=biogeoMtx, biogeoLayers=biogeoLayerNames)

# ...............................................
# Data for this instance (Taxonomy, algorithms, default users)
# ...............................................
if isInitial:
   filler.scribe.log.info('  Insert taxonomy metadata ...')
   for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
      taxSourceId = filler.scribe.findOrInsertTaxonSource(taxInfo['name'],taxInfo['url'])

filler.addAlgorithms()
filler.addTNCEcoregions()
filler.addUsers()

# ...............................................
# Data for this Boom archive
# ...............................................
# This updates the scenPkg with db objects for other operations
filler.addPackageScenariosLayers()

filler.addMaskLayer()
      
# Test a subset of OccurrenceLayer Ids for existing or PUBLIC user
if filler.occIdFname:
   filler._checkOccurrenceSets()
      
scenGrims, boomGridset = filler.addShapeGridGPAMGridset()

filler.addGRIMChains(scenGrims)

tree = filler.addTree(boomGridset)

biogeoMtx, biogeoLayerNames = filler.addBioGeoHypothesesMatrixAndLayers(boomGridset)


# Write config file for this archive
#    filler.writeConfigFile(tree, biogeoMtx, biogeoLayers)
filler.writeConfigFile(tree=tree, biogeoMtx=biogeoMtx, biogeoLayers=biogeoLayerNames)

mfChain = filler.addBoomChain()
   
filler.close()
"""
