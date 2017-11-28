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
import ConfigParser
import mx.DateTime
import os
import time
from types import IntType, FloatType

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
from LmDbServer.boom.pamme import CURR_MJD

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd

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
      (self.usr,
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
       self.maskAlg) = self.readParamVals()
       
      # Fill existing scenarios AND SDM mask layer from configured codes 
      # or create from ScenPackage metadata
      self._fillScenarios()

      # Created by addArchive
      self.shapegrid = None
      
      # If running as root, new user filespace must have permissions corrected
      self._warnPermissions()

      earl = EarlJr()
      self.outConfigFilename = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                                   objCode=self.archiveName, 
                                                   usr=self.usr)
      
   # ...............................................
   def _fillScenarios(self):
      """
      @summary Find Scenarios from codes or create from ScenPackage metadata
      """
      # Configured codes for existing Scenarios
      if self.modelScenCode is not None:
         SPMETA, scenPackageMetaFilename, pkgMeta, elyrMeta = self._pullClimatePackageMetadata()      
         masklyr = self._createMaskLayer(SPMETA, pkgMeta, elyrMeta)
         self.scenPackageMetaFilename = None
         # Fill or reset epsgcode, mapunits, gridbbox
         self.scenPkg, self.epsg, self.mapunits = self._checkScenarios()
      else:
         # If new ScenPackage was provided (not SDM scenario codes), fill codes 
         (self.scenPkg, self.modelScenCode, self.epsg, self.mapunits, 
          self.scenPackageMetaFilename, masklyr) = self._createScenarios()
         self.prjScenCodeList = self.scenPkg.scenarios.keys()
      # Fill mask layer
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
   @property
   def userPath(self):
      earl = EarlJr()
      pth = earl.createDataPath(self.usr, LMFileType.BOOM_CONFIG)
      return pth

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
            if ptype == IntType:
               val = config.getint(algHeading, pname)
            elif ptype == FloatType:
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
      # Find package name or code list, check for scenarios and epsg, mapunits
      scenPackageName = self._getBoomOrDefault(config, 'SCENARIO_PACKAGE')
      if scenPackageName is not None:
         modelScenCode = self._getBoomOrDefault(config, 'SCENARIO_PACKAGE_MODEL_SCENARIO')
         prjScenCodeList = self._getBoomOrDefault(config, 
                     'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', isList=True)
      
      return (usr, usrEmail, archiveName, priority, scenPackageName, 
              modelScenCode, prjScenCodeList, dataSource, 
              occIdFname, gbifFname, idigFname, idigOccSep, bisonFname, 
              userOccFname, userOccSep, minpoints, algs, 
              assemblePams, gridbbox, cellsides, cellsize, gridname, 
              intersectParams, maskAlg)
      
   # ...............................................
   def writeConfigFile(self, fname=None):
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
      config.set(SERVER_BOOM_HEADING, 'ASSEMBLE_PAMS', str(True))
            
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
            user = LMUser(uinfo[0], uinfo[1], uinfo[1], modTime=CURR_MJD)
         except:
            pass
         else:
            self.scribe.log.info('  Find or insert user {} ...'.format(uinfo[0]))
            updatedUser = self.scribe.findOrInsertUser(user)
            if updatedUser.userid == self.usr and self.usrEmail is None:
               self.usrEmail = updatedUser.email
   
   # ...............................................
   def _checkScenarios(self):
      epsg = mapunits = None
      if self.modelScenCode not in self.prjScenCodeList:
         self.prjScenCodeList.append(self.modelScenCode)
      scenPkgs = self.scribe.getScenPackagesForUserCodes(self.usr, 
                                                       self.prjScenCodeList)
      if not scenPkgs:
         scenPkgs = self.scribe.getScenPackagesForUserCodes(PUBLIC_USER, 
                                                          self.prjScenCodeList)
      if len(scenPkgs) == 0:
         raise LMError('There are no matching scenPackages!')
      elif len(scenPkgs) > 1:
         raise LMError('I cannot handle multiple matching scenPackages!')
      else:
         scenPkg = scenPkgs[0]
         scen = scenPkg.getScenario(code=self.prjScenCodeList[0])
         epsg = scen.epsgcode
         mapunits = scen.mapUnits

      return scenPkg, epsg, mapunits
         
   # ...............................................
   def _createScenarios(self):
      # TODO move these next 2 commands into fillScenarios
      SPMETA, scenPackageMetaFilename, pkgMeta, elyrMeta = self._pullClimatePackageMetadata()
      masklyr = self._createMaskLayer(SPMETA, pkgMeta, elyrMeta)

      epsg = elyrMeta['epsg']
      mapunits = elyrMeta['mapunits']
      self.scribe.log.info('  Read ScenPackage {} metadata ...'.format(self.scenPackageName))
      scenPkg = ScenPackage(self.scenPackageName, self.usr, 
                            epsgcode=epsg,
                            bbox=pkgMeta['bbox'],
                            mapunits=mapunits,
                            modTime=CURR_MJD)
      # Current
      basescen, staticLayers = self._createBaselineScenario(pkgMeta, elyrMeta, 
                                                      SPMETA.LAYERTYPE_META,
                                                      SPMETA.OBSERVED_PREDICTED_META,
                                                      SPMETA.CLIMATE_KEYWORDS)
      self.scribe.log.info('     Assembled base scenario {}'.format(basescen.code))
      scenPkg.addScenario(basescen)
      # Predicted Past and Future
      allScens = self._createPredictedScenarios(pkgMeta, elyrMeta, 
                                           SPMETA.LAYERTYPE_META, staticLayers,
                                           SPMETA.OBSERVED_PREDICTED_META,
                                           SPMETA.CLIMATE_KEYWORDS)
      self.scribe.log.info('     Assembled predicted scenarios {}'.format(allScens.keys()))
      for scen in allScens.values():
         scenPkg.addScenario(scen)
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
   def _getbioName(self, obsOrPred, res, gcm=None, tm=None, altpred=None, 
                   lyrtype=None, suffix=None, isTitle=False):
      sep = '-'
      if isTitle: 
         sep = ', '
      name = obsOrPred
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
   def _createMaskLayer(self, SPMETA, pkgMeta, elyrMeta):
      """
      @summary Assembles layer metadata for optional mask from scenario package
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

      # epsg, mapunits and resolution must match the Scenario Package
      epsg = elyrMeta['epsg']
      munits = elyrMeta['mapunits'] 
      res = elyrMeta['resolution']
         
      masklyr = Raster(name, self.usr, 
                       epsg, 
                       mapunits=munits,  
                       resolution=res, 
                       dlocation=dloc, metadata=lyrmeta, 
                       dataFormat=dformat, 
                       gdalType=dtype, 
                       bbox=bbox,
                       modTime=CURR_MJD)
      return masklyr
   
   # ...............................................
   def _getBaselineLayers(self, pkgMeta, baseMeta, elyrMeta, lyrtypeMeta):
      """
      @summary Assembles layer metadata for a single layerset
      """
      layers = []
      staticLayers = {}
      dateCode = baseMeta['times'].keys()[0]
      for envcode in pkgMeta['layertypes']:
         ltmeta = lyrtypeMeta[envcode]
         envKeywords = [k for k in baseMeta['keywords']]
         relfname, isStatic = self._findFileFor(ltmeta, baseMeta['code'], 
                                           gcm=None, tm=None, altPred=None)
         lyrname = self._getbioName(baseMeta['code'], pkgMeta['res'], 
                                    lyrtype=envcode, suffix=pkgMeta['suffix'])
         lyrmeta = {'title': ' '.join((baseMeta['code'], ltmeta['title'])),
                    'description': ' '.join((baseMeta['code'], ltmeta['description']))}
         envmeta = {'title': ltmeta['title'],
                    'description': ltmeta['description'],
                    'keywords': envKeywords.extend(ltmeta['keywords'])}
         dloc = os.path.join(ENV_DATA_PATH, relfname)
         if not os.path.exists(dloc):
            print('Missing local data %s' % dloc)
         envlyr = EnvLayer(lyrname, self.usr, elyrMeta['epsg'], 
                           dlocation=dloc, 
                           lyrMetadata=lyrmeta,
                           dataFormat=elyrMeta['gdalformat'], 
                           gdalType=elyrMeta['gdaltype'],
                           valUnits=ltmeta['valunits'],
                           mapunits=elyrMeta['mapunits'], 
                           resolution=elyrMeta['resolution'], 
                           bbox=pkgMeta['bbox'], 
                           modTime=CURR_MJD, 
                           envCode=envcode, 
                           dateCode=dateCode,
                           envMetadata=envmeta,
                           envModTime=CURR_MJD)
         layers.append(envlyr)
         if isStatic:
            staticLayers[envcode] = envlyr
      return layers, staticLayers

   # ...............................................
   def _findFileFor(self, ltmeta, scencode, gcm=None, tm=None, altPred=None):
      isStatic = False
      ltfiles = ltmeta['files']
      if len(ltfiles) == 1:
         isStatic = True
         relFname = ltfiles.keys()[0]
         if scencode in ltfiles[relFname]:
            return relFname, isStatic
      else:
         for relFname, kList in ltmeta['files'].iteritems():
            if scencode in kList:
               if gcm == None and tm == None and altPred == None:
                  return relFname, isStatic
               elif (gcm in kList and tm in kList and
                     (altPred is None or altPred in kList)):
                  return relFname, isStatic
      print('Failed to find layertype {} for {}, gcm {}, altpred {}, time {}'
            .format(ltmeta['title'], scencode, gcm, altPred, tm))
      return None, None
         
   # ...............................................
   def _getPredictedLayers(self, pkgMeta, elyrMeta, lyrtypeMeta, staticLayers,
                           observedPredictedMeta, predRpt, tm, gcm=None, altpred=None):
      """
      @summary Assembles layer metadata for a single layerset
      """
      mdlvals = observedPredictedMeta[predRpt]['models'][gcm]
      tmvals = observedPredictedMeta[predRpt]['times'][tm]
      layers = []
      rstType = None
      layertypes = pkgMeta['layertypes']
      for envcode in layertypes:
         keywords = [k for k in observedPredictedMeta[predRpt]['keywords']]
         ltmeta = lyrtypeMeta[envcode]
         relfname, isStatic = self._findFileFor(ltmeta, predRpt, 
                                           gcm=gcm, tm=tm, altPred=altpred)
         if not isStatic:
            lyrname = self._getbioName(predRpt, pkgMeta['res'], gcm=gcm, tm=tm, 
                                  altpred=altpred, lyrtype=envcode, 
                                  suffix=pkgMeta['suffix'], isTitle=False)
            lyrtitle = self._getbioName(predRpt, pkgMeta['res'], gcm=gcm, tm=tmvals['name'], 
                                   altpred=altpred, lyrtype=envcode, 
                                   suffix=pkgMeta['suffix'], isTitle=True)
            scentitle = self._getbioName(predRpt, pkgMeta['res'], gcm=mdlvals['name'], 
                                    tm=tmvals['name'], altpred=altpred, 
                                    suffix=pkgMeta['suffix'], isTitle=True)
            lyrdesc = '{} for {}'.format(ltmeta['description'], scentitle)
            
            lyrmeta = {'title': lyrtitle, 'description': lyrdesc}
            envmeta = {'title': ltmeta['title'],
                       'description': ltmeta['description'],
                       'keywords': keywords.extend(ltmeta['keywords'])}
            dloc = os.path.join(ENV_DATA_PATH, relfname)
            if not os.path.exists(dloc):
               print('Missing local data %s' % dloc)
               dloc = None
            envlyr = EnvLayer(lyrname, self.usr, elyrMeta['epsg'], 
                              dlocation=dloc, 
                              lyrMetadata=lyrmeta,
                              dataFormat=elyrMeta['gdalformat'], 
                              gdalType=rstType,
                              valUnits=ltmeta['valunits'],
                              mapunits=elyrMeta['mapunits'], 
                              resolution=elyrMeta['resolution'], 
                              bbox=pkgMeta['bbox'], 
                              modTime=CURR_MJD,
                              envCode=envcode, 
                              gcmCode=gcm, altpredCode=altpred, dateCode=tm,
                              envMetadata=envmeta, 
                              envModTime=CURR_MJD)
         else:
            # Use the observed data
            envlyr = staticLayers[envcode]
         layers.append(envlyr)
      return layers
   
   
   # ...............................................
   def _createBaselineScenario(self, pkgMeta, elyrMeta, lyrtypeMeta, 
                              observedPredictedMeta, climKeywords):
      """
      @summary Assemble Worldclim/bioclim scenario
      """
      baseMeta = observedPredictedMeta['baseline']
      baseCode = baseMeta['code']
   #    tm = baseMeta['times'].keys()[0]
      basekeywords = [k for k in climKeywords]
      basekeywords.extend(baseMeta['keywords'])
      # there should only be one
      dateCode = baseMeta['times'].keys()[0]
      scencode = self._getbioName(baseCode, pkgMeta['res'], suffix=pkgMeta['suffix'])
      lyrs, staticLayers = self._getBaselineLayers(pkgMeta, baseMeta, elyrMeta, 
                                              lyrtypeMeta)
      scenmeta = {ServiceObject.META_TITLE: baseMeta['title'], 
                  ServiceObject.META_AUTHOR: baseMeta['author'], 
                  ServiceObject.META_DESCRIPTION: baseMeta['description'], 
                  ServiceObject.META_KEYWORDS: basekeywords}
      scen = Scenario(scencode, self.usr, elyrMeta['epsg'], 
                      metadata=scenmeta, 
                      units=elyrMeta['mapunits'], 
                      res=elyrMeta['resolution'], 
                      dateCode=dateCode,
                      bbox=pkgMeta['bbox'], 
                      modTime=CURR_MJD,  
                      layers=lyrs)
      return scen, staticLayers
   
   # ...............................................
   def _createPredictedScenarios(self, pkgMeta, elyrMeta, lyrtypeMeta, staticLayers,
                                observedPredictedMeta, climKeywords):
      """
      @summary Assemble predicted future scenarios defined by IPCC report
      """
      predScenarios = {}
      try:
         predScens = pkgMeta['predicted']
      except:
         return predScenarios
      
      for predRpt in predScens.keys():
         for modelDef in predScens[predRpt]:
            gcm = modelDef[0]
            tm = modelDef[1]
            try:
               altpred = modelDef[2]
            except:
               altpred = None
               altvals = {}
            else:
               altvals = observedPredictedMeta[predRpt]['alternatePredictions'][altpred]
            mdlvals = observedPredictedMeta[predRpt]['models'][gcm]
            tmvals = observedPredictedMeta[predRpt]['times'][tm]
            # Reset keywords
            scenkeywords = [k for k in climKeywords]
            scenkeywords.extend(observedPredictedMeta[predRpt]['keywords'])
            for vals in (mdlvals, tmvals, altvals):
               try:
                  scenkeywords.extend(vals['keywords'])
               except:
                  pass
            # LM Scenario code, title, description
            scencode = self._getbioName(predRpt, pkgMeta['res'], gcm=gcm, 
                                   tm=tm, altpred=altpred, 
                                   suffix=pkgMeta['suffix'], isTitle=False)
            scentitle = self._getbioName(predRpt, pkgMeta['res'], gcm=mdlvals['name'], 
                                    tm=tmvals['name'], altpred=altpred, 
                                    suffix=pkgMeta['suffix'], isTitle=True)
            obstitle = observedPredictedMeta['baseline']['title']
            scendesc =  ' '.join((obstitle, 
                     'and predicted climate calculated from {}'.format(scentitle)))
            scenmeta = {ServiceObject.META_TITLE: scentitle, 
                        ServiceObject.META_AUTHOR: mdlvals['author'], 
                        ServiceObject.META_DESCRIPTION: scendesc, 
                        ServiceObject.META_KEYWORDS: scenkeywords}
            lyrs = self._getPredictedLayers(pkgMeta, elyrMeta, lyrtypeMeta, 
                                 staticLayers, observedPredictedMeta, predRpt, tm, 
                                 gcm=gcm, altpred=altpred)
            
            scen = Scenario(scencode, self.usr, elyrMeta['epsg'], 
                            metadata=scenmeta, 
                            units=elyrMeta['mapunits'], 
                            res=elyrMeta['resolution'], 
                            gcmCode=gcm, altpredCode=altpred, dateCode=tm,
                            bbox=pkgMeta['bbox'], 
                            modTime=CURR_MJD, 
                            layers=lyrs)
            predScenarios[scencode] = scen
      return predScenarios
   
   # ...............................................
   def addPackageScenariosLayers(self):
      """
      @summary Add scenPackage, scenario and layer metadata to database, and 
               update the scenPkg attribute with newly inserted objects
      """
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
      SPMETA, scenPackageMetaFilename = self._findClimatePackageMetadata()
      # Combination of scenario and layer attributes making up these data 
      pkgMeta = SPMETA.CLIMATE_PACKAGES[self.scenPackageName]
      try:
         epsg = SPMETA.EPSG
      except:
         raise LMError('Failed to specify EPSG for {}'
                       .format(self.scenPackageName))
      try:
         mapunits = SPMETA.MAPUNITS
      except:
         raise LMError('Failed to specify MAPUNITS for {}'
                       .format(self.scenPackageName))
      try:
         resInMapunits = SPMETA.RESOLUTIONS[pkgMeta['res']]
      except:
         raise LMError('Failed to specify res (or RESOLUTIONS values) for {}'
                       .format(self.scenPackageName))
      try:
         gdaltype = SPMETA.ENVLYR_GDALTYPE
      except:
         raise LMError('Failed to specify ENVLYR_GDALTYPE for {}'
                       .format(self.scenPackageName))
      try:
         gdalformat = SPMETA.ENVLYR_GDALFORMAT
      except:
         raise LMError(currargs='Failed to specify SPMETA.ENVLYR_GDALFORMAT for {}'
                       .format(self.scenPackageName))
      # Spatial and format attributes of data files
      elyrMeta = {'epsg': epsg, 
                    'mapunits': mapunits, 
                    'resolution': resInMapunits, 
                    'gdaltype': gdaltype, 
                    'gdalformat': gdalformat}
      return SPMETA, scenPackageMetaFilename, pkgMeta, elyrMeta

   # ...............................................
   def _addIntersectGrid(self):
      shp = ShapeGrid(self.gridname, self.usr, self.epsg, self.cellsides, 
                      self.cellsize, self.mapunits, self.gridbbox,
                      status=JobStatus.INITIALIZE, statusModTime=CURR_MJD)
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
   def _findOrAddDefaultMatrices(self, gridset, scen):
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
                         status=JobStatus.GENERAL, statusModTime=CURR_MJD)
      gpam = self.scribe.findOrInsertMatrix(tmpGpam)
      # Anonymous and simple SDM booms do not need GRIMs
      grim = None
      if not(self.usr == DEFAULT_POST_USER or self.assemblePams):
         # Create Scenario-GRIM for this archive, scenario
         # GRIM layers are added now
         desc = '{} for Scenario {}'.format(GGRIM_KEYWORD, scen.code)
         grimMeta = {ServiceObject.META_DESCRIPTION: desc,
                    ServiceObject.META_KEYWORDS: [GGRIM_KEYWORD]}
         tmpGrim = LMMatrix(None, matrixType=MatrixType.GRIM, 
                            gcmCode=scen.gcmCode, altpredCode=scen.altpredCode, 
                            dateCode=scen.dateCode, metadata=grimMeta, userId=self.usr, 
                            gridset=gridset, 
                            status=JobStatus.GENERAL, statusModTime=CURR_MJD)
         grim = self.scribe.findOrInsertMatrix(tmpGrim)
         for lyr in scen.layers:
            # Add to GRIM Makeflow ScenarioLayer and MatrixColumn
            mtxcol = self._initGRIMIntersect(lyr, grim)
      return gpam, grim

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
                         status=JobStatus.GENERAL, statusModTime=CURR_MJD)
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
                         status=JobStatus.GENERAL, statusModTime=CURR_MJD)
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
      meta = {ServiceObject.META_DESCRIPTION: ARCHIVE_KEYWORD,
              ServiceObject.META_KEYWORDS: [ARCHIVE_KEYWORD]}
      grdset = Gridset(name=self.archiveName, metadata=meta, shapeGrid=shp, 
                       dlocation=self.scenPackageMetaFilename, epsgcode=self.epsg, 
                       userId=self.usr, modTime=CURR_MJD)
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
                status=JobStatus.GENERAL, statusModTime=CURR_MJD,
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
      grimChains = []

      for code, grim in defaultGrims.iteritems():
         # Create MFChain for this GRIM
         grimChain = self._createGrimMF(code, CURR_MJD)
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
                          modTime=CURR_MJD)
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
                       statusModTime=CURR_MJD)
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
   
   # Write config file for this archive
   filler.writeConfigFile()
   filler.scribe.log.info('')
   filler.scribe.log.info('******')
   filler.scribe.log.info('--config_file={}'.format(filler.outConfigFilename))   
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
filler = BOOMFiller(configFname=cfname)
filler.initializeInputs()


filler = BOOMFiller()
filler.initializeInputs()

filler.usr
filler.usrEmail,
filler.archiveName,
filler.priority,
filler.scenPackageName,
filler.modelScenCode,
filler.prjScenCodeList,
filler.dataSource,
filler.occIdFname,
filler.gbifFname,
filler.idigFname,
filler.idigOccSep,
filler.bisonFname,
filler.userOccFname,
filler.userOccSep,   
filler.minpoints,
filler.algorithms,
filler.assemblePams,
filler.gridbbox,
filler.cellsides,
filler.cellsize,
filler.gridname, 
filler.intersectParams, 
filler.maskAlg
# ...............................................
# Data for this instance (Taxonomy, algorithms, default users)
# ...............................................
if isInitial:
   # Insert all taxonomic sources for now
   filler.scribe.log.info('  Insert taxonomy metadata ...')
   for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
      taxSourceId = filler.scribe.findOrInsertTaxonSource(taxInfo['name'],taxInfo['url'])
filler.initializeInputs()
   filler.addAlgorithms()
   filler.addTNCEcoregions()
      
# This user and default users
# Add param user, PUBLIC_USER, DEFAULT_POST_USER users
filler.addUsers()

# ...............................................
# Data for this Boom archive
# ...............................................
# This updates the scenPkg with db objects for other operations
filler.addPackageScenariosLayers()
      
# Test a subset of OccurrenceLayer Ids for existing or PUBLIC user
if filler.occIdFname:
   filler._checkOccurrenceSets()
      
# Add or get ShapeGrid, Global PAM, Gridset for this archive
# This updates the gridset, shapegrid, default PAMs (rolling, with no 
#     matrixColumns, default GRIMs with matrixColumns
# Anonymous and simple SDM booms do not need Scenario GRIMs and return empty dict
scenGrims, gridset = filler.addShapeGridGPAMGridset()

# If there are Scenario GRIMs, create MFChain for each 
filler.addGRIMChains(scenGrims)
   
# Write config file for this archive
filler.writeConfigFile()

# Create MFChain to run Boomer daemon on these inputs
mfChain = filler.addBoomChain()
filler.scribe.log.info('Wrote {}'.format(filler.outConfigFilename))   
filler.close()

"""
