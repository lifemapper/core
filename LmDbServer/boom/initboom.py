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
import os
import time
from types import IntType

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, LMFormat, 
                        ProcessType, JobStatus, MatrixType, SERVER_BOOM_HEADING)
from LmCommon.common.readyfile import readyFilename
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmDbServer.common.localconstants import (ASSEMBLE_PAMS, 
      GBIF_TAXONOMY_FILENAME, GBIF_PROVIDER_FILENAME, GBIF_OCCURRENCE_FILENAME, 
      BISON_TSN_FILENAME, IDIG_OCCURRENCE_DATA, IDIG_OCCURRENCE_DATA_DELIMITER,
      USER_OCCURRENCE_DATA, USER_OCCURRENCE_DATA_DELIMITER,
      INTERSECT_FILTERSTRING, INTERSECT_VALNAME, INTERSECT_MINPERCENT, 
      INTERSECT_MINPRESENCE, INTERSECT_MAXPRESENCE, SCENARIO_PACKAGE,
      GRID_CELLSIZE, GRID_NUM_SIDES)
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, LMFileType, ENV_DATA_PATH, 
         GPAM_KEYWORD, GGRIM_KEYWORD, ARCHIVE_KEYWORD, PUBLIC_ARCHIVE_NAME, 
         DEFAULT_EMAIL_POSTFIX, Priority, ProcessTool)
from LmServer.common.localconstants import (PUBLIC_USER, DATASOURCE, 
                                            POINT_COUNT_MIN)
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.cmd import MfRule
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.scenario import Scenario, ScenPackage
from LmServer.legion.shapegrid import ShapeGrid

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
      self.inConfigFname = configFname
      # Get database
      try:
         self.scribe = self._getDb()
      except: 
         raise
      self.open()
      
   # ...............................................
   def initializeInputs(self, configFname=None):
      """
      @summary Initialize configured and stored inputs for BOOMFiller class.
      """      
      # Allow reset configuration
      if configFname is not None:
         self.inConfigFname = configFname
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
       self.intersectParams) = self.readConfigArgs()
       
      # Fill existing scenarios from configured codes 
      # or create from ScenPackage metadata
      self._fillScenarios()

      # Created by addArchive
      self.gridset = None
      self.shapegrid = None
      self.defaultPamGrims = {}
      
      # If running as root, new user filespace must have permissions corrected
      self._warnPermissions()

      earl = EarlJr()
      self.outConfigFilename = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                                   objCode=self.archiveName, 
                                                   usr=self.usr)
      
   # ...............................................
   def _fillScenarios(self, configFname=None):
      """
      @summary Find Scenarios from codes or create from ScenPackage metadata
      """
      # Configured codes for existing Scenarios
      if self.modelScenCode and self.prjScenCodeList:
         self.scenPackageMetaFilename = None
         # Fill or reset epsgcode, mapunits, gridbbox
         self.scenPkg, self.epsg, self.mapunits = self._checkScenarios(
                                                      [PUBLIC_USER, self.usr])
      else:
         # If new ScenPackage was provided (not SDM scenario codes), fill codes 
         (self.scenPkg, self.modelScenCode, self.epsg, self.mapunits, 
          self.scenPackageMetaFilename) = self._createScenarios()
         self.prjScenCodeList = self.scenPkg.scenarios.keys()

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
               When not running this script as `lmwriter`, make sure to fix
               permissions on the newly created shapegrid {}
               """.format(self.gridname))
         
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
   def _getAlgorithms(self, config):
      algorithms = []
      # Get algorithms for SDM modeling
      sections = config.getsections('ALGORITHM')
      for algHeading in sections:
         acode =  config.get(algHeading, 'CODE')
         alg = Algorithm(acode)
         alg.fillWithDefaults()
         # override defaults with any option specified
         algoptions = config.getoptions(algHeading)
         for name in algoptions:
            pname, ptype = alg.findParamNameType(name)
            if pname is not None:
               if ptype == IntType:
                  val = config.getint(algHeading, pname)
               else:
                  val = config.getfloat(algHeading, pname)
               alg.setParameter(pname, val)
         algorithms.append(alg)
      return algorithms
      
   # ...............................................
   def readConfigArgs(self):
      if self.inConfigFname is None or not os.path.exists(self.inConfigFname):
         print('Missing config file {}, using defaults'.format(self.inConfigFname))
         configFname = None
      else:
         configFname = self.inConfigFname
      config = Config(siteFn=configFname)
   
      # Fill in missing or null variables for archive.config.ini
      usr = self._findConfigOrDefault(config, 'ARCHIVE_USER', PUBLIC_USER)
      usrEmail = self._findConfigOrDefault(config, 'ARCHIVE_USER_EMAIL', 
                                 '{}{}'.format(PUBLIC_USER, DEFAULT_EMAIL_POSTFIX))
      archiveName = self._findConfigOrDefault(config, 'ARCHIVE_NAME', 
                                              PUBLIC_ARCHIVE_NAME)
      priority = self._findConfigOrDefault(config, 'ARCHIVE_PRIORITY', 
                                              Priority.NORMAL)
      
      dataSource = self._findConfigOrDefault(config, 'DATASOURCE', DATASOURCE)
      dataSource = dataSource.upper()
      occIdFname = self._findConfigOrDefault(config, 'OCCURRENCE_ID_FILENAME', 
                                             None)
      gbifFname = self._findConfigOrDefault(config, 'GBIF_OCCURRENCE_FILENAME', 
                                       GBIF_OCCURRENCE_FILENAME)
      idigFname = self._findConfigOrDefault(config, 'IDIG_OCCURRENCE_DATA', 
                                            IDIG_OCCURRENCE_DATA)
      idigOccSep = self._findConfigOrDefault(config, 
            'IDIG_OCCURRENCE_DATA_DELIMITER', IDIG_OCCURRENCE_DATA_DELIMITER)
      bisonFname = self._findConfigOrDefault(config, 'BISON_TSN_FILENAME', 
                                       BISON_TSN_FILENAME) 
      userOccFname = self._findConfigOrDefault(config, 'USER_OCCURRENCE_DATA', 
                                       USER_OCCURRENCE_DATA)
      userOccSep = self._findConfigOrDefault(config, 
               'USER_OCCURRENCE_DATA_DELIMITER', USER_OCCURRENCE_DATA_DELIMITER)
      minpoints = self._findConfigOrDefault(config, 'POINT_COUNT_MIN', 
                                            POINT_COUNT_MIN)
      algs = self._getAlgorithms(config)
         
      assemblePams = self._findConfigOrDefault(config, 'ASSEMBLE_PAMS', 
                                               ASSEMBLE_PAMS)
      gridbbox = self._findConfigOrDefault(config, 'GRID_BBOX', None, isList=True)
      cellsides = self._findConfigOrDefault(config, 'GRID_NUM_SIDES', 
                                            GRID_NUM_SIDES)
      cellsize = self._findConfigOrDefault(config, 'GRID_CELLSIZE', 
                                           GRID_CELLSIZE)
      gridname = '{}-Grid-{}'.format(archiveName, cellsize)
      # TODO: allow filter
      gridFilter = self._findConfigOrDefault(config, 'INTERSECT_FILTERSTRING', 
                                        INTERSECT_FILTERSTRING)
      gridIntVal = self._findConfigOrDefault(config, 'INTERSECT_VALNAME', 
                                        INTERSECT_VALNAME)
      gridMinPct = self._findConfigOrDefault(config, 'INTERSECT_MINPERCENT', 
                                        INTERSECT_MINPERCENT)
      gridMinPres = self._findConfigOrDefault(config, 'INTERSECT_MINPRESENCE', 
                                         INTERSECT_MINPRESENCE)
      gridMaxPres = self._findConfigOrDefault(config, 'INTERSECT_MAXPRESENCE', 
                                         INTERSECT_MAXPRESENCE)
      intersectParams = {MatrixColumn.INTERSECT_PARAM_FILTER_STRING: gridFilter,
                         MatrixColumn.INTERSECT_PARAM_VAL_NAME: gridIntVal,
                         MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: gridMinPres,
                         MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: gridMaxPres,
                         MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: gridMinPct}
      # Find package name or code list, check for scenarios and epsg, mapunits
      scenPackageName = self._findConfigOrDefault(config, 'SCENARIO_PACKAGE', 
                                                 SCENARIO_PACKAGE)
      if scenPackageName is not None:
         modelScenCode = self._findConfigOrDefault(config, 
                     'SCENARIO_PACKAGE_MODEL_SCENARIO', None, isList=False)
         prjScenCodeList = self._findConfigOrDefault(config, 
                     'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', None, isList=True)
      
      return (usr, usrEmail, archiveName, priority, scenPackageName, 
              modelScenCode, prjScenCodeList, dataSource, 
              occIdFname, gbifFname, idigFname, idigOccSep, bisonFname, 
              userOccFname, userOccSep, minpoints, algs, 
              assemblePams, gridbbox, cellsides, cellsize, gridname, 
              intersectParams)
      
   # ...............................................
   def writeConfigFile(self, fname=None, mdlMaskName=None, prjMaskName=None):
      """
      """
      readyFilename(self.outConfigFilename, overwrite=True)
      f = open(self.outConfigFilename, 'w')
      f.write('[{}]\n'.format(SERVER_BOOM_HEADING))
      f.write('ARCHIVE_USER: {}\n'.format(self.usr))
      f.write('ARCHIVE_NAME: {}\n'.format(self.archiveName))
      if self.usrEmail is not None:
         f.write('TROUBLESHOOTERS: {}\n'.format(self.usrEmail))
      f.write('\n')   
   
      f.write('; ...................\n')
      f.write('; SDM Params\n')
      f.write('; ...................\n')
      # Expiration date triggering re-query and computation
      f.write('SPECIES_EXP_YEAR: {}\n'.format(CURRDATE[0]))
      f.write('SPECIES_EXP_MONTH: {}\n'.format(CURRDATE[1]))
      f.write('SPECIES_EXP_DAY: {}\n'.format(CURRDATE[2]))
      f.write('\n')
      # Minimun number of required species points   
      f.write('POINT_COUNT_MIN: {}\n'.format(self.minpoints))

      f.write('; ...................\n')
      f.write('; Species data vals\n')
      f.write('; ...................\n')
      f.write('DATASOURCE: {}\n'.format(self.dataSource))
      if self.occIdFname is not None:
         f.write('OCCURRENCE_ID_FILENAME: {}\n'.format(self.occIdFname))
      # Species source type (for processing) and file
      if self.dataSource == SpeciesDatasource.GBIF:
         varname = 'GBIF_OCCURRENCE_FILENAME'
         dataFname = self.gbifFname
         # TODO: allow overwrite of these vars in initboom --> archive config file
         f.write('GBIF_TAXONOMY_FILENAME: {}\n'.format(GBIF_TAXONOMY_FILENAME))
         f.write('GBIF_PROVIDER_FILENAME: {}\n'.format(GBIF_PROVIDER_FILENAME))
      elif self.dataSource == SpeciesDatasource.BISON:
         varname = 'BISON_TSN_FILENAME'
         dataFname = self.bisonFname
      elif self.dataSource == SpeciesDatasource.IDIGBIO:
         varname = 'IDIG_OCCURRENCE_DATA'
         dataFname = self.idigFname
         f.write('IDIG_OCCURRENCE_DATA_DELIMITER: {}\n'
                 .format(self.idigOccSep))
      else:
         varname = 'USER_OCCURRENCE_DATA'
         dataFname = self.userOccFname
         f.write('USER_OCCURRENCE_DATA_DELIMITER: {}\n'
                 .format(self.userOccSep))
      f.write('{}: {}\n'.format(varname, dataFname))
      f.write('\n')
   
      f.write('; ...................\n')
      f.write('; Env Package Vals\n')
      f.write('; ...................\n')
      # Input environmental data, pulled from SCENARIO_PACKAGE metadata
      f.write('SCENARIO_PACKAGE: {}\n'.format(self.scenPackageName))
      f.write('SCENARIO_PACKAGE_EPSG: {}\n'.format(self.epsg))
      f.write('SCENARIO_PACKAGE_MAPUNITS: {}\n'.format(self.mapunits))
      # Scenario codes, created from environmental metadata  
      f.write('SCENARIO_PACKAGE_MODEL_SCENARIO: {}\n'.format(self.modelScenCode))
      pcodes = ','.join(self.prjScenCodeList)
      f.write('SCENARIO_PACKAGE_PROJECTION_SCENARIOS: {}\n'.format(pcodes))
      
      if mdlMaskName is not None:
         f.write('MODEL_MASK_NAME: {}\n'.format(mdlMaskName))
      if prjMaskName is not None:
         f.write('PROJECTION_MASK_NAME: {}\n'.format(prjMaskName))
      f.write('\n')
      
      f.write('; ...................\n')
      f.write('; Global PAM vals\n')
      f.write('; ...................\n')
      # Intersection grid
      f.write('GRID_NAME: {}\n'.format(self.gridname))
      f.write('GRID_BBOX: {}\n'.format(','.join(str(v) for v in self.gridbbox)))
      f.write('GRID_CELLSIZE: {}\n'.format(self.cellsize))
      f.write('GRID_NUM_SIDES: {}\n'.format(self.cellsides))
      f.write('\n')
      for k, v in self.intersectParams.iteritems():
         f.write('INTERSECT_{}:  {}\n'.format(k.upper(), v))
      f.write('ASSEMBLE_PAMS: {}\n'.format(str(self.assemblePams)))
      f.write('\n')

      # SDM Algorithms with all parameters   
      counter = 0
      for alg in self.algorithms:
         counter += 1
         f.write('; ...................\n')
         f.write('[ALGORITHM - {}]\n'.format(counter))
         f.write('; ...................\n')
         for name, val in alg.parameters.iteritems():
            f.write('{}: {}\n'.format(name, val))
         f.write('\n')
               
      f.close()
   
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
   def _findConfigOrDefault(self, config, varname, defaultValue, isList=False):
      var = None
      try:
         var = config.get(SERVER_BOOM_HEADING, varname)
      except:
         pass
      if var is None:
         var = defaultValue
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
   def addUsers(self):
      """
      @summary Adds PUBLIC_USER, DEFAULT_POST_USER and USER from metadata to the database
      """
      self.scribe.log.info('  Insert user metadata ...')
      userList = [{'id': PUBLIC_USER,
                   'email': '{}{}'.format(PUBLIC_USER, DEFAULT_EMAIL_POSTFIX)},
                  {'id': DEFAULT_POST_USER,
                   'email': '{}{}'.format(DEFAULT_POST_USER, DEFAULT_EMAIL_POSTFIX)}]
      if self.usr != PUBLIC_USER:
         userList.append({'id': self.usr,'email': self.usrEmail})
   
      for usrmeta in userList:
         try:
            user = LMUser(usrmeta['id'], usrmeta['email'], usrmeta['email'], modTime=CURR_MJD)
         except:
            pass
         else:
            self.scribe.log.info('  Insert user {} ...'.format(usrmeta['id']))
            tmp = self.scribe.findOrInsertUser(user)
            self.scribe.log.info('  Insert user {} ...'.format(usrmeta['id']))
   
   # ...............................................
   def _checkScenarios(self, legalUsers):
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
         mapunits = scen.units
      return scenPkg, epsg, mapunits
         
   # ...............................................
   def _createScenarios(self):
      # Imports META
      META, scenPackageMetaFilename, pkgMeta, elyrMeta = self._pullClimatePackageMetadata()
      if self.gridbbox is None:
         self.gridbbox = pkgMeta['bbox']
      epsg = elyrMeta['epsg']
      mapunits = elyrMeta['mapunits']
      self.scribe.log.info('  Insert climate {} metadata ...'.format(self.scenPackageName))
      scenPkg = ScenPackage(self.scenPackageName, self.usr, modTime=CURR_MJD)
      # Current
      basescen, staticLayers = self._createBaselineScenario(pkgMeta, elyrMeta, 
                                                      META.LAYERTYPE_META,
                                                      META.OBSERVED_PREDICTED_META,
                                                      META.CLIMATE_KEYWORDS)
      scenPkg.addScenario(basescen)
      self.scribe.log.info('     Created base scenario {}'.format(basescen.code))
      # Predicted Past and Future
      allScens = self._createPredictedScenarios(pkgMeta, elyrMeta, 
                                           META.LAYERTYPE_META, staticLayers,
                                           META.OBSERVED_PREDICTED_META,
                                           META.CLIMATE_KEYWORDS)
      self.scribe.log.info('     Created predicted scenarios {}'.format(allScens.keys()))
      for scen in allScens.values():
         scenPkg.addScenario(scen)
      return scenPkg, basescen.code, epsg, mapunits, scenPackageMetaFilename
   
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
   def _getBaselineLayers(self, pkgMeta, baseMeta, elyrMeta, lyrtypeMeta):
      """
      @summary Assembles layer metadata for a single layerset
      """
      layers = []
      staticLayers = {}
      for envcode in pkgMeta['layertypes']:
         ltmeta = lyrtypeMeta[envcode]
         envKeywords = [k for k in baseMeta['keywords']]
         relfname, isStatic = self._findFileFor(ltmeta, pkgMeta['baseline'], 
                                           gcm=None, tm=None, altPred=None)
         lyrname = self._getbioName(pkgMeta['baseline'], pkgMeta['res'], 
                                    lyrtype=envcode, suffix=pkgMeta['suffix'])
         lyrmeta = {'title': ' '.join((pkgMeta['baseline'], ltmeta['title'])),
                    'description': ' '.join((pkgMeta['baseline'], ltmeta['description']))}
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
                           dateCode=pkgMeta['baseline'],
                           envMetadata=envmeta,
                           envModTime=CURR_MJD)
         layers.append(envlyr)
         if isStatic:
            staticLayers[envcode] = envlyr
      return layers, staticLayers
   
   # ...............................................
   def _findFileFor(self, ltmeta, obsOrPred, gcm=None, tm=None, altPred=None):
      isStatic = False
      ltfiles = ltmeta['files']
      if len(ltfiles) == 1:
         isStatic = True
         relFname = ltfiles.keys()[0]
         if obsOrPred in ltfiles[relFname]:
            return relFname, isStatic
      else:
         for relFname, kList in ltmeta['files'].iteritems():
            if obsOrPred in kList:
               if gcm == None and tm == None and altPred == None:
                  return relFname, isStatic
               elif (gcm in kList and tm in kList and
                     (altPred is None or altPred in kList)):
                  return relFname, isStatic
      print('Failed to find layertype {} for {}, gcm {}, altpred {}, time {}'
            .format(ltmeta['title'], obsOrPred, gcm, altPred, tm))
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
      obsKey = pkgMeta['baseline']
      baseMeta = observedPredictedMeta[obsKey]
   #    tm = baseMeta['times'].keys()[0]
      basekeywords = [k for k in climKeywords]
      basekeywords.extend(baseMeta['keywords'])
      
      scencode = self._getbioName(obsKey, pkgMeta['res'], suffix=pkgMeta['suffix'])
      lyrs, staticLayers = self._getBaselineLayers(pkgMeta, baseMeta, elyrMeta, 
                                              lyrtypeMeta)
      scenmeta = {'title': baseMeta['title'], 'author': baseMeta['author'], 
                  'description': baseMeta['description'], 'keywords': basekeywords}
      scen = Scenario(scencode, self.usr, elyrMeta['epsg'], 
                      metadata=scenmeta, 
                      units=elyrMeta['mapunits'], 
                      res=elyrMeta['resolution'], 
                      dateCode=pkgMeta['baseline'],
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
            obstitle = observedPredictedMeta[pkgMeta['baseline']]['title']
            scendesc =  ' '.join((obstitle, 
                     'and predicted climate calculated from {}'.format(scentitle)))
            scenmeta = {'title': scentitle, 'author': mdlvals['author'], 
                        'description': scendesc, 'keywords': scenkeywords}
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
            self.scribe.log.info('Scenario {} exists'.format(scode))
            updatedScens.append(scen)
         else:
            self.scribe.log.info('Insert scenario {}'.format(scode))
            newscen = self.scribe.findOrInsertScenario(scen, 
                                                scenPkgId=updatedScenPkg.getId())
            updatedScens.append(newscen)
      self.scenPkg.setScenarios(updatedScens)
   
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
         META = imp.load_source('currentmetadata', scenPackageMetaFilename)
      except Exception, e:
         raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                       .format(scenPackageMetaFilename, e))
      return META, scenPackageMetaFilename
   
   # ...............................................
   def _pullClimatePackageMetadata(self):
      META, scenPackageMetaFilename = self._findClimatePackageMetadata()
      # Combination of scenario and layer attributes making up these data 
      pkgMeta = META.CLIMATE_PACKAGES[self.scenPackageName]
      
      try:
         epsg = META.EPSG
      except:
         raise LMError('Failed to specify EPSG for {}'
                       .format(self.scenPackageName))
      try:
         mapunits = META.MAPUNITS
      except:
         raise LMError('Failed to specify MAPUNITS for {}'
                       .format(self.scenPackageName))
      try:
         resInMapunits = META.RESOLUTIONS[pkgMeta['res']]
      except:
         raise LMError('Failed to specify res (or RESOLUTIONS values) for {}'
                       .format(self.scenPackageName))
      try:
         gdaltype = META.ENVLYR_GDALTYPE
      except:
         raise LMError('Failed to specify ENVLYR_GDALTYPE for {}'
                       .format(self.scenPackageName))
      try:
         gdalformat = META.ENVLYR_GDALFORMAT
      except:
         raise LMError(currargs='Failed to specify META.ENVLYR_GDALFORMAT for {}'
                       .format(self.scenPackageName))
      # Spatial and format attributes of data files
      elyrMeta = {'epsg': epsg, 
                    'mapunits': mapunits, 
                    'resolution': resInMapunits, 
                    'gdaltype': gdaltype, 
                    'gdalformat': gdalformat}
      return META, scenPackageMetaFilename, pkgMeta, elyrMeta

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
   def addArchive(self):
      """
      @summary: Create a Gridset, Shapegrid, PAMs, GRIMs for this archive, and
                update attributes with new or existing values from DB
      """
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
      self.gridset = updatedGrdset
      # "Global" PAM, GRIM (one each per scenario)
      for code, scen in self.scenPkg.scenarios.iteritems():
         gPam, scenGrim = self._findOrAddDefaultMatrices(updatedGrdset, scen)
         self.defaultPamGrims[code] = (gPam, scenGrim)
   
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
              MFChain.META_DESC: desc}
      newMFC = MFChain(self.usr, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=currtime)
      grimChain = self.scribe.insertMFChain(newMFC)
      return grimChain
   
   # .............................
   def addGRIMChains(self):
      grimChains = []
      currtime = mx.DateTime.gmt().mjd

      for code, (pam, grim) in self.defaultPamGrims.iteritems():
         scen = self.scenPkg.scenarios[code]
         # Create MFChain for this GRIM
         grimChain = self._createGrimMF(code, currtime)
         targetDir = grimChain.getRelativeDirectory()
         mtxcols = self.scribe.getColumnsForMatrix(grim.getId())
         
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

#    # ...............................................
#    def _getScenarios(self):
#       legalUsers = [PUBLIC_USER, self.usr]
#       newModelScenCode = None
#       # Codes for existing Scenarios
#       if self.modelScenCode and self.prjScenCodeList:
#          scenPackageMetaFilename = None
#          # This fills or resets epsgcode, mapunits, gridbbox
#          scenPkg, epsg, mapunits = self._checkScenarios(legalUsers)
#       # Data/metadata for new Scenarios
#       else:
#          # This fills or resets modelScenCode, epsgcode, mapunits, gridbbox
#          (scenPkg, newModelScenCode, epsg, mapunits, 
#           scenPackageMetaFilename) = self._createScenarios()
#       return scenPkg, newModelScenCode, epsg, mapunits, scenPackageMetaFilename

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
              MFChain.META_DESC: 'Boom start for User {}, Archive {}'
      .format(self.usr, self.archiveName)}
      newMFC = MFChain(self.usr, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=CURR_MJD)
      mfChain = self.scribe.insertMFChain(newMFC)

      cmdArgs = ['LOCAL', '$PYTHON',
                 ProcessTool.get(ProcessType.BOOM_DAEMON),
                 '--config_file={}'.format(self.outConfigFilename),
                 'start']
      boomCmd = ' '.join(cmdArgs)

      baseAbsFilename, ext = os.path.splitext(self.outConfigFilename)
      # Boomer.ChristopherWalken writes this file when finished walking through 
      # species data (initiated by this Makeflow).  
      walkedArchiveFname = baseAbsFilename + LMFormat.LOG.ext

      # Create a rule from the MF and Arf file creation
      rule = MfRule(boomCmd, [walkedArchiveFname], 
                    dependencies=[self.outConfigFilename])
      mfChain.addCommands([rule])
      mfChain.write()
      mfChain.updateStatus(JobStatus.INITIALIZE)
      self.scribe.updateObject(mfChain)
      return mfChain
   
   
# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('-', '--config_file', default=None,
            help=('Configuration file for the archive, gridset, and grid ' +
                  'to be created from these data.'))
   parser.add_argument('-grim', '--doGrim', action='store_true',
            help=('Compute multi-species matrix outputs for the matrices ' +
                  'in this Gridset.'))
   args = parser.parse_args()
   configFname = args.config_file
      
   if configFname is not None and not os.path.exists(configFname):
      print ('Missing configuration file {}'.format(configFname))
      exit(-1)

   filler = BOOMFiller(configFname=configFname)
   filler.initializeInputs()
   
   # ...............................................
   # Data for any user
   # ...............................................
   # Insert all taxonomic sources for now
   filler.scribe.log.info('  Insert taxonomy metadata ...')
   for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
      taxSourceId = filler.scribe.findOrInsertTaxonSource(taxInfo['name'],taxInfo['url'])

   # For ALL users, add Algorithms if they do not exist
   filler.addAlgorithms()
   
         
   # ...............................................
   # This user and default users
   # ...............................................
   # Add user and PUBLIC_USER and DEFAULT_POST_USER users if they do not exist
   filler.addUsers()
   
   # ...............................................
   # Data for this Boom user
   # ...............................................
   # Add or get Scenarios 
   # This updates the scenPkg with db objects for other operations
   filler.addPackageScenariosLayers()
         
   # Test provided OccurrenceLayer Ids for existing user or PUBLIC occurrence data
   # Test a subset of OccurrenceIds provided as BOOM species input
   if filler.occIdFname:
      filler._checkOccurrenceSets()
         
   # Add or get ShapeGrid, Global PAM, Gridset for this archive
   # This updates the gridset, shapegrid, default PAMs (rolling, with no 
   #     matrixColumns, default GRIMs with matrixColumns
   filler.addArchive()
   
   # Create, add, write MFChain for creating each Scenario GRIM
   filler.addGRIMChains()
      
   # Write config file for this archive
   filler.writeConfigFile()
   
   # Create, add, write MFChain running the Boomer daemon on these SDM inputs
   mfChain = filler.addBoomChain()
   filler.scribe.log.info('Wrote {}'.format(filler.outConfigFilename))   
   filler.close()
    
"""
import mx.DateTime
import os
import time
from types import IntType

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, LMFormat, 
                        ProcessType, JobStatus, MatrixType, SERVER_BOOM_HEADING)
from LmCommon.common.readyfile import readyFilename
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmDbServer.common.localconstants import (ASSEMBLE_PAMS, 
      GBIF_TAXONOMY_FILENAME, GBIF_PROVIDER_FILENAME, GBIF_OCCURRENCE_FILENAME, 
      BISON_TSN_FILENAME, IDIG_OCCURRENCE_DATA, IDIG_OCCURRENCE_DATA_DELIMITER,
      USER_OCCURRENCE_DATA, USER_OCCURRENCE_DATA_DELIMITER,
      INTERSECT_FILTERSTRING, INTERSECT_VALNAME, INTERSECT_MINPERCENT, 
      INTERSECT_MINPRESENCE, INTERSECT_MAXPRESENCE, SCENARIO_PACKAGE,
      GRID_CELLSIZE, GRID_NUM_SIDES)
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, LMFileType, ENV_DATA_PATH, 
         GPAM_KEYWORD, GGRIM_KEYWORD, ARCHIVE_KEYWORD, PUBLIC_ARCHIVE_NAME, 
         DEFAULT_EMAIL_POSTFIX, Priority, ProcessTool)
from LmServer.common.localconstants import (PUBLIC_USER, DATASOURCE, 
                                            POINT_COUNT_MIN)
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.cmd import MfRule
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.scenario import Scenario, ScenPackage
from LmServer.legion.shapegrid import ShapeGrid

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd

from LmDbServer.boom.initboom import BOOMFiller

configFname = '/state/partition1/tmpdata/biotaphyHeuchera.boom.ini'
configFname = '/state/partition1/tmpdata/biotaphyHeucheraLowres.boom.ini'
configFname = '/state/partition1/tmpdata/atest.boom.ini'
configFname = '/state/partition1/tmpdata/file_90310.ini'

filler = BOOMFiller(configFname=configFname)

filler.initializeInputs()

filler.scribe.log.info('  Insert taxonomy metadata ...')
for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
   taxSourceId = filler.scribe.findOrInsertTaxonSource(taxInfo['name'],taxInfo['url'])
      
# Add user and PUBLIC_USER and DEFAULT_POST_USER users if they do not exist
filler.addUsers()

# For ALL users, add Algorithms if they do not exist
filler.addAlgorithms()

# ...............................................
# Data for this Boom user
# ...............................................
# Add or get Scenarios 
# This updates the allScens with db objects for other operations
filler.addPackageScenariosLayers()
      
# Test provided OccurrenceLayer Ids for existing user or PUBLIC occurrence data
# Test a subset of OccurrenceIds provided as BOOM species input
if filler.occIdFname:
   filler._checkOccurrenceSets()
      

   
# Write config file for this archive
filler.writeConfigFile()

# Create, add, write MFChain running the Boomer daemon on these SDM inputs
mfChain = filler.addBoomChain()
filler.scribe.log.info('Wrote {}'.format(filler.outConfigFilename))   
filler.close()

shpGrid, archiveGridset, pamGrims = filler.addArchive()

# Create, add, write MFChain for creating each Scenario GRIM
filler.addGRIMChains()



grimChains = filler.createGRIMChains(shpGrid, pamGrims)


filler.writeConfigFile(fname='/tmp/testFillerConfig.ini')
filler.initBoom()
filler.close()



   
# Write config file for this archive
filler.writeConfigFile()
mfChain = filler.createMFBoom()
filler.scribe.log.info('Wrote {}'.format(filler.outConfigFilename))

"""
