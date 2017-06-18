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

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, LMFormat, 
                        ProcessType, JobStatus, MatrixType, SERVER_BOOM_HEADING)
from LmCommon.common.readyfile import readyFilename
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmDbServer.common.localconstants import (ALGORITHMS, ASSEMBLE_PAMS, 
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
from LmServer.legion.scenario import Scenario
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.base.layer2 import Vector

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd
BOOM_DAEMON = 'LmDbServer/boom/daboom.py'

# .............................................................................
class ArchiveFiller(LMObject):
   """
   Class to populate a Lifemapper database with inputs for a BOOM archive, and 
   write a configuration file for computations on the inputs.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, configFname=None):
      """
      @summary Constructor for ArchiveFiller class.
      """
      super(ArchiveFiller, self).__init__()
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
      @summary Constructor for ArchiveFiller class.
      """
      # Allow reset configuration
      if configFname is not None:
         self.inConfigFname = configFname
      (self.usr,
       self.usrEmail,
       self.archiveName,
       self.priority,
       self.envPackageName,
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
       
      # If running as root, new user filespace must have permissions corrected
      self._warnPermissions()

      earl = EarlJr()
      self.outConfigFilename = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                                   objCode=self.archiveName, 
                                                   usr=self.usr)
      # Create new or pull existing scenarios
      (self.allScens, 
       newModelScenCode,
       self.epsgcode, 
       self.mapunits, 
       self.envPackageMetaFilename) = self._getScenarios()
      if newModelScenCode is  not None:
         self.modelScenCode = newModelScenCode
      self.prjScenCodeList = self.allScens.keys()
      
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
      logname = '{}.{}'.format(self.__class__.__name__.lower(), timestamp)
      logger = ScriptLogger(logname, level=loglevel)
      # DB connection
      scribe = BorgScribe(logger)
      return scribe
      
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
      algCodeList = self._findConfigOrDefault(config, 'ALGORITHMS', ALGORITHMS, 
                                            isList=True)
         
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
      envPackageName = self._findConfigOrDefault(config, 'SCENARIO_PACKAGE', 
                                                 SCENARIO_PACKAGE)
      if envPackageName is not None:
         modelScenCode = self._findConfigOrDefault(config, 
                     'SCENARIO_PACKAGE_MODEL_SCENARIO', None, isList=False)
         prjScenCodeList = self._findConfigOrDefault(config, 
                     'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', None, isList=True)
      
      return (usr, usrEmail, archiveName, priority, envPackageName, 
              modelScenCode, prjScenCodeList, dataSource, 
              occIdFname, gbifFname, idigFname, idigOccSep, bisonFname, 
              userOccFname, userOccSep, minpoints, algCodeList, 
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
      # SDM Algorithm and minimun number of required species points   
      f.write('POINT_COUNT_MIN: {}\n'.format(self.minpoints))
      f.write('ALGORITHMS: {}\n'.format(','.join(self.algorithms)))
      f.write('\n')
      
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
      f.write('SCENARIO_PACKAGE: {}\n'.format(self.envPackageName))
      f.write('SCENARIO_PACKAGE_EPSG: {}\n'.format(self.epsgcode))
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
   
   # ...............................................
   def _checkScenarios(self, legalUsers):
      epsg = mapunits = None
      allScens = {}
      if self.modelScenCode not in self.prjScenCodeList:
         self.prjScenCodeList.append(self.modelScenCode)
      for code in self.prjScenCodeList:
         scen = self.scribe.getScenario(code, fillLayers=True)
         if scen is None:
            raise LMError('Missing Scenario for code or id {}'.format(code))
         if scen.getUserId() not in legalUsers:
            raise LMError('legalUsers {} missing {}'.format(legalUsers,
                                                            scen.getUserId()))
         allScens[code] = scen
         if epsg is None:
            epsg = scen.epsgcode
            mapunits = scen.units
            bbox = scen.bbox
         # Fill or reset 
         if self.gridbbox is None:
            self.gridbbox = bbox
      return allScens, epsg, mapunits
         
   # ...............................................
   def _createScenarios(self):
      # Imports META
      META, envPackageMetaFilename, pkgMeta, elyrMeta = self._pullClimatePackageMetadata()
      if self.gridbbox is None:
         self.gridbbox = pkgMeta['bbox']
      epsg = elyrMeta['epsg']
      mapunits = elyrMeta['mapunits']
      self.scribe.log.info('  Insert climate {} metadata ...'.format(self.envPackageName))
      # Current
      basescen, staticLayers = self._createBaselineScenario(pkgMeta, elyrMeta, 
                                                      META.LAYERTYPE_META,
                                                      META.OBSERVED_PREDICTED_META,
                                                      META.CLIMATE_KEYWORDS)
      self.scribe.log.info('     Created base scenario {}'.format(basescen.code))
      # Predicted Past and Future
      allScens = self._createPredictedScenarios(pkgMeta, elyrMeta, 
                                           META.LAYERTYPE_META, staticLayers,
                                           META.OBSERVED_PREDICTED_META,
                                           META.CLIMATE_KEYWORDS)
      self.scribe.log.info('     Created predicted scenarios {}'.format(allScens.keys()))
      allScens[basescen.code] = basescen
      return allScens, basescen.code, epsg, mapunits, envPackageMetaFilename
   
   # ...............................................
   def _checkOccurrenceSets(self, limit=10):
      legalUsers = [PUBLIC_USER, self.usr]
      missingCount = 0
      wrongUserCount = 0
      nonIntCount = 0
      if not os.path.exists(self.occIdFname):
         raise LMError('Missing OCCURRENCE_ID_FILENAME {}'.format(self.occIdFname))
      else:
         f = open(self.occIdFname, 'r')
         for i in range(limit):
            try:
               tmp = f.readline()
            except Exception, e:
               self.scribe.log.info('Failed to readline {} on line {}, stopping'
                               .format(str(e), i))
               break
            try:
               id = int(tmp.strip())
            except Exception, e:
               self.scribe.log.info('Unable to get Id from data {} on line {}'
                               .format(tmp, i))
               nonIntCount += 1
            else:
               occ = self.scribe.getOccurrenceSet(occId=id)
               if occ is None:
                  missingCount += 1
               elif occ.getUserId() not in legalUsers:
                  self.scribe.log.info('Unauthorized user {} for ID {}'
                                  .format(occ.getUserId(), id))
                  wrongUserCount += 1
      self.scribe.log.info('Errors out of the first {} occurrenceIds:'. format(limit))
      self.scribe.log.info('  Missing: {} '.format(missingCount))
      self.scribe.log.info('  Unauthorized data: {} '.format(wrongUserCount))

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
   def addScenariosAndLayers(self):
      """
      @summary Add scenario and layer metadata to database, and update the 
               allScens attribute with newly inserted scenarios and layers
      """
      updatedScens = {}
      for scode, scen in self.allScens.iteritems():
         if scen.getId() is not None:
            self.scribe.log.info('Scenario {} exists'.format(scode))
            updatedScens[scode] = scen
         else:
            self.scribe.log.info('Insert scenario {}'.format(scode))
            newscen = self.scribe.findOrInsertScenario(scen)
            updatedScens[scode] = newscen
      self.allScens = updatedScens
   
   # ...............................................
   def _findClimatePackageMetadata(self):
      envPackageMetaFilename = os.path.join(ENV_DATA_PATH, 
                     '{}{}'.format(self.envPackageName, LMFormat.PYTHON.ext))      
      if not os.path.exists(envPackageMetaFilename):
         raise LMError(currargs='Climate metadata {} does not exist'
                       .format(envPackageMetaFilename))
      # TODO: change to importlib on python 2.7 --> 3.3+  
      try:
         import imp
         META = imp.load_source('currentmetadata', envPackageMetaFilename)
      except Exception, e:
         raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                       .format(envPackageMetaFilename, e))
      return META, envPackageMetaFilename
   
   # ...............................................
   def _pullClimatePackageMetadata(self):
      META, envPackageMetaFilename = self._findClimatePackageMetadata()
      # Combination of scenario and layer attributes making up these data 
      pkgMeta = META.CLIMATE_PACKAGES[self.envPackageName]
      
      try:
         epsg = META.EPSG
      except:
         raise LMError('Failed to specify EPSG for {}'
                       .format(self.envPackageName))
      try:
         mapunits = META.MAPUNITS
      except:
         raise LMError('Failed to specify MAPUNITS for {}'
                       .format(self.envPackageName))
      try:
         resInMapunits = META.RESOLUTIONS[pkgMeta['res']]
      except:
         raise LMError('Failed to specify res (or RESOLUTIONS values) for {}'
                       .format(self.envPackageName))
      try:
         gdaltype = META.ENVLYR_GDALTYPE
      except:
         raise LMError('Failed to specify ENVLYR_GDALTYPE for {}'
                       .format(self.envPackageName))
      try:
         gdalformat = META.ENVLYR_GDALFORMAT
      except:
         raise LMError(currargs='Failed to specify META.ENVLYR_GDALFORMAT for {}'
                       .format(self.envPackageName))
      # Spatial and format attributes of data files
      elyrMeta = {'epsg': epsg, 
                    'mapunits': mapunits, 
                    'resolution': resInMapunits, 
                    'gdaltype': gdaltype, 
                    'gdalformat': gdalformat}
      return META, envPackageMetaFilename, pkgMeta, elyrMeta

   # ...............................................
   def _addIntersectGrid(self):
      shp = ShapeGrid(self.gridname, self.usr, self.epsgcode, self.cellsides, 
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
            success = self.scribe.updateShapeGrid(newshp)
            if success is False:
               self.scribe.log.warning('Failed to update Shapegrid record')
      else:
         raise LMError(currargs='Failed to find or insert Shapegrid')
      return newshp
      
   # ...............................................
   def _createDefaultMatrices(self, gridset, scen):
      # Create Global PAM for this archive, scenario
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
      desc = '{} for Scenario {}'.format(GGRIM_KEYWORD, scen.code)
      grimMeta = {ServiceObject.META_DESCRIPTION: desc,
                 ServiceObject.META_KEYWORDS: [GGRIM_KEYWORD]}
      tmpGrim = LMMatrix(None, matrixType=MatrixType.GRIM, 
                         gcmCode=scen.gcmCode, altpredCode=scen.altpredCode, 
                         dateCode=scen.dateCode, metadata=grimMeta, userId=self.usr, 
                         gridset=gridset, 
                         status=JobStatus.GENERAL, statusModTime=CURR_MJD)
      grim = self.scribe.findOrInsertMatrix(tmpGrim)
      return gpam, grim

   # ...............................................
   def addArchive(self):
      """
      @summary: Create a Shapegrid, PAM, and Gridset for this archive's Global PAM
      """
      self.scribe.log.info('  Insert, build shapegrid {} ...'.format(self.gridname))
      shp = self._addIntersectGrid()
      # "BOOM" Archive
      meta = {ServiceObject.META_DESCRIPTION: ARCHIVE_KEYWORD,
              ServiceObject.META_KEYWORDS: [ARCHIVE_KEYWORD]}
      grdset = Gridset(name=self.archiveName, metadata=meta, shapeGrid=shp, 
                       dlocation=self.envPackageMetaFilename, epsgcode=self.epsgcode, 
                       userId=self.usr, modTime=CURR_MJD)
      updatedGrdset = self.scribe.findOrInsertGridset(grdset)
      # "Global" PAM, GRIM (one each per scenario)
      pamGrims = {}
      for code, scen in self.allScens.iteritems():
         gPam, scenGrim = self._createDefaultMatrices(updatedGrdset, scen)
         pamGrims[code] = (gPam, scenGrim)
      
      return shp, updatedGrdset, pamGrims
   
# ...............................................
   def _initGRIMIntersect(self, lyr, mtx, shpGrid, intersectParams, currtime):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      mtxcol = None
      if lyr is not None:
         # TODO: Save processType into the DB??
         if LMFormat.isGDAL(driver=lyr.dataFormat):
            ptype = ProcessType.INTERSECT_RASTER_GRIM
         else:
            self.scribe.log.debug('Vector intersect not yet implemented for GRIM column {}'
                                  .format(mtxcol.getId()))
   
         tmpCol = MatrixColumn(None, mtx.getId(), self.usr, 
                layer=lyr, shapegrid=shpGrid, 
                intersectParams=intersectParams, 
                squid=lyr.squid, ident=lyr.ident, processType=ptype, 
                status=JobStatus.GENERAL, statusModTime=currtime,
                postToSolr=False)
         mtxcol = self.scribe.findOrInsertMatrixColumn(tmpCol)
         # TODO: This is a hack, post to solr needs to be retrieved from DB
         mtxcol.postToSolr = False
         if mtxcol is not None:
            self.scribe.log.debug('Found/inserted MatrixColumn {}'.format(mtxcol.getId()))
            # Reset processType (not in db)
            mtxcol.processType = ptype            
      return mtxcol

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
   def createGRIMChains(self, shpGrid, pamGrims):
      grimChains = []
      currtime = mx.DateTime.gmt().mjd
      intersectParams = {MatrixColumn.INTERSECT_PARAM_WEIGHTED_MEAN: True}

      for code, (pam, grim) in pamGrims.iteritems():
         scen = self.allScens[code]
         # Create MFChain for this GRIM
         grimChain = self._createGrimMF(code, currtime)
         targetDir = grimChain.getRelativeDirectory()
         
         # Need to keep track of intersections for matrix concatenation
         colFilenames = []

         for lyr in scen.layers:
            # TODO: Remove this, checking to see if shapegrid is getting added 
            #          to scenario layers
            if not isinstance(lyr, (ShapeGrid, Vector)):
               # Add to GRIM Makeflow ScenarioLayer and MatrixColumn
               mtxcol = self._initGRIMIntersect(lyr, grim, shpGrid, intersectParams, 
                                            currtime)
               rules = mtxcol.computeMe(workDir=targetDir)
               grimChain.addCommands(rules)
               colFilenames.append(os.path.join(targetDir, 
                                 os.path.splitext(lyr.getRelativeDLocation())[0], 
                                 mtxcol.getTargetFilename()))
         # TODO: Matrix Concatenate and Stockpile Rules should be created by 
         #       grim.computeMe().  LMMatrix obj should check MatrixType to  
         #       determine whether triage files are used (True for SDM PAM,  
         #       False for GRIM, BioGeo, output matrices) 
         # TODO: Create a default "successFile" from object
         # Add concatenate command
         grimRules = []
         wsGrim = os.path.join(targetDir, 'grim_{}.{}'
                               .format(grim.getId(), LMFormat.JSON.ext))
         concatArgs = ['$PYTHON',
                       ProcessTool.get(ProcessType.CONCATENATE_MATRICES),
                       # Axis
                       '1', 
                       wsGrim, 
                       ' '.join(colFilenames)
                       ]
         concatCmd = ' '.join(concatArgs)
         grimRules.append(MfRule(concatCmd, [wsGrim], dependencies=colFilenames))
         # Stockpile GRIM
         grimSuccessFilename = os.path.join(targetDir, 
                                        'grim_{}.success'.format(grim.getId()))
         stockpileArgs = ['LOCAL',
                          '$PYTHON',
                          ProcessTool.get(ProcessType.UPDATE_OBJECT),
                          '-s {}'.format(JobStatus.COMPLETE),
                          str(ProcessType.INTERSECT_RASTER_GRIM),
                          str(grim.getId()),
                          grimSuccessFilename,
                          wsGrim]
         stockpileCmd = ' '.join(stockpileArgs)
         grimRules.append(MfRule(stockpileCmd, [grimSuccessFilename], 
                             dependencies=[wsGrim]))
         
         grimChain.addCommands(grimRules)
         grimChain.write()
         grimChain.updateStatus(JobStatus.INITIALIZE)
         self.scribe.updateObject(grimChain)
         grimChains.append(grimChain)
         self.scribe.log.info('  Wrote GRIM Makeflow {} for scencode {}'
                       .format(grimChain.objId, code))
               
      return grimChains

   # ...............................................
   def _getScenarios(self):
      legalUsers = [PUBLIC_USER, self.usr]
      newModelScenCode = None
      # Codes for existing Scenarios
      if self.modelScenCode and self.prjScenCodeList:
         envPackageMetaFilename = None
         # This fills or resets epsgcode, mapunits, gridbbox
         allScens, epsg, mapunits = self._checkScenarios(legalUsers)
      # Data/metadata for new Scenarios
      else:
         # This fills or resets modelScenCode, epsgcode, mapunits, gridbbox
         (allScens, newModelScenCode, epsg, mapunits, 
          envPackageMetaFilename) = self._createScenarios()
      return allScens, newModelScenCode, epsg, mapunits, envPackageMetaFilename

   # ...............................................
   def addAlgorithms(self):
      """
      @summary Adds algorithms to the database from the algorithm dictionary
      """
      ids = []
      for alginfo in Algorithms.implemented():
         meta = {'name': alginfo.name, 
                 'isDiscreteOutput': alginfo.isDiscreteOutput,
                 'outputFormat': alginfo.outputFormat,
                 'acceptsCategoricalMaps': alginfo.acceptsCategoricalMaps}
         alg = Algorithm(alginfo.code, metadata=meta)
         self.scribe.log.info('  Insert algorithm {} ...'.format(alginfo.code))
         algid = self.scribe.findOrInsertAlgorithm(alg)
         ids.append(algid)
      return ids
   
   # ...............................................
   def createMFBoom(self):
      """
      @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                and configFile written by ArchiveFiller.initBoom.
      """
      meta = {MFChain.META_CREATED_BY: os.path.basename(__file__),
              MFChain.META_DESC: 'Boom start for User {}, Archive {}'
      .format(self.usr, self.archiveName)}
      newMFC = MFChain(self.usr, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=CURR_MJD)
      mfChain = self.scribe.insertMFChain(newMFC)

      cmdArgs = ['LOCAL', '$PYTHON',
                 BOOM_DAEMON,
                 '--config_file={}'.format(self.outConfigFilename),
                 'start']
      boomCmd = ' '.join(cmdArgs)

      baseAbsFilename, ext = os.path.splitext(self.outConfigFilename)
      # Boomer.ChristopherWalken writes this file when finished walking through 
      # species data (initiated by this Makeflow).  
      walkedArchiveFname = baseAbsFilename + LMFormat.LOG.ext

      outputFname = mfChain.getDLocation()
      # Create a rule from the MF and Arf file creation
      rule = MfRule(boomCmd, [walkedArchiveFname], 
                    dependencies=[self.outConfigFilename])
      mfChain.addCommands([rule])
      mfChain.write()
      mfChain.updateStatus(JobStatus.INITIALIZE)
      self.scribe.updateObject(mfChain)
      return mfChain
   
   # ...............................................
   def initBoom(self):
      # Add user and PUBLIC_USER and DEFAULT_POST_USER users if they do not exist
      self.addUsers()
   
      # Add Algorithms if they do not exist
      aIds = self.addAlgorithms()
   
      # Add or get Scenarios
      self.addScenariosAndLayers()
         
      # Test provided OccurrenceLayer Ids for existing user or PUBLIC occurrence data
      # Test a subset of OccurrenceIds provided as BOOM species input
      if self.occIdFname:
         self._checkOccurrenceSets()
         
      # Add ShapeGrid, Global PAM, Gridset, 
      shpGrid, archiveGridset, pamGrims = self.addArchive()
   
      # Assemble a MFChain for creating each Scenario GRIM
      grimChains = self.createGRIMChains(shpGrid, pamGrims)
      
      # Insert all taxonomic sources for now
      self.scribe.log.info('  Insert taxonomy metadata ...')
      for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
         taxSourceId = self.scribe.findOrInsertTaxonSource(taxInfo['name'],taxInfo['url'])
         
      # Write config file for this archive
      self.writeConfigFile()
      mfChain = self.createMFBoom()
      self.scribe.log.info('Wrote {}'.format(self.outConfigFilename))
      
      return archiveGridset
   
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
   args = parser.parse_args()
   configFname = args.config_file
      
   if configFname is not None and not os.path.exists(configFname):
      print ('Missing configuration file {}'.format(configFname))
      exit(-1)

   filler = ArchiveFiller(configFname=configFname)
   filler.initializeInputs()
   filler.initBoom()
   filler.close()
    
"""
import mx.DateTime
import os
import time

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, LMFormat, 
                        ProcessType, JobStatus, MatrixType, SERVER_BOOM_HEADING)
from LmCommon.common.readyfile import readyFilename
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmDbServer.common.localconstants import (ALGORITHMS, ASSEMBLE_PAMS, 
      GBIF_TAXONOMY_FILENAME, GBIF_PROVIDER_FILENAME, GBIF_OCCURRENCE_FILENAME, 
      BISON_TSN_FILENAME, IDIG_OCCURRENCE_DATA, IDIG_OCCURRENCE_DATA_DELIMITER,
      USER_OCCURRENCE_DATA, USER_OCCURRENCE_DATA_DELIMITER,
      INTERSECT_FILTERSTRING, INTERSECT_VALNAME, INTERSECT_MINPERCENT, 
      INTERSECT_MINPRESENCE, INTERSECT_MAXPRESENCE, SCENARIO_PACKAGE,
      GRID_CELLSIZE, GRID_NUM_SIDES)
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, LMFileType, ENV_DATA_PATH, 
         GPAM_KEYWORD, GGRIM_KEYWORD, ARCHIVE_KEYWORD, PUBLIC_ARCHIVE_NAME, 
         DEFAULT_EMAIL_POSTFIX, Priority)
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
from LmServer.legion.scenario import Scenario
from LmServer.legion.shapegrid import ShapeGrid

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd
BOOM_SCRIPT = 'LmDbServer/boom/boomer.py'

from LmDbServer.boom.boominput import ArchiveFiller

configFname = '/state/partition1/tmpdata/biotaphyHeucheraLowres.boom.ini'

filler = ArchiveFiller(configFname=configFname)
filler.initializeInputs()

filler.writeConfigFile(fname='/tmp/testFillerConfig.ini')
# filler.initBoom()
# filler.close()


layers = []
staticLayers = {}
envcode = pkgMeta['layertypes'][0]
ltmeta = lyrtypeMeta[envcode]
envKeywords = [k for k in baseMeta['keywords']]
relfname, isStatic = filler._findFileFor(ltmeta, pkgMeta['baseline'], 
                                  gcm=None, tm=None, altPred=None)
lyrname = filler._getbioName(pkgMeta['baseline'], pkgMeta['res'], 
                           lyrtype=envcode, suffix=pkgMeta['suffix'])
lyrmeta = {'title': ' '.join((pkgMeta['baseline'], ltmeta['title'])),
           'description': ' '.join((pkgMeta['baseline'], ltmeta['description']))}
envmeta = {'title': ltmeta['title'],
           'description': ltmeta['description'],
           'keywords': envKeywords.extend(ltmeta['keywords'])}
dloc = os.path.join(ENV_DATA_PATH, relfname)
if not os.path.exists(dloc):
   print('Missing local data %s' % dloc)
envlyr = EnvLayer(lyrname, filler.usr, elyrMeta['epsg'], 
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


"""
