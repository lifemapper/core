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
import argparse
import mx.DateTime
import os
import sys

from LmCommon.common.lmconstants import (DEFAULT_POST_USER, OutputFormat, 
                                         JobStatus, MatrixType)
from LmDbServer.common.localconstants import (DEFAULT_ALGORITHMS, 
         SCENARIO_PACKAGE, DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS)
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmDbServer.common.localconstants import (GBIF_OCCURRENCE_FILENAME, 
                                              BISON_TSN_FILENAME, IDIG_FILENAME, 
                                              USER_OCCURRENCE_DATA)
from LmDbServer.boom.boom import Archivist
from LmServer.base.lmobj import LMError
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (ALGORITHM_DATA, ENV_DATA_PATH, 
         GPAM_KEYWORD, ARCHIVE_NAME, ARCHIVE_KEYWORD, LMFileType)
from LmServer.common.localconstants import (ARCHIVE_USER, POINT_COUNT_MIN,
                                            DEFAULT_EPSG, DEFAULT_MAPUNITS)
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.db.borgscribe import BorgScribe
from LmServer.sdm.algorithm import Algorithm
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix            
from LmServer.legion.scenario import Scenario
from LmServer.legion.shapegrid import ShapeGrid

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd
# ...............................................
def addUsers(scribe, userId, userEmail):
   """
   @summary Adds ARCHIVE_USER, anon user and USER from metadata to the database
   """
   userList = [{'id': ARCHIVE_USER,
                'email': '{}@nowhere.org'.format(ARCHIVE_USER)},
               {'id': DEFAULT_POST_USER,
                'email': '{}@nowhere.org'.format(DEFAULT_POST_USER)}]
   if userId != ARCHIVE_USER:
      userList.append({'id': userId,'email': userEmail})

   for usrmeta in userList:
      try:
         user = LMUser(usrmeta['id'], usrmeta['email'], usrmeta['email'], modTime=CURR_MJD)
      except:
         pass
      else:
         scribe.log.info('  Insert user {} ...'.format(usrmeta['id']))
         tmp = scribe.insertUser(user)

# ...............................................
def addAlgorithms(scribe):
   """
   @summary Adds algorithms to the database from the algorithm dictionary
   """
   ids = []
   for algcode, algdict in ALGORITHM_DATA.iteritems():
      algmeta = {}
      for k, v in algdict.iteritems():
         if k != 'parameters':
            algmeta[k] = v
      alg = Algorithm(algcode, metadata=algmeta)
      scribe.log.info('  Insert algorithm {} ...'.format(algcode))
      algid = scribe.insertAlgorithm(alg)
      ids.append(algid)
   return ids

# ...............................................
def _addIntersectGrid(scribe, gridname, cellsides, cellsize, mapunits, epsg, bbox, usr):
   shp = ShapeGrid(gridname, usr, epsg, cellsides, cellsize, mapunits, bbox,
                   status=JobStatus.INITIALIZE, statusModTime=CURR_MJD)
   newshp = scribe.findOrInsertShapeGrid(shp)
   try:
      newshp.buildShape()
   except Exception, e:
      scribe.log.warning('Unable to build Shapegrid ({})'.format(str(e)))
   else:
      newshp.updateStatus(JobStatus.COMPLETE)
      success =  scribe.updateShapeGrid(newshp)
      if success is False:
         scribe.log.warning('Failed to update Shapegrid record')
   return newshp
   
# ...............................................
def addArchive(scribe, gridname, configFname, archiveName, cellsides, cellsize, 
               mapunits, epsg, bbox, usr):
   """
   @summary: Create a Shapegrid, PAM, and Gridset for this archive's Global PAM
   """
   shp = _addIntersectGrid(scribe, gridname, cellsides, cellsize, mapunits, epsg, 
                           bbox, usr)
   # "BOOM" Archive
   meta = {ServiceObject.META_DESCRIPTION: ARCHIVE_KEYWORD,
           ServiceObject.META_KEYWORDS: [ARCHIVE_KEYWORD]}
   grdset = Gridset(name=archiveName, metadata=meta, shapeGrid=shp, 
                    configFilename=configFname, epsgcode=epsg, 
                    userId=usr, modTime=CURR_MJD)
   updatedGrdset = scribe.findOrInsertGridset(grdset)
   # "Global" PAM
   meta = {ServiceObject.META_DESCRIPTION: GPAM_KEYWORD,
           ServiceObject.META_KEYWORDS: [GPAM_KEYWORD]}
   gpam = LMMatrix(None, matrixType=MatrixType.PAM, metadata=meta,
                 userId=usr, gridset=updatedGrdset,
                 status=JobStatus.GENERAL, statusModTime=CURR_MJD)
   updatedGpam = scribe.findOrInsertMatrix(gpam)
   
   return shp, updatedGrdset, updatedGpam
   
# ...............................................
def _getbioName(obsOrPred, res, 
                gcm=None, tm=None, altpred=None, 
                lyrtype=None, 
                suffix=None, isTitle=False):
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
def _getBaselineLayers(usr, pkgMeta, baseMeta, configMeta, lyrtypeMeta):
   """
   @summary Assembles layer metadata for a single layerset
   """
   layers = []
   staticLayers = {}
   for envcode in pkgMeta['layertypes']:
      ltmeta = lyrtypeMeta[envcode]
      envKeywords = [k for k in baseMeta['keywords']]
      relfname, isStatic = _findFileFor(ltmeta, pkgMeta['baseline'], 
                                        gcm=None, tm=None, altPred=None)
      lyrname = _getbioName(pkgMeta['baseline'], pkgMeta['res'], lyrtype=envcode, 
                            suffix=pkgMeta['suffix'])
      lyrmeta = {'title': ' '.join((pkgMeta['baseline'], ltmeta['title'])),
                 'description': ' '.join((pkgMeta['baseline'], ltmeta['description']))}
      envmeta = {'title': ltmeta['title'],
                 'description': ltmeta['description'],
                 'keywords': envKeywords.extend(ltmeta['keywords'])}
      dloc = os.path.join(ENV_DATA_PATH, relfname)
      if not os.path.exists(dloc):
         print('Missing local data %s' % dloc)
      envlyr = EnvLayer(lyrname, usr, configMeta['epsg'], 
                        dlocation=dloc, 
                        lyrMetadata=lyrmeta,
                        dataFormat=configMeta['gdalformat'], 
                        gdalType=configMeta['gdaltype'],
                        valUnits=ltmeta['valunits'],
                        mapunits=configMeta['mapunits'], 
                        resolution=configMeta['resolution'], 
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
def _findFileFor(ltmeta, obsOrPred, gcm=None, tm=None, altPred=None):
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
def _getPredictedLayers(usr, pkgMeta, configMeta, lyrtypeMeta, staticLayers,
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
      relfname, isStatic = _findFileFor(ltmeta, predRpt, 
                                        gcm=gcm, tm=tm, altPred=altpred)
      if not isStatic:
         lyrname = _getbioName(predRpt, pkgMeta['res'], gcm=gcm, tm=tm, 
                               altpred=altpred, lyrtype=envcode, 
                               suffix=pkgMeta['suffix'], isTitle=False)
         lyrtitle = _getbioName(predRpt, pkgMeta['res'], gcm=gcm, tm=tmvals['name'], 
                                altpred=altpred, lyrtype=envcode, 
                                suffix=pkgMeta['suffix'], isTitle=True)
         scentitle = _getbioName(predRpt, pkgMeta['res'], gcm=mdlvals['name'], 
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
         envlyr = EnvLayer(lyrname, usr, configMeta['epsg'], 
                           dlocation=dloc, 
                           lyrMetadata=lyrmeta,
                           dataFormat=configMeta['gdalformat'], 
                           gdalType=rstType,
                           valUnits=ltmeta['valunits'],
                           mapunits=configMeta['mapunits'], 
                           resolution=configMeta['resolution'], 
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
def createBaselineScenario(usr, pkgMeta, configMeta, lyrtypeMeta, 
                           observedPredictedMeta, climKeywords):
   """
   @summary Assemble Worldclim/bioclim scenario
   """
   obsKey = pkgMeta['baseline']
   baseMeta = observedPredictedMeta[obsKey]
#    tm = baseMeta['times'].keys()[0]
   basekeywords = [k for k in climKeywords]
   basekeywords.extend(baseMeta['keywords'])
   
   scencode = _getbioName(obsKey, pkgMeta['res'], suffix=pkgMeta['suffix'])
   lyrs, staticLayers = _getBaselineLayers(usr, pkgMeta, baseMeta, configMeta, 
                                           lyrtypeMeta)
   scenmeta = {'title': baseMeta['title'], 'author': baseMeta['author'], 
               'description': baseMeta['description'], 'keywords': basekeywords}
   scen = Scenario(scencode, usr, configMeta['epsg'], 
                   metadata=scenmeta, 
                   units=configMeta['mapunits'], 
                   res=configMeta['resolution'], 
                   dateCode=pkgMeta['baseline'],
                   bbox=pkgMeta['bbox'], 
                   modTime=CURR_MJD,  
                   layers=lyrs)
   return scen, staticLayers

# ...............................................
def createPredictedScenarios(usr, pkgMeta, configMeta, lyrtypeMeta, staticLayers,
                             observedPredictedMeta, climKeywords):
   """
   @summary Assemble predicted future scenarios defined by IPCC report
   """
   predScenarios = {}
   predScens = pkgMeta['predicted']
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
         scencode = _getbioName(predRpt, pkgMeta['res'], gcm=gcm, 
                                tm=tm, altpred=altpred, 
                                suffix=pkgMeta['suffix'], isTitle=False)
         scentitle = _getbioName(predRpt, pkgMeta['res'], gcm=mdlvals['name'], 
                                 tm=tmvals['name'], altpred=altpred, 
                                 suffix=pkgMeta['suffix'], isTitle=True)
         obstitle = observedPredictedMeta[pkgMeta['baseline']]['title']
         scendesc =  ' '.join((obstitle, 
                  'and predicted climate calculated from {}'.format(scentitle)))
         scenmeta = {'title': scentitle, 'author': mdlvals['author'], 
                     'description': scendesc, 'keywords': scenkeywords}
         lyrs = _getPredictedLayers(usr, pkgMeta, configMeta, lyrtypeMeta, 
                              staticLayers, observedPredictedMeta, predRpt, tm, 
                              gcm=gcm, altpred=altpred)
         
         scen = Scenario(scencode, usr, configMeta['epsg'], 
                         metadata=scenmeta, 
                         units=configMeta['mapunits'], 
                         res=configMeta['resolution'], 
                         gcmCode=gcm, altpredCode=altpred, dateCode=tm,
                         bbox=pkgMeta['bbox'], 
                         modTime=CURR_MJD, 
                         layers=lyrs)
         predScenarios[scencode] = scen
   return predScenarios

# ...............................................
def addScenarioAndLayerMetadata(scribe, scenarios):
   """
   @summary Add scenario and layer metadata to database  
   """
   for scode, scen in scenarios.iteritems():
      scribe.log.info('Insert scenario {}'.format(scode))
      newscen = scribe.insertScenario(scen)

# ...............................................
def _getConfiguredMetadata(META, pkgMeta):
   try:
      epsg = META.EPSG
   except:
      epsg = DEFAULT_EPSG
      
   try:
      mapunits = META.MAPUNITS
   except:
      if epsg == DEFAULT_EPSG:
         mapunits = DEFAULT_MAPUNITS
      else:
         raise LMError('Failed to specify MAPUNITS for EPSG {}'.format(epsg))
   try:
      res = META.RESOLUTIONS[pkgMeta['res']]
   except:
      raise LMError('Failed to specify res or RESOLUTIONS for CLIMATE_PACKAGE')
   try:
      gdaltype = META.ENVLYR_GDALTYPE
   except:
      raise LMError('Failed to specify ENVLYR_GDALTYPE')
   try:
      gdalformat = META.ENVLYR_GDALFORMAT
   except:
      raise LMError('Failed to specify META.ENVLYR_GDALFORMAT')
   expYear = CURRDATE[0]
   expMonth = CURRDATE[1]
   expDay = CURRDATE[2]

   configMeta = {'epsg': epsg, 
                 'mapunits': mapunits, 
                 'resolution': res, 
                 'gdaltype': gdaltype, 
                 'gdalformat': gdalformat,
                 'expdate': (expYear, expMonth, expDay)}
   return configMeta

# ...............................................
def _importClimatePackageMetadata(envPackageName):
   # TODO: Remove `v2`
   metafname = os.path.join(ENV_DATA_PATH, '{}.v2{}'.format(envPackageName, 
                                                            OutputFormat.PYTHON))
   if not os.path.exists(metafname):
      raise LMError(currargs='Climate metadata {} does not exist'
                    .format(metafname))
   # TODO: change to importlib on python 2.7 --> 3.3+  
   try:
      import imp
      META = imp.load_source('currentmetadata', metafname)
   except Exception, e:
      raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                    .format(metafname, e))
   return META, metafname

# ...............................................
def writeConfigFile(archiveName, envPackageName, userid, userEmail, 
                     speciesSource, speciesData, 
                     configMeta, minpoints, algorithms, 
                     gridname, grid_cellsize, grid_cellsides, 
                     mdlScen=None, prjScens=None):
   """
   """
   earl = EarlJr()
   pth = earl.createDataPath(userid, LMFileType.BOOM_CONFIG)
   newConfigFilename = os.path.join(pth, 
                              '{}{}'.format(archiveName, OutputFormat.CONFIG))
   f = open(newConfigFilename, 'w')
   f.write('[LmServer - environment]\n')
   f.write('ARCHIVE_USER: {}\n'.format(userid))

   f.write('[LmServer - pipeline]\n')
   f.write('ARCHIVE_DATASOURCE: {}\n\n'.format(speciesSource))
   f.write('ARCHIVE_NAME: {}\n\n'.format(archiveName))
   if userEmail is not None:
      f.write('ARCHIVE_TROUBLESHOOTERS: {}\n\n'.format(userEmail))
   
   # Expiration date triggering re-query and computation
   f.write('ARCHIVE_SPECIES_EXP_YEAR: {}\n'.format(CURRDATE[0]))
   f.write('ARCHIVE_SPECIES_EXP_MONTH: {}\n'.format(CURRDATE[1]))
   f.write('ARCHIVE_SPECIES_EXP_DAY: {}\n\n'.format(CURRDATE[2]))
   
   # SDM Algorithm and minimun number of required species points   
   f.write('ARCHIVE_POINT_COUNT_MIN: {}\n\n'.format(minpoints))
   if len(algorithms) > 0:
      algs = ','.join(algorithms)
   else:
      algs = DEFAULT_ALGORITHMS
   f.write('ARCHIVE_ALGORITHMS: {}\n\n'.format(algs))

   # Intersection grid
   f.write('ARCHIVE_GRID_NAME: {}\n'.format(gridname))
   f.write('ARCHIVE_GRID_CELLSIZE: {}\n\n'.format(grid_cellsize))
   f.write('ARCHIVE_GRID_NUM_SIDES: {}\n'.format(grid_cellsides))

   # Species source type (for processing) and file
   if speciesSource == SpeciesDatasource.GBIF:
      varname = 'GBIF_OCCURRENCE_FILENAME'
      if speciesData is None:
         speciesData = GBIF_OCCURRENCE_FILENAME
   elif speciesSource == SpeciesDatasource.BISON:
      varname = 'BISON_TSN_FILENAME'
      if speciesData is None:
         speciesData = BISON_TSN_FILENAME
   elif speciesSource == SpeciesDatasource.IDIGBIO:
      varname = 'IDIG_FILENAME'
      if speciesData is None:
         speciesData = IDIG_FILENAME
   else:
      varname = 'ARCHIVE_USER_OCCURRENCE_DATA'
      if speciesData is None:
         speciesData = USER_OCCURRENCE_DATA
   f.write('{}: {}\n\n'.format(varname, speciesData))
      
   # Input environmental data, pulled from environmental metadata  
   f.write('ARCHIVE_SCENARIO_PACKAGE: {}\n\n'.format(envPackageName))
   f.write('ARCHIVE_EPSG: {}\n\n'.format(configMeta['epsg']))
   f.write('ARCHIVE_MAPUNITS: {}\n\n'.format(configMeta['mapunits']))
   
   # Scenario codes, created from environmental metadata  
   if mdlScen is None:
      mdlScen = DEFAULT_MODEL_SCENARIO
   f.write('ARCHIVE_MODEL_SCENARIO: {}\n'.format(mdlScen))
   if not prjScens:
      prjScens = DEFAULT_PROJECTION_SCENARIOS
   pcodes = ','.join(prjScens)
   f.write('ARCHIVE_PROJECTION_SCENARIOS: {}\n'.format(pcodes))
   
   f.close()
   return newConfigFilename

# ...............................................
if __name__ == '__main__':
   if not Archivist.isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)

   algs=','.join(DEFAULT_ALGORITHMS)
   allAlgs = ','.join(ALGORITHM_DATA.keys())
   apiUrl = 'http://lifemapper.github.io/api.html'
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper database with metadata ' +
                         'specific to an \'archive\', populated with '
                         'command line arguments, including values in the ' +
                         'specified environmental data package.'))
   parser.add_argument('-n', '--archive_name', default=ARCHIVE_NAME,
            help=('Name for the archive, gridset, and grid created from ' +
                  'these data.  Do not use special characters in this name.'))
   parser.add_argument('-u', '--user', default=ARCHIVE_USER,
            help=('Owner of this archive this archive. The default is '
                  'ARCHIVE_USER ({}), an existing user '.format(ARCHIVE_USER) +
                  'not requiring an email. '))
   parser.add_argument('-m', '--email', default=None,
            help=('If the owner is a new user, provide an email address '))
   parser.add_argument('-e', '--environmental_metadata', default=SCENARIO_PACKAGE,
            help=('Metadata file should exist in the {} '.format(ENV_DATA_PATH) +
                  'directory and be named with the arg value and .py extension'))
   parser.add_argument('-s', '--species_source', default='GBIF',
            help=('Species source will be: ' + 
                  '\'GBIF\' for GBIF-provided CSV data; ' +
                  '\'IDIGBIO\' iDigBio queries ' +
                  '\'BISON\' for a list of ITIS TSNs for querying the BISON API. ' +
                  'Any other value will indicate that user-supplied CSV ' +
                  'data, documented with metadata describing the fields, is ' +
                  'to be used for the archive'))
   parser.add_argument('-f', '--species_file', default=None,
            help=('Species file (without full path) will be: ' + 
                  '1) CSV data sorted by taxon id for \'GBIF\' species source ' +
                  '(include extension); ' +
                  '2) Text filename containing a list of GBIF accepted taxon ids ' +
                  'for \'IDIGBIO\' species source (include extension); ' +
                  '3) Text filename containing a  list of ITIS TSNs for \'BISON\' ' +
                  'species source (include extension); '
                  '4) Basename of the data and metadata files for ' + 
                  'user-provided data.  They must have ' +
                  'the same basename.  The data file must have \'.csv\' ' +
                  'extension, metadata file must have \'.meta\' extension. ' +
                  'Metadata describes the data fields. ' ))
   parser.add_argument('-p', '--min_points', type=int, default=POINT_COUNT_MIN,
            help=('Minimum number of points required for SDM computation ' +
                  'The default is POINT_COUNT_MIN in config.lmserver.ini or ' +
                  'the site-specific configuration file config.site.ini' ))
   parser.add_argument('-a', '--algorithms', default=algs,
            help=('Comma-separated list of algorithm codes for computing  ' +
                  'SDM experiments in this archive.  Options are described at ' +
                  '{} and include the codes: {} '.format(apiUrl, allAlgs)))
   parser.add_argument('-c', '--grid_cellsize', default=1,
            help=('Size of cells in the grid used for Global PAM. ' +
                  'Units are mapunits'))
   parser.add_argument('-q', '--grid_shape', choices=('square', 'hexagon'),
            default='square', help=('Shape of cells in the grid used for Global PAM.'))

   args = parser.parse_args()
   archiveName = args.archive_name.replace(' ', '_')
   usr = args.user
   usrEmail = args.email
   envPackageName = args.environmental_metadata
   speciesSource = args.species_source.upper()
   speciesData = args.species_file
   minpoints = args.min_points
   algstring = args.algorithms.upper()
   algorithms = [alg.strip() for alg in algstring.split(',')]
   cellsize = args.grid_cellsize
   gridname = '{}-Grid'.format(archiveName)
   if args.grid_shape == 'hexagon':
      cellsides = 6
   else:
      cellsides = 4
   # Imports META
   META, metafname = _importClimatePackageMetadata(envPackageName)
   pkgMeta = META.CLIMATE_PACKAGES[envPackageName]
   configMeta = _getConfiguredMetadata(META, pkgMeta)
      
# .............................
   basefilename = os.path.basename(__file__)
   basename, ext = os.path.splitext(basefilename)
   logger = ScriptLogger(basename+'_borg')
   scribeWithBorg = BorgScribe(logger)
   success = scribeWithBorg.openConnections()

   if not success: 
      logger.critical('Failed to open database')
      exit(0)
   
   try:      
# .............................
      logger.info('  Insert user metadata ...')
      addUsers(scribeWithBorg, usr, usrEmail)
# .............................
      logger.info('  Insert algorithm metadata ...')
      aIds = addAlgorithms(scribeWithBorg)
# .............................
      logger.info('  Insert climate {} metadata ...'.format(envPackageName))
      # Current
      basescen, staticLayers = createBaselineScenario(usr, pkgMeta, configMeta, 
                                                      META.LAYERTYPE_META,
                                                      META.OBSERVED_PREDICTED_META,
                                                      META.CLIMATE_KEYWORDS)
      logger.info('     Created base scenario {}'.format(basescen.code))
      # Predicted Past and Future
      predScens = createPredictedScenarios(usr, pkgMeta, configMeta, 
                                           META.LAYERTYPE_META, staticLayers,
                                           META.OBSERVED_PREDICTED_META,
                                           META.CLIMATE_KEYWORDS)
      logger.info('     Created predicted scenarios {}'.format(predScens.keys()))
      predScens[basescen.code] = basescen
      addScenarioAndLayerMetadata(scribeWithBorg, predScens)
# .............................
      # Shapefile, Gridset, Matrix for GPAM/BOOMArchive
      logger.info('  Insert, build shapegrid {} ...'.format(gridname))
      updatedShp, updatedGrdset, updatedGpam = addArchive(scribeWithBorg, 
                         gridname, metafname, archiveName, cellsides, cellsize, 
                         configMeta['mapunits'], configMeta['epsg'], 
                         pkgMeta['bbox'], usr)
      
# .............................
      # Insert all taxonomic sources for now
      logger.info('  Insert taxonomy metadata ...')
      for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
         taxSourceId = scribeWithBorg.insertTaxonomySource(taxInfo['name'],
                                                           taxInfo['url'])
# .............................
      # Write config file for this archive
      mdlScencode = basescen.code
      prjScencodes = predScens.keys()
      newConfigFilename = writeConfigFile(archiveName, envPackageName, usr, 
                           usrEmail, speciesSource, speciesData, configMeta, 
                           minpoints, algorithms, gridname, cellsize, cellsides, 
                           mdlScen=mdlScencode, prjScens=prjScencodes)
   except Exception, e:
      logger.error(str(e))
      raise
   finally:
      scribeWithBorg.closeConnections()
       
"""
$PYTHON LmDbServer/boom/initboom.py --help
$PYTHON LmDbServer/boom/initboom.py -n "Aimee test archive" \
  -u aimee -m zzeppozz@gmail.com -e 10min-past-present-future \
  -s gbif -p 25 -a bioclim -c 1 -q square

import mx.DateTime
import os
from LmDbServer.common.localconstants import (DEFAULT_ALGORITHMS, 
         DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, DEFAULT_GRID_NAME, 
         DEFAULT_GRID_CELLSIZE, SCENARIO_PACKAGE, USER_OCCURRENCE_DATA)
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, OutputFormat, 
                                         JobStatus, MatrixType)
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import (ALGORITHM_DATA, ENV_DATA_PATH, 
         GPAM_KEYWORD, ARCHIVE_NAME, ARCHIVE_KEYWORD)
from LmServer.common.localconstants import (ARCHIVE_USER, POINT_COUNT_MIN,
                                            DEFAULT_EPSG, DEFAULT_MAPUNITS)
from LmServer.common.log import ScriptLogger
from LmServer.common.lmuser import LMUser
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.db.borgscribe import BorgScribe
from LmServer.sdm.algorithm import Algorithm
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import Matrix            
from LmServer.legion.scenario import Scenario
from LmServer.legion.shapegrid import ShapeGrid

archiveName = 'Aimee test archive'.replace(' ', '_')
usr = 'aimee'
usrEmail = 'zzeppozz@gmail.com'
envPackageName = '10min-past-present-future'
speciesSource = 'gbif'.upper()
speciesData = None
minpoints = 25
algstring = 'BIOCLIM'
algorithms = [alg.strip() for alg in algstring.split(',')]
cellsize = 1
gridname = '{}-Grid'.format(archiveName)
cellsides = 4


CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd
from LmDbServer.tools.initBoom import *
from LmDbServer.tools.initBoom import ( _importClimatePackageMetadata,
          _getConfiguredMetadata, _getbioName, _getBaselineLayers, _findFileFor,
          _addIntersectGrid, _writeConfigFile)
          
META, metafname = _importClimatePackageMetadata(envPackageName)
pkgMeta = META.CLIMATE_PACKAGES[envPackageName]
configMeta = _getConfiguredMetadata(META, pkgMeta)
lyrtypeMeta = META.LAYERTYPE_META

logger = ScriptLogger('testing')
scribe = BorgScribe(logger)
success = scribe.openConnections()

# ...................................................
# User testing
addUsers(scribe, usr, usrEmail)

# ...................................................
# Scenario testing
basescen, staticLayers = createBaselineScenario(usr, pkgMeta, configMeta, 
                                                META.LAYERTYPE_META,
                                                META.OBSERVED_PREDICTED_META,
                                                META.CLIMATE_KEYWORDS)
predScens = createPredictedScenarios(usr, pkgMeta, configMeta, 
                                     META.LAYERTYPE_META, staticLayers,
                                     META.OBSERVED_PREDICTED_META,
                                     META.CLIMATE_KEYWORDS)
predScens[basescen.code] = basescen
addScenarioAndLayerMetadata(scribe, predScens)

# ...................................................
# Shapegrid testing
(gridname, configFname, archiveName, cellsides, cellsize, 
 mapunits, epsg, bbox, usr) = (configMeta['gridname'], 
                         metafname, archiveName, 
                         configMeta['gridsides'], 
                         configMeta['gridsize'], configMeta['mapunits'], 
                         configMeta['epsg'], pkgMeta['bbox'], usr)

shp = _addIntersectGrid(scribe, gridname, cellsides, cellsize, mapunits, epsg, 
                        bbox, usr)
# "BOOM" Archive
meta = {ServiceObject.META_DESCRIPTION: ARCHIVE_KEYWORD,
        ServiceObject.META_KEYWORDS: [ARCHIVE_KEYWORD]}
grdset = Gridset(name=archiveName, metadata=meta, shapeGridId=shp.getId(), 
                 configFilename=configFname, epsgcode=shp.epsgcode, 
                 userId=usr, modTime=CURR_MJD)
updatedGrdset = scribe.findOrInsertGridset(grdset)
# "Global" PAM
meta = {ServiceObject.META_DESCRIPTION: GPAM_KEYWORD,
        ServiceObject.META_KEYWORDS: [GPAM_KEYWORD]}
gpam = Matrix(None, matrixType=MatrixType.PAM, metadata=meta,
              userId=usr, gridsetId=updatedGrdset.getId(),
              status=JobStatus.GENERAL, statusModTime=CURR_MJD)
updatedGpam = scribe.findOrInsertMatrix(gpam)
 


"""