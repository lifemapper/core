"""
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research
 
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

# # TODO: These should be included in the package of data
# import LmDbServer.tools.charlieMetaExp3 as META
from LmDbServer.common.localconstants import (DEFAULT_ALGORITHMS, 
         DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, DEFAULT_GRID_NAME, 
         DEFAULT_GRID_CELLSIZE, SCENARIO_PACKAGE, USER_OCCURRENCE_DATA,
         USER_OCCURRENCE_CSV_FILENAME)
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, DEFAULT_EPSG, 
                                         DEFAULT_MAPUNITS, OutputFormat)
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import ALGORITHM_DATA, ENV_DATA_PATH
from LmServer.common.localconstants import (ARCHIVE_USER, DATASOURCE)
from LmServer.common.log import ScriptLogger
from LmServer.common.lmuser import LMUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.sdm.algorithm import Algorithm
from LmServer.sdm.envlayer import EnvironmentalType, EnvironmentalLayer                    
from LmServer.sdm.scenario import Scenario
from LmServer.rad.shapegrid import ShapeGrid

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd
# CURR_MJD = 57686
# ...............................................
def addUsers(scribe, configMeta):
   """
   @summary Adds ARCHIVE_USER, anon user and USER from metadata to the database
   """
   userList = [{'id': ARCHIVE_USER,
                'email': '{}@nowhere.org'.format(ARCHIVE_USER)},
               {'id': DEFAULT_POST_USER,
                'email': '{}@nowhere.org'.format(DEFAULT_POST_USER)}]
   if configMeta['userid'] != ARCHIVE_USER:
      userList.append({'id': configMeta['userid'],'email': configMeta['email']})

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
def addIntersectGrid(scribe, gridname, cellsides, cellsize, mapunits, epsg, bbox, usr):
   shp = ShapeGrid(gridname, cellsides, cellsize, mapunits, epsg, bbox, userId=usr)
   newshp = scribe.insertShapeGrid(shp)
   newshp.buildShape()
   return newshp.getId()
   
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
   for ltype in pkgMeta['layertypes']:
      ltmeta = lyrtypeMeta[ltype]
      keywords = [k for k in baseMeta['keywords']]
      relfname, isStatic = _findFileFor(ltmeta, pkgMeta['baseline'], 
                                        gcm=None, tm=None, altPred=None)
      lyrname = _getbioName(pkgMeta['baseline'], pkgMeta['res'], lyrtype=ltype, 
                            suffix=pkgMeta['suffix'])
      lyrMeta = {'title': ' '.join((pkgMeta['baseline'], ltmeta['title'])),
                 'description': ' '.join((pkgMeta['baseline'], ltmeta['description']))}
      envmeta = {'title': ltmeta['title'],
                 'description': ltmeta['description'],
                 'keywords': keywords.extend(ltmeta['keywords'])}
      dloc = os.path.join(ENV_DATA_PATH, relfname)
      if not os.path.exists(dloc):
         print('Missing local data %s' % dloc)
      envlyr = EnvironmentalLayer(lyrname, lyrMetadata=lyrMeta,
                                  valUnits=ltmeta['valunits'],
                                  dlocation=dloc, 
                                  bbox=pkgMeta['bbox'], 
                                  gdalFormat=configMeta['gdalformat'], 
                                  gdalType=configMeta['gdaltype'],
                                  mapunits=configMeta['mapunits'], 
                                  resolution=configMeta['resolution'], 
                                  epsgcode=configMeta['epsg'], 
                                  layerType=ltype, envMetadata=envmeta,
                                  userId=usr, modTime=CURR_MJD)
      layers.append(envlyr)
      if isStatic:
         staticLayers[ltype] = envlyr
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
   for ltype in layertypes:
      keywords = [k for k in observedPredictedMeta[predRpt]['keywords']]
      ltmeta = lyrtypeMeta[ltype]
      relfname, isStatic = _findFileFor(ltmeta, predRpt, 
                                        gcm=gcm, tm=tm, altPred=altpred)
      if not isStatic:
         lyrname = _getbioName(predRpt, pkgMeta['res'], gcm=gcm, tm=tm, 
                               altpred=altpred, lyrtype=ltype, 
                               suffix=pkgMeta['suffix'], isTitle=False)
         lyrtitle = _getbioName(predRpt, pkgMeta['res'], gcm=gcm, tm=tmvals['name'], 
                                altpred=altpred, lyrtype=ltype, 
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
         envlyr = EnvironmentalLayer(lyrname, lyrMetadata=lyrmeta,
                                     valUnits=ltmeta['valunits'],
                                     dlocation=dloc, 
                                     bbox=pkgMeta['bbox'], 
                                     gdalFormat=configMeta['gdalformat'], 
                                     gdalType=rstType,
                                     mapunits=configMeta['mapunits'], 
                                     resolution=configMeta['resolution'], 
                                     epsgcode=configMeta['epsg'], 
                                     layerType=ltype, 
                                     gcmCode=gcm, altpredCode=altpred, dateCode=tm,
                                     envMetadata=envmeta,
                                     userId=usr, modTime=CURR_MJD)
      else:
         # Use the observed data
         envlyr = staticLayers[ltype]
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
   tm = baseMeta['times'].keys()[0]
   basekeywords = [k for k in climKeywords]
   basekeywords.extend(baseMeta['keywords'])
   
   scencode = _getbioName(obsKey, pkgMeta['res'], suffix=pkgMeta['suffix'])
   lyrs, staticLayers = _getBaselineLayers(usr, pkgMeta, baseMeta, configMeta, 
                                           lyrtypeMeta)
   scenmeta = {'title': baseMeta['title'], 'author': baseMeta['author'], 
               'description': baseMeta['description'], 'keywords': basekeywords}
   scen = Scenario(scencode, metadata=scenmeta, 
                   units=configMeta['mapunits'], 
                   res=configMeta['resolution'], 
                   bbox=pkgMeta['bbox'], 
                   modTime=CURR_MJD,  
                   epsgcode=configMeta['epsg'], 
                   layers=lyrs, 
                   userId=usr)
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
         
         scen = Scenario(scencode, metadata=scenmeta, 
                         gcmCode=gcm, altpredCode=altpred, dateCode=tm,
                         units=configMeta['mapunits'], res=configMeta['resolution'], 
                         bbox=pkgMeta['bbox'], modTime=CURR_MJD, 
                         epsgcode=configMeta['epsg'], layers=lyrs, userId=usr)
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
      userid = META.USER['id']
   except:
      userid = ARCHIVE_USER
   try:
      email = META.USER['email']
   except:
      email = None
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
   try:
      grdname = META.GRID_NAME
   except:
      grdname = DEFAULT_GRID_NAME
   try:
      grdsize = META.GRID_CELLSIZE
   except:
      if mapunits == DEFAULT_MAPUNITS:
         grdsize = DEFAULT_GRID_CELLSIZE
      else:
         raise LMError('Failed to specify GRID_CELLSIZE for MAPUNITS {}'
                       .format(mapunits))
   try:
      grdsides = META.GRID_NUM_SIDES
   except:
      grdsides = 4
   expYear = CURRDATE[0]
   expMonth = CURRDATE[1]
   expDay = CURRDATE[2]
   try:
      algs = META.ALGORITHM_CODES
   except:
      algs = DEFAULT_ALGORITHMS
   try:
      speciesDataName = META.SPECIES_DATA
   except:
      speciesDataName = USER_OCCURRENCE_DATA

   configMeta = {'userid': userid,
                 'email': email,
                 'epsg': epsg, 
                 'mapunits': mapunits, 
                 'resolution': res, 
                 'gdaltype': gdaltype, 
                 'gdalformat': gdalformat,
                 'gridname': grdname, 
                 'gridsides': grdsides, 
                 'gridsize': grdsize,
                 'expdate': (expYear, expMonth, expDay),
                 'algorithms': algs,
                 'speciesdata': speciesDataName}
   return configMeta

# ...............................................
def _importClimatePackageMetadata(envPackageName):
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
   return META

# ...............................................
def _writeConfigFile(envPackageName, userid, configMeta, mdlScen=None, prjScens=None):
   SERVER_CONFIG_FILENAME = os.getenv('LIFEMAPPER_SERVER_CONFIG_FILE') 
   pth, temp = os.path.split(SERVER_CONFIG_FILENAME)
   newConfigFilename = os.path.join(pth, '{}{}'.format(envPackageName, 
                                                       OutputFormat.CONFIG))
   f = open(newConfigFilename, 'w')
   f.write('[LmServer - environment]\n')
   f.write('ARCHIVE_USER: {}\n'.format(userid))
   f.write('DATASOURCE: User\n\n')

   f.write('[LmServer - pipeline]\n')
   if configMeta['email'] is not None:
      f.write('TROUBLESHOOTERS: {}\n\n'.format(configMeta['email']))
   
   f.write('SPECIES_EXP_YEAR: {}\n'.format(configMeta['speciesdata'][0]))
   f.write('SPECIES_EXP_MONTH: {}\n'.format(configMeta['speciesdata'][1]))
   f.write('SPECIES_EXP_DAY: {}\n\n'.format(configMeta['speciesdata'][2]))

   algs = ','.join(configMeta['algorithms'])
   f.write('DEFAULT_ALGORITHMS: {}\n\n'.format(algs))

   f.write('DEFAULT_GRID_NAME: {}\n'.format(configMeta['gridname']))
   f.write('DEFAULT_GRID_CELLSIZE: {}\n\n'.format(configMeta['gridsize']))

   f.write('USER_OCCURRENCE_DATA: {}\n\n'.format(configMeta['speciesdata']))

   f.write('SCENARIO_PACKAGE: {}\n\n'.format(envPackageName))

   f.write('DEFAULT_EPSG: {}\n\n'.format(configMeta['epsg']))
   
   if mdlScen is None:
      mdlScen = DEFAULT_MODEL_SCENARIO
   f.write('DEFAULT_MODEL_SCENARIO: {}\n'.format(mdlScen))
   if not prjScens:
      prjScens = DEFAULT_PROJECTION_SCENARIOS
   pcodes = ','.join(prjScens)
   f.write('DEFAULT_PROJECTION_SCENARIOS: {}\n'.format(pcodes))
   
   f.close()
   return newConfigFilename

# ...............................................
if __name__ == '__main__':
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper database with metadata ' +
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('-m', '--metadata', default='config',
            help=('Metadata file should exist in the {} '.format(ENV_DATA_PATH) +
                  'directory and be named with the arg value and .py extension'))

   args = parser.parse_args()
   envPackageName = args.metadata
   if envPackageName.lower() == 'config':
      envPackageName = SCENARIO_PACKAGE
   # Imports META
   META = _importClimatePackageMetadata(envPackageName)
   pkgMeta = META.CLIMATE_PACKAGES[envPackageName]
   configMeta = _getConfiguredMetadata(META, pkgMeta)
   usr = configMeta['userid']
   
# .............................
   try:
      taxSource = TAXONOMIC_SOURCE[DATASOURCE] 
   except:
      taxSource = None
      
# .............................
   basefilename = os.path.basename(__file__)
   basename, ext = os.path.splitext(basefilename)
   try:
      logger = ScriptLogger(basename+'_borg')
      scribeWithBorg = BorgScribe(logger)
      success = scribeWithBorg.openConnections()

      if not success: 
         logger.critical('Failed to open database')
         exit(0)
      
# .............................
      logger.info('  Insert user metadata ...')
      addUsers(scribeWithBorg, configMeta)
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
      # Grid for GPAM
      logger.info('  Insert, build shapegrid {} ...'.format(configMeta['gridname']))
      shpId = addIntersectGrid(scribeWithBorg, configMeta['gridname'], 
                     configMeta['gridsides'], configMeta['gridsize'], 
                     configMeta['mapunits'], configMeta['epsg'], pkgMeta['bbox'], 
                     usr)
# .............................
      # Insert all taxonomic sources for now
      logger.info('  Insert taxonomy metadata ...')
      for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
         taxSourceId = scribeWithBorg.insertTaxonomySource(taxInfo['name'],
                                                           taxInfo['url'])
# .............................
      # Write config file for this archive
      mdlScencode = basescen.code
      prjScencodes = predScens.keys().append(mdlScencode)
      newConfigFilename = _writeConfigFile(envPackageName, usr, 
                                           configMeta, mdlScen=mdlScencode, 
                                           prjScens=prjScencodes)
   except Exception, e:
      logger.error(str(e))
      raise
   finally:
      scribeWithBorg.closeConnections()
       
"""
import mx.DateTime
# import LmDbServer.tools.charlieMetaExp3 as META
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmDbServer.common.localconstants import (SCENARIO_PACKAGE, 
         DEFAULT_GRID_NAME, DEFAULT_GRID_CELLSIZE)
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import ALGORITHM_DATA, ENV_DATA_PATH
from LmServer.common.localconstants import (ARCHIVE_USER, DATASOURCE)
from LmServer.common.log import ScriptLogger
from LmServer.common.lmuser import LMUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.sdm.algorithm import Algorithm
from LmServer.sdm.envlayer import EnvironmentalType, EnvironmentalLayer                    
from LmServer.sdm.scenario import Scenario
from LmServer.rad.shapegrid import ShapeGrid
CURR_MJD = mx.DateTime.gmt().mjd
from LmDbServer.tools.initBoom import *
from LmDbServer.tools.initBoom import (_getBaselineLayers, _getbioName, 
          _findFileFor, _getPredictedLayers, _importClimatePackageMetadata,
          _getConfiguredMetadata)
taxSource = TAXONOMIC_SOURCE[DATASOURCE] 
envPackageName = SCENARIO_PACKAGE
META = _importClimatePackageMetadata(envPackageName)
pkgMeta = META.CLIMATE_PACKAGES[envPackageName]
configMeta = _getConfiguredMetadata(META, pkgMeta)
lyrtypeMeta = META.LAYERTYPE_META
usr = configMeta['userid']

logger = ScriptLogger('testing')
scribe = BorgScribe(logger)
success = scribe.openConnections()
addUsers(scribe, configMeta)
scen, staticLayers = createBaselineScenario(usr, pkgMeta, configMeta, lyrtypeMeta,
                                           META.OBSERVED_PREDICTED_META,
                                           META.CLIMATE_KEYWORDS)
predScens = createPredictedScenarios(usr, pkgMeta, configMeta, lyrtypeMeta, 
                                     staticLayers,
                                     META.OBSERVED_PREDICTED_META,
                                     META.CLIMATE_KEYWORDS)
scode = 'observed-1km'
scen = scens[scode]
newOrExistingScen = scribe.insertScenario(scen)

select * from lm_v3.lm_findOrInsertEnvLayer(NULL,'kubi',NULL,NULL,'bio1-AR5-GISS-E2-R-RCP4.5-2050-1km','/share/lm/data/archive/kubi/2163/Layers/bio1-AR5-GISS-E2-R-RCP4.5-2050-1km.tif','http://badenov-vc1.nhm.ku.edu/services/sdm/layers/#id#','{"description": "Annual Mean Temperature for AR5, NASA GISS GCM ModelE, RCP4.5, 2041-2060, 1km", "title": "bio1, AR5, GISS-E2-R, RCP4.5, 2041-2060, 1km"}','GTiff',NULL,NULL,'degreesCelsiusTimes10',NULL,NULL,NULL,2163,'meters',1000,'-180.00,-60.00,180.00,90.00',NULL,57692.8772879,NULL,'bio1','GISS-E2-R','RCP4.5','2050','{"keywords": null, "description": "Annual Mean Temperature", "title": "Annual Mean Temperature"}',NULL);

shpId = addIntersectGrid(scribe, configMeta['gridname'], configMeta['gridsides'], 
                           configMeta['gridsize'], configMeta['mapunits'], configMeta['epsg'], 
                           pkgMeta['bbox'], usr)
select * from lm_v3.lm_findOrInsertShapeGrid(NULL, 'kubi', '10km-grid', 
      NULL,NULL,NULL,
      '/share/lm/data/archive/kubi/2163/Layers/shpgrid_10km-grid.shp',
      NULL,3,FALSE,
      'ESRI Shapefile',2163,'meters',10000,NULL,
      '-180.00,-60.00,180.00,90.00',NULL,
      'http://badenov-vc1.nhm.ku.edu/services/rad/layers/#id#',
      4,10000,0,'siteid','centerX','centerY',NULL,NULL);


                           
"""