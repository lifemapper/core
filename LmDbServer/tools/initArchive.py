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

# TODO: These should be included in the package of data
import LmDbServer.tools.charlieMetaExp3 as META

from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmDbServer.common.localconstants import (SCENARIO_PACKAGE)
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

CURRTIME = mx.DateTime.gmt().mjd
# CURRTIME = 57686
# ...............................................
def addUsers(scribe, newUser):
   """
   @summary Adds ARCHIVE_USER, anon user and USER from metadata to the database
   """
   metaUserId = ARCHIVE_USER
   userList = [{'id': ARCHIVE_USER,
                'email': '{}@nowhere.org'.format(ARCHIVE_USER)},
               {'id': DEFAULT_POST_USER,
                'email': '{}@nowhere.org'.format(DEFAULT_POST_USER)}]
   try:
      metaUserEmail = newUser['email']
      metaUserId = newUser['id']
      userList.append(newUser)
   except:
      pass
   
   for usrmeta in userList:
      try:
         user = LMUser(usrmeta['id'], usrmeta['email'], usrmeta['email'], modTime=CURRTIME)
      except:
         pass
      else:
         scribe.log.info('  Insert user {} ...'.format(usrmeta['id']))
         updatedUser = scribe.insertUser(user)
   return metaUserId

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
def _getBaselineLayers(usr, pkgMeta, baseMeta, lyrMeta, lyrtypeMeta):
   """
   @summary Assembles layer metadata for a single layerset
   """
   layers = []
   staticLayers = {}
   for ltype, ltmeta in lyrtypeMeta.iteritems():
      keywords = [k for k in baseMeta['keywords']]
      relfname, isStatic = _findFileFor(ltmeta, pkgMeta['baseline'], 
                                        gcm=None, tm=None, altPred=None)
      lyrname = _getbioName(pkgMeta['baseline'], pkgMeta['res'], lyrtype=ltype, 
                            suffix=pkgMeta['suffix'])
      lyrmeta = {'title': ' '.join((pkgMeta['baseline'], ltmeta['title'])),
                 'description': ' '.join((pkgMeta['baseline'], ltmeta['description']))}
      envmeta = {'title': ltmeta['title'],
                 'description': ltmeta['description'],
                 'keywords': keywords.extend(ltmeta['keywords'])}
      dloc = os.path.join(ENV_DATA_PATH, pkgMeta['topdir'], relfname)
      if not os.path.exists(dloc):
         print('Missing local data %s' % dloc)
      envlyr = EnvironmentalLayer(lyrname, lyrMetadata=lyrmeta,
                                  valUnits=ltmeta['valunits'],
                                  dlocation=dloc, 
                                  bbox=pkgMeta['bbox'], 
                                  gdalFormat=lyrMeta['gdalformat'], 
                                  gdalType=lyrMeta['gdaltype'],
                                  mapunits=lyrMeta['mapunits'], 
                                  resolution=lyrMeta['resolution'], 
                                  epsgcode=lyrMeta['epsg'], 
                                  layerType=ltype, envMetadata=envmeta,
                                  userId=usr, modTime=CURRTIME)
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
def _getPredictedLayers(usr, pkgMeta, lyrMeta, lyrtypeMeta, staticLayers,
                        predRpt, tm, gcm=None, altpred=None):
   """
   @summary Assembles layer metadata for a single layerset
   """
   mdlvals = META.OBSERVED_PREDICTED_META[predRpt]['models'][gcm]
   tmvals = META.OBSERVED_PREDICTED_META[predRpt]['times'][tm]
   layers = []
   rstType = None
   layertypes = pkgMeta['layertypes']
   for ltype in layertypes:
      keywords = [k for k in META.OBSERVED_PREDICTED_META[predRpt]['keywords']]
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
         dloc = os.path.join(ENV_DATA_PATH, pkgMeta['topdir'], relfname)
         if not os.path.exists(dloc):
            print('Missing local data %s' % dloc)
            dloc = None
         envlyr = EnvironmentalLayer(lyrname, lyrMetadata=lyrmeta,
                                     valUnits=ltmeta['valunits'],
                                     dlocation=dloc, 
                                     bbox=pkgMeta['bbox'], 
                                     gdalFormat=lyrMeta['gdalformat'], 
                                     gdalType=rstType,
                                     mapunits=lyrMeta['mapunits'], 
                                     resolution=lyrMeta['resolution'], 
                                     epsgcode=lyrMeta['epsg'], 
                                     layerType=ltype, 
                                     gcmCode=gcm, altpredCode=altpred, dateCode=tm,
                                     envMetadata=envmeta,
                                     userId=usr, modTime=CURRTIME)
      else:
         # Use the observed data
         envlyr = staticLayers[ltype]
      layers.append(envlyr)
   return layers


# ...............................................
def createBaselineScenario(usr, pkgMeta, lyrMeta, lyrtypeMeta):
   """
   @summary Assemble Worldclim/bioclim scenario
   """
   obsKey = pkgMeta['baseline']
   baseMeta = META.OBSERVED_PREDICTED_META[obsKey]
   tm = baseMeta['times'].keys()[0]
   basekeywords = [k for k in META.ENV_KEYWORDS]
   basekeywords.extend(baseMeta['keywords'])
   
   scencode = _getbioName(obsKey, pkgMeta['res'], suffix=pkgMeta['suffix'])
   lyrs, staticLayers = _getBaselineLayers(usr, pkgMeta, baseMeta, lyrMeta, 
                                           lyrtypeMeta)
   scenmeta = {'title': baseMeta['title'], 'author': baseMeta['author'], 
               'description': baseMeta['description'], 'keywords': basekeywords}
   scen = Scenario(scencode, metadata=scenmeta, 
                   units=lyrMeta['mapunits'], 
                   res=lyrMeta['resolution'], 
                   bbox=pkgMeta['bbox'], 
                   modTime=CURRTIME,  
                   epsgcode=lyrMeta['epsg'], 
                   layers=lyrs, 
                   userId=usr)
   return scen, staticLayers

# ...............................................
def createPredictedScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, staticLayers):
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
            altvals = META.OBSERVED_PREDICTED_META[predRpt]['alternatePredictions'][altpred]
         mdlvals = META.OBSERVED_PREDICTED_META[predRpt]['models'][gcm]
         tmvals = META.OBSERVED_PREDICTED_META[predRpt]['times'][tm]
         # Reset keywords
         scenkeywords = [k for k in META.ENV_KEYWORDS]
         scenkeywords.extend(META.OBSERVED_PREDICTED_META[predRpt]['keywords'])
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
         obstitle = META.OBSERVED_PREDICTED_META[pkgMeta['baseline']]['title']
         scendesc =  ' '.join((obstitle, 
                  'and predicted climate calculated from {}'.format(scentitle)))
         scenmeta = {'title': scentitle, 'author': mdlvals['author'], 
                     'description': scendesc, 'keywords': scenkeywords}
         lyrs = _getPredictedLayers(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
                              staticLayers, predRpt, tm, gcm=gcm, altpred=altpred)
         
         scen = Scenario(scencode, metadata=scenmeta, 
                         gcmCode=gcm, altpredCode=altpred, dateCode=tm,
                         units=lyrMeta['mapunits'], res=lyrMeta['resolution'], 
                         bbox=pkgMeta['bbox'], modTime=CURRTIME, 
                         epsgcode=lyrMeta['epsg'], layers=lyrs, userId=usr)
         predScenarios[scencode] = scen
   return predScenarios

# ...............................................
def createAllScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta):
   """
   @summary Assemble current, predicted past, predicted future scenarios 
   """
   # Current
   basescen, staticLayers = createBaselineScenario(usr, pkgMeta, lyrMeta, 
                                                   lyrtypeMeta)
   scribeWithBorg.log.info('Created base scenario')
   # Predicted Past and Future
   allScenarios = createPredictedScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
                                             staticLayers)
   scribeWithBorg.log.info('Created predicted scenarios')
   # Join all sets and dictionaries
   allScenarios[basescen.code] = basescen
   return allScenarios
      
# ...............................................
def addScenarioAndLayerMetadata(scribe, scenarios):
   """
   @summary Add scenario and layer metadata to database  
   """
   for scode, scen in scenarios.iteritems():
      scribe.log.info('Insert scenario {}'.format(scode))
      newscen = scribe.insertScenario(scen)

# ...............................................
def _importClimatePackageMetadata(envPackageName):
   if envPackageName.lower() == 'config':
      envPackageName = SCENARIO_PACKAGE
   metafname = os.path.join(ENV_DATA_PATH, envPackageName + '.py')
   if not os.path.exists(metafname):
      raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                    .format(metafname, e))
   # TODO: change to importlib on python 2.7 --> 3.3+  
   try:
      import imp
      meta = imp.load_source('currentmetadata', metafname)
   except Exception, e:
      raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                    .format(metafname, e))
   return meta

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
   META = _importClimatePackageMetadata(envPackageName)
   
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

      pkgMeta = META.CLIMATE_PACKAGES[envPackageName]
      lyrMeta = {'epsg': META.EPSG, 
                 'topdir': pkgMeta['topdir'],
                 'mapunits': META.MAPUNITS, 
                 'resolution': META.RESOLUTIONS[pkgMeta['res']], 
                 'gdaltype': META.ENVLYR_GDALTYPE, 
                 'gdalformat': META.ENVLYR_GDALFORMAT,
                 'gridname': META.GRID_NAME, 
                 'gridsides': META.GRID_NUM_SIDES, 
                 'gridsize': META.GRID_CELLSIZE}
         
# .............................
      logger.info('  Insert user metadata ...')
      metaUserId = addUsers(scribeWithBorg, META.USER)
# .............................
      logger.info('  Insert algorithm metadata ...')
      aIds = addAlgorithms(scribeWithBorg)
# .............................
      logger.info('  Insert climate {} metadata ...'.format(envPackageName))
      scens = createAllScenarios(metaUserId, pkgMeta, lyrMeta, META.LAYERTYPE_META)
      addScenarioAndLayerMetadata(scribeWithBorg, scens)
# .............................
      # Grid for GPAM
      logger.info('  Insert, build shapegrid {} ...'.format(lyrMeta['gridname']))
      shpId = addIntersectGrid(scribeWithBorg, lyrMeta['gridname'], lyrMeta['gridsides'], 
                        lyrMeta['gridsize'], lyrMeta['mapunits'], lyrMeta['epsg'], 
                        pkgMeta['bbox'], metaUserId)
# .............................
      # Insert all taxonomic sources for now
      logger.info('  Insert taxonomy metadata ...')
      for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
         taxSourceId = scribeWithBorg.insertTaxonomySource(taxInfo['name'],
                                                           taxInfo['url'])      
   except Exception, e:
      logger.error(str(e))
      raise
   finally:
      scribeWithBorg.closeConnections()
       
"""
import mx.DateTime
import LmDbServer.tools.charlieMetaExp3 as META
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
CURRTIME = mx.DateTime.gmt().mjd
from LmDbServer.tools.initBorg import *
from LmDbServer.tools.initBorg import (_getBaselineLayers, _getbioName, 
          _findFileFor, _getPredictedLayers)
defUser = {'id': ARCHIVE_USER,
           'email': '{}@nowhere.org'.format(ARCHIVE_USER)}
anonUser = {'id': DEFAULT_POST_USER,
            'email': '{}@nowhere.org'.format(DEFAULT_POST_USER)}
newUser = META.USER
metaUserId = META.USER['id']
taxSource = TAXONOMIC_SOURCE[DATASOURCE] 
logger = ScriptLogger('testing')
scribe = BorgScribe(logger)
success = scribe.openConnections()
pkgMeta = META.CLIMATE_PACKAGES[SCENARIO_PACKAGE]
lyrMeta = {'epsg': META.EPSG, 
           'topdir': pkgMeta['topdir'],
           'mapunits': META.MAPUNITS, 
           'resolution': META.RESOLUTIONS[pkgMeta['res']], 
           'gdaltype': META.ENVLYR_GDALTYPE, 
           'gdalformat': META.ENVLYR_GDALFORMAT,
           'gridname': META.GRID_NAME, 
           'gridsides': META.GRID_NUM_SIDES, 
           'gridsize': META.GRID_CELLSIZE}
usr = ARCHIVE_USER
lyrtypeMeta = META.LAYERTYPE_META
scenPkgName = SCENARIO_PACKAGE
usrlist = addUsers(scribe, [defUser, anonUser, newUser])
scens, msgs = createAllScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta)
scode = 'observed-1km'
scen = scens[scode]
newOrExistingScen = scribe.insertScenario(scen)

select * from lm_v3.lm_findOrInsertEnvLayer(NULL,'kubi',NULL,NULL,'bio1-AR5-GISS-E2-R-RCP4.5-2050-1km','/share/lm/data/archive/kubi/2163/Layers/bio1-AR5-GISS-E2-R-RCP4.5-2050-1km.tif','http://badenov-vc1.nhm.ku.edu/services/sdm/layers/#id#','{"description": "Annual Mean Temperature for AR5, NASA GISS GCM ModelE, RCP4.5, 2041-2060, 1km", "title": "bio1, AR5, GISS-E2-R, RCP4.5, 2041-2060, 1km"}','GTiff',NULL,NULL,'degreesCelsiusTimes10',NULL,NULL,NULL,2163,'meters',1000,'-180.00,-60.00,180.00,90.00',NULL,57692.8772879,NULL,'bio1','GISS-E2-R','RCP4.5','2050','{"keywords": null, "description": "Annual Mean Temperature", "title": "Annual Mean Temperature"}',NULL);

shpId = addIntersectGrid(scribe, lyrMeta['gridname'], lyrMeta['gridsides'], 
                           lyrMeta['gridsize'], lyrMeta['mapunits'], lyrMeta['epsg'], 
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