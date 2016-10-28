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
def addUsers(scribe, userList):
   """
   @summary Adds ARCHIVE_USER, anon user and USER from metadata to the database
   """
   users = []
   for userMeta in userList:
      if userMeta is not None:
         user = LMUser(userMeta['id'], userMeta['email'], userMeta['email'], modTime=CURRTIME)
         scribe.log.info('  Insert user {} ...'.format(userMeta['id']))
         newuser = scribe.insertUser(user)
         users.append(newuser)
   return users

# ...............................................
def addAlgorithms(scribe):
   """
   @summary Adds algorithms to the database from the algorithm dictionary
   """
   ids = []
   for algcode, algdict in ALGORITHM_DATA.iteritems():
      alg = Algorithm(algcode, name=algdict['name'])
      scribe.log.info('  Insert algorithm {} ...'.format(algcode))
      algid = scribe.insertAlgorithm(alg)
      ids.append(algid)
   return ids

# # ...............................................
# def addLayerTypes(scribe, lyrtypeMeta, usr): 
#    etypes = [] 
#    for typecode, typeinfo in lyrtypeMeta.iteritems():
#       ltype = EnvironmentalType(typecode, typeinfo['title'], 
#                                 typeinfo['description'], usr, 
#                                 keywords=typeinfo['keywords'], 
#                                 modTime=CURRTIME)
#       scribe.log.info('  Insert or get layertype {} ...'.format(typecode))
#       etype = scribe.insertLayerTypeCode(ltype)
#       etypes.append(etype)
#    return etypes

# ...............................................
def addIntersectGrid(scribe, gridname, cellsides, cellsize, mapunits, epsg, bbox, usr):
   shp = ShapeGrid(gridname, cellsides, cellsize, mapunits, epsg, bbox, userId=usr)
   newshp = scribe.insertShapeGrid(shp)
   scribe.log.info('Inserted, build shapegrid {} ...'.format(gridname))
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
   msgs = []
   # Current
   basescen, staticLayers = createBaselineScenario(usr, pkgMeta, lyrMeta, 
                                                   lyrtypeMeta)
   msgs.append('Created base scenario')
   # Predicted Past and Future
   allScenarios = createPredictedScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
                                             staticLayers)
   msgs.append('Created predicted scenarios')
   # Join all sets and dictionaries
   allScenarios[basescen.code] = basescen
   return allScenarios, msgs
      
# ...............................................
def addScenarioAndLayerMetadata(scribe, usr, scenarios):
   """
   @summary Add scenario and layer metadata to database  
   """
   for scode, scen in scenarios.iteritems():
      scribe.log.info('Insert scenario {}'.format(scode))
      newscen = scribe.insertScenario(scen)

# ...............................................
def _importClimatePackageMetadata():
   # Override the above imports if scenario metadata file exists
   metabasename = SCENARIO_PACKAGE+'.py'
   metafname = os.path.join(ENV_DATA_PATH, metabasename)
   # TODO: change on update python from 2.7 to 3.3+  
   try:
      import imp
      meta = imp.load_source('currentmetadata', metafname)
   except Exception, e:
      raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                    .format(metafname, e))

# ...............................................
if __name__ == '__main__':
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Initialize a new Lifemapper database with metadata ' +
                         'specific to the configured input data'))
   parser.add_argument('-m', '--metadata', default='all',
            choices=['algorithm', 'climate', 'taxonomy', 'all'], 
            help="Which metadata to catalog (algorithm, climate, taxonomy, all (default))")
   args = parser.parse_args()
   metaType = args.metadata
# .............................
   defUser = {'id': ARCHIVE_USER,
              'email': '{}@nowhere.org'.format(ARCHIVE_USER)}
   anonUser = {'id': DEFAULT_POST_USER,
               'email': '{}@nowhere.org'.format(DEFAULT_POST_USER)}
   try:
      newUser = META.USER
      currUserid = META.USER['id']
   except:
      newUser = None
      currUserid = ARCHIVE_USER
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

      logger.info('  Insert user metadata ...')
      thisUserid = addUsers(scribeWithBorg, [defUser, anonUser, newUser])
# .............................
      if metaType in ('algorithm', 'all'):
         logger.info('  Insert algorithm metadata ...')
         aIds = addAlgorithms(scribeWithBorg)
# .............................
      if metaType in ('climate', 'all'):
         logger.info('  Insert climate {} metadata ...'
                     .format(SCENARIO_PACKAGE))
         scens, msgs = createAllScenarios(currUserid, pkgMeta, lyrMeta, 
                                          META.LAYERTYPE_META)
         for msg in msgs:
            scribeWithBorg.log.info(msg)
         addScenarioAndLayerMetadata(scribeWithBorg, currUserid, scens)
# .............................
      if metaType in ('grid', 'all'):
            # Grid for GPAM
         shpId = addIntersectGrid(scribeWithBorg, lyrMeta['gridname'], lyrMeta['gridsides'], 
                           lyrMeta['gridsize'], lyrMeta['mapunits'], lyrMeta['epsg'], 
                           pkgMeta['bbox'], ARCHIVE_USER)
# .............................
      if metaType in ('taxonomy', 'all'):
         # Insert all taxonomic sources for now
         for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
            logger.info('  Insert taxonomy {} metadata ...'
                        .format(taxInfo['name']))
            taxSourceId = scribeWithBorg.insertTaxonomySource(taxInfo['name'],
                                                      taxInfo['url'])      
   except Exception, e:
      logger.error(str(e))
      raise
   finally:
      scribeWithBorg.closeConnections()
       
"""
import mx.DateTime

# TODO: These should be included in the package of data
import LmDbServer.tools.charlieMetaExp3 as META

from LmCommon.common.lmconstants import (DEFAULT_EPSG, 
         DEFAULT_MAPUNITS)

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
from LmDbServer.tools.initBorg import (_getBaselineLayers, _getClimateMeta, 
                                      _getbioName, _findFileFor, _getPredictedLayers)
logger = ScriptLogger('testing')
scribe = BorgScribe(logger)
success = scribe.openConnections()

pkgMeta, lyrMeta = _getClimateMeta(SCENARIO_PACKAGE)

usr = ARCHIVE_USER
lyrtypeMeta = META.LAYERTYPE_META
scenPkgName = SCENARIO_PACKAGE
obsOrPredRpt = pkgMeta['baseline']
baseMeta = META.OBSERVED_PREDICTED_META[obsOrPredRpt]
tm = baseMeta['times'].keys()[0]

ltype = lyrtypeMeta.keys()[0]
ltmeta = lyrtypeMeta[ltype]
relfname, isStatic = _findFileFor(ltmeta, pkgMeta['baseline'], 
                                  gcm=None, tm=None, altPred=None)
lyrname = _getbioName(pkgMeta['baseline'], pkgMeta['res'], lyrtype=ltype, 
                      suffix=pkgMeta['suffix'])
lyrmeta = {'title': ' '.join((pkgMeta['baseline'], ltmeta['title'])),
           'description': ' '.join((pkgMeta['baseline'], ltmeta['description']))}
envmeta = {'title': ltmeta['title'],
           'description': ltmeta['description'],
           'keywords': ltmeta['keywords']}
dloc = os.path.join(ENV_DATA_PATH, pkgMeta['topdir'], relfname)


scens, msgs = createAllScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta)
scode = 'observed-1km'
scen = scens[scode]

usrlist = addUsers(scribe)

newOrExistingScen = scribe._borg.findOrInsertScenario(scen)
scenid = newOrExistingScen.getId()
lyr = scen.layers[3]
nlyr = scribe._borg.findOrInsertEnvLayer(lyr, scenarioId=scenid)


def _getColumnValue(r, idxs, fldnameList):
   val = None
   for fldname in fldnameList:
      try: 
         val = r[idxs[fldname]]
      except:
         pass
      else:
         return val

                           
for lyr in scen.layers:
   print 'existing: ', lyr.name, lyr.getId()
   newOrExistingLyr = scribe._borg.findOrInsertEnvLayer(lyr, scenarioId=scenid)
   print '     new: ', newOrExistingLyr.name, newOrExistingLyr.getId()

"""