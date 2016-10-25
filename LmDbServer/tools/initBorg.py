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
# CURRTIME = 57686
# ...............................................
def addUsers(scribe):
   """
   @summary Adds algorithms to the database from the algorithm dictionary
   """
   em = '{}@nowhere.com'.format(ARCHIVE_USER)
   defaultUser = LMUser(ARCHIVE_USER, em, em, modTime=CURRTIME)
   scribe.log.info('  Insert ARCHIVE_USER {} ...'.format(ARCHIVE_USER))
   usrid = scribe.insertUser(defaultUser)

   anonName = 'anon'
   anonEmail = '%s@nowhere.com' % anonName
   anonUser = LMUser(anonName, anonEmail, anonEmail, modTime=CURRTIME)
   scribe.log.info('  Insert anon {} ...'.format(anonName))
   usrid2 = scribe.insertUser(anonUser)

   return [usrid, usrid2]

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

# ...............................................
def addLayerTypes(scribe, lyrtypeMeta, usr): 
   etypes = [] 
   for typecode, typeinfo in lyrtypeMeta.iteritems():
      ltype = EnvironmentalType(typecode, typeinfo['title'], 
                                typeinfo['description'], usr, 
                                keywords=typeinfo['keywords'], 
                                modTime=CURRTIME)
      scribe.log.info('  Insert or get layertype {} ...'.format(typecode))
      etype = scribe.insertLayerTypeCode(ltype)
      etypes.append(etype)
   return etypes

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
   rstType = lyrMeta['gdaltype']
   
   for ltype, ltmeta in lyrtypeMeta.iteritems():
      relfname, isStatic = _findFileFor(ltmeta, pkgMeta['baseline'], 
                                        gcm=None, tm=None, altPred=None)
      lyrname = _getbioName(pkgMeta['baseline'], pkgMeta['res'], lyrtype=ltype, 
                            suffix=pkgMeta['suffix'])
      lyrtitle = _getbioName(pkgMeta['baseline'], pkgMeta['res'], lyrtype=ltype, 
                             suffix=pkgMeta['suffix'], isTitle=True)
      dloc = os.path.join(ENV_DATA_PATH, pkgMeta['topdir'], relfname)
      if not os.path.exists(dloc):
         print('Missing local data %s' % dloc)
      envlyr = EnvironmentalLayer(lyrname, 
               title=lyrtitle, 
               valUnits=ltmeta['valunits'],
               dlocation=dloc, 
               bbox=pkgMeta['bbox'], 
               gdalFormat=lyrMeta['gdalformat'], 
               gdalType=rstType,
               author=None, 
               mapunits=lyrMeta['mapunits'], 
               resolution=lyrMeta['resolution'], 
               epsgcode=lyrMeta['epsg'], 
               keywords=ltmeta['keywords'], 
               description=lyrtitle, 
               layerType=ltype, layerTypeTitle=ltmeta['title'], 
               layerTypeDescription=ltmeta['description'], 
               userId=usr, createTime=CURRTIME, modTime=CURRTIME)
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
                        obsOrPredRpt, tm, gcm=None, altpred=None):
   """
   @summary Assembles layer metadata for a single layerset
   """
   layers = []
   rstType = None
   layertypes = pkgMeta['layertypes']
   for ltype in layertypes:
      ltmeta = lyrtypeMeta[ltype]
      relfname, isStatic = _findFileFor(ltmeta, obsOrPredRpt, 
                                        gcm=gcm, tm=tm, altpred=altpred)
      if not isStatic:
         lyrname = _getbioName(obsOrPredRpt, pkgMeta['res'], gcm=gcm, tm=tm, 
                               altpred=altpred, lyrtype=ltype, 
                               suffix=pkgMeta['suffix'], isTitle=False)
         lyrtitle = _getbioName(obsOrPredRpt, pkgMeta['res'], gcm=gcm, tm=tm, 
                                altpred=altpred, lyrtype=ltype, 
                                suffix=pkgMeta['suffix'], isTitle=True)
         dloc = os.path.join(ENV_DATA_PATH, pkgMeta['topdir'], relfname)
         if not os.path.exists(dloc):
            print('Missing local data %s' % dloc)
            dloc = None
         envlyr = EnvironmentalLayer(lyrname, 
                  title=lyrtitle, 
                  valUnits=ltmeta['valunits'],
                  dlocation=dloc, 
                  bbox=pkgMeta['bbox'], 
                  gdalFormat=lyrMeta['gdalformat'], 
                  gdalType=rstType,
                  author=None, 
                  mapunits=lyrMeta['mapunits'], 
                  resolution=lyrMeta['resolution'], 
                  epsgcode=lyrMeta['epsg'], 
                  keywords=ltmeta['keywords'], 
                  description=lyrtitle, 
                  layerType=ltype, layerTypeTitle=ltmeta['title'], 
                  layerTypeDescription=ltmeta['description'], 
                  gcmCode=gcm, rcpCode=altpred, dateCode=tm,
                  userId=usr, createTime=CURRTIME, modTime=CURRTIME)
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
   tmcode = baseMeta['times'][tm]['shortcode']
   basekeywords = [k for k in META.ENV_KEYWORDS]
   basekeywords.extend(baseMeta['keywords'])
   
   scencode = _getbioName(obsKey, pkgMeta['res'], suffix=pkgMeta['suffix'])
   lyrs, staticLayers = _getBaselineLayers(usr, pkgMeta, baseMeta, lyrMeta, 
                                           lyrtypeMeta)
   scen = Scenario(scencode, 
            title=baseMeta['title'], 
            author=baseMeta['author'], 
            description=baseMeta['description'], 
            units=lyrMeta['mapunits'], 
            res=lyrMeta['resolution'], 
            bbox=pkgMeta['bbox'], 
            modTime=CURRTIME, keywords=basekeywords, 
            epsgcode=lyrMeta['epsg'], layers=lyrs, userId=usr)
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
            altShortcode = altvals['shortcode']
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
         scencode = _getbioName(predRpt, tmvals['shortcode'], pkgMeta['res'], 
                                gcm=mdlvals['shortcode'], altpred=altShortcode, 
                                suffix=pkgMeta['suffix'], isTitle=False)
         scentitle = _getbioName(META.OBSERVED_PREDICTED_META[predRpt]['name'], 
                                 tm, pkgMeta['res'], gcm=mdlvals['name'], 
                                 altpred=altpred, suffix=pkgMeta['suffix'], 
                                 isTitle=True)
         scendesc =  ' '.join(('Predicted climate calculated from',
             '{} and Worldclim 1.4 observed mean climate,'.format(scentitle),
             'plus static layers such as elevation and soils' ))
         lyrs = _getPredictedLayers(usr, scentitle, pkgMeta, lyrMeta, 
                                    lyrtypeMeta, staticLayers, predRpt, gcm, tm, 
                                    altpred=altpred)
         scen = Scenario(scencode, title=scentitle, author=mdlvals['author'], 
                         description=scendesc, 
                         startdt=tmvals['startdate'], enddt=tmvals['enddate'], 
                         units=lyrMeta['mapunits'], res=lyrMeta['resolution'], 
                         bbox=pkgMeta['bbox'], modTime=CURRTIME, 
                         keywords=scenkeywords, epsgcode=lyrMeta['epsg'],
                         layers=lyrs, userId=usr)
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
   unionScenarios = createPredictedScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
                                             staticLayers)
   msgs.append('Created predicted scenarios')
   # Join all sets and dictionaries
   unionScenarios[basescen.code] = basescen
   return unionScenarios, msgs
      
# ...............................................
def addScenarioPackageMetadata(scribe, usr, pkgMeta, lyrMeta, lyrtypeMeta, scenPkgName):
   """
   @summary Assemble climate, taxonomy metadata and add to database  
            lyrMeta = {'epsg': DEFAULT_EPSG, 
                       'mapunits': DEFAULT_MAPUNITS, 
                       'resolution': RESOLUTIONS[pkgMeta['res']], 
                       'gdaltype': ENVLYR_GDALTYPE, 
                       'gridname': DEFAULT_GRID_NAME, 
                       'gridsides': 4, 
                       'gridsize': DEFAULT_GRID_CELLSIZE}
   """
   # Add layertypes for this archive user
   ltIds = addLayerTypes(scribe, lyrtypeMeta, usr)   
   # Grid for GPAM
   shpId = addIntersectGrid(scribe, lyrMeta['gridname'], lyrMeta['gridsides'], 
                     lyrMeta['gridsize'], lyrMeta['mapunits'], lyrMeta['epsg'], 
                     pkgMeta['bbox'], usr)
   scens, msgs = createAllScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta)
   for msg in msgs:
      scribe.log.info(msg)
   for scode, scen in scens.iteritems():
      scribe.log.info('Insert scenario {}'.format(scode))
      scribe.insertScenario(scen)

# ...............................................
def _getClimateMeta(scenPkg):
   pkgMeta = META.CLIMATE_PACKAGES[scenPkg]
   lyrMeta = {'epsg': DEFAULT_EPSG, 
              'topdir': pkgMeta['topdir'],
              'mapunits': DEFAULT_MAPUNITS, 
              'resolution': META.RESOLUTIONS[pkgMeta['res']], 
              'gdaltype': META.ENVLYR_GDALTYPE, 
              'gdalformat': META.ENVLYR_GDALFORMAT,
#               'remoteurl': REMOTE_DATA_URL,
              'gridname': DEFAULT_GRID_NAME, 
              'gridsides': 4, 
              'gridsize': DEFAULT_GRID_CELLSIZE}
   return pkgMeta, lyrMeta

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

#    _importClimatePackageMetadata()

   try:
      taxSource = TAXONOMIC_SOURCE[DATASOURCE] 
   except:
      taxSource = None
   
   basefilename = os.path.basename(__file__)
   basename, ext = os.path.splitext(basefilename)
   try:
      logger = ScriptLogger(basename+'_borg')
      scribeWithBorg = BorgScribe(logger)
      success = scribeWithBorg.openConnections()

      if not success: 
         logger.critical('Failed to open database')
         exit(0)
      
      logger.info('  Insert user {} metadata ...'.format(ARCHIVE_USER))
      archiveUserId, anonUserId = addUsers(scribeWithBorg)
      
      if metaType in ('algorithm', 'all'):
         logger.info('  Insert algorithm metadata ...')
         aIds = addAlgorithms(scribeWithBorg)

      if metaType in ('climate', 'all'):
         logger.info('  Insert climate {} metadata ...'
                     .format(SCENARIO_PACKAGE))
         pkgMeta, lyrMeta = _getClimateMeta(SCENARIO_PACKAGE)
         addScenarioPackageMetadata(scribeWithBorg, ARCHIVE_USER, pkgMeta, lyrMeta, 
                                    META.LAYERTYPE_META, SCENARIO_PACKAGE)

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
from LmDbServer.tools.initBorg im
logger = ScriptLogger('testing')
scribe = BorgScribe(logger)
success = scribe.openConnections()

pkgMeta, lyrMeta = _getClimateMeta(SCENARIO_PACKAGE)

usr = ARCHIVE_USER
lyrtypeMeta = META.LAYERTYPE_META
scenPkgName = SCENARIO_PACKAGE
obsKey = pkgMeta['baseline']
baseMeta = META.OBSERVED_PREDICTED_META[obsKey]
tm = baseMeta['times'].keys()[0]
tmcode = baseMeta['times'][tm]['shortcode']

lyrs, staticLayers = _getBaselineLayers(usr, pkgMeta, baseMeta, lyrMeta, 
                                           lyrtypeMeta)

scens, msgs = createAllScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta)
scode = 'WC-10min'
scen = scens[scode]

newOrExistingScen = scribe._borg.findOrInsertScenario(scen)
scenid = newOrExistingScen.getId()
lyr = scen.layers[0]
for lyr in scen.layers:
   newOrExistingLyr = scribe._borg.findOrInsertEnvLayer(lyr, scenarioId=scenarioid)
"""