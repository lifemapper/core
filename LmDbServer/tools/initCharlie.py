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
import mx.DateTime as DT
import os

# TODO: These should be included in the package of data
import LmDbServer.tools.charlieMetaExp3 as META

from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmDbServer.common.localconstants import (SCENARIO_PACKAGE, 
         DEFAULT_GRID_NAME, DEFAULT_GRID_CELLSIZE)
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import ALGORITHM_DATA, ENV_DATA_PATH
from LmServer.common.localconstants import (ARCHIVE_USER, DATASOURCE)
from LmServer.common.log import ScriptLogger
from LmServer.common.lmuser import LMUser
from LmServer.db.scribe import Scribe
from LmServer.sdm.algorithm import Algorithm
from LmServer.sdm.envlayer import EnvironmentalType, EnvironmentalLayer                    
from LmServer.sdm.scenario import Scenario
from LmServer.rad.shapegrid import ShapeGrid


# ...............................................
def addUsers(scribe):
   """
   @summary Adds algorithms to the database from the algorithm dictionary
   """
   em = '%s@nowhere.com' % ARCHIVE_USER
   defaultUser = LMUser(ARCHIVE_USER, em, em, modTime=DT.gmt().mjd)
   scribe.log.info('  Insert ARCHIVE_USER {} ...'.format(ARCHIVE_USER))
   usrid = scribe.insertUser(defaultUser)

   anonName = 'anon'
   anonEmail = '%s@nowhere.com' % anonName
   anonUser = LMUser(anonName, anonEmail, anonEmail, modTime=DT.gmt().mjd)
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
   ids = [] 
   for typecode, typeinfo in lyrtypeMeta.iteritems():
      ltype = EnvironmentalType(typecode, typeinfo['title'], 
                                typeinfo['description'], usr, 
                                keywords=typeinfo['keywords'], 
                                modTime=DT.gmt().mjd)
      scribe.log.info('  Insert or get layertype {} ...'.format(typecode))
      etypeid = scribe.getOrInsertLayerTypeCode(ltype)
      ids.append(etypeid)
   return ids

# ...............................................
def addIntersectGrid(scribe, gridname, cellsides, cellsize, mapunits, epsg, bbox, usr):
   shp = ShapeGrid(gridname, cellsides, cellsize, mapunits, epsg, bbox, userId=usr)
   newshp = scribe.insertShapeGrid(shp)
   scribe.log.info('Inserted, build shapegrid {} ...'.format(gridname))
   newshp.buildShape()
   return newshp.getId()
   
# ...............................................
def _getbioName(basename, res, suffix=None, isTitle=False):
   sep = '-'
   if isTitle:
      sep = ', '
   name = sep.join((basename, res))
   if suffix:
      name = sep.join((name, suffix)) 
   return name

# ...............................................
def _getbioFname(lyrtype, rptcode=None, mdlcode=None, famcode=None, 
                 tmcode=None):
   ltcode = lyrtype.lower()
   beg = 'bio'
   elevation = 'alt'
   if ltcode == elevation:
      basename = ltcode
   else:
      bionum = ltcode[len(beg):]
      if rptcode.startswith('WC'):
         basename = '%s%d' % (beg, int(bionum))
         
      elif rptcode == 'AR5':
         basename = '%s%sbi%s%s' % (mdlcode, famcode, tmcode[2:], bionum)
         
      elif rptcode == 'CMIP5':
         basename = '%s%sbi%s' % (mdlcode, tmcode, bionum)
   fname = basename+'.tif'
   return fname 
 
# ...............................................
def _getBaselineLayers(usr, pkgMeta, baseMeta, lyrMeta, lyrtypeMeta):
   """
   @summary Assembles layer metadata for a single layerset
   """
   layers = []
   staticLayers = {}
   currtime = DT.gmt().mjd
   (starttime, endtime) = baseMeta['time']
   relativePath = os.path.join(pkgMeta['topdir'], baseMeta['directory'])
   scenpth = os.path.join(ENV_DATA_PATH, relativePath)
   rstType = lyrMeta['gdaltype']
   
   for ltype, ltvals in lyrtypeMeta.iteritems():
      fname = _getbioFname(ltype, rptcode=pkgMeta['present'])
      lyrname = _getbioName(os.path.splitext(fname)[0], pkgMeta['res'], suffix=pkgMeta['suffix'])
      lyrtitle = _getbioName('%s, %s' % (ltvals['title'], baseMeta['title']),
                             pkgMeta['res'], suffix=pkgMeta['suffix'],
                             isTitle=True)
      dloc = os.path.join(scenpth, fname)
      if not os.path.exists(dloc):
         raise LMError('Missing local data %s' % dloc)
      envlyr = EnvironmentalLayer(lyrname, 
                title=lyrtitle, 
                valUnits=ltvals['valunits'], 
                dlocation=dloc, 
                bbox=pkgMeta['bbox'],
                gdalFormat=lyrMeta['gdalformat'], 
                gdalType=rstType,
                startDate=starttime, endDate=endtime, 
                mapunits=lyrMeta['mapunits'], 
                resolution=lyrMeta['resolution'], 
                epsgcode=lyrMeta['epsg'], 
                keywords=ltvals['keywords'], 
                description='%s, %s' % (ltvals['description'], 
                                        baseMeta['description']), 
                layerType=ltype, 
                layerTypeTitle=ltvals['title'], 
                layerTypeDescription=ltvals['description'], 
                userId=usr, createTime=currtime, modTime=currtime)
      layers.append(envlyr)
      if ltype in baseMeta['staticLayerTypes']:
         staticLayers[ltype] = envlyr
   return layers, staticLayers

def _findFileFor(ltmeta, obsOrPred, gcm=None, tm=None, altPred=None):
   for relFname, kList in ltmeta['files'].iteritems():
      if obsOrPred in kList:
         if len(kList) == 1:
            return relFname
         
   
# ...............................................
def _getLayers(usr, scentitle, pkgMeta, lyrMeta, lyrtypeMeta, 
               obsOrPredRpt, tm, gcm=None, altpred=None):
   """
   @summary Assembles layer metadata for a single layerset
   lyrMeta = {'epsg': META.EPSG, 
              'topdir': pkgMeta['topdir'],
              'mapunits': META.MAPUNITS, 
              'resolution': META.RESOLUTIONS[pkgMeta['res']], 
              'gdaltype': META.ENVLYR_GDALTYPE, 
              'gdalformat': META.ENVLYR_GDALFORMAT,
              'gridname': DEFAULT_GRID_NAME, 
              'gridsides': 4, 
              'gridsize': DEFAULT_GRID_CELLSIZE}
   """
   currtime = DT.gmt().mjd
   layers = []
   rstType = None
   layertypes = pkgMeta['layertypes']
   for ltype in layertypes:
      ltmeta = lyrtypeMeta[ltype]
      relfname = _findFileFor(ltmeta, obsOrPredRpt, gcm=gcm, tm=tm, altpred=altpred)
      pth, basename = os.path.split(relfname)
      namebase, ext = os.path.splitext(basename)
      titlebase = '{}, {}, {}'.format(ltmeta['title'], obsOrPredRpt, tm)
      for desc in (gcm,  altpred):
         if desc: titlebase += ', {}'.format(desc)
      lyrname = _getbioName(namebase, pkgMeta['res'], suffix=pkgMeta['suffix'])
      lyrtitle = _getbioName(titlebase, pkgMeta['res'], suffix=pkgMeta['suffix'], 
                             isTitle=True)
      dloc = os.path.join(pkgMeta['topdir'], relfname)
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
               description='{} for {}'.format(ltmeta['description'],scentitle), 
               layerType=ltype, layerTypeTitle=ltmeta['title'], 
               layerTypeDescription=ltmeta['description'], 
               gcmCode=gcm, rcpCode=altpred, dateCode=tm,
               userId=usr, createTime=currtime, modTime=currtime)
      layers.append(envlyr)
   return layers

# ...............................................
def _getFutureLayers(usr, pkgMeta, lyrMeta, lyrtypeMeta, staticLayers, relativePath, scendesc, 
                     rpt, mdlvals, sfam, sfamvals, tm, tmvals):
   """
   @summary Assembles layer metadata for a single layerset
   """
   currtime = DT.gmt().mjd
   layers = []
   rstType = None
   scenpth = os.path.join(ENV_DATA_PATH, relativePath)
   for ltype, ltvals in lyrtypeMeta.iteritems():
      if ltype not in staticLayers.keys():
         fname = _getbioFname(ltype, rptcode=rpt, tmcode=tm, 
                  famcode=sfamvals['shortcode'], mdlcode=mdlvals['shortcode'])
         lyrname = _getbioName(os.path.splitext(fname)[0], pkgMeta['res'], suffix=pkgMeta['suffix'])
         lyrtitle = _getbioName('%s, IPCC %s %s, %s' % 
                                (ltvals['title'], rpt, sfam, tm), 
                                pkgMeta['res'], suffix=pkgMeta['suffix'], isTitle=True)
         dloc = os.path.join(scenpth, fname)
         if not os.path.exists(dloc):
            print('Missing local data %s' % dloc)
            dloc = None
         envlyr = EnvironmentalLayer(lyrname, 
                  title=lyrtitle, 
                  valUnits=ltvals['valunits'],
                  dlocation=dloc, 
                  bbox=pkgMeta['bbox'], 
                  gdalFormat=lyrMeta['gdalformat'], 
                  gdalType=rstType,
                  author=mdlvals['author'], 
                  startDate=tmvals['startdate'], 
                  endDate=tmvals['enddate'], 
                  mapunits=lyrMeta['mapunits'], 
                  resolution=lyrMeta['resolution'], 
                  epsgcode=lyrMeta['epsg'], 
                  keywords=ltvals['keywords'], 
                  description='%s, %s' % (ltvals['description'], scendesc), 
                  layerType=ltype, layerTypeTitle=ltvals['title'], 
                  layerTypeDescription=ltvals['description'], 
                  userId=usr, createTime=currtime, modTime=currtime)
         layers.append(envlyr)
   return layers
      
# ...............................................
def _getPastLayers(usr, pkgMeta, lyrMeta, lyrtypeMeta, staticLayers, 
                   relativePath, scendesc, rpt, mdlvals, tm, tmvals):
   currtime = DT.gmt().mjd
   layers = []
   scenpth = os.path.join(ENV_DATA_PATH, relativePath)
   rstType = lyrMeta['gdaltype']
   for ltype, ltvals in lyrtypeMeta.iteritems():
      if ltype not in staticLayers.keys():
         fname = _getbioFname(ltype, rptcode=rpt, mdlcode=mdlvals['shortcode'], tmcode=tm)
         lyrname = _getbioName(os.path.splitext(fname)[0], pkgMeta['res'], 
                               suffix=pkgMeta['suffix'])
         lyrtitle = _getbioName('%s, %s' % (ltvals['title'], tmvals['name']),
                                pkgMeta['res'], suffix=pkgMeta['suffix'], 
                                isTitle=True)
         dloc = os.path.join(scenpth, fname)
         if not os.path.exists(dloc):
            print('Missing local data %s' % dloc)
            dloc = None
         envlyr = EnvironmentalLayer(lyrname, 
                  title=lyrtitle, 
                  valUnits=ltvals['valunits'],
                  dlocation=dloc, 
                  bbox=pkgMeta['bbox'],
                  gdalFormat=lyrMeta['gdalformat'], 
                  gdalType=rstType,
                  author=mdlvals['author'], 
                  mapunits=lyrMeta['mapunits'], 
                  resolution=lyrMeta['resolution'], 
                  epsgcode=lyrMeta['epsg'], 
                  keywords=ltvals['keywords'], 
                  description='%s, %s' % (ltvals['description'], scendesc), 
                  layerType=ltype, 
                  layerTypeTitle=ltvals['title'], 
                  layerTypeDescription=ltvals['description'], 
                  userId=usr, createTime=currtime, modTime=currtime)
         layers.append(envlyr)
   return layers

# ...............................................
def createBaselineScenario(usr, pkgMeta, lyrMeta, lyrtypeMeta):
   """
   @summary Assemble Worldclim/bioclim scenario
   """
   baseMeta = META.OBSERVED_PREDICTED_META['Base']
   basekeywords = [k for k in META.CLIMATE_KEYWORDS]
   basekeywords.extend(baseMeta['keywords'])
   (starttime, endtime) = baseMeta['time']
   scencode = _getbioName(pkgMeta['present'], pkgMeta['res'], 
                          suffix=pkgMeta['suffix'])
   lyrs, staticLayers = _getBaselineLayers(usr, pkgMeta, 
                                                baseMeta, lyrMeta, lyrtypeMeta)
   scen = Scenario(scencode, 
            title=baseMeta['title'], 
            author=baseMeta['author'], 
            description=baseMeta['description'], 
            startdt=DT.DateTime(starttime).mjd, 
            enddt=DT.DateTime(endtime).mjd, 
            units=lyrMeta['mapunits'], 
            res=lyrMeta['resolution'], 
            bbox=pkgMeta['bbox'], 
            modTime=DT.gmt().mjd, keywords=basekeywords, 
            epsgcode=lyrMeta['epsg'], layers=lyrs, userId=usr)
   return scen, staticLayers

# ...............................................
def createPredictedScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta):
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
         if altvals:
            codebase = '{}-{}-{}'.format(
               mdlvals['shortcode'], tmvals['shortcode'], altvals['shortcode'])
            titlebase = '{}, {}, {}, {}'.format(
               META.OBSERVED_PREDICTED_META[predRpt]['name'], altpred, tm, pkgMeta['res'])
         else:
            codebase = '{}-{}'.format(mdlvals['shortcode'], tmvals['shortcode'])
            titlebase = '{}, {}, {}'.format(
               META.OBSERVED_PREDICTED_META[predRpt]['name'], tm, pkgMeta['res'])
         
         scencode = _getbioName(codebase, pkgMeta['res'], suffix=pkgMeta['suffix'])
         scentitle = _getbioName(titlebase, suffix=pkgMeta['suffix'],isTitle=True)
         scendesc =  ' '.join(('Predicted climate calculated from',
             '{} and Worldclim 1.4 observed mean climate,'.format(scentitle),
             'plus static layers such as elevation and soils' ))
         lyrs = _getLayers(usr, scentitle, pkgMeta, lyrMeta, lyrtypeMeta, 
                           predRpt, gcm, tm, altpred=altpred)
         scen = Scenario(scencode, title=scentitle, author=mdlvals['author'], 
                         description=scendesc, 
                         startdt=tmvals['startdate'], enddt=tmvals['enddate'], 
                         units=lyrMeta['mapunits'], res=lyrMeta['resolution'], 
                         bbox=pkgMeta['bbox'], modTime=DT.gmt().mjd, 
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
   # Past
   unionScenarios = createPredictedScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
                                        staticLayers)
   msgs.append('Created past scenarios')
   # Future
   futScenarios = createFutureScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
                                        staticLayers)
   msgs.append('Created future scenarios')
   # Join all sets and dictionaries
   unionScenarios[basescen.code] = basescen
   for k,v in futScenarios.iteritems():
      unionScenarios[k] = v
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
   lyrMeta = {'epsg': META.EPSG, 
              'topdir': pkgMeta['topdir'],
              'mapunits': META.MAPUNITS, 
              'resolution': META.RESOLUTIONS[pkgMeta['res']], 
              'gdaltype': META.ENVLYR_GDALTYPE, 
              'gdalformat': META.ENVLYR_GDALFORMAT,
#               'remoteurl': REMOTE_DATA_URL,
              'gridname': DEFAULT_GRID_NAME, 
              'gridsides': 4, 
              'gridsize': DEFAULT_GRID_CELLSIZE}
   return pkgMeta, lyrMeta

# # ...............................................
# def _importClimatePackageMetadata():
#    # Override the above imports if scenario metadata file exists
#    metabasename = SCENARIO_PACKAGE+'.py'
#    metafname = os.path.join(ENV_DATA_PATH, metabasename)
#    # TODO: change on update python from 2.7 to 3.3+  
#    try:
#       import imp
#       META = imp.load_source('currentmetadata', metafname)
#    except Exception, e:
#       raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
#                     .format(metafname, e))

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

#    # imports into a global variable/dictionary 'META'
#    _importClimatePackageMetadata()

   try:
      taxSource = TAXONOMIC_SOURCE[DATASOURCE] 
   except:
      taxSource = None
   
   basefilename = os.path.basename(__file__)
   basename, ext = os.path.splitext(basefilename)
   try:
      logger = ScriptLogger(basename)
      scribe = Scribe(logger)
      success = scribe.openConnections()

      if not success: 
         logger.critical('Failed to open database')
         exit(0)
      
      logger.info('  Insert user {} metadata ...'.format(ARCHIVE_USER))
      archiveUserId, anonUserId = addUsers(scribe)
      
      if metaType in ('algorithm', 'all'):
         logger.info('  Insert algorithm metadata ...')
         aIds = addAlgorithms(scribe)

      if metaType in ('climate', 'all'):
         logger.info('  Insert climate {} metadata ...'
                     .format(SCENARIO_PACKAGE))
         pkgMeta, lyrMeta = _getClimateMeta(SCENARIO_PACKAGE)
         addScenarioPackageMetadata(scribe, ARCHIVE_USER, pkgMeta, lyrMeta, 
                                    META.LAYERTYPE_DATA, SCENARIO_PACKAGE)

      if metaType in ('taxonomy', 'all'):
         # Insert all taxonomic sources for now
         for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
            logger.info('  Insert taxonomy {} metadata ...'
                        .format(taxInfo['name']))
            taxSourceId = scribe.insertTaxonomySource(taxInfo['name'],
                                                      taxInfo['url'])      
   except Exception, e:
      logger.error(str(e))
      raise
   finally:
      scribe.closeConnections()
       
"""
from LmDbServer.tools.initCatalog import *
from LmDbServer.tools.initCatalog import _getClimateMeta

logger = ScriptLogger('testing')
scribe = Scribe(logger)
success = scribe.openConnections()

pkgMeta, lyrMeta = _getClimateMeta(SCENARIO_PACKAGE)

usr = ARCHIVE_USER
lyrtypeMeta = META.LAYERTYPE_DATA
scenPkgName = SCENARIO_PACKAGE
scens, msgs = createAllScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta)
scode = 'WC-10min'
scen = scens[scode]

newOrExistingScen = scribe._borg.findOrInsertScenario(scen)
scenid = newOrExistingScen.getId()
lyr = scen.layers[0]
for lyr in scen.layers:
   newOrExistingLyr = scribe._borg.findOrInsertEnvLayer(lyr, scenarioId=scenarioid)
"""