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

from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmDbServer.common.localconstants import (SCENARIO_PACKAGE, 
         DEFAULT_GRID_NAME, DEFAULT_GRID_CELLSIZE)
import LmDbServer.tools.bioclimMeta as meta
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import ALGORITHM_DATA, ENV_DATA_PATH
from LmServer.common.localconstants import (ARCHIVE_USER, DATASOURCE, 
                                            DEFAULT_EPSG, DEFAULT_MAPUNITS)
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
   baseMeta = meta.BASELINE_DATA[pkgMeta['present']]
   basekeywords = [k for k in meta.CLIMATE_KEYWORDS]
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
def createFutureScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, staticLayers):
   """
   @summary Assemble predicted future scenarios defined by IPCC report
   """
   futScenarios = {}
   futScens = pkgMeta['future']
   for rpt in futScens.keys():
      for (sfam, tm) in futScens[rpt]:
         mdlvals = meta.REPORTS[rpt]['model']
         sfamvals = meta.REPORTS[rpt]['scenarios'][sfam]
         tmvals = meta.TIME_PERIODS[tm]
         # Reset keywords
         scenkeywords = [k for k in meta.CLIMATE_KEYWORDS]
         for vals in (mdlvals, sfamvals, tmvals):
            try:
               scenkeywords.extend(vals['keywords'])
            except:
               pass
         # LM Scenario code, title, description
         scencode = _getbioName('%s-%s-%s' % (mdlvals['code'], sfam, tm), 
                                pkgMeta['res'], suffix=pkgMeta['suffix'])
         scentitle = _getbioName('%s, IPCC %s %s, %s' % 
                                 (mdlvals['code'], rpt, sfam, tmvals['name']),
                                 pkgMeta['res'], suffix=pkgMeta['suffix'],
                                 isTitle=True)
         scendesc =  ' '.join(
            ('Predicted %s climate calculated from' % (tmvals['name']),
             'change modeled by %s, %s for the %s, Scenario %s' 
               % (mdlvals['name'], mdlvals['author'], meta.REPORTS[rpt]['name'], sfam),
             'plus Worldclim 1.4 observed mean climate'))
         # Relative path to data
         relativePath = os.path.join(pkgMeta['topdir'], mdlvals['code'], 
                                     tm, sfam)            
         lyrs = _getFutureLayers(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
                                 staticLayers, relativePath, scendesc, rpt, 
                                 mdlvals, sfam, sfamvals, tm, tmvals)
         lyrs.extend(stlyr for stlyr in staticLayers.values())
         scen = Scenario(scencode, title=scentitle, author=mdlvals['author'], 
                         description=scendesc, 
                         startdt=tmvals['startdate'], enddt=tmvals['enddate'], 
                         units=lyrMeta['mapunits'], res=lyrMeta['resolution'], 
                         bbox=pkgMeta['bbox'], modTime=DT.gmt().mjd, 
                         keywords=scenkeywords, epsgcode=lyrMeta['epsg'],
                         layers=lyrs, userId=usr)
         futScenarios[scencode] = scen
   return futScenarios

# ...............................................
def createPastScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, staticLayers):
   """
   @summary Assemble predicted past scenarios defined by CMIP5
   """
   pastScenarios = {}
   pastScens = pkgMeta['past']
   for rpt in pastScens.keys():
      for tm in pastScens[rpt]:
         mdlvals = meta.REPORTS[rpt]['model']
         tmvals = meta.TIME_PERIODS[tm]
         # Reset keywords
         scenkeywords = [k for k in meta.CLIMATE_KEYWORDS]
         scenkeywords.extend(tmvals['keywords'])
         # LM Scenario code, title, description
         scencode = _getbioName('%s-%s' % (mdlvals['code'], tm),
                                pkgMeta['res'], suffix=pkgMeta['suffix'])
         scentitle = _getbioName('%s, %s, %s' % (mdlvals['code'], tmvals['name'], rpt),
                                 pkgMeta['res'], suffix=pkgMeta['suffix'],
                                 isTitle=True)
         scendesc =  ' '.join(
            ('Predicted %s climate calculated from' % (tmvals['name'].lower()),
             'change modeled by %s, %s for %s' 
               % (mdlvals['name'], mdlvals['author'], meta.REPORTS[rpt]['name']),
             'plus Worldclim 1.4 observed mean climate'))
         # Relative path to data
         relativePath = os.path.join(pkgMeta['topdir'], mdlvals['code'], tm)            
         lyrs = _getPastLayers(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
                               staticLayers, relativePath, scendesc, 
                               rpt, mdlvals, tm, tmvals)
         scen = Scenario(scencode, title=scentitle, author=mdlvals['author'], 
                         description=scendesc, 
                         units=lyrMeta['mapunits'], res=lyrMeta['resolution'], 
                         bbox=pkgMeta['bbox'], modTime=DT.gmt().mjd, 
                         keywords=scenkeywords, 
                         epsgcode=lyrMeta['epsg'], layers=lyrs, userId=usr)
         pastScenarios[scen.code] = scen
   return pastScenarios

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
   unionScenarios = createPastScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
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
   pkgMeta = meta.CLIMATE_PACKAGES[scenPkg]
   lyrMeta = {'epsg': DEFAULT_EPSG, 
              'topdir': pkgMeta['topdir'],
              'mapunits': DEFAULT_MAPUNITS, 
              'resolution': meta.RESOLUTIONS[pkgMeta['res']], 
              'gdaltype': meta.ENVLYR_GDALTYPE, 
              'gdalformat': meta.ENVLYR_GDALFORMAT,
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

   _importClimatePackageMetadata()

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
                                    meta.LAYERTYPE_DATA, SCENARIO_PACKAGE)

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
lyrtypeMeta = meta.LAYERTYPE_DATA
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