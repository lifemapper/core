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
import mx.DateTime as DT
import os
from osgeo import ogr
import sys

from LmCommon.common.lmconstants import (DEFAULT_EPSG, 
         DEFAULT_MAPUNITS)

from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmDbServer.common.localconstants import (SCENARIO_PACKAGE, 
         DEFAULT_GRID_NAME, DEFAULT_GRID_CELLSIZE)
from LmDbServer.tools.bioclimMeta import (BASELINE_DATA, 
         CLIMATE_KEYWORDS, CLIMATE_PACKAGES, ENVLYR_GDALFORMAT, ENVLYR_GDALTYPE, 
         LAYERTYPE_DATA, REPORTS, RESOLUTIONS, TIME_PERIODS)

from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import ALGORITHM_DATA, ENV_DATA_PATH
from LmServer.common.localconstants import ARCHIVE_USER, DATA_PATH
from LmServer.common.log import ScriptLogger
from LmServer.common.lmuser import LMUser
from LmServer.db.scribe import Scribe
from LmServer.sdm.algorithm import Algorithm
from LmServer.sdm.envlayer import EnvironmentalType, EnvironmentalLayer                    
from LmServer.sdm.scenario import Scenario
from LmServer.rad.shapegrid import ShapeGrid


# ...............................................
def addDefaultUser(scribe):
   """
   @summary Adds algorithms to the database from the algorithm dictionary
   """
   em = '%s@nowhere.com' % ARCHIVE_USER
   defaultUser = LMUser(ARCHIVE_USER, em, em, modTime=DT.gmt().mjd)
   scribe.log.info('  Inserting ARCHIVE_USER {} ...'.format(ARCHIVE_USER))
   id = scribe.insertUser(defaultUser)
   return id

# ...............................................
def addAlgorithms(scribe):
   """
   @summary Adds algorithms to the database from the algorithm dictionary
   """
   ids = []
   for algcode, algdict in ALGORITHM_DATA.iteritems():
      alg = Algorithm(algcode, name=algdict['name'])
      scribe.log.info('  Inserting algorithm {} ...'.format(algcode))
      algid = scribe.insertAlgorithm(alg)
      ids.append(algid)
   return ids

# ...............................................
def addLayerTypes(scribe, layertypeData, usr): 
   ids = [] 
   for typecode, typeinfo in layertypeData.iteritems():
      ltype = EnvironmentalType(typecode, typeinfo['title'], 
                                typeinfo['description'], usr, 
                                keywords=typeinfo['keywords'], 
                                modTime=DT.gmt().mjd)
      scribe.log.info('  Inserting or getting layertype {} ...'.format(ltype))
      etypeid = scribe.getOrInsertLayerTypeCode(ltype)
      ids.append(etypeid)
   return ids

# ...............................................
def addIntersectGrid(scribe, gridname, cellsides, cellsize, mapunits, epsg, bbox, usr):
   shp = ShapeGrid(gridname, cellsides, cellsize, mapunits, epsg, bbox, userId=usr)
   # TODO: Insert a ShapeGrid job here (would also insert object), delete insert
   scribe.log.info('Inserting shapegrid {} ...'.format(gridname))
   newshp = scribe.insertShapeGrid(shp)
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
   lyrKeys = set()
   currtime = DT.gmt().mjd
   (starttime, endtime) = baseMeta['time']
   relativePath = os.path.join(pkgMeta['topdir'], baseMeta['directory'])

   scenpth = os.path.join(DATA_PATH, ENV_DATA_PATH, relativePath)
   rstType = lyrMeta['gdaltype']
   
   for ltype, ltvals in lyrtypeMeta.iteritems():
      fname = _getbioFname(ltype, rptcode=pkgMeta['present'])
      lyrname = _getbioName(os.path.splitext(fname)[0], pkgMeta['res'], suffix=pkgMeta['suffix'])
      lyrtitle = _getbioName('%s, %s' % (ltvals['title'], baseMeta['title']),
                             pkgMeta['res'], suffix=pkgMeta['suffix'],
                             isTitle=True)
      # TODO: this will be a locally unique identifier for seeding and
      #       identifying layers between lmcompute and lmserver
      lyrKeys.add(os.path.join(relativePath, fname))
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
   return layers, staticLayers, lyrKeys

# ...............................................
def writeRemoteDataPairs(remoteData, scenPkgName):
   if remoteData is not None:
      fname = os.path.join(DATA_PATH, ENV_DATA_PATH, '%s.csv' % scenPkgName)
      if os.path.exists(fname):
         os.remove(fname)
      f = open(fname, 'w')
      for remoteurl, relfilepath in remoteData.iteritems():
         f.write('%s, %s\n' % (remoteurl + '/GTiff', relfilepath))
      f.close()
   
# ...............................................
def writeRemoteDataList(localKeys, scenPkgName):
   if localKeys is not None:
      fname = os.path.join(DATA_PATH, ENV_DATA_PATH, '%s.txt' % scenPkgName)
      if os.path.exists(fname):
         os.remove(fname)
      f = open(fname, 'w')
      for relfilepath in localKeys:
         f.write('%s:%s\n' % (scenPkgName, relfilepath))
      f.close()

# ...............................................
def _getFutureLayers(usr, pkgMeta, lyrMeta, lyrtypeMeta, staticLayers, relativePath, scendesc, 
                     rpt, mdlvals, sfam, sfamvals, tm, tmvals):
   """
   @summary Assembles layer metadata for a single layerset
   """
   lyrKeys = set()
   currtime = DT.gmt().mjd
   layers = []
   rstType = None
   scenpth = os.path.join(DATA_PATH, ENV_DATA_PATH, relativePath)
   if lyrMeta['remoteurl'] is not None:
      rstType = lyrMeta['gdaltype']
      scenpth = '/'.join((lyrMeta['remoteurl'], relativePath))

   for ltype, ltvals in lyrtypeMeta.iteritems():
      if ltype not in staticLayers.keys():
         fname = _getbioFname(ltype, rptcode=rpt, tmcode=tm, 
                  famcode=sfamvals['shortcode'], mdlcode=mdlvals['shortcode'])
         lyrname = _getbioName(os.path.splitext(fname)[0], pkgMeta['res'], suffix=pkgMeta['suffix'])
         lyrtitle = _getbioName('%s, IPCC %s %s, %s' % 
                                (ltvals['title'], rpt, sfam, tm), 
                                pkgMeta['res'], suffix=pkgMeta['suffix'], isTitle=True)
         lyrKeys.add(os.path.join(relativePath, fname))
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
   return layers, lyrKeys
      
# ...............................................
def _getPastLayers(usr, pkgMeta, lyrMeta, lyrtypeMeta, staticLayers, 
                   relativePath, scendesc, rpt, mdlvals, tm, tmvals):
   lyrKeys = set()
   currtime = DT.gmt().mjd
   layers = []
   
   scenpth = os.path.join(DATA_PATH, ENV_DATA_PATH, relativePath)
   rstType = lyrMeta['gdaltype']

   for ltype, ltvals in lyrtypeMeta.iteritems():
      if ltype not in staticLayers.keys():
         fname = _getbioFname(ltype, rptcode=rpt, mdlcode=mdlvals['shortcode'], tmcode=tm)
         lyrname = _getbioName(os.path.splitext(fname)[0], pkgMeta['res'], 
                               suffix=pkgMeta['suffix'])
         lyrtitle = _getbioName('%s, %s' % (ltvals['title'], tmvals['name']),
                                pkgMeta['res'], suffix=pkgMeta['suffix'], 
                                isTitle=True)
         lyrKeys.add(os.path.join(relativePath, fname))
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
   return layers, lyrKeys

# ...............................................
def createBaselineScenario(usr, pkgMeta, lyrMeta, lyrtypeMeta):
   """
   @summary Assemble Worldclim/bioclim scenario
   """
   baseMeta = BASELINE_DATA[pkgMeta['present']]
   basekeywords = [k for k in CLIMATE_KEYWORDS]
   basekeywords.extend(baseMeta['keywords'])
   (starttime, endtime) = baseMeta['time']
   scencode = _getbioName(pkgMeta['present'], pkgMeta['res'], 
                          suffix=pkgMeta['suffix'])
   lyrs, staticLayers, lyrKeys = _getBaselineLayers(usr, pkgMeta, 
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
   return scen, lyrKeys, staticLayers

# ...............................................
def createFutureScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, staticLayers):
   """
   @summary Assemble predicted future scenarios defined by IPCC report
   """
   futScenarios = {}
   remoteLocs = {}
   futScens = pkgMeta['future']
   for rpt in futScens.keys():
      for (sfam, tm) in futScens[rpt]:
         mdlvals = REPORTS[rpt]['model']
         sfamvals = REPORTS[rpt]['scenarios'][sfam]
         tmvals = TIME_PERIODS[tm]
         # Reset keywords
         scenkeywords = [k for k in CLIMATE_KEYWORDS]
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
               % (mdlvals['name'], mdlvals['author'], REPORTS[rpt]['name'], sfam),
             'plus Worldclim 1.4 observed mean climate'))
         # Relative path to data
         relativePath = os.path.join(pkgMeta['topdir'], mdlvals['code'], 
                                     tm, sfam)            
         lyrs, lyrKeys = _getFutureLayers(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
                              staticLayers, relativePath, scendesc, rpt, mdlvals, 
                              sfam, sfamvals, tm, tmvals)
         lyrs.extend(stlyr for stlyr in staticLayers.values())
         scen = Scenario(scencode, title=scentitle, author=mdlvals['author'], 
                         description=scendesc, 
                         startdt=tmvals['startdate'], enddt=tmvals['enddate'], 
                         units=lyrMeta['mapunits'], res=lyrMeta['resolution'], 
                         bbox=pkgMeta['bbox'], modTime=DT.gmt().mjd, 
                         keywords=scenkeywords, epsgcode=lyrMeta['epsg'],
                         layers=lyrs, userId=usr)
         futScenarios[scencode] = scen
   return futScenarios, lyrKeys

# ...............................................
def createPastScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta, staticLayers):
   """
   @summary Assemble predicted past scenarios defined by CMIP5
   """
   pastScenarios = {}
   lyrKeys = set()
   pastScens = pkgMeta['past']
   for rpt in pastScens.keys():
      for tm in pastScens[rpt]:
         mdlvals = REPORTS[rpt]['model']
         tmvals = TIME_PERIODS[tm]
         # Reset keywords
         scenkeywords = [k for k in CLIMATE_KEYWORDS]
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
               % (mdlvals['name'], mdlvals['author'], REPORTS[rpt]['name']),
             'plus Worldclim 1.4 observed mean climate'))
         # Relative path to data
         relativePath = os.path.join(pkgMeta['topdir'], mdlvals['code'], tm)            
         lyrs, lyrKeys = _getPastLayers(usr, pkgMeta, lyrMeta, lyrtypeMeta, 
                                 staticLayers, relativePath, scendesc, 
                                 rpt, mdlvals, tm, tmvals)
         scen = Scenario(scencode, title=scentitle, author=mdlvals['author'], 
                         description=scendesc, 
                         units=lyrMeta['mapunits'], res=lyrMeta['resolution'], 
                         bbox=pkgMeta['bbox'], modTime=DT.gmt().mjd, 
                         keywords=scenkeywords, 
                         epsgcode=lyrMeta['epsg'], layers=lyrs, userId=usr)
         pastScenarios[scen.code] = scen
   return pastScenarios, lyrKeys

# ...............................................
def createAllScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta):
   """
   @summary Assemble current, predicted past, predicted future scenarios 
   """
   msgs = []
   # Current
   basescen, baseLyrKeys, staticLayers = createBaselineScenario(usr, pkgMeta, 
                                                         lyrMeta, lyrtypeMeta)
   msgs.append('Created base scenario with base layerkeys')
   # Past
   unionScenarios, unionLyrKeys = createPastScenarios(usr, pkgMeta, lyrMeta, 
                                                     lyrtypeMeta, staticLayers)
   msgs.append('Created past scenarios with past layerkeys'.format(
                                       len(unionScenarios), len(unionLyrKeys)))
   # Future
   futScenarios, futLyrKeys = createFutureScenarios(usr, pkgMeta, lyrMeta, 
                                                       lyrtypeMeta, staticLayers)
   msgs.append('Created future scenarios with future layerkeys'.format(
                                       len(futScenarios), len(futLyrKeys)))
   # Join all sets and dictionaries
   unionLyrKeys.union(baseLyrKeys, futLyrKeys)
   unionScenarios[basescen.code] = basescen
   for k,v in futScenarios.iteritems():
      unionScenarios[k] = v
      
   return unionScenarios, unionLyrKeys, msgs
      

# ...............................................
def addScenarioPackageMetadata(scribe, usr, pkgMeta, lyrMeta, lyrtypeMeta, scenPkgName):
   """
   @summary Assemble climate, taxonomy metadata and add to database  
            lyrMeta = {'epsg': DEFAULT_EPSG, 
                       'mapunits': DEFAULT_MAPUNITS, 
                       'resolution': RESOLUTIONS[pkgMeta['res']], 
                       'gdaltype': ENVLYR_GDALTYPE, 
                       'remoteurl': REMOTE_DATA_URL,
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

   # TODO: lyrKeys will be GUIDs, and stored with layer metadata
   scens, lyrKeys, msgs = createAllScenarios(usr, pkgMeta, lyrMeta, lyrtypeMeta)
   for msg in msgs:
      scribe.log.info()
    
   for scode, scen in scens.iteritems():
      scribe.log.info('Inserting scenario {}'.format(scode))
      scribe.insertScenario(scen)
   # LayerPairs for seeding LmCompute
   writeRemoteDataList(lyrKeys, scenPkgName)

# ...............................................
# def _readScenarioMeta(scenPkg):
#    # Metadata is packaged with data and should be uncompressed into the same location 
#    metafile = os.path.join(DATA_PATH, ENV_DATA_PATH, scenPkg+'.py')
#    f = open(metafile, 'r')
#    content = f.read()
#    f.close()
#    
#    eval(content)
      

# ...............................................
def _getClimateMeta(scenPkg):
#    pkgMeta = _readScenarioMeta(scenPkg)
   pkgMeta = CLIMATE_PACKAGES[scenPkg]
   lyrMeta = {'epsg': DEFAULT_EPSG, 
              'mapunits': DEFAULT_MAPUNITS, 
              'resolution': RESOLUTIONS[pkgMeta['res']], 
              'gdaltype': ENVLYR_GDALTYPE, 
              'gdalformat': ENVLYR_GDALFORMAT,
              'remoteurl': REMOTE_DATA_URL,
              'gridname': DEFAULT_GRID_NAME, 
              'gridsides': 4, 
              'gridsize': DEFAULT_GRID_CELLSIZE}
   return pkgMeta, lyrMeta

# ...............................................
def usage():
   output = """
   Usage:
      initCatalog [algorithms | scenario | taxonomy | user | all) 
   """
   print output

# ...............................................
if __name__ == '__main__':  
   if ARCHIVE_USER == 'bison':
      REMOTE_DATA_URL = 'http://notyeti'
      taxSource = TAXONOMIC_SOURCE['ITIS'] 
   elif ARCHIVE_USER == 'kubi':
      REMOTE_DATA_URL = None
      taxSource = TAXONOMIC_SOURCE['GBIF'] 
   elif ARCHIVE_USER == 'idigbio':
      REMOTE_DATA_URL = 'http://felix'
      taxSource = TAXONOMIC_SOURCE['GBIF'] 
   else:
      taxSource = None
      REMOTE_DATA_URL = None
   
   if len(sys.argv) != 2:
      usage()
      exit(0)
   
   action = sys.argv[1].lower()
   if action in ('algorithms', 'scenario', 'taxonomy', 'user', 'all'):
      try:
         logger = ScriptLogger('initCatalog')
         scribe = Scribe(logger)
         success = scribe.openConnections()
         if not success: 
            logger.critical('Failed to open database')
            exit(0)
         
         uId = addDefaultUser(scribe)
         
         if action == 'all':
            aIds = addAlgorithms(scribe)
            pkgMeta, lyrMeta = _getClimateMeta(SCENARIO_PACKAGE)
            addScenarioPackageMetadata(scribe, ARCHIVE_USER, pkgMeta, lyrMeta, 
                                       LAYERTYPE_DATA, SCENARIO_PACKAGE)
            if taxSource is not None:
               logger.info('  Inserting taxonomy source {} ...'.format(
                                                            taxSource['name']))
               taxSourceId = scribe.insertTaxonomySource(taxSource['name'],
                                                         taxSource['url'])
         
         elif action == 'algorithms':   
            aIds = addAlgorithms(scribe)
            
         elif action == 'scenario':
            pkgMeta, lyrMeta = _getClimateMeta(SCENARIO_PACKAGE)
            addScenarioPackageMetadata(scribe, ARCHIVE_USER, pkgMeta, lyrMeta, 
                                       LAYERTYPE_DATA, SCENARIO_PACKAGE)
            
         elif action == 'taxonomy':
            if taxSource is not None:
               logger.info('  Inserting taxonomy source {} ...'.format(
                                                            taxSource['name']))
               taxSourceId = scribe.insertTaxonomySource(taxSource['name'],
                                                         taxSource['url']) 
      finally:
         scribe.closeConnections()
       
