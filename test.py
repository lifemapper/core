import mx.DateTime
import os
from osgeo.ogr import wkbPolygon
import time
from types import IntType, FloatType

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, LMFormat, 
   ProcessType, JobStatus, MatrixType, SERVER_PIPELINE_HEADING, 
   SERVER_BOOM_HEADING, SERVER_SDM_MASK_HEADING_PREFIX, DEFAULT_MAPUNITS, DEFAULT_EPSG)
from LmCommon.common.readyfile import readyFilename
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource,
                                           TNCMetadata)
from LmDbServer.common.localconstants import (GBIF_TAXONOMY_FILENAME, 
                                              GBIF_PROVIDER_FILENAME)
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, LMFileType, ENV_DATA_PATH, 
         GPAM_KEYWORD, GGRIM_KEYWORD, ARCHIVE_KEYWORD, PUBLIC_ARCHIVE_NAME, 
         DEFAULT_EMAIL_POSTFIX, Priority, ProcessTool)
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmBackend.common.cmd import MfRule
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.scenario import Scenario, ScenPackage
from LmServer.legion.shapegrid import ShapeGrid
from LmDbServer.boom.initboom import BOOMFiller

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd

cfname='/state/partition1/lmscratch/temp/sax_biotaphy.ini'
cfname='/state/partition1/lmscratch/temp/heuchera_boom_params.ini'
cfname = '/state/partition1/lmscratch/temp/taiwan_boom_params.ini'
filler = BOOMFiller(configFname=cfname)
# filler.initializeInputs()

(filler.usr, filler.usrPath,
       filler.usrEmail,
       filler.archiveName,
       filler.priority,
       filler.scenPackageName,
       filler.modelScenCode,
       filler.prjScenCodeList,
       filler.dataSource,
       filler.occIdFname,
       filler.gbifFname,
       filler.idigFname,
       filler.idigOccSep,
       filler.bisonFname,
       filler.userOccFname,
       filler.userOccSep,   
       filler.minpoints,
       filler.algorithms,
       filler.assemblePams,
       filler.gridbbox,
       filler.cellsides,
       filler.cellsize,
       filler.gridname, 
       filler.intersectParams, 
       filler.maskAlg, 
       filler.treeFname, 
       filler.bghypFnames,
       filler.doComputePAMStats) = filler.readParamVals()
       
       
config = Config(siteFn=filler.inParamFname)
doMapBaseline = filler._getBoomOrDefault(config, 'MAP_BASELINE', defaultValue=1)
# filler._fillScenarios(doMapBaseline=doMapBaseline)

# Created by addArchive
filler.shapegrid = None

# If running as root, new user filespace must have permissions corrected
filler._warnPermissions()

earl = EarlJr()
filler.outConfigFilename = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                               objCode=filler.archiveName, 
                                               usr=filler.usr)

SPMETA, scenPackageMetaFilename, pkgMeta, elyrMeta = filler._pullClimatePackageMetadata()
masklyr = filler._createMaskLayer(SPMETA, pkgMeta, elyrMeta)

epsg = elyrMeta['epsg']
mapunits = elyrMeta['mapunits']
filler.scribe.log.info('  Read ScenPackage {} metadata ...'.format(filler.scenPackageName))
scenPkg = ScenPackage(filler.scenPackageName, filler.usr, 
                        epsgcode=epsg,
                        bbox=pkgMeta['bbox'],
                        mapunits=mapunits,
                        modTime=mx.DateTime.gmt().mjd)

lyrtypeMeta = SPMETA.LAYERTYPE_META
observedPredictedMeta = SPMETA.OBSERVED_PREDICTED_META
climKeywords = SPMETA.CLIMATE_KEYWORDS


baseMeta = observedPredictedMeta['baseline']
baseCode = baseMeta['code']
resolution = baseMeta['res']
region = baseMeta['region']
#    tm = baseMeta['times'].keys()[0]
basekeywords = [k for k in climKeywords]
basekeywords.extend(baseMeta['keywords'])
# there should only be one
dateCode = baseMeta['times'].keys()[0]
scencode = filler._getbioName(baseCode, resolution, suffix=pkgMeta['suffix'])

currtime = mx.DateTime.gmt().mjd
layers = []
staticLayers = {}
dateCode = baseMeta['times'].keys()[0]
resolution = baseMeta['res']
region = baseMeta['region']


for envcode in pkgMeta['layertypes']:
   print envcode
   ltmeta = lyrtypeMeta[envcode]
   envKeywords = [k for k in baseMeta['keywords']]
   relfname, isStatic = filler._findFileFor(ltmeta, baseMeta['code'], 
                                     gcm=None, tm=None, altPred=None)
   print relfname
   lyrname = filler._getbioName(baseMeta['code'], resolution, 
                              lyrtype=envcode, suffix=pkgMeta['suffix'])
   lyrmeta = {'title': ' '.join((baseMeta['code'], ltmeta['title'])),
              'description': ' '.join((baseMeta['code'], ltmeta['description']))}
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
                     resolution=resolution, 
                     bbox=region, 
                     modTime=currtime, 
                     envCode=envcode, 
                     dateCode=dateCode,
                     envMetadata=envmeta,
                     envModTime=currtime)
   layers.append(envlyr)
   if isStatic:
      staticLayers[envcode] = envlyr
# return layers, staticLayers

# lyrs, staticLayers = filler._getBaselineLayers(pkgMeta, baseMeta, elyrMeta, 
#                                         lyrtypeMeta)
scenmeta = {ServiceObject.META_TITLE: baseMeta['title'], 
            ServiceObject.META_AUTHOR: baseMeta['author'], 
            ServiceObject.META_DESCRIPTION: baseMeta['description'], 
            ServiceObject.META_KEYWORDS: basekeywords}
scen = Scenario(scencode, filler.usr, elyrMeta['epsg'], 
                metadata=scenmeta, 
                units=elyrMeta['mapunits'], 
                res=resolution, 
                dateCode=dateCode,
                bbox=region, 
                modTime=mx.DateTime.gmt().mjd,  
                layers=lyrs)
# return scen, staticLayers

# Current
# basescen, staticLayers = filler._createBaselineScenario(pkgMeta, elyrMeta, 
#                                                 SPMETA.LAYERTYPE_META,
#                                                 SPMETA.OBSERVED_PREDICTED_META,
#                                                 SPMETA.CLIMATE_KEYWORDS)
filler.scribe.log.info('     Assembled base scenario {}'.format(basescen.code))
scenPkg.addScenario(basescen)
# Predicted Past and Future
allScens = filler._createPredictedScenarios(pkgMeta, elyrMeta, 
                                     SPMETA.LAYERTYPE_META, staticLayers,
                                     SPMETA.OBSERVED_PREDICTED_META,
                                     SPMETA.CLIMATE_KEYWORDS)
filler.scribe.log.info('     Assembled predicted scenarios {}'.format(allScens.keys()))
for scen in allScens.values():
   scenPkg.addScenario(scen)

(filler.scenPkg, filler.modelScenCode, filler.epsg, filler.mapunits, 
 filler.scenPackageMetaFilename, masklyr) = filler._createScenarios()
filler.prjScenCodeList = filler.scenPkg.scenarios.keys()

if not doMapBaseline:
	filler.prjScenCodeList.remove(filler.modelScenCode)

filler.masklyr = masklyr

if filler.gridbbox is None:
	filler.gridbbox = filler.scenPkg.bbox
   
# ...............................................
def _findFileFor(ltmeta, scencode, gcm=None, tm=None, altPred=None):
   isStatic = False
   ltfiles = ltmeta['files']
   if len(ltfiles) == 1:
      isStatic = True
      relFname = ltfiles.keys()[0]
      if scencode in ltfiles[relFname]:
         return relFname, isStatic
   else:
      for relFname, kList in ltmeta['files'].iteritems():
         print scencode, relFname, kList
         if scencode in kList:
            return relFname, isStatic
         elif (gcm in kList and tm in kList and
               (altPred is None or altPred in kList)):
            return relFname, isStatic
   print('Failed to find layertype {} for scencode {}, gcm {}, altpred {}, time {}'
         .format(ltmeta['title'], scencode, gcm, altPred, tm))
   return None, None

                                               