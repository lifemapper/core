"""
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
import inspect
from osgeo.gdalconst import (GDT_Unknown, GDT_Byte, GDT_UInt16, GDT_Int16, 
                     GDT_UInt32, GDT_Int32, GDT_Float32, GDT_Float64, 
                     GDT_CInt16, GDT_CInt32, GDT_CFloat32, GDT_CFloat64)
from osgeo.ogr import (wkbPoint, wkbLineString, wkbPolygon, wkbMultiPoint, 
                       wkbMultiLineString, wkbMultiPolygon)
   
import os.path
from types import IntType, FloatType,StringType

from LmCommon.common.lmconstants import (JobStatus, MatrixType, LMFormat, 
                                         ProcessType)
from LmServer.common.localconstants import (APP_PATH, CS_PORT, DATA_PATH, 
                        DEFAULT_EPSG, EXTRA_CS_OPTIONS, EXTRA_MAKEFLOW_OPTIONS, 
                        EXTRA_WORKER_FACTORY_OPTIONS, EXTRA_WORKER_OPTIONS, 
                        LM_DISK, MASTER_WORKER_PATH, MAX_WORKERS, PID_PATH, 
                        PUBLIC_FQDN, SCRATCH_PATH, SHARED_DATA_PATH, 
                        WEBSERVICES_ROOT, WORKER_PATH, SPECIES_DIR)

WEB_SERVICE_VERSION = 'v2'
API_PATH = 'api'
API_URL = '/'.join([WEBSERVICES_ROOT, API_PATH, WEB_SERVICE_VERSION])
OGC_SERVICE_URL = '/'.join([API_URL, 'ogc'])
DEFAULT_EMAIL_POSTFIX = '@nowhere.org'
BIN_PATH = os.path.join(APP_PATH, 'bin')
# Relative paths
USER_LAYER_DIR = 'Layers'
USER_MAKEFLOW_DIR = 'makeflow'
# On shared data directory (shared if lifemapper-compute is also installed)
ENV_DATA_PATH = os.path.join(SHARED_DATA_PATH,'layers')
ARCHIVE_PATH = os.path.join(SHARED_DATA_PATH,'archive')
# On shared sge directory
SHARED_SGE_PATH = os.path.join(LM_DISK, 'sge')
# On lmserver data directory
SPECIES_DATA_PATH = os.path.join(DATA_PATH, SPECIES_DIR)
TEST_DATA_PATH = os.path.join(DATA_PATH, 'test') 
IMAGE_PATH = os.path.join(DATA_PATH, 'image')
# On scratch disk
UPLOAD_PATH = os.path.join(SCRATCH_PATH, 'tmpUpload')
TEMP_PATH = os.path.join(SCRATCH_PATH, 'temp')
LOG_PATH = os.path.join(SCRATCH_PATH, 'log')
USER_LOG_PATH = os.path.join(LOG_PATH, 'users')
ERROR_LOG_PATH = os.path.join(LOG_PATH, 'errors')

CHERRYPY_CONFIG_FILE = os.path.join(APP_PATH,'config', 'cherrypy.conf')
MATT_DAEMON_PID_FILE = os.path.join(PID_PATH, 'mattDaemon.pid')

# CC Tools constants
CCTOOLS_BIN_PATH = os.path.join(APP_PATH, 'cctools', 'bin')
CATALOG_SERVER_BIN = os.path.join(CCTOOLS_BIN_PATH, 'catalog_server')
WORKER_FACTORY_BIN = os.path.join(CCTOOLS_BIN_PATH, 'work_queue_factory')
MAKEFLOW_BIN = os.path.join(CCTOOLS_BIN_PATH, 'makeflow')
MAKEFLOW_WORKSPACE = os.path.join(SCRATCH_PATH, 'makeflow')

# Catalog server
CS_PID_FILE = os.path.join(PID_PATH, 'catalog_server.pid')
CS_LOG_FILE = os.path.join(LOG_PATH, 'catalog_server.log')
CS_HISTORY_FILE = os.path.join(SCRATCH_PATH, 'catalog.history')

CS_OPTIONS = '-n {} -B {} -p {} -o {} -H {} {}'.format(PUBLIC_FQDN, 
                           CS_PID_FILE, CS_PORT, CS_LOG_FILE, CS_HISTORY_FILE, 
                           EXTRA_CS_OPTIONS)  

# Worker options
WORKER_OPTIONS = '-C {}:{} -s {} {}'.format(PUBLIC_FQDN, CS_PORT, WORKER_PATH, 
                                            EXTRA_WORKER_OPTIONS)

# Makeflow options
MAKEFLOW_OPTIONS = '-X {} -a -C {}:{} {}'.format(MASTER_WORKER_PATH, 
                                 PUBLIC_FQDN, CS_PORT, EXTRA_MAKEFLOW_OPTIONS) 

# Worker factory options
WORKER_FACTORY_OPTIONS = '-w {} -W {} -E "{}" -S {} {}'.format(MAX_WORKERS, 
                                 MAX_WORKERS, WORKER_OPTIONS, SHARED_SGE_PATH, 
                                 EXTRA_WORKER_FACTORY_OPTIONS)

# Remove old worker directories command
RM_OLD_WORKER_DIRS_CMD = 'rocks run host compute "rm -rf {}/worker-*"'.format(
                           WORKER_PATH)


DEFAULT_CONFIG = 'config'

# Depth of path for archive SDM experiment data - this is the number of levels 
# that the occurrencesetid associated with a model and its projections 
# is split into i.e. occurrencesetid = 123456789 --> path 000/123/456/789/
MODEL_DEPTH = 4

LM_SCHEMA = 'lm3'
LM_SCHEMA_BORG = 'lm_v3'
SALT = "4303e8f7129243fd42a57bcc18a19f5b"

# database name
DB_STORE = 'borg'

# Relative paths
# For LmCompute command construction by LmServer (for Makeflow)
SINGLE_SPECIES_SCRIPTS_DIR = 'LmCompute/tools/single'
MULTI_SPECIES_SCRIPTS_DIR = 'LmCompute/tools/multi'
COMMON_SCRIPTS_DIR = 'LmCompute/tools/common'
BOOM_SCRIPTS_DIR = 'LmDbServer/boom'
SERVER_SCRIPTS_DIR = 'LmServer/tools'

# ............................................................................
class DbUser:
    Map = 'mapuser'
    WebService = 'wsuser'
    Pipeline = 'sdlapp'
    Job = 'jobuser'
    Anon = 'anon'

# ............................................................................
class PrimaryEnvironment:
    TERRESTRIAL = 1
    MARINE = 2
   
# ............................................................................
class ReferenceType:
    SDMProjection = 102
    OccurrenceSet = 104
    MatrixColumn = 201
    ShapeGrid = 202
    Matrix = 301
    Gridset = 401
   
    @staticmethod
    def name(rt):
        attrs = inspect.getmembers(ReferenceType, lambda a:not(inspect.isroutine(a)))
        for att in attrs:
            if rt == att[1]:
                return att[0]
        return None 
        
    @staticmethod
    def sdmTypes():
        return [ReferenceType.OccurrenceSet, ReferenceType.SDMProjection]
        
    @staticmethod
    def isSDM(rtype):
        if rtype in ReferenceType.sdmTypes():
            return True
        return False
        
    @staticmethod
    def radTypes():
        return [ReferenceType.ShapeGrid, ReferenceType.MatrixColumn, 
                ReferenceType.Matrix, ReferenceType.Gridset]
        
    @staticmethod
    def isRAD(rtype):
        if rtype in ReferenceType.radTypes():
            return True
        return False
        
    @staticmethod
    def boomTypes():
        allTypes = ReferenceType.sdmTypes()
        allTypes.extend(ReferenceType.radTypes())
        return allTypes
        
    @staticmethod
    def isBOOM(rtype):
        if rtype in ReferenceType.boomTypes():
            return True
        return False
        
    @staticmethod
    def progressTypes():
        return [ReferenceType.OccurrenceSet, ReferenceType.SDMProjection, 
                ReferenceType.MatrixColumn, ReferenceType.Matrix]
        
    @staticmethod
    def statusTypes():
        tps = ReferenceType.progressTypes()
        tps.append(ReferenceType.ShapeGrid)
        return tps
   

from LmCommon.common.lmconstants import DWCNames
class OccurrenceFieldNames:
    LOCAL_ID = ['localid', 'localId', 'occkey']
    UUID = ['uuid']
    LONGITUDE = ['longitude', 'x', 'lon', 'long', 
                 DWCNames.DECIMAL_LONGITUDE['SHORT'], DWCNames.DECIMAL_LONGITUDE['FULL']]
    LATITUDE =  ['latitude', 'y', 'lat', DWCNames.DECIMAL_LATITUDE['SHORT'],
                 DWCNames.DECIMAL_LATITUDE['FULL']]
    DATANAME = ['canname', 'species']
    GEOMETRY_WKT = ['geomwkt']
   
# Web directories
# TODO: See how many of these are still in use.  They should probably be 
#          constants in LmWebServer if they are still needed
SESSION_DIR = 'sessions'
MAP_DIR = 'maps'

# Mapfile symbolizations
WEB_MODULES_DIR = 'LmWebServer'
WEB_DIR = os.path.join(WEB_MODULES_DIR, 'public_html')
PROJ_LIB = os.path.join(APP_PATH, 'share/proj/')
SYMBOL_FILENAME = os.path.join(APP_PATH, WEB_DIR, MAP_DIR, 'symbols.txt')
DEFAULT_POINT_COLOR = 'ff7f00'
DEFAULT_LINE_COLOR = 'ffffff'
DEFAULT_PROJECTION_PALETTE = 'red'
DEFAULT_ENVIRONMENTAL_PALETTE = 'gray'

DEFAULT_SRS = 'epsg:%s' % str(DEFAULT_EPSG)
DEFAULT_WCS_FORMAT = 'image/tiff' 
MAXENT_WCS_FORMAT = 'image/x-aaigrid'
DEFAULT_WMS_FORMAT = 'image/png'

# Map/data service parameters
MAP_TEMPLATE = 'template'
QUERY_TEMPLATE = 'query.html'
BLUE_MARBLE_IMAGE = 'BMNG.geotiff'
QUERY_TOLERANCE = 3
# POINT_SYMBOL and LINE_SYMBOL values are in symbols.txt, referenced in mapfiles
POINT_SYMBOL = 'filledcircle'
LINE_SYMBOL = 'line'
POLYGON_SYMBOL = None
POINT_SIZE = 5
LINE_SIZE = 3
# Outline Width
POLYGON_SIZE = 0.91

# Gridset
RAD_EXPERIMENT_DIR_PREFIX = 'RAD'

class MapPrefix:
    SDM = 'data'
    USER = 'usr'
    # Computed map products for bucket
    RAD = 'rad'
    ANC = 'anc'
    SCEN = 'scen'

class LMFileType:
    # User level
    OTHER_MAP = 1
    TMP_JSON = 2
    ANCILLARY_MAP = 3
    # ..............................
    # Single species
    ENVIRONMENTAL_LAYER = 101
    SCENARIO_MAP = 102
    # Occurrence level (SDM data are organized by OccurrenceSets)
    SDM_MAP = 110
    OCCURRENCE_FILE = 111
    OCCURRENCE_RAW_FILE = 112
    OCCURRENCE_LARGE_FILE = 114
    MODEL_REQUEST = 120
    MODEL_STATS = 121
    MODEL_RESULT = 122
    MODEL_ATT_RESULT = 123
    PROJECTION_REQUEST = 130
    PROJECTION_PACKAGE = 131
    PROJECTION_LAYER = 132
    # ..............................
    # Multi-species
    UNSPECIFIED_RAD = 200
    ATTR_MATRIX = 210    # not yet used
    SHAPEGRID = 201     # TODO: delete??
    PAM = 222
    GRIM = 223
    ANC_PAM = 224
    
    SUM_CALCS = 241
    SUM_SHAPE = 242
    CALCS = 243 # New rad calcs
    RAD_MAP = 250
    # RAD statistic matrices
    SITES_OBSERVED = 261
    SPECIES_OBSERVED = 262
    DIVERSITY_OBSERVED = 263
    SCHLUTER_OBSERVED = 264
    SPECIES_COV_OBSERVED = 265
    SITES_COV_OBSERVED = 266
    RANDOM_CALC = 267
    SITES_RANDOM = 268
    SPECIES_RANDOM = 269
    DIVERSITY_RANDOM = 270
    SCHLUTER_RANDOM = 271
    SPECIES_COV_RANDOM = 272
    SITES_COV_RANDOM = 273
       
    BIOGEO_HYPOTHESES = 322
    PADDED_PAM = 323
    MCPA_OUTPUTS = 324
    TREE = 325
   
    GRIDSET_PACKAGE = 401
    
    USER_LAYER = 510
    USER_SHAPEGRID = 511
    # TODO: delete?
    USER_ATTRIBUTE_MATRIX = 520
    USER_TREE = 530
    MF_DOCUMENT = 540
    BOOM_CONFIG = 550
   
    # ..............................
    @staticmethod
    def isSDM(rtype):
        if rtype in [LMFileType.SDM_MAP, 
                     LMFileType.OCCURRENCE_FILE, 
                     LMFileType.OCCURRENCE_RAW_FILE, 
                     LMFileType.OCCURRENCE_LARGE_FILE,
                     LMFileType.MODEL_REQUEST, LMFileType.MODEL_STATS, 
                     LMFileType.MODEL_RESULT, LMFileType.MODEL_ATT_RESULT, 
                     LMFileType.PROJECTION_REQUEST, LMFileType.PROJECTION_PACKAGE, 
                     LMFileType.PROJECTION_LAYER]:
            return True
        return False
   
    @staticmethod
    def isRAD(rtype):
        if rtype in [LMFileType.UNSPECIFIED_RAD, LMFileType.RAD_MAP,
                     LMFileType.ATTR_MATRIX, 
                     LMFileType.PAM, LMFileType.GRIM, 
                     LMFileType.SUM_CALCS, LMFileType.SUM_SHAPE, 
                     LMFileType.BIOGEO_HYPOTHESES, LMFileType.TREE,
                     LMFileType.CALCS, 
                     LMFileType.SITES_OBSERVED, LMFileType.SPECIES_OBSERVED,
                     LMFileType.DIVERSITY_OBSERVED, LMFileType.SCHLUTER_OBSERVED,
                     LMFileType.SPECIES_COV_OBSERVED, LMFileType.SITES_COV_OBSERVED,
                     LMFileType.RANDOM_CALC, LMFileType.SITES_RANDOM,
                     LMFileType.SPECIES_RANDOM, LMFileType.DIVERSITY_RANDOM,
                     LMFileType.SCHLUTER_RANDOM, LMFileType.SPECIES_COV_RANDOM,
                     LMFileType.SITES_COV_RANDOM, LMFileType.MCPA_OUTPUTS,
                     LMFileType.GRIDSET_PACKAGE, LMFileType.ANC_PAM]:
            return True
        return False

    @staticmethod
    def isUserSpace(rtype):
        if rtype in [LMFileType.USER_ATTRIBUTE_MATRIX, 
                     LMFileType.TREE, LMFileType.USER_TREE, 
                     LMFileType.BOOM_CONFIG, LMFileType.TMP_JSON]:
            return True
        return False
         
    @staticmethod
    def isUserLayer(rtype):
        if rtype in [LMFileType.ENVIRONMENTAL_LAYER, LMFileType.SHAPEGRID,
                     LMFileType.USER_LAYER, LMFileType.USER_SHAPEGRID]:
            return True
        return False

    @staticmethod
    def mapTypes():
        return [LMFileType.OTHER_MAP, LMFileType.SCENARIO_MAP, LMFileType.SDM_MAP, 
                LMFileType.RAD_MAP, LMFileType.ANCILLARY_MAP]
    @staticmethod
    def isMap(rtype):
        if rtype in LMFileType.mapTypes():
            return True
        return False
        
    @staticmethod
    def isMatrix(rtype):
        if rtype in [LMFileType.PAM, LMFileType.GRIM, LMFileType.BIOGEO_HYPOTHESES, 
                     LMFileType.TREE, LMFileType.PADDED_PAM, LMFileType.MCPA_OUTPUTS,
                     LMFileType.CALCS, 
                     LMFileType.SITES_OBSERVED, LMFileType.SPECIES_OBSERVED,
                     LMFileType.DIVERSITY_OBSERVED, LMFileType.SCHLUTER_OBSERVED,
                     LMFileType.SPECIES_COV_OBSERVED, LMFileType.SITES_COV_OBSERVED,
                     LMFileType.RANDOM_CALC, LMFileType.SITES_RANDOM,
                     LMFileType.SPECIES_RANDOM, LMFileType.DIVERSITY_RANDOM,
                     LMFileType.SCHLUTER_RANDOM, LMFileType.SPECIES_COV_RANDOM,
                     LMFileType.SITES_COV_RANDOM, LMFileType.ANC_PAM]:
            return True
        return False
   
    @staticmethod
    def getMatrixFiletype(mtype):
            if mtype in (MatrixType.PAM, MatrixType.ROLLING_PAM):
                return LMFileType.PAM
            elif mtype == MatrixType.ANC_PAM:
                return LMFileType.ANC_PAM
            elif mtype == MatrixType.GRIM:
                return LMFileType.GRIM
            elif mtype == MatrixType.BIOGEO_HYPOTHESES:
                return LMFileType.BIOGEO_HYPOTHESES
            #elif mtype == MatrixType.PADDED_PAM:
            #   return LMFileType.PADDED_PAM
            elif mtype == MatrixType.MCPA_OUTPUTS:
                return LMFileType.MCPA_OUTPUTS
            elif mtype == MatrixType.SITES_OBSERVED:
                return LMFileType.SITES_OBSERVED
            elif mtype == MatrixType.SPECIES_OBSERVED:
                return LMFileType.SPECIES_OBSERVED
            elif mtype == MatrixType.DIVERSITY_OBSERVED:
                return LMFileType.DIVERSITY_OBSERVED
            elif mtype == MatrixType.SCHLUTER_OBSERVED:
                return LMFileType.SCHLUTER_OBSERVED
            elif mtype == MatrixType.SPECIES_COV_OBSERVED:
                return LMFileType.SPECIES_COV_OBSERVED
            elif mtype == MatrixType.SITES_COV_OBSERVED:
                return LMFileType.SITES_COV_OBSERVED
            elif mtype == MatrixType.RANDOM_CALC:
                return LMFileType.RANDOM_CALC
            elif mtype == MatrixType.SITES_RANDOM:
                return LMFileType.SITES_RANDOM
            elif mtype == MatrixType.SPECIES_RANDOM:
                return LMFileType.SPECIES_RANDOM
            elif mtype == MatrixType.DIVERSITY_RANDOM:
                return LMFileType.DIVERSITY_RANDOM
            elif mtype == MatrixType.SCHLUTER_RANDOM:
                return LMFileType.SCHLUTER_RANDOM
            elif mtype == MatrixType.SPECIES_COV_RANDOM:
                return LMFileType.SPECIES_COV_RANDOM
            elif mtype == MatrixType.SITES_COV_RANDOM:
                return LMFileType.SITES_COV_RANDOM

NAME_SEPARATOR = '_'

# Mapfile layer name
OCC_NAME_PREFIX = 'occ'
GENERIC_LAYER_NAME_PREFIX = 'lyr'
   
OCC_PREFIX = 'pt'
PRJ_PREFIX = 'prj'
SPLOTCH_PREFIX = 'splotch'
PAMSUM_PREFIX = 'pamsum'

class FileFix:
    PREFIX = {LMFileType.ANCILLARY_MAP: MapPrefix.ANC,
              LMFileType.OTHER_MAP: MapPrefix.USER,
              LMFileType.SCENARIO_MAP: MapPrefix.SCEN,
              LMFileType.SDM_MAP: MapPrefix.SDM,
              LMFileType.RAD_MAP: MapPrefix.RAD,
              
              LMFileType.TMP_JSON: None,
              LMFileType.ENVIRONMENTAL_LAYER: None,
              LMFileType.OCCURRENCE_FILE: OCC_PREFIX,
              LMFileType.OCCURRENCE_RAW_FILE: OCC_PREFIX,
              LMFileType.OCCURRENCE_LARGE_FILE: 'big' + OCC_PREFIX,
              LMFileType.MODEL_REQUEST: 'modReq',
              LMFileType.MODEL_STATS: None,
              LMFileType.MODEL_RESULT: None,
              LMFileType.MODEL_ATT_RESULT: None,
              LMFileType.PROJECTION_REQUEST: 'projReq',
              LMFileType.PROJECTION_PACKAGE: PRJ_PREFIX,
              LMFileType.PROJECTION_LAYER: PRJ_PREFIX,
              
              LMFileType.SHAPEGRID: 'shpgrid',
              LMFileType.ATTR_MATRIX: 'attrMtx',
              LMFileType.PAM: 'pam',
              LMFileType.ANC_PAM: 'anc_pam',
              LMFileType.GRIM: 'grim',
              LMFileType.SUM_CALCS: PAMSUM_PREFIX,
              LMFileType.CALCS: 'calc', # TODO: Add to this?
              LMFileType.SUM_SHAPE: PAMSUM_PREFIX,
              # RAD calcs
              LMFileType.SITES_OBSERVED: 'sites', 
              LMFileType.SPECIES_OBSERVED: 'species',
              LMFileType.DIVERSITY_OBSERVED: 'diversity', 
              LMFileType.SCHLUTER_OBSERVED: 'schluter',
              LMFileType.SPECIES_COV_OBSERVED: 'speciesCovObs',
              LMFileType.SITES_COV_OBSERVED: 'siteCovObs',
              LMFileType.RANDOM_CALC: 'randCalc', 
              LMFileType.SITES_RANDOM: 'sitesRand',
              LMFileType.SPECIES_RANDOM: 'speciesRand', 
              LMFileType.DIVERSITY_RANDOM: 'diversityRand',
              LMFileType.SCHLUTER_RANDOM: 'schluterRand', 
              LMFileType.SPECIES_COV_RANDOM: 'speciesCovRand',
              LMFileType.SITES_COV_RANDOM: 'siteCovRand',             
    
              LMFileType.BOOM_CONFIG: None,
              LMFileType.UNSPECIFIED_RAD: None,
              LMFileType.USER_LAYER: GENERIC_LAYER_NAME_PREFIX,
              LMFileType.USER_SHAPEGRID: None,
              LMFileType.USER_ATTRIBUTE_MATRIX: 'attributes',
              LMFileType.USER_TREE: 'tree',
              LMFileType.MF_DOCUMENT: 'mf',
              LMFileType.BIOGEO_HYPOTHESES: 'biogeo',
              LMFileType.TREE: 'tree',
              LMFileType.PADDED_PAM: 'ppam',
              LMFileType.MCPA_OUTPUTS: 'mcpa',
              # MCPA
              LMFileType.GRIDSET_PACKAGE: 'gsPkg',
    }
    # Postfix
    EXTENSION = {LMFileType.ANCILLARY_MAP: LMFormat.MAP.ext,
                 LMFileType.OTHER_MAP: LMFormat.MAP.ext,
                 LMFileType.SCENARIO_MAP: LMFormat.MAP.ext,
                 LMFileType.SDM_MAP: LMFormat.MAP.ext,
                 LMFileType.RAD_MAP: LMFormat.MAP.ext,
              
                 LMFileType.TMP_JSON: LMFormat.JSON.ext,
                 LMFileType.ENVIRONMENTAL_LAYER: LMFormat.GTIFF.ext,
                 LMFileType.SCENARIO_MAP: LMFormat.MAP.ext,
                 LMFileType.SDM_MAP: LMFormat.MAP.ext,
                 LMFileType.OCCURRENCE_FILE: LMFormat.SHAPE.ext,
                 LMFileType.OCCURRENCE_RAW_FILE: LMFormat.CSV.ext,
                 LMFileType.OCCURRENCE_LARGE_FILE: LMFormat.SHAPE.ext,
                 LMFileType.MODEL_REQUEST: LMFormat.XML.ext,
                 LMFileType.MODEL_STATS: LMFormat.ZIP.ext,
                 LMFileType.MODEL_RESULT: LMFormat.XML.ext,
                 LMFileType.MODEL_ATT_RESULT: LMFormat.TXT.ext,
                 LMFileType.PROJECTION_REQUEST: LMFormat.XML.ext,
                 LMFileType.PROJECTION_PACKAGE: LMFormat.ZIP.ext,
                 LMFileType.PROJECTION_LAYER: LMFormat.GTIFF.ext,
                 
                 LMFileType.SHAPEGRID:  LMFormat.SHAPE.ext,
                 LMFileType.ATTR_MATRIX: LMFormat.NUMPY.ext,
                 LMFileType.PAM: LMFormat.MATRIX.ext,
                 LMFileType.ANC_PAM: LMFormat.MATRIX.ext,
                 LMFileType.GRIM: LMFormat.MATRIX.ext,
                 LMFileType.SUM_CALCS: LMFormat.PICKLE.ext,
                 LMFileType.SUM_SHAPE: LMFormat.SHAPE.ext,
                 # RAD calcs
                 LMFileType.SITES_OBSERVED: LMFormat.MATRIX.ext, 
                 LMFileType.SPECIES_OBSERVED: LMFormat.MATRIX.ext,
                 LMFileType.DIVERSITY_OBSERVED: LMFormat.MATRIX.ext, 
                 LMFileType.SCHLUTER_OBSERVED: LMFormat.MATRIX.ext,
                 LMFileType.SPECIES_COV_OBSERVED: LMFormat.MATRIX.ext,
                 LMFileType.SITES_COV_OBSERVED: LMFormat.MATRIX.ext,
                 LMFileType.RANDOM_CALC: LMFormat.MATRIX.ext, 
                 LMFileType.SITES_RANDOM: LMFormat.MATRIX.ext,
                 LMFileType.SPECIES_RANDOM: LMFormat.MATRIX.ext, 
                 LMFileType.DIVERSITY_RANDOM: LMFormat.MATRIX.ext,
                 LMFileType.SCHLUTER_RANDOM: LMFormat.MATRIX.ext, 
                 LMFileType.SPECIES_COV_RANDOM: LMFormat.MATRIX.ext,
                 LMFileType.SITES_COV_RANDOM: LMFormat.MATRIX.ext,
    
                 LMFileType.BOOM_CONFIG: LMFormat.CONFIG.ext,
                 LMFileType.UNSPECIFIED_RAD: None,
                 LMFileType.USER_LAYER: None,
                 LMFileType.USER_SHAPEGRID:  LMFormat.SHAPE.ext,
                 LMFileType.USER_ATTRIBUTE_MATRIX: LMFormat.NUMPY.ext,
                 LMFileType.USER_TREE: LMFormat.NEXUS.ext,
                 LMFileType.MF_DOCUMENT: LMFormat.MAKEFLOW.ext,
                 LMFileType.BIOGEO_HYPOTHESES: LMFormat.MATRIX.ext,
                 LMFileType.TREE: LMFormat.NEXUS.ext,
                 LMFileType.PADDED_PAM: LMFormat.MATRIX.ext,
                 LMFileType.MCPA_OUTPUTS: LMFormat.MATRIX.ext,
                 LMFileType.CALCS: LMFormat.MATRIX.ext,
                 
                 LMFileType.GRIDSET_PACKAGE: LMFormat.ZIP.ext
    }
    @staticmethod
    def getMaptypeFromName(prefix=None, ext=None):
        if prefix is not None:
            for ftype in LMFileType.mapTypes():
                if FileFix.PREFIX[ftype] == prefix:
                    return ftype
        elif ext is not None:
            for ftype in LMFileType.mapTypes():
                if FileFix.EXTENSION[ftype] == ext:
                    return ftype
        return None
   
NAME_SEPARATOR = '_'
   
# FIXME: change this
# Development desktops, debug users, and beta server names
DEBUG_USER_PREFIX = 'debug_'

HINT_PREFIX = 'hint'

# SPECIES LAYER STYLES FOR CHANGETHINKING
CT_SPECIES_KEYWORD = 'NatureServe'
CT_SPECIES_LAYER_STYLES  = {'blue':  '           SYMBOL \'hatch\'\n' +
                                     '           SIZE 5\n' +
                                     '           ANGLE 35\n' +
                                     '           WIDTH 0.91\n' +
                                     '           OUTLINECOLOR 0 0 0\n' +
                                     '           COLOR 90 116 232\n',
                            'green': '           SYMBOL \'line-horizontal\'\n' +
                                     '           WIDTH 0.91\n' + 
                                     '           OUTLINECOLOR 0 0 0\n' +
                                     '           COLOR 180 230 102\n',
                            'red':   '           SYMBOL \'hatch\'\n' +
                                     '           SIZE 5\n' +
                                     '           ANGLE 65\n' +
                                     '           WIDTH 0.91\n' +
                                     '           OUTLINECOLOR 0 0 0\n' +
                                     '           COLOR 190 16 32\n'}

# .............................................................................
# List of error statuses that can be recovered
RECOVERABLE_ERRORS = [
   JobStatus.DB_CREATE_ERROR,
   JobStatus.DB_DELETE_ERROR,
   JobStatus.DB_INSERT_ERROR,
   JobStatus.DB_READ_ERROR,
   JobStatus.DB_UPDATE_ERROR,
   JobStatus.IO_READ_ERROR,
   JobStatus.IO_WRITE_ERROR,
   JobStatus.IO_WAIT_ERROR
]

# ............................................................................
class Priority:
# ............................................................................
    """ 
    Constants to define the priority of a Job.
    """
    LOW = 0
    NORMAL = 1
    HIGH = 2
    OBSOLETE = 3
    ABSENT = 4
    REQUESTED = 5

# ............................................................................
# Change to enum.Enum with Python 3.4
# Corresponds to LmCommon individual constants MODELS_SERVICE, LAYERS_SERVICE, etc
class LMServiceType:
    TREES = 'tree'
    GRIDSETS = 'gridset'
    OCCURRENCES = 'occurrence'
    MATRIX_LAYERS = 'mtxlayers'
    ENVIRONMENTAL_LAYERS = 'envlayer'
    PROJECTIONS = 'sdmProject'
    SCENARIOS = 'scenario'
    SCEN_PACKAGES = 'scenpackage'
    SHAPEGRIDS = 'shapegrid'
    # Generic layers/layersets/matrices
    LAYERS = 'layer'
    MATRICES = 'matrix'
    MATRIX_COLUMNS = 'column'
   
# Archive and Global PAM descriptions
GPAM_KEYWORD = 'Global PAM'
GGRIM_KEYWORD = 'Scenario GRIM'
PUBLIC_ARCHIVE_NAME = 'BOOM_Archive'
ARCHIVE_KEYWORD = 'archive'
   
# ............................................................................
# Lifemapper RAD constants
ID_PLACEHOLDER = '#id#'

# # TODO: remove these, use LmCommon.common.lmconstants.LMFormat
# GDALFormatCodes = {'AAIGrid': {'FILE_EXT': LMFormat.ASCII.ext, 
#                                'DECIMAL_PRECISION': 6, 
#                                'FORCE_CELLSIZE':'YES'},
#                    'GTiff': {'FILE_EXT': LMFormat.GTIFF.ext},
#                    'HFA': {'FILE_EXT': LMFormat.HFA.ext}
#                    }
# DEFAULT_PROJECTION_FORMAT = LMFormat.GTIFF.driver
GDALDataTypes = (GDT_Unknown, GDT_Byte, GDT_UInt16, GDT_Int16, 
                 GDT_UInt32, GDT_Int32, GDT_Float32, GDT_Float64, 
                 GDT_CInt16, GDT_CInt32, GDT_CFloat32, GDT_CFloat64)
# # TODO: remove these, use LmCommon.common.lmconstants.LMFormat
# # OGR string constants supported here, and associated file extensions
# OGRFormatCodes = {'CSV': {'FILE_EXT': LMFormat.CSV.ext},
#                   'ESRI Shapefile': {'FILE_EXT': LMFormat.SHAPE.ext}
#                   }
# TODO: delete
OGRFormats = {'CSV': '.csv', 
              'ESRI Shapefile': '.shp'
              }
OGRDataTypes = (wkbPoint, wkbLineString, wkbPolygon, 
                wkbMultiPoint, wkbMultiLineString, wkbMultiPolygon)


MAP_KEY = 'map'
WMS_LAYER_KEY = 'layers'
WCS_LAYER_KEY = 'coverage'

# Log file constants
# Log parameters
# Log format string, each section is separated by a space
LOG_FORMAT = ' '.join(["%(asctime)s",
                       "%(threadName)s.%(module)s.%(funcName)s",
                       "line",
                       "%(lineno)d",
                       "%(levelname)-8s",
                       "%(message)s"])
# Date format for log dates
LOG_DATE_FORMAT = '%d %b %Y %H:%M'
# Maximum log file size before new log file is started
LOGFILE_MAX_BYTES = 52000000 
# The number of backups to keep.  as the log file approaches MAX_BYTES in size
#    it will be renamed and a new log file will be created.  The renamed file
#    will have the same name with a number appended (.1 - .BACKUP_COUNT).
#    When the maximum number of backups has been met, the oldest will be 
#    discarded.
LOGFILE_BACKUP_COUNT = 5

# ...............................................
# String escapes
#TODO: Complete these lists, they may not include everything they need to
JSON_ESCAPES = [
                ["\n", "None"],
                ["\N", "None"],
                ["\t", "   "],
                ["\T", "   "],
                ["\"", "'"]
               ]

HTML_ESCAPES = [
                ["&", "&amp;"],
                ["\n", "<br />"],
                ["\t", "&nbsp;&nbsp;&nbsp;"],
                ["\"", "&quot;"]
               ]

SQL_ESCAPES = []

XML_ESCAPES = [
               ["&", "&amp;"],
               ["<", "&lt;"],
               [">", "&gt;"],
               ["\"", "&quot;"]
              ]

STRING_ESCAPE_FORMATS = {
                           "html" : HTML_ESCAPES,
                           "json" : JSON_ESCAPES,
                           "sql"  : SQL_ESCAPES,
                           "xml"  : XML_ESCAPES
                        }



# ...............................................
# Algorithm constants
BIOCLIM_PARAMS = {'StandardDeviationCutoff':  {'type': FloatType,
                                               'min': 0.0, 'default': 0.674, 
                                               'max': None} }
CSMBS_PARAMS = {'Randomisations': 
                  {'type': IntType,
                   'min': 1, 'default': 8, 'max': 1000},
                'StandardDeviations':
                  {'type': FloatType,
                   'min': -10.0, 'default': 2.0, 'max': 10.0},
                'MinComponents': 
                  {'type': IntType,
                   'min': 1, 'default': 1, 'max': 20},
                'VerboseDebugging': 
                  {'type': IntType,
                   'min': 0, 'default': 0, 'max': 1}}
ENVDIST_PARAMS = {'DistanceType':
                     {'type': IntType,
                      'min': 1, 'default': 1, 'max': 4 },
                  'NearestPoints':
                     {'type': IntType,
                      'min': 0, 'default': 1, 'max': None },
                  'MaxDistance':
                     {'type': FloatType,
                      'min': 0.1, 'default': 0.1, 'max': 1.0 }}

GARP_PARAMS =  {'MaxGenerations':
                {'type': IntType,
                 'min': 1, 'default': 400, 'max': None },
                'ConvergenceLimit':
                {'type': FloatType,
                 'min': 0.0, 'default': 0.01, 'max': 1.0 },
                'PopulationSize':
                {'type': IntType,
                 'min': 1, 'default': 50, 'max': 500 },
                'Resamples':
                {'type': IntType,
                 'min': 1, 'default': 2500, 'max': 100000 }}

GARP_BS_PARAMS = {'TrainingProportion': 
                   {'type': FloatType,
                    'min': 0, 'default': 50, 'max': 100 },
                  'TotalRuns': 
                   {'type': IntType,
                    'min': 0, 'default': 20, 'max': 10000 },
                  'HardOmissionThreshold': 
                   {'type': FloatType,
                    'min': 0, 'default': 100, 'max': 100 },
                  'ModelsUnderOmissionThreshold': 
                   {'type': IntType,
                    'min': 0, 'default': 20, 'max': 10000 },
                  'CommissionThreshold': 
                   {'type': FloatType,
                    'min': 0, 'default': 50, 'max': 100 },
                  'CommissionSampleSize': 
                   {'type': IntType,
                    'min': 1, 'default': 10000, 'max': None },
                  'MaxThreads': 
                   {'type': IntType,
                    'min': 1, 'default': 1, 'max': 1024 },
                  'MaxGenerations': 
                   {'type': IntType,
                    'min': 1, 'default': 400, 'max': None },
                  'ConvergenceLimit': 
                   {'type': FloatType,
                    'min': 0.0, 'default': 0.1, 'max': 1.0 },
                  'PopulationSize': 
                   {'type': IntType,
                    'min': 1, 'default': 50, 'max': 500 },
                  'Resamples': 
                   {'type': IntType,
                    'min': 1, 'default': 2500, 'max': 10000 } }

OM_MAXENT_PARAMS = {"NumberOfBackgroundPoints":
                      {'type' : IntType,
                       'min': 0,
                       'default': 10000,
                       'max': 10000 },
                    'UseAbsencesAsBackground':
                      {'min': 0,
                       'max': 1,
                       'type': IntType,
                       'default': 0},
                    "IncludePresencePointsInBackground":
                      {'min': 0,
                       'max': 1,
                       'type': IntType,
                       'default': 1},
                    "NumberOfIterations":
                      {'min': 1,
                       'max': None,
                       'type': IntType,
                       'default': 500},
                    "TerminateTolerance":
                      {'min': 0,
                       'max': None,
                       'type': FloatType,
                       'default': 0.00001},
                    "OutputFormat":
                      {'min': 1,
                       'max': 2,
                       'type': IntType,
                       'default': 2},
                    "QuadraticFeatures":
                      {'min': 0,
                       'max': 1,
                       'type': IntType,
                       'default': 1},
                    "ProductFeatures":
                      {'min': 0,
                       'max': 1,
                       'type': IntType,
                       'default': 1},
                    "HingeFeatures":
                      {'min': 0,
                       'max': 1,
                       'type': IntType,
                       'default': 1},
                    "ThresholdFeatures":
                      {'min': 0,
                       'max': 1,
                       'type': IntType,
                       'default': 1},
                    "AutoFeatures":
                      {'min': 0,
                       'max': 1,
                       'type': IntType,
                       'default': 1},
                    "MinSamplesForProductThreshold":
                      {'min': 1,
                       'max': None,
                       'type': IntType,
                       'default': 80},
                    "MinSamplesForQuadratic":
                      {'min': 1,
                       'max': None,
                       'type': IntType,
                       'default': 10},
                    "MinSamplesForHinge":
                      {'min': 1,
                       'max': None,
                       'type': IntType,
                       'default': 15} } 
ATT_MAXENT_PARAMS = {'responsecurves': 
                         {'type': IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'pictures': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'jackknife': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'outputformat': # 0 - raw, 1 - logistic, 2 - cumulative, 3 - cloglog
                         {'type' : IntType, 'min' : 0, 'max' : 3, 'default' : 3},
                     'randomseed': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'logscale': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'removeduplicates': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'writeclampgrid': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'writemess':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'randomtestpoints':
                         {'type' : IntType, 'min' : 0, 'max' : 100, 'default' : 0},
                     'betamultiplier':
                         {'type' : FloatType, 'min' : 0, 'max' : None, 'default' : 1.0},
                     'maximumbackground':
                         {'type' : IntType, 'min' : 0, 'max' : None, 'default' : 10000},
                     'replicates':
                         {'type' : IntType, 'min' : 1, 'max' : None, 'default' : 1},
                     'replicatetype': # 0 - cross validate, 1 - bootstrap, 2 - subsample
                         {'type' : IntType, 'min' : 0, 'max' : 2, 'default' : 0},
                     'perspeciesresults':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'writebackgroundpredictions':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'responsecurvesexponent':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'linear':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'quadratic':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'product':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'threshold':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'hinge':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'writeplotdata':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'fadebyclamping':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'extrapolate':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'autofeature':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'doclamp':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'outputgrids':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'plots':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'appendtoresultsfile':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'maximumiterations':
                         {'type' : IntType, 'min' : 0, 'max' : None, 'default' : 500},
                     'convergencethreshold':
                         {'type' : FloatType, 'min' : 0, 'max' : None, 'default' : 0.00001},
                     'adjustsampleradius':
                         {'type' : IntType, 'min' : 0, 'max' : None, 'default' : 0},
                     'lq2lqptthreshold':
                         {'type' : IntType, 'min' : 0, 'max' : None, 'default' : 80},
                     'l2lqthreshold':
                         {'type' : IntType, 'min' : 0, 'max' : None, 'default' : 10},
                     'hingethreshold':
                         {'type' : IntType, 'min' : 0, 'max' : None, 'default' : 15},
                     'beta_threshold':
                         {'type' : FloatType, 'min' : None, 'max' : None, 'default' : -1.0},
                     'beta_categorical':
                         {'type' : FloatType, 'min' : None,'max' : None, 'default' : -1.0},
                     'beta_lqp':
                         {'type' : FloatType, 'min' : None, 'max' : None, 'default' : -1.0},
                     'beta_hinge':
                         {'type' : FloatType, 'min' : None, 'max' : None, 'default' : -1.0},
                     'defaultprevalence':
                         {'type' : FloatType, 'min' : 0.0, 'max' : 1.0, 'default' : 0.5},
                     'addallsamplestobackground':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'addsamplestobackground':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'allowpartialdata':
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'applythresholdrule':
                         # 0 - None
                         # 1 - Fixed cumulative value 1
                         # 2 - Fixed cumulative value 5
                         # 3 - Fixed cumulative value 10
                         # 4 - Minimum training presence
                         # 5 - 10 percentile training presence
                         # 6 - Equal training sensitivity and specificity
                         # 7 - Maximum training sensitivity plus specificity
                         # 8 - Equal test sensitivity and specificity
                         # 9 - Maximum test sensitivity plus specificity
                         # 10 - Equate entropy of thresholded and origial distributions
                         {'type' : IntType, 'min' : 0, 'max' : 10, 'default' : 0},
                     'verbose': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0}
                     # Disabled
                      # askoverwrite - not needed for us
                      # autorun - we need this to always be on
                      # logfile - The default (maxent.log) is fine for us
                      # outputfiletype - We rely on this being set to ASCII grids
                      # prefixes - not needed for us
                      # skipifexists - not needed for us
                      # visible - We don't want the GUI enabled
                      # warnings - Would produce pop-ups and we don't have GUI
                      # togglelayerselected - We don't need
                      # togglespeciesselected - We don't need (for now)
                      # tooltips - We don't have the GUI for it
               
                     # Hidden
                      # cache - generate mxe files or not
                      # threads - How many threads can maxent use
                      # togglelayertype - For specifiying categorical layers
                      # nodata - For SWD samples               
                      # biasfile - Specifies relative sampling effort (grid)
                      # testsamplesfile - Specifies presence locations for testing (points)
                     }
SVM_PARAMS = {'svmtype': {'type': IntType,
                           'min': 0, 'default': 0, 'max': 2},
              'KernelType': {'type': IntType,
                              'min': 0, 'default': 2, 'max': 4},
              'Degree': {'type': IntType,
                          'min': 0, 'default': 3, 'max': None},
              'Gamma': {'type': FloatType,
                         'min': None, 'default': 0.0, 
                         'max': None},
              'Coef0': {'type': FloatType,
                         'min': None, 'default': 0.0, 
                         'max': None},
              # Cost
              'C': {'type': FloatType,
                        'min': 0.001, 'default': 1.0, 
                        'max': None},
              'Nu': {'type': FloatType, 
                      'min': 0.001, 'default': 0.5, 'max': 1},
              'ProbabilisticOutput': {'type': IntType,
                                       'min': 0, 'default': 1, 
                                       'max': 1 },
              'NumberOfPseudoAbsences': {'type': IntType,
                                          'min': 0, 
                                          'default': 0, 
                                          'max': None } } 
ANN_PARAMS = {'HiddenLayerNeurons':
                {'type': IntType,
                 'min': 0, 'default': 8, 'max': None },
              'LearningRate':
                {'type': FloatType,
                 'min': 0.0, 'default': 0.3, 'max': 1.0 },
              'Momentum':
                {'type': FloatType,
                 'min': 0.0, 'default': 0.05, 'max': 1.0 },
              'Choice':
                {'type': IntType,
                 'min': 0, 'default': 0, 'max': 1 },
              'Epoch':
                {'type': IntType,
                 'min': 1, 'default': 100000, 'max': None },
              'MinimumError':
                {'type': FloatType,
                 'min': 0.0, 'default': 0.01, 'max': 0.05} }

AQUAMAPS_PARAMS = {'UseSurfaceLayers':
                      {'type': IntType,
                       'min': -1, 'default': -1, 'max': 1 },
                   'UseDepthRange':
                      {'type': IntType,
                       'min': 0,  'default': 1, 'max': 1 },
                   'UseIceConcentration':
                      {'type': IntType,
                       'min': 0, 'default': 1, 'max': 1 },
                   'UseDistanceToLand':
                      {'type': IntType,
                       'min': 0, 'default': 1, 'max': 1 },
                   'UsePrimaryProduction':
                      {'type': IntType,
                       'min': 0, 'default': 1, 'max': 1 },
                   'usesalinity':
                      {'type': IntType,
                       'min': 0, 'default': 1, 'max': 1 },
                   'usetemperature':
                      {'type': IntType,
                       'min': 0, 'default': 1, 'max': 1 }} 

# .............................................................................
class AlgQualities:
    """
    @summary: This class contains file format meta information
    @todo: Add GDAL / OGR type as optional parameters
    """
    # ...........................
    def __init__(self, code, name, 
              isDiscreteOutput=False, 
              outputFormat=LMFormat.getDefaultGDAL().driver,
              acceptsCategoricalMaps=False, 
              parameters={}):
        """
        @summary: Constructor
        @param extension: This is the default extension if a format has multiple, 
                             or it could be the only extension
        @param mimeType: The MIME-Type for this format
        @param allExtensions: (optional) Provide a list of all possible extensions
                                 for this format
        """
        self.code = code
        self.name = name
        self.isDiscreteOutput = isDiscreteOutput
        self.outputFormat = outputFormat
        self.acceptsCategoricalMaps = acceptsCategoricalMaps
        self.parameters = parameters

class Algorithms:
    BIOCLIM = AlgQualities('BIOCLIM', 'Bioclimatic Envelope Algorithm',
                           # output is 0, 0.5, 1.0
                           isDiscreteOutput=True,
                           parameters = BIOCLIM_PARAMS)
    CSMBS = AlgQualities('CSMBS', 
                         'Climate Space Model - Broken-Stick Implementation',
                         parameters=CSMBS_PARAMS)
    ENVDIST = AlgQualities('ENVDIST', 'Environmental Distance',
                           parameters = ENVDIST_PARAMS)
    ENVSCORE = AlgQualities('ENVSCORE', 'Envelope Score')
    GARP = AlgQualities('GARP', 
                        'GARP (single run) - new openModeller implementation',
                        isDiscreteOutput = True,
                        parameters = GARP_PARAMS)
    DG_GARP = AlgQualities('DG_GARP',
                           'GARP (single run) - DesktopGARP implementation',
                           isDiscreteOutput = True,
                           parameters = GARP_PARAMS)
    GARP_BS = AlgQualities('GARP_BS', 
                           'GARP with Best Subsets - new openModeller implementation ',
                           parameters = GARP_BS_PARAMS)
    DG_GARP_BS = AlgQualities('DG_GARP_BS', 
                              'GARP with Best Subsets - DesktopGARP implementation',
                              parameters = GARP_BS_PARAMS)
    MAXENT = AlgQualities('MAXENT', 
                          'Maximum Entropy (openModeller Implementation)',
                          parameters = OM_MAXENT_PARAMS)
    ATT_MAXENT = AlgQualities('ATT_MAXENT', 'Maximum Entropy (ATT Implementation)',
                              acceptsCategoricalMaps = True,
                              parameters = ATT_MAXENT_PARAMS)
    SVM = AlgQualities('SVM', 'SVM (Support Vector Machines)',
                       parameters = SVM_PARAMS)
    ANN = AlgQualities('ANN', 'Artificial Neural Network',
                       parameters = ANN_PARAMS)
    AQUAMAPS = AlgQualities('AQUAMAPS', 'AquaMaps (beta version)',
                            parameters = AQUAMAPS_PARAMS)
    # Not yet implemented
    ENFA = AlgQualities('ENFA', 'Ecological-Niche Factor Analysis')
    # Not yet implemented
    RNDFOREST = AlgQualities('RNDFOREST', 'Random Forests')
    # Masking algorithm
    HULL_INTERSECT = AlgQualities('hull_region_intersect', 
                                  'Convex Hull Region Intersect',
                                  isDiscreteOutput=True,
                                  parameters = {'buffer':
                                                 {'type': FloatType,
                                                  'min': 0, 'default': 0.5, 'max': 2 },
                                                # Region MUST be supplied by user
                                                'region':
                                                 {'type': StringType, 
                                                  'default': None }})
    
    @staticmethod
    def implemented():
        return (Algorithms.BIOCLIM, Algorithms.CSMBS, 
                Algorithms.ENVDIST, Algorithms.ENVSCORE, Algorithms.GARP, 
                Algorithms.DG_GARP, Algorithms.GARP_BS, Algorithms.DG_GARP_BS, 
                Algorithms.MAXENT, Algorithms.ATT_MAXENT, Algorithms.SVM, 
                Algorithms.ANN, Algorithms.AQUAMAPS, Algorithms.HULL_INTERSECT)
    
    @staticmethod
    def codes():
        return [alg.code for alg in Algorithms.implemented()]
    
    @staticmethod
    def isOpenModeller(code):
        atype = Algorithms.get(code)
        if atype == Algorithms.ATT_MAXENT:
            return False
        return True
    
    @staticmethod
    def isATT(code):
        atype = Algorithms.get(code)
        if atype == Algorithms.ATT_MAXENT:
            return True
        return False

    @staticmethod
    def get(code):
        for alg in Algorithms.implemented():
            if alg.code == code:
                return alg
        
    @staticmethod
    def returnsDiscreteOutput(code):
        atype = Algorithms.get(code)
        return atype.isDiscreteOutput


class SdmMasks(object):
    HULL_INTERSECT = AlgQualities('hull_region_intersect', 
                                 'Convex Hull Region Intersect',
                                 isDiscreteOutput=True,
                                 parameters = {'buffer':
                                                {'type': FloatType,
                                                 'min': 0, 'default': 0.5, 'max': 2 },
                                               # Region MUST be supplied by user
                                               'region':
                                                {'type': StringType }})

# ============================================================================
# =                              Snippet Constants                              =
# ============================================================================
# =============================================================================
class SnippetOperations(object):
    """
    @summary: Class of available snippet operations
    """
    DOWNLOADED = 'downloaded'
    VIEWED = 'viewed'
    ADDED_TO = 'addedTo'
    USED_IN = 'usedIn'

# =============================================================================
class SnippetFields(object):
    """
    @summary: Snippet fields in Solr index
    """
    AGENT = 'agent'
    CATALOG_NUMBER = 'catalogNumber'
    COLLECTION = 'collection'
    ID = 'id'
    IDENT_1 = 'ident1'
    IDENT_2 = 'ident2'
    OP_TIME = 'opTime'
    OPERATION = 'operation'
    PROVIDER = 'provider'
    URL = 'url'
    WHO = 'who'
    WHY = 'why'

# ============================================================================
# =                              Solr Constants                              =
# ============================================================================
SOLR_ARCHIVE_COLLECTION = 'lmArchive'
SOLR_SNIPPET_COLLECTION = 'snippets'
SOLR_TAXONOMY_COLLECTION = 'taxonomy'
SOLR_POST_COMMAND = '/opt/solr/bin/post'
SOLR_SERVER = 'http://localhost:8983/solr/'
# TODO: Consider moving to localconstants
NUM_DOCS_PER_POST = 100

class SOLR_FIELDS(object):
    """
    @summary: This class contains constants for SOLR index field names
    """
    ALGORITHM_CODE = 'algorithmCode'
    ALGORITHM_PARAMETERS = 'algorithmParameters'
    COMPRESSED_PAV = 'compressedPAV'
    DISPLAY_NAME = 'displayName'
    EPSG_CODE = 'epsgCode'
    GRIDSET_ID = 'gridSetId'
    GRIDSET_META_URL = 'gridSetMetaUrl'
    ID = 'id'
    MODEL_SCENARIO_ALT_PRED_CODE = 'modelScenarioAltPredCode'
    MODEL_SCENARIO_CODE = 'modelScenarioCode'
    MODEL_SCENARIO_DATE_CODE = 'modelScenarioDateCode'
    MODEL_SCENARIO_GCM = 'modelScenarioGCM'
    MODEL_SCENARIO_ID = 'modelScenarioId'
    MODEL_SCENARIO_URL = 'modelScenarioUrl'
    OCCURRENCE_DATA_URL = 'occurrenceDataUrl'
    OCCURRENCE_ID = 'occurrenceId'
    OCCURRENCE_META_URL = 'occurrenceMetaUrl'
    OCCURRENCE_MOD_TIME = 'occurrenceModTime'
    PAV_DATA_URL = 'pavDataUrl'
    PAV_META_URL = 'pavMetaUrl'
    POINT_COUNT = 'pointCount'
    PRESENCE = 'presence'
    PROJ_DATA_URL = 'sdmProjDataUrl'
    PROJ_ID = 'sdmProjId'
    PROJ_META_URL = 'sdmProjMetaUrl'
    PROJ_MOD_TIME = 'sdmProjModTime'
    PROJ_SCENARIO_ALT_PRED_CODE = 'sdmProjScenarioAltPredCode'
    PROJ_SCENARIO_CODE = 'sdmProjScenarioCode'
    PROJ_SCENARIO_DATE_CODE = 'sdmProjScenarioDateCode'
    PROJ_SCENARIO_GCM = 'sdmProjScenarioGCM'
    PROJ_SCENARIO_ID = 'sdmProjScenarioId'
    PROJ_SCENARIO_URL = 'sdmProjScenarioUrl'
    SHAPEGRID_DATA_URL = 'shapegridDataUrl'
    SHAPEGRID_ID = 'shapegridId'
    SHAPEGRID_META_URL = 'shapegridMetaUrl'
    SQUID = 'squid'
    TAXON_CLASS = 'taxonClass'
    TAXON_FAMILY = 'taxonFamily'
    TAXON_GENUS = 'taxonGenus'
    TAXON_KINGDOM = 'taxonKingdom'
    TAXON_ORDER = 'taxonOrder'
    TAXON_PHYLUM = 'taxonPhylum'
    TAXON_SPECIES = 'taxonSpecies'
    USER_ID = 'userId'

# .............................................................................
class SOLR_TAXONOMY_FIELDS(object):
    """Constants for Taxonomy Solr index fields
    """
    CANONICAL_NAME = 'canonical_name'
    ID = 'id'
    SCIENTIFIC_NAME = 'scientific_name'
    SQUID = 'squid'
    TAXON_CLASS = 'taxon_class'
    TAXON_FAMILY = 'taxon_family'
    TAXON_GENUS = 'taxon_genus'
    TAXON_KEY = 'taxon_key'
    TAXON_KINGDOM = 'taxon_kingdom'
    TAXON_ORDER = 'taxon_order'
    TAXON_PHYLUM = 'taxon_phylum'
    TAXON_SPECIES = 'taxon_species'
    USER_ID = 'user_id'
    TAXONOMY_SOURCE_ID = 'taxon_source_id'

# ============================================================================
# =                             Scaling Constants                            =
# ============================================================================
SCALE_PROJECTION_MINIMUM = 0
SCALE_PROJECTION_MAXIMUM = 100
SCALE_DATA_TYPE = "int"

# ============================================================================
# =                            Processing Constants                          =
# ============================================================================
# TODO: AMS : Alter as necessary
BUFFER_KEY = 'buffer'
CODE_KEY = 'code'
ECOREGION_MASK_METHOD = 'hull_region_intersect'
MASK_KEY = 'mask'
MASK_LAYER_KEY = 'mask_layer'
MASK_LAYER_NAME_KEY = 'mask_name'
PRE_PROCESS_KEY = 'preprocess'
PROCESSING_KEY = 'processing'

class SubsetMethod(object):
    """
    """
    COLUMN = 0 # Same shapegrid, just cut out columns
    SPATIAL = 1 # Spatial subset of original shapegrid.  Column and row, 
                #    cut out columns and remove sites
    REINTERSECT = 2 # New shapegrid with possibly different resolution, 
    #                    reintersect all columns 
    
    
    
