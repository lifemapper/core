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
import inspect
from osgeo.gdalconst import (GDT_Unknown, GDT_Byte, GDT_UInt16, GDT_Int16, 
                     GDT_UInt32, GDT_Int32, GDT_Float32, GDT_Float64, 
                     GDT_CInt16, GDT_CInt32, GDT_CFloat32, GDT_CFloat64)
from osgeo.ogr import (wkbPoint, wkbLineString, wkbPolygon, wkbMultiPoint, 
                       wkbMultiLineString, wkbMultiPolygon)
   
import os.path
from types import IntType, FloatType

from LmCommon.common.lmconstants import (JobStatus, MatrixType, LMFormat)
from LmServer.common.localconstants import (APP_PATH, DATA_PATH,
                              SHARED_DATA_PATH, SCRATCH_PATH, PID_PATH, 
                              SCENARIO_PACKAGE_EPSG, WEBSERVICES_ROOT)

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
# On lmserver data directory
SPECIES_DATA_PATH = os.path.join(DATA_PATH, 'species')
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
CATALOG_SERVER_BIN = os.path.join(BIN_PATH, 'catalog_server')
WORKER_FACTORY_BIN = os.path.join(BIN_PATH, 'work_queue_factory')
MAKEFLOW_BIN = os.path.join(BIN_PATH, 'makeflow')
MAKEFLOW_WORKSPACE = os.path.join(SCRATCH_PATH, 'makeflow')

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
   SDMModel = 101
   SDMProjection = 102
   SDMExperiment = 103
   OccurrenceSet = 104
   GridPAV = 150
   RADExperiment = 201
   Bucket = 202
   OriginalPamSum = 203
   RandomPamSum = 204
   ShapeGrid = 205
   
   @staticmethod
   def name(rt):
      attrs = inspect.getmembers(ReferenceType, lambda a:not(inspect.isroutine(a)))
      for att in attrs:
         if rt == att[1]:
            return att[0]
      return None 
   
   @staticmethod
   def sdmTypes():
      return [ReferenceType.OccurrenceSet, ReferenceType.SDMModel, 
              ReferenceType.SDMProjection, ReferenceType.SDMExperiment]
      
   @staticmethod
   def isSDM(rtype):
      if rtype in ReferenceType.sdmTypes():
         return True
      return False
      
   @staticmethod
   def radTypes():
      return [ReferenceType.ShapeGrid, ReferenceType.Bucket, 
              ReferenceType.OriginalPamSum, ReferenceType.RandomPamSum,
              ReferenceType.RADExperiment]

   @staticmethod
   def isRAD(rtype):
      if rtype in ReferenceType.radTypes():
         return True
      return False
   
   @staticmethod
   def boomTypes():
      return [ReferenceType.OccurrenceSet, ReferenceType.SDMModel, 
              ReferenceType.SDMProjection, ReferenceType.GridPAV, 
              ReferenceType.SDMExperiment]

   @staticmethod
   def isBOOM(rtype):
      if rtype in ReferenceType.boomTypes():
         return True
      return False
   
   @staticmethod
   def dependencies(rtype):
      if rtype == ReferenceType.OccurrenceSet:
         return []
      elif rtype == ReferenceType.SDMModel:
         return [ReferenceType.OccurrenceSet]
      elif rtype == ReferenceType.SDMProjection:
         return [ReferenceType.SDMModel]
      elif rtype == ReferenceType.GridPAV:
         return [ReferenceType.SDMProjection]
      
      elif rtype == ReferenceType.ShapeGrid:
         return []
      elif rtype == ReferenceType.Bucket:
         return [ReferenceType.ShapeGrid]
      elif rtype == ReferenceType.OriginalPamSum:
         return [ReferenceType.Bucket]
      elif rtype == ReferenceType.RandomPamSum:
         return [ReferenceType.OriginalPamSum]
      
   @staticmethod
   def bottomUpChain(rtype):
      """
      @return: a nested tuple of type with nested list of dependencies as:
         (pavRefType, [(prjRefType, [(mdlRefType, [(occRefType, [])])])])
      @note: bottom up
      """
      print 'bottomUpChain has been called with {}'.format(rtype)
      deps = ReferenceType.dependencies(rtype)
      if not deps:
         return (rtype, deps)
      else:
         theseDeps = []
         for dep in deps:
            print '  intermediate dep {}'.format(dep)
            theseDeps.append(ReferenceType.bottomUpChain(dep))
         return (rtype, theseDeps)

   @staticmethod
   def dependents(rtype):
      if rtype == ReferenceType.OccurrenceSet:
         return [ReferenceType.SDMModel]
      elif rtype == ReferenceType.SDMModel:
         return [ReferenceType.SDMProjection]
      elif rtype == ReferenceType.SDMProjection:
         return [ReferenceType.GridPAV]
      elif rtype == ReferenceType.GridPAV:
         return []
      
      elif rtype == ReferenceType.ShapeGrid:
         return [ReferenceType.Bucket]
      elif rtype == ReferenceType.Bucket:
         return [ReferenceType.OriginalPamSum]
      elif rtype == ReferenceType.OriginalPamSum:
         return [ReferenceType.RandomPamSum]
      elif rtype == ReferenceType.RandomPamSum:
         return []

   @staticmethod
   def topDownChain(rtype):
      """
      @return: a nested tuple of type with nested list of dependents as:
         (occRefType, [(mdlRefType, [(prjRefType, [(pavRefType, [])])])])
      @note: top down
      """
      print 'topDownChain has been called with {}'.format(rtype)
      deps = ReferenceType.dependents(rtype)
      if not deps:
         return (rtype, deps)
      else:
         theseDeps = []
         for dep in deps:
            print '  intermediate dep {}'.format(dep)
            theseDeps.append(ReferenceType.topDownChain(dep))
         return (rtype, theseDeps)

from LmCommon.common.lmconstants import DWCNames
class OccurrenceFieldNames:
   LOCAL_ID = ['localid', 'localId', 'id', 'occkey', 
               DWCNames.OCCURRENCE_ID['SHORT'], DWCNames.OCCURRENCE_ID['FULL']]
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

DEFAULT_SRS = 'epsg:%s' % str(SCENARIO_PACKAGE_EPSG)
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
   SDM_MAKEFLOW_FILE = 113
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
   SUM_CALCS = 241
   SUM_SHAPE = 242
   RAD_MAP = 250
   
   BIOGEO_HYPOTHESES = 322
   PADDED_PAM = 323
   MCPA_OUTPUTS = 324
   TREE = 325

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
                   LMFileType.SDM_MAKEFLOW_FILE, 
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
                   LMFileType.BIOGEO_HYPOTHESES, LMFileType.TREE]:
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
                   LMFileType.TREE, LMFileType.PADDED_PAM, LMFileType.MCPA_OUTPUTS]:
         return True
      return False
   
   @staticmethod
   def getMatrixFiletype(mtype):
      if mtype in (MatrixType.PAM, MatrixType.ROLLING_PAM):
         return LMFileType.PAM
      elif mtype == MatrixType.GRIM:
         return LMFileType.GRIM
      elif mtype == MatrixType.BIOGEO_HYPOTHESES:
         return LMFileType.BIOGEO_HYPOTHESES
      elif mtype == MatrixType.PADDED_PAM:
         return LMFileType.PADDED_PAM
      elif mtype == MatrixType.MCPA_OUTPUTS:
         return LMFileType.MCPA_OUTPUTS

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
             LMFileType.SDM_MAKEFLOW_FILE: OCC_NAME_PREFIX,
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
             LMFileType.GRIM: 'grim',
             LMFileType.SUM_CALCS: PAMSUM_PREFIX,
             LMFileType.SUM_SHAPE: PAMSUM_PREFIX,

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
             LMFileType.MCPA_OUTPUTS: 'mcpa'}
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
                LMFileType.SDM_MAKEFLOW_FILE: LMFormat.MAKEFLOW.ext,
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
                LMFileType.PAM: LMFormat.JSON.ext,
                LMFileType.GRIM: LMFormat.JSON.ext,
                LMFileType.SUM_CALCS: LMFormat.PICKLE.ext,
                LMFileType.SUM_SHAPE: LMFormat.SHAPE.ext,

                LMFileType.BOOM_CONFIG: LMFormat.CONFIG.ext,
                LMFileType.UNSPECIFIED_RAD: None,
                LMFileType.USER_LAYER: None,
                LMFileType.USER_SHAPEGRID:  LMFormat.SHAPE.ext,
                LMFileType.USER_ATTRIBUTE_MATRIX: LMFormat.NUMPY.ext,
                LMFileType.USER_TREE: LMFormat.JSON.ext,
                LMFileType.MF_DOCUMENT: LMFormat.MAKEFLOW.ext,
                LMFileType.BIOGEO_HYPOTHESES: LMFormat.JSON.ext,
                LMFileType.TREE: LMFormat.JSON.ext,
                LMFileType.PADDED_PAM: LMFormat.JSON.ext,
                LMFileType.MCPA_OUTPUTS: LMFormat.JSON.ext
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

CT_USER = 'changeThinking'
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
class JobFamily:
# ............................................................................
   SDM = 1
   RAD = 2

# ............................................................................
# Change to enum.Enum with Python 3.4
# Corresponds to LmCommon individual constants MODELS_SERVICE, LAYERS_SERVICE, etc
class LMServiceType:
   BUCKETS = 'buckets' # TODO: Remove, obsolete 
   TREES = 'tree'
   EXPERIMENTS = 'experiments' # TODO: Remove, obsolete
   GRIDSETS = 'gridset'
   SDM_EXPERIMENTS = 'experiments' # TODO: Remove, obsolete
   LAYERTYPES = 'typecodes' # TODO: Remove, obsolete
   MODELS = 'models' # TODO: Remove, obsolete
   OCCURRENCES = 'occurrence'
   PAMSUMS = 'pamsums' # TODO: Remove, obsolete?
   ANCILLARY_LAYERS = 'anclayers' # TODO: Remove, obsolete?
   PRESENCEABSENCE_LAYERS = 'palayers' # TODO: Remove, obsolete?
   MATRIX_LAYERS = 'mtxlayers'
   ENVIRONMENTAL_LAYERS = 'envlayer'
   PROJECTIONS = 'sdmProject'
   SCENARIOS = 'scenario'
   SHAPEGRIDS = 'shapegrid'
   # Generic layers/layersets/matrices
   LAYERS = 'layer'
   LAYERSETS = 'layersets' # TODO: Remove, obsolete?
   MATRICES = 'matrix'
   MATRIX_COLUMNS = 'column'
   
# Archive and Global PAM descriptions
GPAM_KEYWORD = 'Global PAM'
PUBLIC_ARCHIVE_NAME = 'BOOM_Archive'
ARCHIVE_KEYWORD = 'archive'
   
# ............................................................................
# Lifemapper RAD constants
ORGANISM_LAYER_KEY = 'orgLayerIndices'
ENVIRONMENTAL_LAYER_KEY = 'envLayerIndices'

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
                     'outputformat': # 0 - raw, 1 - logistic, 2 - cumulative
                         {'type' : IntType, 'min' : 0, 'max' : 2, 'default' : 1},
                     'randomseed': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
                     'logscale': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'removeduplicates': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
                     'writeclampgrid': 
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
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
                         {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
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
              'Cost': {'type': FloatType,
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
                             'GARP (single run) - DesktopGARP implementation',
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
   
   @staticmethod
   def implemented():
      return (Algorithms.BIOCLIM, Algorithms.CSMBS, 
               Algorithms.ENVDIST, Algorithms.ENVSCORE, Algorithms.GARP, 
               Algorithms.DG_GARP, Algorithms.GARP_BS, Algorithms.DG_GARP_BS, 
               Algorithms.MAXENT, Algorithms.ATT_MAXENT, Algorithms.SVM, 
               Algorithms.ANN, Algorithms.AQUAMAPS)

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


# ...............................................
# Algorithm constants
# TODO: Replace with Algorithms Class
BIOCLIM_PARAMETERS = {'code': 'BIOCLIM',
                      'name': 'Bioclimatic Envelope Algorithm',
                      # output is 0, 0.5, 1.0
                      'isDiscreteOutput': True,
                      'outputFormat': 'GTiff',
                      'acceptsCategoricalMaps': False,
                      'parameters' : {'StandardDeviationCutoff': 
                                      {'type': FloatType,
                                       'min': 0.0, 'default': 0.674, 
                                       'max': None} }}
# TODO: Replace with Algorithms Class
CSMBS_PARAMETERS = {'code': 'CSMBS',
                    'name': 'Climate Space Model - Broken-Stick Implementation',
                   # TODO: discrete output?
                   'isDiscreteOutput': False,
                   'outputFormat': 'GTiff',
                   'acceptsCategoricalMaps': False,
                   'parameters' : {'Randomisations': 
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
                                     'min': 0, 'default': 0, 'max': 1}}}
# TODO: Replace with Algorithms Class
ENVDIST_PARAMETERS = {'code': 'ENVDIST',
                      'name': 'Environmental Distance',
                      # TODO: output is??
                      'isDiscreteOutput': False,
                      'outputFormat': 'GTiff',
                      'acceptsCategoricalMaps': False,
                      'parameters': {'DistanceType':
                                       {'type': IntType,
                                        'min': 1, 'default': 1, 'max': 4 },
                                       'NearestPoints':
                                       {'type': IntType,
                                        'min': 0, 'default': 1, 'max': None },
                                     'MaxDistance':
                                       {'type': FloatType,
                                        'min': 0.1, 'default': 0.1, 'max': 1.0 }}}
# TODO: Replace with Algorithms Class
ENVSCORE_PARAMETERS = {'code': 'ENVSCORE',
                       'name': 'Envelope Score',
                       # probability between 0.0 and 1.0
                       'isDiscreteOutput': False,
                       'outputFormat': 'GTiff',
                       'acceptsCategoricalMaps': False,
                       'parameters': { } }
# TODO: Replace with Algorithms Class
GARP_PARAMETERS =  {'code': 'GARP',
                    'name': 'GARP (single run) - new openModeller implementation',
                    'isDiscreteOutput': True,
                    'outputFormat': 'GTiff',
                    'acceptsCategoricalMaps': False,
                    'parameters': {'MaxGenerations':
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
                                      'min': 1, 'default': 2500, 'max': 100000 }}}
# TODO: Replace with Algorithms Class
GARP_BS_PARAMETERS = {'code': 'GARP_BS',
                      'name': 'GARP with Best Subsets - new openModeller implementation ',
                      'isDiscreteOutput': False,
                      'outputFormat': 'GTiff',
                      'acceptsCategoricalMaps': False,
                      'parameters': {'TrainingProportion': 
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
                                      'min': 1, 'default': 2500, 'max': 10000 } } }
# TODO: Replace with Algorithms Class
DG_GARP_PARAMETERS =  {'code': 'DG_GARP',
                       'name': 'GARP (single run) - DesktopGARP implementation',
                       'isDiscreteOutput': True,
                       'outputFormat': 'GTiff',
                       'acceptsCategoricalMaps': False,
                       'parameters': GARP_PARAMETERS['parameters']}
# TODO: Replace with Algorithms Class
DG_GARP_BS_PARAMETERS = {'code': 'DG_GARP_BS',
                         'name': 'GARP (single run) - DesktopGARP implementation',
                         'isDiscreteOutput': False,
                         'outputFormat': 'GTiff',
                         'acceptsCategoricalMaps': False,
                         'parameters': GARP_BS_PARAMETERS['parameters']}
# TODO: Replace with Algorithms Class
ATT_MAXENT_PARAMETERS = {'code': 'ATT_MAXENT',
                         'name' : 'Maximum Entropy (ATT Implementation)',
                         'isDiscreteOutput' : False,
                         'outputFormat': 'GTiff',
                         'acceptsCategoricalMaps' : True,
                         'parameters' :
      {
       'responsecurves': 
          {'type': IntType, 'min' : 0, 'max' : 1, 'default' : 0},
       'pictures': 
          {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
       'jackknife': 
          {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
       'outputformat': # 0 - raw, 1 - logistic, 2 - cumulative
          {'type' : IntType, 'min' : 0, 'max' : 2, 'default' : 1},
       'randomseed': 
          {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 0},
       'logscale': 
          {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
       'removeduplicates': 
          {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
       'writeclampgrid': 
          {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
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
          {'type' : IntType, 'min' : 0, 'max' : 1, 'default' : 1},
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
   }
# TODO: Replace with Algorithms Class
OM_MAXENT_PARAMETERS = {'code': 'MAXENT',
                        'name': 'Maximum Entropy (openModeller Implementation)',
                        'isDiscreteOutput': False,
                        'outputFormat': 'GTiff',
                        'acceptsCategoricalMaps': False,
                        'parameters': {"NumberOfBackgroundPoints":
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
                                         'default': 15} } }
# TODO: Replace with Algorithms Class
SVM_PARAMETERS = {'code': 'SVM',
                  'name': 'SVM (Support Vector Machines)',
                  'isDiscreteOutput': False,
                  'outputFormat': 'GTiff',
                  'acceptsCategoricalMaps': False,
                  'parameters': {'svmtype': {'type': IntType,
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
                                 'Cost': {'type': FloatType,
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
                                                            'max': None } } }
# TODO: Replace with Algorithms Class
ANN_PARAMETERS = {'code': 'ANN',
                  'name': 'Artificial Neural Network',
                  # TODO: discrete output?
                  'isDiscreteOutput': False,
                  'outputFormat': 'GTiff',
                  'acceptsCategoricalMaps': False,
                  'parameters': {'HiddenLayerNeurons':
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
                                   'min': 0.0, 'default': 0.01, 'max': 0.05} }}
 
# TODO: Implement this!
# TODO: Replace with Algorithms Class
ENFA_PARAMETERS = {'code': 'ENFA',
                   'name': 'Ecological-Niche Factor Analysis',
                   'isDiscreteOutput': None,
                   'outputFormat': 'GTiff',
                   'acceptsCategoricalMaps': False,
                   'parameters': {} } 
 
# TODO: Implement this!
# TODO: Replace with Algorithms Class
RNDFOREST_PARAMETERS = {'code': 'RNDFOREST',
                        'name': 'Random Forests',
                        'isDiscreteOutput': None,
                        'outputFormat': 'GTiff',
                        'acceptsCategoricalMaps': False,
                        'parameters': {} } 
# TODO: Replace with Algorithms Class
AQUAMAPS_PARAMETERS = {'code': 'AQUAMAPS',
                       'name': 'AquaMaps (beta version) ',
                       'isDiscreteOutput': False,
                       'outputFormat': 'GTiff',
                       'acceptsCategoricalMaps': False,
                       'parameters': {'UseSurfaceLayers':
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
                                         'min': 0, 'default': 1, 'max': 1 }} }
# TODO: Replace with Algorithms Class
ALGORITHM_DATA  = {
'BIOCLIM': BIOCLIM_PARAMETERS,
'CSMBS': CSMBS_PARAMETERS,
'ENVDIST': ENVDIST_PARAMETERS,
'ENVSCORE': ENVSCORE_PARAMETERS,
'DG_GARP': DG_GARP_PARAMETERS,
'GARP': GARP_PARAMETERS,
'DG_GARP_BS': DG_GARP_BS_PARAMETERS,
'GARP_BS': GARP_BS_PARAMETERS,
'MAXENT': OM_MAXENT_PARAMETERS,
'ATT_MAXENT': ATT_MAXENT_PARAMETERS,
'SVM': SVM_PARAMETERS,
'ANN': ANN_PARAMETERS,
'AQUAMAPS': AQUAMAPS_PARAMETERS,
# Not yet implemented
'ENFA': ENFA_PARAMETERS, 
'RNDFOREST': RNDFOREST_PARAMETERS
}

# ============================================================================
# =                              Solr Constants                              =
# ============================================================================
SOLR_ARCHIVE_COLLECTION = 'lmArchive'
SOLR_POST_COMMAND = '/opt/solr/bin/post'
SOLR_SERVER = 'http://localhost:8983/solr/'

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
