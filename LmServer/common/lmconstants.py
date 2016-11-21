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
import inspect
try:
   from osgeo.gdalconst import (GDT_Unknown, GDT_Byte, GDT_UInt16, GDT_Int16, 
                        GDT_UInt32, GDT_Int32, GDT_Float32, GDT_Float64, 
                        GDT_CInt16, GDT_CInt32, GDT_CFloat32, GDT_CFloat64)
except:
   GDT_Unknown = 0
   GDT_Byte = 1
   GDT_UInt16 = 2
   GDT_Int16 = 3
   GDT_UInt32 = 4
   GDT_Int32 = 5
   GDT_Float32 = 6
   GDT_Float64 = 7
   GDT_CInt16 = 8
   GDT_CInt32 = 9
   GDT_CFloat32 = 10
   GDT_CFloat64 = 11
   
try:
   from osgeo.ogr import (wkbPoint, wkbLineString, wkbPolygon, wkbMultiPoint, 
                          wkbMultiLineString, wkbMultiPolygon)
except:
   wkbPoint = 1
   wkbLineString = 2
   wkbPolygon = 3
   wkbMultiPoint = 4
   wkbMultiLineString = 5
   wkbMultiPolygon = 6
   
import os.path
from types import IntType, FloatType

from LmCommon.common.lmconstants import (DEFAULT_EPSG, JobStatus, OutputFormat)
from LmServer.common.localconstants import (APP_PATH, DATA_PATH, SHARED_DATA_PATH, 
                                            SCRATCH_PATH, TEMP_PATH, PID_PATH)

BIN_PATH = os.path.join(APP_PATH, 'bin')
# Relative paths
USER_LAYER_DIR = 'Layers'
# On shared data directory (shared if lifemapper-compute is also installed)
ENV_DATA_PATH = os.path.join(SHARED_DATA_PATH,'layers')
ARCHIVE_PATH = os.path.join(SHARED_DATA_PATH,'archive')
# On lmserver data directory
SPECIES_DATA_PATH = os.path.join(DATA_PATH, 'species')
TEST_DATA_PATH = os.path.join(DATA_PATH, 'test') 
IMAGE_PATH = os.path.join(DATA_PATH, 'image')
# On scratch disk
UPLOAD_PATH = os.path.join(SCRATCH_PATH, 'tmpUpload')
LOG_PATH = os.path.join(SCRATCH_PATH, 'log')
USER_LOG_PATH = os.path.join(LOG_PATH, 'users')
ERROR_LOG_PATH = os.path.join(LOG_PATH, 'errors')

CHERRYPY_CONFIG_FILE = os.path.join(APP_PATH,'config', 'cherrypy.conf')
MATT_DAEMON_PID_FILE = os.path.join(PID_PATH, 'mattDaemon.pid')

# CC Tools constants
CATALOG_SERVER_BIN = os.path.join(BIN_PATH, 'catalog_server')
WORKER_FACTORY_BIN = os.path.join(BIN_PATH, 'work_queue_factory')
MAKEFLOW_BIN = os.path.join(BIN_PATH, 'makeflow')

# Depth of path for archive SDM experiment data - this is the number of levels 
# that the occurrencesetid associated with a model and its projections 
# is split into i.e. occurrencesetid = 123456789 --> path 000/123/456/789/
MODEL_DEPTH = 4

LM_SCHEMA = 'lm3'
LM_SCHEMA_BORG = 'lm_v3'
SALT = "4303e8f7129243fd42a57bcc18a19f5b"

# MAL, RAD, gbifCache database names
RAD_STORE = 'speco'
MAL_STORE = 'mal'
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
   # ..............................
   # SDM
   # User level
   ENVIRONMENTAL_LAYER = 101
   SCENARIO_MAP = 102
   # Occurrence level (SDM data are organized by OccurrenceSets)
   UNSPECIFIED_OCC = 100
   OCCURRENCE_FILE = 111
   OCCURRENCE_RAW_FILE = 112
   # Experiment Level
   SDM_MAP = 110
   SDM_MAKEFLOW_FILE = 113
   MODEL_REQUEST = 120
   MODEL_STATS = 121
   MODEL_RESULT = 122
   MODEL_ATT_RESULT = 123
   PROJECTION_REQUEST = 130
   PROJECTION_PACKAGE = 131
   PROJECTION_LAYER = 132
   PROJECTION_ATT_LAYER = 133
   # Associated with an SDM experiment-projection (and default shapegrid), 
   # but not a RAD experiment
   PROJECTION_INTERSECT = 140
   # ..............................
   # RAD
   # User level
   UNSPECIFIED_USER = 500
   SHAPEGRID = 201     # TODO: delete
   USER_LAYER = 510
   USER_SHAPEGRID = 511
   USER_ATTRIBUTE_MATRIX = 520
   USER_TREE = 530
   
   # Experiment Level (maybe a pruned user-level object)
   UNSPECIFIED_RAD = 200
   ATTR_MATRIX = 210    # not yet used
   ATTR_TREE = 211     # TODO: delete
   # Bucket level
   BUCKET_MAP = 221     # TODO: delete
   # Matrices
   PAM = 222
   GRIM = 223
   # Species and Site indices  
   PRESENCE_INDICES = 225     # TODO: delete
   # PamSum level
   # Calculations on PAM (Original or Randomized Swap)
   COMPRESSED_PAM = 240     # TODO: delete
   SUM_CALCS = 241
   SUM_SHAPE = 242
   # Randomized Splotch  PamSums
   SPLOTCH_PAM = 243     # TODO: delete
   SPLOTCH_SITES = 244     # TODO: delete
   # ..............................
   # New Borg, Associated with RAD Experiment
   # Matrix
   BIOGEO_HYPOTHESES = 322
   # Associated with an Experiment-Matrix
   MATRIX_COLUMN = 340
   LAYER_INDICES = 350
   # Experiment wide
   SITE_INDICES = 360
   
   @staticmethod
   def isSDM(rtype):
      if rtype in [LMFileType.SDM_MAP, LMFileType.OCCURRENCE_FILE, 
                   LMFileType.OCCURRENCE_RAW_FILE, LMFileType.SDM_MAKEFLOW_FILE, 
                   LMFileType.MODEL_REQUEST, LMFileType.MODEL_STATS, 
                   LMFileType.MODEL_RESULT, LMFileType.MODEL_ATT_RESULT, 
                   LMFileType.PROJECTION_REQUEST, LMFileType.PROJECTION_PACKAGE, 
                   LMFileType.PROJECTION_LAYER, LMFileType.PROJECTION_ATT_LAYER, 
                   LMFileType.PROJECTION_INTERSECT]:
         return True
      return False
   
   @staticmethod
   def isOccurrence(rtype):
      if rtype in [LMFileType.SDM_MAP, LMFileType.OCCURRENCE_FILE, 
                   LMFileType.OCCURRENCE_RAW_FILE, LMFileType.SDM_MAKEFLOW_FILE]:
         return True
      return False

   @staticmethod
   def isModel(rtype):
      if rtype in [LMFileType.MODEL_REQUEST, LMFileType.MODEL_STATS, 
                   LMFileType.MODEL_RESULT, LMFileType.MODEL_ATT_RESULT]:
         return True
      return False

   @staticmethod
   def isProjection(rtype):
      if rtype in [LMFileType.PROJECTION_REQUEST, LMFileType.PROJECTION_PACKAGE, 
                   LMFileType.PROJECTION_LAYER, LMFileType.PROJECTION_ATT_LAYER, 
                   LMFileType.PROJECTION_INTERSECT]:
         return True
      return False
   
   @staticmethod
   def isRAD(rtype):
      if rtype in [LMFileType.UNSPECIFIED_RAD,
                   LMFileType.ATTR_MATRIX, LMFileType.ATTR_TREE, LMFileType.BUCKET_MAP, 
                   LMFileType.PAM, LMFileType.GRIM, 
                   LMFileType.LAYER_INDICES, LMFileType.PRESENCE_INDICES, 
                   LMFileType.COMPRESSED_PAM, 
                   LMFileType.SUM_CALCS, LMFileType.SUM_SHAPE, 
                   LMFileType.SPLOTCH_PAM, LMFileType.SPLOTCH_SITES, 
                   LMFileType.BIOGEO_HYPOTHESES, LMFileType.MATRIX_COLUMN, 
                   LMFileType.LAYER_INDICES, LMFileType.SITE_INDICES]:
         return True
      return False

   @staticmethod
   def isRADExperiment(rtype):
      if rtype in [LMFileType.PAM, LMFileType.GRIM, 
                   LMFileType.SUM_CALCS, LMFileType.SUM_SHAPE, 
                   LMFileType.BIOGEO_HYPOTHESES, LMFileType.SITE_INDICES]:
         return True
      return False

   @staticmethod
   def isMatrix(rtype):
      if rtype in [LMFileType.UNSPECIFIED_RAD,
                   LMFileType.ATTR_MATRIX, LMFileType.ATTR_TREE, LMFileType.BUCKET_MAP, 
                   LMFileType.PAM, LMFileType.GRIM, 
                   LMFileType.LAYER_INDICES, LMFileType.PRESENCE_INDICES, 
                   LMFileType.COMPRESSED_PAM, 
                   LMFileType.SUM_CALCS, LMFileType.SUM_SHAPE, 
                   LMFileType.SPLOTCH_PAM, LMFileType.SPLOTCH_SITES, 
                   LMFileType.BIOGEO_HYPOTHESES, LMFileType.MATRIX_COLUMN, 
                   LMFileType.LAYER_INDICES, LMFileType.SITE_INDICES]:
         return True
      return False
   
   @staticmethod
   def isUserSpace(rtype):
      if rtype in [LMFileType.UNSPECIFIED_USER, LMFileType.OTHER_MAP, 
                   LMFileType.ENVIRONMENTAL_LAYER, LMFileType.SCENARIO_MAP, 
                   LMFileType.SHAPEGRID, 
                   LMFileType.USER_LAYER, LMFileType.USER_SHAPEGRID, 
                   LMFileType.USER_ATTRIBUTE_MATRIX, LMFileType.USER_TREE]:
         return True
      return False
         
   @staticmethod
   def isMap(rtype):
      if rtype in [LMFileType.OTHER_MAP, LMFileType.SCENARIO_MAP, 
                   LMFileType.SDM_MAP, LMFileType.BUCKET_MAP]:
         return True
      return False
   
   @staticmethod
   def isUserLayer(rtype):
      if rtype in [LMFileType.ENVIRONMENTAL_LAYER, LMFileType.SHAPEGRID,
                   LMFileType.USER_LAYER, LMFileType.USER_SHAPEGRID]:
         return True
      return False
   


   
NAME_SEPARATOR = '_'

# Mapfile layer name
OCC_NAME_PREFIX = 'occ'
GENERIC_LAYER_NAME_PREFIX = 'lyr'
   
OCC_PREFIX = 'pt'
PRJ_PREFIX = 'prj'
SPLOTCH_PREFIX = 'splotch'
PAMSUM_PREFIX = 'pamsum'

class FileFix:
   PREFIX = {LMFileType.OTHER_MAP: MapPrefix.USER,
             LMFileType.ENVIRONMENTAL_LAYER: None,
             LMFileType.SCENARIO_MAP: MapPrefix.SCEN,
             LMFileType.SDM_MAP: MapPrefix.SDM,
             LMFileType.SDM_MAKEFLOW_FILE: OCC_NAME_PREFIX,
             LMFileType.OCCURRENCE_FILE: OCC_PREFIX,
             LMFileType.OCCURRENCE_RAW_FILE: OCC_PREFIX,
             LMFileType.MODEL_REQUEST: 'modReq',
             LMFileType.MODEL_STATS: None,
             LMFileType.MODEL_RESULT: None,
             LMFileType.MODEL_ATT_RESULT: None,
             LMFileType.PROJECTION_REQUEST: 'projReq',
             LMFileType.PROJECTION_PACKAGE: PRJ_PREFIX,
             LMFileType.PROJECTION_LAYER: PRJ_PREFIX,
             LMFileType.PROJECTION_INTERSECT: 'int',         
             
             LMFileType.SHAPEGRID: 'shpgrid',
             LMFileType.ATTR_MATRIX: 'attrMtx',
             LMFileType.ATTR_TREE: 'attrTree',
             LMFileType.BUCKET_MAP: MapPrefix.RAD, 
             LMFileType.PAM: 'pam',
             LMFileType.GRIM: 'grim',
             LMFileType.LAYER_INDICES: 'lyridx',
             LMFileType.PRESENCE_INDICES: 'indices',
             LMFileType.COMPRESSED_PAM: PAMSUM_PREFIX,
             LMFileType.SUM_CALCS: PAMSUM_PREFIX,
             LMFileType.SUM_SHAPE: PAMSUM_PREFIX,
             LMFileType.SPLOTCH_PAM: SPLOTCH_PREFIX,
             LMFileType.SPLOTCH_SITES: SPLOTCH_PREFIX,

             LMFileType.UNSPECIFIED_USER: None,
             LMFileType.UNSPECIFIED_RAD: None,
             LMFileType.UNSPECIFIED_OCC: None,
             LMFileType.USER_LAYER: GENERIC_LAYER_NAME_PREFIX,
             LMFileType.USER_SHAPEGRID: None,
             LMFileType.USER_ATTRIBUTE_MATRIX: 'attributes',
             LMFileType.USER_TREE: 'tree',
             LMFileType.BIOGEO_HYPOTHESES: 'biogeo',
             LMFileType.MATRIX_COLUMN: 'col',
             LMFileType.SITE_INDICES: 'siteidx',
}
   # Postfix
   EXTENSION = {LMFileType.OTHER_MAP: OutputFormat.MAP,
                LMFileType.ENVIRONMENTAL_LAYER: OutputFormat.GTIFF,
                LMFileType.SCENARIO_MAP: OutputFormat.MAP,
                LMFileType.SDM_MAP: OutputFormat.MAP,
                LMFileType.SDM_MAKEFLOW_FILE: OutputFormat.MAKEFLOW,
                LMFileType.OCCURRENCE_FILE: OutputFormat.SHAPE,
                LMFileType.OCCURRENCE_RAW_FILE: OutputFormat.CSV,
                LMFileType.MODEL_REQUEST: OutputFormat.XML,
                LMFileType.MODEL_STATS: OutputFormat.ZIP,
                LMFileType.MODEL_RESULT: OutputFormat.XML,
                LMFileType.MODEL_ATT_RESULT: OutputFormat.TXT,
                LMFileType.PROJECTION_REQUEST: OutputFormat.XML,
                LMFileType.PROJECTION_PACKAGE: OutputFormat.ZIP,
                LMFileType.PROJECTION_LAYER: OutputFormat.GTIFF,
#                 LMFileType.PROJECTION_ATT_LAYER: OutputFormat.ASCII,
                LMFileType.PROJECTION_INTERSECT: OutputFormat.NUMPY,
                
                LMFileType.SHAPEGRID:  OutputFormat.SHAPE,
                LMFileType.ATTR_MATRIX: OutputFormat.NUMPY,
                LMFileType.ATTR_TREE: OutputFormat.JSON,
                LMFileType.BUCKET_MAP: OutputFormat.MAP, 
                LMFileType.PAM: OutputFormat.NUMPY,
                LMFileType.GRIM: OutputFormat.NUMPY,
                LMFileType.LAYER_INDICES: OutputFormat.PICKLE,
                LMFileType.PRESENCE_INDICES: OutputFormat.PICKLE,
                LMFileType.COMPRESSED_PAM: OutputFormat.NUMPY,
                LMFileType.SUM_CALCS: OutputFormat.PICKLE,
                LMFileType.SUM_SHAPE: OutputFormat.SHAPE,
                LMFileType.SPLOTCH_PAM: OutputFormat.NUMPY,
                LMFileType.SPLOTCH_SITES: OutputFormat.PICKLE,
                
                LMFileType.UNSPECIFIED_USER: None,
                LMFileType.UNSPECIFIED_RAD: None,
                LMFileType.UNSPECIFIED_OCC: None,
                LMFileType.USER_LAYER: None,
                LMFileType.USER_SHAPEGRID:  OutputFormat.SHAPE,
                LMFileType.USER_ATTRIBUTE_MATRIX: OutputFormat.NUMPY,
                LMFileType.USER_TREE: OutputFormat.JSON,
                LMFileType.BIOGEO_HYPOTHESES: OutputFormat.NUMPY,
                LMFileType.MATRIX_COLUMN: OutputFormat.NUMPY,
                LMFileType.SITE_INDICES: OutputFormat.PICKLE,
   }
   
NAME_SEPARATOR = '_'
   
# FIXME: change this
# Development desktops, debug users, and beta server names
DEBUG_USER_PREFIX = 'debug_'

SERVICES_PREFIX = 'services'
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
   BUCKETS = 'buckets'
   TREES = 'trees'
   EXPERIMENTS = 'experiments'
   RAD_EXPERIMENTS = 'experiments'
   SDM_EXPERIMENTS = 'experiments'
   LAYERTYPES = 'typecodes'
   MODELS = 'models'
   OCCURRENCES = 'occurrences'
   PAMSUMS = 'pamsums'
   ANCILLARY_LAYERS = 'anclayers'
   PRESENCEABSENCE_LAYERS = 'palayers'
   MATRIX_LAYERS = 'mtxlayers'
   PROJECTIONS = 'projections'
   SCENARIOS = 'scenarios'
   SHAPEGRIDS = 'shpgrid'
   # Generic layers/layersets/matrices
   LAYERS = 'layers'
   LAYERSETS = 'layersets'
   MATRICES = 'matrices'
   
# ............................................................................
# Change to enum.Enum with Python 3.4
class LMServiceModule:
   LM = 'lm'
   SDM = 'sdm'
   RAD = 'rad'

# ............................................................................
# Lifemapper RAD constants
ORGANISM_LAYER_KEY = 'orgLayerIndices'
ENVIRONMENTAL_LAYER_KEY = 'envLayerIndices'

ID_PLACEHOLDER = '#id#'

GDALFormatCodes = {'AAIGrid': {'FILE_EXT': OutputFormat.ASCII, 
                               'DECIMAL_PRECISION': 6, 
                               'FORCE_CELLSIZE':'YES'},
                   'GTiff': {'FILE_EXT': OutputFormat.GTIFF},
                   'HFA': {'FILE_EXT': OutputFormat.HFA}
                   }
DEFAULT_PROJECTION_FORMAT = 'GTiff'
GDALDataTypes = (GDT_Unknown, GDT_Byte, GDT_UInt16, GDT_Int16, 
                 GDT_UInt32, GDT_Int32, GDT_Float32, GDT_Float64, 
                 GDT_CInt16, GDT_CInt32, GDT_CFloat32, GDT_CFloat64)
# OGR string constants supported here, and associated file extensions
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
BIOCLIM_PARAMETERS = {
                  'name': 'Bioclimatic Envelope Algorithm',
                  # output is 0, 0.5, 1.0
                  'isDiscreteOutput': True,
                  'outputFormat': 'GTiff',
                  'acceptsCategoricalMaps': False,
                  'parameters' : 
   {'StandardDeviationCutoff': 
   {'type': FloatType,
    'min': 0.0, 'default': 0.674, 'max': None } 
   } }
 
CSMBS_PARAMETERS = {'name': 'Climate Space Model - Broken-Stick Implementation',
                # TODO: discrete output?
                'isDiscreteOutput': False,
                'outputFormat': 'GTiff',
                'acceptsCategoricalMaps': False,
                'parameters' : 
   {
 'Randomisations': 
   {'type': IntType,
    'min': 1, 'default': 8, 'max': 1000 }
 ,
 'StandardDeviations':
   {'type': FloatType,
    'min': -10.0, 'default': 2.0, 'max': 10.0 } 
 ,
 'MinComponents': 
   {'type': IntType,
    'min': 1, 'default': 1, 'max': 20 }
 ,
 'VerboseDebugging': 
   {'type': IntType,
    'min': 0, 'default': 0, 'max': 1 }
   } }
ENVDIST_PARAMETERS = {'name': 'Environmental Distance',
                  # TODO: output is??
                  'isDiscreteOutput': False,
                  'outputFormat': 'GTiff',
                  'acceptsCategoricalMaps': False,
                  'parameters': 
   {
 'DistanceType':
   {'type': IntType,
    'min': 1, 'default': 1, 'max': 4 }
   ,
   'NearestPoints':
   {'type': IntType,
    'min': 0, 'default': 1, 'max': None }
 ,
 'MaxDistance':
   {'type': FloatType,
    'min': 0.1, 'default': 0.1, 'max': 1.0 }
   } }
 
ENVSCORE_PARAMETERS = {'name': 'Envelope Score',
                   # probability between 0.0 and 1.0
                   'isDiscreteOutput': False,
                   'outputFormat': 'GTiff',
                   'acceptsCategoricalMaps': False,
                   'parameters': { } }
 
GARP_PARAMETERS =  {'name': 'GARP (single run) - new openModeller implementation',
                'isDiscreteOutput': True,
                'outputFormat': 'GTiff',
                'acceptsCategoricalMaps': False,
                'parameters': {
 'MaxGenerations':
 {'type': IntType,
  'min': 1, 'default': 400, 'max': None }
 ,
 'ConvergenceLimit':
 {'type': FloatType,
  'min': 0.0, 'default': 0.01, 'max': 1.0 }
 ,
 'PopulationSize':
 {'type': IntType,
  'min': 1, 'default': 50, 'max': 500 }
 ,
 'Resamples':
 {'type': IntType,
  'min': 1, 'default': 2500, 'max': 100000 }
 } }
 
GARP_BS_PARAMETERS = {'name': 'GARP with Best Subsets - new openModeller implementation ',
                  'isDiscreteOutput': False,
                  'outputFormat': 'GTiff',
                  'acceptsCategoricalMaps': False,
                  'parameters': 
   {
 'TrainingProportion': 
    {'type': FloatType,
     'min': 0, 'default': 50, 'max': 100 }
    ,
    'TotalRuns': 
    {'type': IntType,
     'min': 0, 'default': 20, 'max': 10000 }
    ,
    'HardOmissionThreshold': 
    {'type': FloatType,
     'min': 0, 'default': 100, 'max': 100 }
    ,
    'ModelsUnderOmissionThreshold': 
    {'type': IntType,
     'min': 0, 'default': 20, 'max': 10000 }
    ,
    'CommissionThreshold': 
    {'type': FloatType,
     'min': 0, 'default': 50, 'max': 100 }
    ,
    'CommissionSampleSize': 
    {'type': IntType,
     'min': 1, 'default': 10000, 'max': None }
    ,
    'MaxThreads': 
    {'type': IntType,
     'min': 1, 'default': 1, 'max': 1024 }
    ,
    'MaxGenerations': 
    {'type': IntType,
     'min': 1, 'default': 400, 'max': None }
    ,
    'ConvergenceLimit': 
    {'type': FloatType,
     'min': 0.0, 'default': 0.1, 'max': 1.0 }
    ,
    'PopulationSize': 
    {'type': IntType,
     'min': 1, 'default': 50, 'max': 500 }
    ,
    'Resamples': 
    {'type': IntType,
     'min': 1, 'default': 2500, 'max': 10000 }
    } }
 
DG_GARP_PARAMETERS =  {'name': 'GARP (single run) - DesktopGARP implementation',
                   'isDiscreteOutput': True,
                   'outputFormat': 'GTiff',
                   'acceptsCategoricalMaps': False,
                   'parameters': GARP_PARAMETERS['parameters']}
 
 
DG_GARP_BS_PARAMETERS = {'name': 'GARP (single run) - DesktopGARP implementation',
                     'isDiscreteOutput': False,
                     'outputFormat': 'GTiff',
                     'acceptsCategoricalMaps': False,
                     'parameters': GARP_BS_PARAMETERS['parameters']}
 
ATT_MAXENT_PARAMETERS = {
      "name" : "Maximum Entropy (ATT Implementation)",
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
 
OM_MAXENT_PARAMETERS = {'name': 'Maximum Entropy (openModeller Implementation)',
                 'isDiscreteOutput': False,
                 'outputFormat': 'GTiff',
                 'acceptsCategoricalMaps': False,
                 'parameters': 
   {
    "NumberOfBackgroundPoints":
    {
     'type' : IntType,
     'min': 0,
     'default': 10000,
     'max': 10000
    },
    'UseAbsencesAsBackground':
    {
     'min': 0,
     'max': 1,
     'type': IntType,
     'default': 0
    },
    "IncludePresencePointsInBackground":
    {
     'min': 0,
     'max': 1,
     'type': IntType,
     'default': 1
    },
    "NumberOfIterations":
    {
     'min': 1,
     'max': None,
     'type': IntType,
     'default': 500
    },
    "TerminateTolerance":
    {
     'min': 0,
     'max': None,
     'type': FloatType,
     'default': 0.00001
    },
    "OutputFormat":
    {
     'min': 1,
     'max': 2,
     'type': IntType,
     'default': 2
    },
    "QuadraticFeatures":
    {
     'min': 0,
     'max': 1,
     'type': IntType,
     'default': 1
    },
    "ProductFeatures":
    {
     'min': 0,
     'max': 1,
     'type': IntType,
     'default': 1
    },
    "HingeFeatures":
    {
     'min': 0,
     'max': 1,
     'type': IntType,
     'default': 1
    },
    "ThresholdFeatures":
    {
     'min': 0,
     'max': 1,
     'type': IntType,
     'default': 1
    },
    "AutoFeatures":
    {
     'min': 0,
     'max': 1,
     'type': IntType,
     'default': 1
    },
    "MinSamplesForProductThreshold":
    {
     'min': 1,
     'max': None,
     'type': IntType,
     'default': 80
    },
    "MinSamplesForQuadratic":
    {
     'min': 1,
     'max': None,
     'type': IntType,
     'default': 10
    },
    "MinSamplesForHinge":
    {
     'min': 1,
     'max': None,
     'type': IntType,
     'default': 15
    }
    } }
 
SVM_PARAMETERS = {'name': 'SVM (Support Vector Machines)',
              'isDiscreteOutput': False,
              'outputFormat': 'GTiff',
              'acceptsCategoricalMaps': False,
              'parameters': 
   {
    'svmtype':
    {'type': IntType,
     'min': 0, 'default': 0, 'max': 2 }
    ,
    'KernelType':
    {'type': IntType,
     'min': 0, 'default': 2, 'max': 4 }
    ,
    'Degree':
    {'type': IntType,
     'min': 0, 'default': 3, 'max': None }
    ,
    'Gamma':
    {'type': FloatType,
     'min': None, 'default': 0.0, 'max': None }
    ,
    'Coef0':
    {'type': FloatType,
     'min': None, 'default': 0.0, 'max': None }
    ,
    'Cost':
    {'type': FloatType,
     'min': 0.001, 'default': 1.0, 'max': None }
    ,
    'Nu':
    {'type': FloatType, 
     'min': 0.001, 'default': 0.5, 'max': 1 }
    ,
    'ProbabilisticOutput':
    {'type': IntType,
     'min': 0, 'default': 1, 'max': 1 }
    ,
    'NumberOfPseudoAbsences':
    {'type': IntType,
     'min': 0, 'default': 0, 'max': None }
    } }
 
ANN_PARAMETERS = {'name': 'Artificial Neural Network',
              # TODO: discrete output?
              'isDiscreteOutput': False,
              'outputFormat': 'GTiff',
              'acceptsCategoricalMaps': False,
              'parameters': {
    'HiddenLayerNeurons':
    {'type': IntType,
     'min': 0, 'default': 8, 'max': None }
    ,
    'LearningRate':
    {'type': FloatType,
     'min': 0.0, 'default': 0.3, 'max': 1.0 }
    ,
    'Momentum':
    {'type': FloatType,
     'min': 0.0, 'default': 0.05, 'max': 1.0 }
    ,
    'Choice':
    {'type': IntType,
     'min': 0, 'default': 0, 'max': 1 }
    ,
    'Epoch':
    {'type': IntType,
     'min': 1, 'default': 100000, 'max': None }
    ,
    'MinimumError':
    {'type': FloatType,
     'min': 0.0, 'default': 0.01, 'max': 0.05 }
    } }
 
# TODO: Implement this!
ENFA_PARAMETERS = {'name': 'Ecological-Niche Factor Analysis',
               'isDiscreteOutput': None,
               'outputFormat': 'GTiff',
               'acceptsCategoricalMaps': False,
               'parameters': 
               {}
} 
 
# TODO: Implement this!
RNDFOREST_PARAMETERS = {'name': 'Random Forests',
               'isDiscreteOutput': None,
               'outputFormat': 'GTiff',
               'acceptsCategoricalMaps': False,
               'parameters': 
               {}
} 
 
AQUAMAPS_PARAMETERS = {'name': 'AquaMaps (beta version) ',
                   'isDiscreteOutput': False,
                   'outputFormat': 'GTiff',
                   'acceptsCategoricalMaps': False,
                   'parameters': 
   {'UseSurfaceLayers':
    {'type': IntType,
     'min': -1, 'default': -1, 'max': 1 }
    ,
    'UseDepthRange':
    {'type': IntType,
     'min': 0,  'default': 1, 'max': 1 }
    ,
    'UseIceConcentration':
    {'type': IntType,
     'min': 0, 'default': 1, 'max': 1 }
    ,
    'UseDistanceToLand':
    {'type': IntType,
     'min': 0, 'default': 1, 'max': 1 }
    ,
    'UsePrimaryProduction':
    {'type': IntType,
     'min': 0, 'default': 1, 'max': 1 }
    ,
    'usesalinity':
    {'type': IntType,
     'min': 0, 'default': 1, 'max': 1 }
    ,
    'usetemperature':
    {'type': IntType,
     'min': 0, 'default': 1, 'max': 1 }
    } }

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
