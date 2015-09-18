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
from osgeo.gdalconst import GDT_Unknown, GDT_Byte, GDT_UInt16, GDT_Int16, \
                            GDT_UInt32, GDT_Int32, GDT_Float32, GDT_Float64, \
                            GDT_CInt16, GDT_CInt32, GDT_CFloat32, GDT_CFloat64
from osgeo.ogr import wkbPoint, wkbLineString, wkbPolygon, wkbMultiPoint, \
                      wkbMultiLineString, wkbMultiPolygon
import os.path
from types import IntType, FloatType

from LmCommon.common.lmconstants import DEFAULT_EPSG, JobStatus, OutputFormat
from LmServer.common.localconstants import APP_PATH

LM_SCHEMA = 'lm3'
SALT = "4303e8f7129243fd42a57bcc18a19f5b"

# MAL, RAD, gbifCache database names
RAD_STORE = 'speco'
MAL_STORE = 'mal'
# ............................................................................
class DbUser:
   Map = 'mapuser'
   WebService = 'wsuser'
   Pipeline = 'sdlapp'
   Job = 'jobuser'

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
   RADExperiment = 201
   Bucket = 202
   OriginalPamSum = 203
   RandomPamSum = 204
   ShapeGrid = 205
   
   
# .............................................................................
class FeatureNames:
   """
   @summary: Shapefile feature names
   """
   SITE_ID = 'siteid'
   SITE_X = 'centerX'
   SITE_Y = 'centerY'
   CANONICAL_NAME = "canname"
   CATALOG_NUMBER = "catnum"
   COLLECTION_CODE = "collcode"
   COLLECTION_DATE = "colldate"
   COLLECTOR = "collectr"
   GBIF_LATITUDE = "lat"
   GBIF_LONGITUDE = "lon"
   GEOMETRY_WKT = "geomwkt"
   INSTITUTION_CODE = "instcode"
   LOCAL_ID = "localid"
   MODIFICATION_DATE = "moddate"
   NAME_KEY = "nmkey"
   OCCURRENCE_KEY = "occkey"
   PROVIDER_KEY = "provkey"
   PROVIDER_NAME = "provname"
   RESOURCE_KEY = "reskey"
   RESOURCE_NAME = "resname"
   URL = "url"
   USER_LATITUDE = "latitude"
   USER_LONGITUDE = "longitude"

ARCHIVE_DELETE_YEAR = 2014
ARCHIVE_DELETE_MONTH = 7
ARCHIVE_DELETE_DAY = 13

# Web directories
# TODO: See how many of these are still in use.  They should probably be 
#          constants in LmWebServer if they are still needed
IMAGE_PATH = 'image'
SESSION_PATH = 'sessions'
MAP_PATH = 'maps'
USER_DATA_PATH = 'fixme'

# Mapfile symbolizations
WEB_MODULES_PATH = 'LmWebServer'
WEB_PATH = os.path.join(WEB_MODULES_PATH, 'public_html')
SYMBOL_FILENAME = os.path.join(APP_PATH, WEB_PATH, MAP_PATH, 'symbols.txt')
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
# # Mapservice for OccurrenceSets and their Projections.
# SDM_MAP_PREFIX = 'data'
# # other/ancillary layer map prefix
# USER_MAP_PREFIX = 'usr'
# # RAD computed layer map prefix (not input)
# RAD_MAP_PREFIX = 'rad'
# ANC_MAP_PREFIX = 'anc'
# SCEN_MAP_PREFIX = 'scen'

class MapPrefix:
   SDM = 'data'
   USER = 'usr'
   # Computed map products for bucket
   RAD = 'rad'
   ANC = 'anc'
   SCEN = 'scen'
   
# # DATA FORMATS
# class OutputFormat:
# # ............................................................................
#    TAR_GZ = '.tar.gz'
#    TXT = '.txt'
#    XML = '.xml'
#    ZIP = '.zip'
#    TMP = '.tmp'
#    MAP = '.map'
#    CSV = '.csv'
#    JSON = '.json'
#    NUMPY = '.npy'
#    PICKLE = '.pkl'
#    TIFF = '.tif'
#    SHAPE = '.shp'

class LMFileType:
   # User level
   OTHER_MAP = 1
   # ..............................
   # SDM
   # User level
   ENVIRONMENTAL_LAYER = 101
   SCENARIO_MAP = 102
   # Experiment Level
   SDM_MAP = 110
   OCCURRENCE_FILE = 111
   OCCURRENCE_RAW_FILE = 112
   # Model level
   MODEL_REQUEST = 120
   MODEL_STATS = 121
   MODEL_RESULT = 122
   MODEL_ATT_RESULT = 123
   # Projection level
   PROJECTION_REQUEST = 130
   PROJECTION_PACKAGE = 131
   PROJECTION_LAYER = 132
   PROJECTION_ATT_LAYER = 133
   # Intersection level
   INTERSECT_VECTOR = 140
   # ..............................
   # RAD
   # User level
   SHAPEGRID = 201
   # Experiment Level
   ATTR_MATRIX = 210
   ATTR_TREE = 211
   # Bucket level
   BUCKET_MAP = 221
   # Uncompressed Intersections
   PAM = 222
   GRIM = 223
   # Species and Site indices  
   LAYER_INDICES = 224
   PRESENCE_INDICES = 225
   # PamSum level
   # Calculations on PAM (Original or Randomized Swap)
   COMPRESSED_PAM = 240
   SUM_CALCS = 241
   SUM_SHAPE = 242
   # Randomized Splotch  PamSums
   SPLOTCH_PAM = 243
   SPLOTCH_SITES = 244
   
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
             LMFileType.OCCURRENCE_FILE: OCC_PREFIX,
             LMFileType.OCCURRENCE_RAW_FILE: OCC_PREFIX,
             LMFileType.MODEL_REQUEST: 'modReq',
             LMFileType.MODEL_STATS: None,
             LMFileType.MODEL_RESULT: None,
             LMFileType.MODEL_ATT_RESULT: None,
             LMFileType.PROJECTION_REQUEST: 'projReq',
             LMFileType.PROJECTION_PACKAGE: PRJ_PREFIX,
             LMFileType.PROJECTION_LAYER: PRJ_PREFIX,
#              LMFileType.PROJECTION_ATT_LAYER: PRJ_PREFIX,
             LMFileType.INTERSECT_VECTOR: 'int',
             
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
             LMFileType.SPLOTCH_SITES: SPLOTCH_PREFIX
   }
   # Postfix
   EXTENSION = {LMFileType.OTHER_MAP: OutputFormat.MAP,
                LMFileType.ENVIRONMENTAL_LAYER: OutputFormat.GTIFF,
                LMFileType.SCENARIO_MAP: OutputFormat.MAP,
                LMFileType.SDM_MAP: OutputFormat.MAP,
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
                LMFileType.INTERSECT_VECTOR: OutputFormat.NUMPY,
                
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
                LMFileType.SPLOTCH_SITES: OutputFormat.PICKLE
   }
# SDM prefixes
# ENM filename info
# MODEL_REQUEST_PREFIX = 'modReq'
# PROJECTION_REQUEST_PREFIX = 'projReq'
# PROJ_PREFIX = 'prj_'

# RAD prefixes
# USER_RAD_PATH_PREFIX = 'rad'

# # TODO: Delete these when finished moving to EarlJr
# PRESENCE_INDEX_PREFIX = 'indices'
# LAYER_INDEX_PREFIX = 'lyridx_'
# SHAPEGRID_PREFIX = 'shpgrid_'
# ATTR_MATRIX_PREFIX = 'attrMtx_'
# ATTR_TREE_PREFIX = 'attrTree_'
# PAM_PREFIX = 'pam_'
# GRIM_PREFIX = 'grim_'
   
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
# Relative paths
LOG_PATH = 'log'
USER_LOG_PATH = 'users'
ERROR_LOG_PATH = 'errors'
POINT_PATH = 'points'
# These and 'species' are all subdirectories of DATA_PATH, in config.ini
MODEL_PATH = 'archive'
ENV_DATA_PATH = 'climate'
SPECIES_DATA_PATH = 'species'
USER_LAYER_PATH = 'Layers'

# Depth of path for archive SDM experiment data - this is the number of levels 
# that the occurrencesetid associated with a model and its projections 
# is split into i.e. occurrencesetid = 123456789 --> path 000/123/456/789/
MODEL_DEPTH = 4




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
   EXPERIMENTS = 'experiments'
   RAD_EXPERIMENTS = 'radexperiments'
   SDM_EXPERIMENTS = 'sdmexperiments'
   LAYERTYPES = 'typecodes'
   MODELS = 'models'
   OCCURRENCES = 'occurrences'
   PAMSUMS = 'pamsums'
   ANCILLARY_LAYERS = 'anclayers'
   PRESENCEABSENCE_LAYERS = 'palayers'
   PROJECTIONS = 'projections'
   SCENARIOS = 'scenarios'
   SHAPEGRIDS = 'shpgrid'
   # Generic layers/layersets
   LAYERS = 'layers'
   LAYERSETS = 'layersets'
   
# ............................................................................
# Change to enum.Enum with Python 3.4
class LMServiceModule:
   LM = 'lm'
   SDM = 'sdm'
   RAD = 'rad'

LM_MODULE = 'lm'
SDM_MODULE = 'sdm'
RAD_MODULE = 'rad'

# ............................................................................
# Lifemapper RAD constants
ORGANISM_LAYER_KEY = 'orgLayerIndices'
ENVIRONMENTAL_LAYER_KEY = 'envLayerIndices'

ID_PLACEHOLDER = '#id#'

# # {GDALDriverName: (ext, {Keyword Args/ create options})}
# GDALFormats = {'AAIGrid': ('.asc', {'DECIMAL_PRECISION': 6, 
#                                     'FORCE_CELLSIZE':'YES'}),
#                'GTiff': ('.tif', {}),
#                'HFA': ('.img', {})
#                }
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
