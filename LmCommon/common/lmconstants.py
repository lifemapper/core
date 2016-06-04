"""
@summary: Module containing common Lifemapper constants

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
try:
   from osgeo.ogr import OFTInteger, OFTReal, OFTString, OFTBinary
except:
   OFTInteger = 0
   OFTReal = 2
   OFTString = 4
   OFTBinary = 8
   
# .............................................................................
# .                               File constants                              .
# .............................................................................
# DATA FORMATS
MASK_TYPECODE = 'MASK'
ENCODING =  'utf-8'   

class OutputFormat:
# ............................................................................
   TAR_GZ = '.tar.gz'
   TXT = '.txt'
   XML = '.xml'
   ZIP = '.zip'
   TMP = '.tmp'
   MAP = '.map'
   CSV = '.csv'
   JSON = '.json'
   MXE = '.mxe'
   NUMPY = '.npy'
   PICKLE = '.pkl'
   GTIFF = '.tif'
   ASCII = '.asc'
   HFA = '.img'
   SHAPE = '.shp'
   LOG = '.log'
   MAKEFLOW = '.mf'
   
SHAPEFILE_EXTENSIONS = [".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".fbn", 
                        ".fbx", ".ain", ".aih", ".ixs", ".mxs", ".atx", 
                        ".shp.xml", ".cpg", ".qix"]
SHAPEFILE_MAX_STRINGSIZE = 255

# .............................................................................
# .                               Job constants                               .
# .............................................................................
class InputDataType:
   LM_LOWRES_CLIMATE = 1
   LM_HIRES_CLIMATE = 2
   EDAC = 3
   
   USER_PRESENCE_ABSENCE = 11
   USER_ANCILLARY = 12

class JobStage:
   """ 
   Constants to define the stages of any Job:
         RAD w/ Experiment, Bucket, or PamSum
         SDM w/ Model or Projection
         Common w/ General (exists) and Notify
   """
   # ==========================================================================   
   #           Valid Stage for All Jobs and Objects                     
   # ==========================================================================
   # This Job object has not yet been processed
   GENERAL = 0
   # ==========================================================================   
   #           Valid Stage for RAD Jobs and Objects                        
   # ==========================================================================
   # RADBuildShapegridJob
   BUILD_SHAPEGRID = 5
   # RADIntersectJob contains RADExperiment object
   INTERSECT = 10
   # _RadCompressJob contains Original or Splotch Pam 
   COMPRESS = 20
   # _RadSwapJob contains RADBucket and random PamSum
   SWAP = 31
   SPLOTCH = 32
   GRADY_FILL = 33
   # _RadCalculateJob contains original or random PamSum
   CALCULATE = 40
   # ==========================================================================   
   #           Valid Stage for SDM Jobs and Objects                     
   # ==========================================================================
   # SDMOccurrenceJob contains OccurrenceSet 
   OCCURRENCE = 105
   # SDMModelJob contains SDMModel 
   MODEL = 110
   # SDMProjectionJob contains SDMProjection 
   PROJECT = 120
   # ==========================================================================   
   #           Valid Stage for Notification Jobs and Objects                     
   # ==========================================================================
   # This Job object is complete, and the user must be notified
   NOTIFY = 500

class JobStatus:
   """ 
   @summary: Constants to define the status of a job
   """
   # Pull / Push job statuses.  Replaces older statuses
   GENERAL = 0
   INITIALIZE = 1
   PULL_REQUESTED = 90
   PULL_COMPLETE = 100
   ACQUIRING_INPUTS = 105
   COMPUTE_INITIALIZED = 110
   RUNNING = 120
   COMPUTED = 130
   PUSH_REQUESTED = 140
   PUSHED = 150
   PUSH_COMPLETE = 200
#    NOTIFY_READY = 210
   COMPLETE = 300
   
   # ==========================================================================   
   # =                             General Errors                             =
   # ==========================================================================
   # Not found in database, could be prior to insertion
   NOT_FOUND = 404

   GENERAL_ERROR = 1000
   UNKNOWN_ERROR = 1001
   DEPENDENCY_ERROR = 1002
   UNKNOWN_CLUSTER_ERROR = 1003
   PUSH_FAILED = 1100
   
   # Remote kill status.  This happens when something signals a stop
   REMOTE_KILL = 1150
   
   # ==========================================================================
   # =                              Common Errors                             =
   # ==========================================================================
   # Database
   # ............................................
   DB_CREATE_ERROR = 1201
   DB_DELETE_ERROR = 1202
   DB_INSERT_ERROR = 1203
   DB_READ_ERROR = 1204
   DB_UPDATE_ERROR = 1205
   
   # I/O
   # ............................................
   IO_READ_ERROR = 1301
   IO_WRITE_ERROR = 1302
   IO_WAIT_ERROR = 1303
   
   # ==========================================================================   
   # =                            Lifemapper Errors                           =
   # ==========================================================================
   #LM_GENERAL_ERROR = 2000 - conflicts with MODEL_ERROR and is not used.
   
   # Python errors
   # ............................................
   LM_PYTHON_ERROR = 2100
   LM_PYTHON_MODULE_IMPORT_ERROR = 2101
   LM_PYTHON_ATTRIBUTE_ERROR = 2102
   LM_PYTHON_EXPAT_ERROR = 2103

   # Lifemapper job errors
   # ............................................
   LM_JOB_ERROR = 2200
   LM_JOB_NOT_FOUND = 2201
   LM_JOB_NOT_READY = 2202
   LM_JOB_APPLICATION_NOT_FOUND = 2204
      
   # Lifemapper data errors
   # ............................................
   LM_DATA_ERROR = 2300
   LM_POINT_DATA_ERROR = 2301
   LM_RAW_POINT_DATA_ERROR = 2302
   
   # Lifemapper Pipeline errors
   LM_PIPELINE_ERROR = 2400
   LM_PIPELINE_WRITEFILE_ERROR = 2401
   LM_PIPELINE_WRITEDB_ERROR = 2402
   LM_PIPELINE_UPDATEOCC_ERROR = 2403

   LM_PIPELINE_DISPATCH_ERROR = 2415

   # ==========================================================================   
   # =                           SDM Errors                          =
   # ==========================================================================
   # General model error, previously 1002
   MODEL_ERROR = 2000
   # openModeller errors
   # ............................................
   OM_GENERAL_ERROR = 3000
      
   # Error in request file
   # ............................................
   OM_REQ_ERROR = 3100
      
   # Algorithm error
   # ............................................
   OM_REQ_ALGO_ERROR = 3110
   OM_REQ_ALGO_MISSING_ERROR = 3111
   OM_REQ_ALGO_INVALID_ERROR = 3112
      
   # Algorithm Parameter error
   # ............................................
   # ............................................
   OM_REQ_ALGOPARAM_ERROR = 3120
   OM_REQ_ALGOPARAM_MISSING_ERROR = 3121
   OM_REQ_ALGOPARAM_INVALID_ERROR = 3122
   OM_REQ_ALGOPARAM_OUT_OF_RANGE_ERROR = 3123
    
   # Layer error
   # ............................................
   OM_REQ_LAYER_ERROR = 3130
   OM_REQ_LAYER_MISSING_ERROR = 3131
   OM_REQ_LAYER_INVALID_ERROR = 3132
   OM_REQ_LAYER_BAD_FORMAT_ERROR = 3134
   OM_REQ_LAYER_BAD_URL_ERROR = 3135
    
   # Points error
   # ............................................
   OM_REQ_POINTS_ERROR = 3140
   OM_REQ_POINTS_MISSING_ERROR = 3141
   OM_REQ_POINTS_OUT_OF_RANGE_ERROR = 3143
   
   # Projection error
   # ............................................
   OM_REQ_PROJECTION_ERROR = 3150
   
   # Coordinate system error
   # ............................................
   OM_REQ_COORDSYS_ERROR = 3160
   OM_REQ_COORDSYS_MISSING_ERROR = 3161
   OM_REQ_COORDSYS_INVALID_ERROR = 3162
   
   # Error in openModeller execution
   # ............................................
   OM_EXEC_ERROR = 3200
   
   # Error generating model
   # ............................................
   OM_EXEC_MODEL_ERROR = 3210
   
   # Error generating projection
   # ............................................
   OM_EXEC_PROJECTION_ERROR = 3220
    
   # ............................................
   # Maxent errors
   # ............................................
   ME_GENERAL_ERROR = 3500
   
   # Maxent layer errors
   # ............................................
   ME_MISMATCHED_LAYER_DIMENSIONS = 3601
   ME_CORRUPTED_LAYER = 3602 # Could be issue with header or data
   ME_LAYER_MISSING = 3603
   ME_FILE_LOCK_ERROR = 3604
   
   # Maxent points issues
   # ............................................
   ME_POINTS_ERROR = 3740
   
   # Maxent configuration issues
   # ............................................
   ME_CONFIG_ERROR = 3750 # Base error status for problems with ME configuration

   # Not enough points to trigger any feature classes
   ME_NO_FEATURES_CLASSES_AVAILABLE = 3751 
   
   # Other Maxent problems
   # ............................................
   ME_HEAP_SPACE_ERROR = 3801
   
   # .......................................
   #  Occurrence set job errors
   # ...............................
   OCC_NO_POINTS_ERROR = 3901
   
   # ==========================================================================   
   # =                               HTTP Errors                              =
   # ==========================================================================
   # Last 3 digits are the http error code, only 400 and 500 levels listed
   HTTP_GENERAL_ERROR = 4000
      
   # Client error
   # ............................................
   HTTP_CLIENT_BAD_REQUEST = 4400
   HTTP_CLIENT_UNAUTHORIZED = 4401
   HTTP_CLIENT_FORBIDDEN = 4403
   HTTP_CLIENT_NOT_FOUND = 4404
   HTTP_CLIENT_METHOD_NOT_ALLOWED = 4405
   HTTP_CLIENT_NOT_ACCEPTABLE = 4406
   HTTP_CLIENT_PROXY_AUTHENTICATION_REQUIRED = 4407
   HTTP_CLIENT_REQUEST_TIMEOUT = 4408
   HTTP_CLIENT_CONFLICT = 4409
   HTTP_CLIENT_GONE = 4410
   HTTP_CLIENT_LENGTH_REQUIRED = 4411
   HTTP_CLIENT_PRECONDITION_FAILED = 4412
   HTTP_CLIENT_REQUEST_ENTITY_TOO_LARGE = 4413
   HTTP_CLIENT_REQUEST_URI_TOO_LONG = 4414
   HTTP_CLIENT_UNSUPPORTED_MEDIA_TYPE = 4415
   HTTP_CLIENT_REQUEST_RANGE_NOT_SATISFIABLE = 4416
   HTTP_CLIENT_EXPECTATION_FAILED = 4417

   # Server error
   # ............................................
   HTTP_SERVER_INTERNAL_SERVER_ERROR = 4500
   HTTP_SERVER_NOT_IMPLEMENTED = 4501
   HTTP_SERVER_BAD_GATEWAY = 4502
   HTTP_SERVER_SERVICE_UNAVAILABLE = 4503
   HTTP_SERVER_GATEWAY_TIMEOUT = 4504
   HTTP_SERVER_HTTP_VERSION_NOT_SUPPORTED = 4505
   
   # Not found in database, could be prior to insertion
   NOT_FOUND = 404
   
   # ==========================================================================   
   # =                             Database Errors                            =
   # ==========================================================================
   #   """
   #   Last digit meaning:
   #      0: General error
   #      1: Failed to read
   #      2: Failed to write
   #      3: Failed to delete
   #   """
   DB_GENERAL_ERROR = 5000
   
   # Job
   # ............................................
   DB_JOB_ERROR = 5100
   DB_JOB_READ_ERROR = 5101
   DB_JOB_WRITE_ERROR = 5102
   DB_JOB_DELETE_ERROR = 5103
   
   # Layer
   # ............................................
   DB_LAYER_ERROR = 5200
   DB_LAYER_READ_ERROR = 5201
   DB_LAYER_WRITE_ERROR = 5202
   DB_LAYER_DELETE_ERROR = 5203
   
   # Layer node
   # ............................................
   DB_LAYERNODE_ERROR = 5300
   DB_LAYERNODE_READ_ERROR = 5301
   DB_LAYERNODE_WRITE_ERROR = 5302
   DB_LAYERNODE_DELETE_ERROR = 5303
   
   # ==========================================================================   
   # =                                IO Errors                               =
   # ==========================================================================
   #   """
   #   Last digit meaning:
   #      0: General error
   #      1: Failed to read
   #      2: Failed to write
   #      3: Failed to delete
   #   """
   IO_GENERAL_ERROR = 6000
   IO_NOT_FOUND = 6001
   
   # Model
   # ............................................
   IO_MODEL_ERROR = 6100

   # Model request
   # ............................................
   IO_MODEL_REQUEST_ERROR = 6110
   IO_MODEL_REQUEST_READ_ERROR = 6111
   IO_MODEL_REQUEST_WRITE_ERROR = 6112
   IO_MODEL_REQUEST_DELETE_ERROR = 6113
   
   # Model script
   # ............................................
   IO_MODEL_SCRIPT_ERROR = 6120
   IO_MODEL_SCRIPT_READ_ERROR = 6121
   IO_MODEL_SCRIPT_WRITE_ERROR = 6122
   IO_MODEL_SCRIPT_DELETE_ERROR = 6123

   # Model output
   # ............................................
   IO_MODEL_OUTPUT_ERROR = 6130
   IO_MODEL_OUTPUT_READ_ERROR = 6131
   IO_MODEL_OUTPUT_WRITE_ERROR = 6132
   IO_MODEL_OUTPUT_DELETE_ERROR = 6133
   
   # Projection
   # ............................................
   IO_PROJECTION_ERROR = 6200

   # Projection request
   # ............................................
   IO_PROJECTION_REQUEST_ERROR = 6210
   IO_PROJECTION_REQUEST_READ_ERROR = 6211
   IO_PROJECTION_REQUEST_WRITE_ERROR = 6212
   IO_PROJECTION_REQUEST_DELETE_ERROR = 6213
   
   # Projection script
   # ............................................
   IO_PROJECTION_SCRIPT_ERROR = 6220
   IO_PROJECTION_SCRIPT_READ_ERROR = 6221
   IO_PROJECTION_SCRIPT_WRITE_ERROR = 6222
   IO_PROJECTION_SCRIPT_DELETE_ERROR = 6223

   # Projection output
   # ............................................
   IO_PROJECTION_OUTPUT_ERROR = 6230
   IO_PROJECTION_OUTPUT_READ_ERROR = 6231
   IO_PROJECTION_OUTPUT_WRITE_ERROR = 6232
   IO_PROJECTION_OUTPUT_DELETE_ERROR = 6233
   
   # Layer
   # ............................................
   IO_LAYER_ERROR = 6300
   IO_LAYER_READ_ERROR = 6301
   IO_LAYER_WRITE_ERROR = 6302
   IO_LAYER_DELETE_ERROR = 6303
   
   # Matrix
   # ............................................
   IO_MATRIX_ERROR = 6400
   IO_MATRIX_READ_ERROR = 6401
   IO_MATRIX_WRITE_ERROR = 6402
   IO_MATRIX_DELETE_ERROR = 6403

   # Pickled RAD Objects
   # ............................................
   IO_INDICES_ERROR = 6500
   IO_INDICES_READ_ERROR = 6501
   IO_INDICES_WRITE_ERROR = 6502
   IO_INDICES_DELETE_ERROR = 6503

   # Occurrence Set jobs
   # ............................................
   IO_OCCURRENCE_SET_ERROR = 6600
   IO_OCCURRENCE_SET_READ_ERROR = 6601
   IO_OCCURRENCE_SET_WRITE_ERROR = 6602
   IO_OCCURRENCE_SET_DELETE_ERROR = 6603

   # ==========================================================================   
   # =                               SGE Errors                               =
   # ==========================================================================
   SGE_GENERAL_ERROR = 7000
   SGE_BASH_ERROR = 7100

   # ==========================================================================   
   # =                           RAD Errors                                   =
   # ==========================================================================
   RAD_GENERAL_ERROR = 8000
  
   RAD_INTERSECT_ERROR = 8100
   RAD_INTERSECT_ZERO_LAYERS_ERROR = 8110

   RAD_COMPRESS_ERROR = 8200
   
   RAD_CALCULATE_ERROR = 8300
   RAD_CALCULATE_FAILED_TO_CREATE_SHAPEFILE = 8312
   
   RAD_SWAP_ERROR = 8400
   RAD_SWAP_TOO_FEW_COLUMNS_OR_ROWS_ERROR = 8410
   
   RAD_SPLOTCH_ERROR = 8500
   RAD_SPLOTCH_PYSAL_NEIGHBOR_ERROR = 8510

   RAD_SHAPEGRID_ERROR = 8600
   RAD_SHAPEGRID_INVALID_PARAMETERS = 8601
   RAD_SHAPEGRID_NO_CELLS = 8610

   # ==========================================================================   
   #                               Compute Status                             =
   # ==========================================================================
   # 301000-301999  - Process (3) openModeller Model SDM (01)   
   # ............................................
   # Error in request file
   OM_MOD_REQ_ERROR = 301100
   # Algorithm error
   OM_MOD_REQ_ALGO_INVALID_ERROR = 301112
   # Algorithm Parameter error
   OM_MOD_REQ_ALGOPARAM_MISSING_ERROR = 301121
   # Layer error
   OM_MOD_REQ_LAYER_ERROR = 301130
   # Points error
   OM_MOD_REQ_POINTS_MISSING_ERROR = 301141
   OM_MOD_REQ_POINTS_OUT_OF_RANGE_ERROR = 301143
   
   # 301000-301999  - Process (3) openModeller SDM Projection (02)   
   # ............................................
   OM_PROJECTION_ERROR = 302150 
   
   # ............................................
   @staticmethod
   def waiting(stat):
      if stat == JobStatus.GENERAL or stat == JobStatus.INITIALIZE:
         return True
      else:
         return False

   @staticmethod
   def inProcess(stat):
      if stat > JobStatus.INITIALIZE and stat < JobStatus.COMPLETE:
         return True
      else:
         return False

   @staticmethod
   def finished(stat):
      if stat >= JobStatus.COMPLETE:
         return True
      else:
         return False

   @staticmethod
   def failed(stat):
      if stat == JobStatus.NOT_FOUND or stat >= JobStatus.GENERAL_ERROR:
         return True
      else:
         return False

# ............................................................................
# Aka reqSoftware in LmJob table
class ProcessType:
   # .......... SDM ..........
   ATT_MODEL = 110
   ATT_PROJECT = 120
   OM_MODEL = 210
   OM_PROJECT = 220
   # .......... RAD ..........
   RAD_BUILDGRID = 305
   RAD_INTERSECT = 310
   RAD_COMPRESS = 320
   RAD_SWAP = 331
   RAD_SPLOTCH = 332
   RAD_GRADY = 333
   RAD_CALCULATE = 340
   # .......... Occurrences ..........
   GBIF_TAXA_OCCURRENCE = 405
   BISON_TAXA_OCCURRENCE = 410
   IDIGBIO_TAXA_OCCURRENCE = 415
   # .......... User-defined ..........
   USER_TAXA_OCCURRENCE = 420
   # .......... Notify ..........
   SMTP = 510
   
   @staticmethod
   def sdmTypes():
      return [ProcessType.SMTP, ProcessType.ATT_MODEL, ProcessType.ATT_PROJECT, 
              ProcessType.OM_MODEL, ProcessType.OM_PROJECT, 
              ProcessType.GBIF_TAXA_OCCURRENCE, 
              ProcessType.BISON_TAXA_OCCURRENCE, 
              ProcessType.IDIGBIO_TAXA_OCCURRENCE,
              ProcessType.USER_TAXA_OCCURRENCE]
      
   @staticmethod
   def isSDM(ptype):
      if ptype in ProcessType.sdmTypes():
         return True
      return False
      
   @staticmethod
   def radTypes():
      return [ProcessType.SMTP, ProcessType.RAD_BUILDGRID, 
              ProcessType.RAD_INTERSECT, ProcessType.RAD_COMPRESS, 
              ProcessType.RAD_SWAP, ProcessType.RAD_SPLOTCH, 
              ProcessType.RAD_CALCULATE, ProcessType.RAD_GRADY]

   @staticmethod
   def isRAD(ptype):
      if ptype in ProcessType.radTypes():
         return True
      return False

   @staticmethod
   def randomTypes():
      return [ProcessType.RAD_SWAP, ProcessType.RAD_SPLOTCH, 
              ProcessType.RAD_GRADY]
   
   @staticmethod
   def isRandom(ptype):
      if ptype in ProcessType.randomTypes():
         return True
      return False
   
# .............................................................................
# .                               RAD constants                               .
# .............................................................................
class RandomizeMethods:
   NOT_RANDOM = 0
   SWAP = 1
   SPLOTCH = 2
   GRADY = 3

# .............................................................................
# .                             Service constants                             .
# .............................................................................
BUCKETS_SERVICE = 'buckets'
EXPERIMENTS_SERVICE = 'experiments'
RAD_EXPERIMENTS_SERVICE = 'radexperiments'
SDM_EXPERIMENTS_SERVICE = 'sdmexperiments'
LAYERS_SERVICE = 'layers'
LAYERTYPES_SERVICE = 'typecodes'
MODELS_SERVICE = 'models'
OCCURRENCES_SERVICE = 'occurrences'
PAMSUMS_SERVICE = 'pamsums'
ANCILLARY_LAYERS_SERVICE = 'anclayers'
PRESENCEABSENCE_LAYERS_SERVICE = 'palayers'
PROJECTIONS_SERVICE = 'projections'
SCENARIOS_SERVICE = 'scenarios'

# Generic layersets, not Scenarios
LAYERSETS_SERVICE = 'layersets'
# Biogeography Tools
SHAPEGRIDS_SERVICE = 'shpgrid'

# .............................................................................
# .                              Time constants                               .
# .............................................................................
# Time constants in Modified Julian Day (MJD) units 
ONE_MONTH = 1.0 * 30
ONE_DAY = 1.0
ONE_HOUR = 1.0/24.0
ONE_MIN = 1.0/1440.0
ONE_SEC = 1.0/86400.0

# Time formats
ISO_8601_TIME_FORMAT_FULL = "%Y-%m-%dT%H:%M:%SZ"
ISO_8601_TIME_FORMAT_TRUNCATED = "%Y-%m-%d"
YMD_HH_MM_SS = "%Y-%m-%d %H:%M%S"

# .............................................................................
# .                               User constants                              .
# .............................................................................
DEFAULT_POST_USER = 'anon'

# .............................................................................
# .                             Instance constants                            .
# .............................................................................
LM_CLIENT_VERSION_URL = "http://svc.lifemapper.org/clients/versions.xml"
LM_INSTANCES_URL = "http://svc.lifemapper.org/clients/instances.xml"

class Instances:
   """
   @summary: These are Lifemapper instances that we know how to work with 
                externally
   """
   IDIGBIO = "IDIGBIO"
   BISON = "BISON"
   GBIF = "GBIF"
   CHARLIE = "Charlie"
   LIFEMAPPER = "Lifemapper"

DWC_QUALIFIER = 'dwc:'
class DWCNames:
   OCCURRENCE_ID = {'FULL': 'occurrenceID', 'SHORT': 'occurid'}
   INSTITUTION_CODE = {'FULL': 'institutionCode', 'SHORT': 'inst_code'}
   COLLECTION_CODE = {'FULL': 'collectionCode', 'SHORT': 'coll_code'}
   CATALOG_NUMBER = {'FULL': 'catalogNumber', 'SHORT': 'catnum'}
   BASIS_OF_RECORD = {'FULL': 'basisOfRecord', 'SHORT': 'basisofrec'}
   DECIMAL_LATITUDE = {'FULL': 'decimalLatitude', 'SHORT': 'dec_lat'}
   DECIMAL_LONGITUDE = {'FULL': 'decimalLongitude', 'SHORT': 'dec_long'}
   SCIENTIFIC_NAME = {'FULL': 'scientificName', 'SHORT': 'sciname'}
   DAY = {'FULL': 'day', 'SHORT': 'day'}
   MONTH = {'FULL': 'month', 'SHORT': 'month'}
   YEAR = {'FULL': 'year', 'SHORT': 'year'}
   RECORDED_BY = {'FULL': 'recordedBy', 'SHORT': 'rec_by'}
   COUNTRY_CODE = {'FULL': 'countryCode', 'SHORT': 'ctrycode'}

# Bison
BISON_COUNT_KEYS = ['response', 'numFound']
  
# ......................................................
# For parsing GBIF data download, Jan 2015 and beyond
# Dictionary key is column number; value is (column name, datatype)
# occColnames = ['gbifId', 'occurrenceID_dwc', 'taxonKey', 'datasetKey', 
#                'publishingOrgKey', 'basisOfRecord', 'kingdomKey', 'phylumKey', 
#                'classKey', 'orderKey', 'familyKey', 'genusKey', 'speciesKey', 
#                'scientificName', 'decimalLatitude', 'decimalLongitude', 'day', 
#                'month', 'year', 'recordedBy']

# ......................................................
GBIF_TAXONKEY_FIELD = 'specieskey'
GBIF_TAXONNAME_FIELD = DWCNames.SCIENTIFIC_NAME['SHORT']
GBIF_PROVIDER_FIELD = 'puborgkey'
GBIF_ID_FIELD = 'gbifid'

GBIF_TAXON_FIELDS = {0: ('taxonkey', OFTString), 
                     1: ('kingdom', OFTString),
                     2: ('phylum', OFTString),
                     3: ('class', OFTString), 
                     4: ('order', OFTString),
                     5: ('family', OFTString),
                     6: ('genus', OFTString),
                     7: (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                     8: ('genuskey', OFTInteger),
                     9: (GBIF_TAXONKEY_FIELD, OFTInteger),
                     10:('count', OFTInteger)
                     }

GBIF_EXPORT_FIELDS = {0: (GBIF_ID_FIELD, OFTInteger), 
                      1: (DWCNames.OCCURRENCE_ID['SHORT'], OFTInteger), 
                      2: ('taxonkey', OFTInteger),
                      3: ('datasetkey', OFTString),
                      4: (GBIF_PROVIDER_FIELD, OFTString),
                      5: (DWCNames.BASIS_OF_RECORD['SHORT'], OFTString),
                      6: ('kingdomkey', OFTInteger),
                      7: ('phylumkey', OFTInteger),
                      8: ('classkey', OFTInteger),
                      9: ('orderkey', OFTInteger),
                      10: ('familykey', OFTInteger), 
                      11: ('genuskey', OFTInteger),
                      12: (GBIF_TAXONKEY_FIELD, OFTInteger),
                      13: (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                      14: (DWCNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
                      15: (DWCNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
                      16: (DWCNames.DAY['SHORT'], OFTInteger),
                      17: (DWCNames.MONTH['SHORT'], OFTInteger),
                      18: (DWCNames.YEAR['SHORT'], OFTInteger),
                      19: (DWCNames.RECORDED_BY['SHORT'], OFTString),
                      20: (DWCNames.INSTITUTION_CODE['SHORT'], OFTString),
                      21: (DWCNames.COLLECTION_CODE['SHORT'], OFTString),
                      22: (DWCNames.CATALOG_NUMBER['SHORT'], OFTString),
                    }

# .............................................................................
# .                               GBIF constants                              .
# .............................................................................
# seconds to wait before retrying unresponsive services
GBIF_WAIT_TIME = 3 * ONE_MIN
GBIF_LIMIT = 300
GBIF_REST_URL = 'http://api.gbif.org/v1'
GBIF_SPECIES_SERVICE = 'species'
GBIF_OCCURRENCE_SERVICE = 'occurrence'
GBIF_DATASET_SERVICE = 'dataset'
GBIF_ORGANIZATION_SERVICE = 'organization'

GBIF_REQUEST_SIMPLE_QUERY_KEY = 'q'
GBIF_REQUEST_NAME_QUERY_KEY = 'name'
GBIF_REQUEST_TAXON_KEY = 'TAXON_KEY'
GBIF_REQUEST_RANK_KEY = 'rank'
GBIF_REQUEST_DATASET_KEY = 'dataset_key'                

GBIF_DATASET_BACKBONE_VALUE = 'GBIF Backbone Taxonomy'

GBIF_SEARCH_COMMAND = 'search'
GBIF_COUNT_COMMAND = 'count'
GBIF_MATCH_COMMAND = 'match'
GBIF_DOWNLOAD_COMMAND = 'download'
GBIF_DOWNLOAD_REQUEST_COMMAND = 'request'

GBIF_QUERY_PARAMS = {GBIF_SPECIES_SERVICE: {'status': 'ACCEPTED',
                                            GBIF_REQUEST_RANK_KEY: None,
                                            GBIF_REQUEST_DATASET_KEY: None,
                                            GBIF_REQUEST_NAME_QUERY_KEY: None},
                     GBIF_OCCURRENCE_SERVICE: {"GEOREFERENCED": True,
                                               "SPATIAL_ISSUES": False,
#                                                "BASIS_OF_RECORD": ["PRESERVED_SPECIMEN"],
                                               GBIF_REQUEST_TAXON_KEY: None},
                     GBIF_DOWNLOAD_COMMAND: {"creator": "aimee",
                                             "notification_address": ["lifemapper@mailinator.com"]}
                     }


GBIF_RESPONSE_IDENTIFIER_KEY = 'key'
GBIF_RESPONSE_RESULT_KEY = 'results'
GBIF_RESPONSE_END_KEY = 'endOfRecords'
GBIF_RESPONSE_COUNT_KEY = 'count'
GBIF_RESPONSE_GENUS_ID_KEY = 'genusKey'
GBIF_RESPONSE_GENUS_KEY = 'genus'
GBIF_RESPONSE_SPECIES_ID_KEY = 'speciesKey'
GBIF_RESPONSE_SPECIES_KEY = 'species'
GBIF_RESPONSE_MATCH_KEY = 'matchType'
GBIF_RESPONSE_NOMATCH_VALUE = 'NONE'

# For writing files from GBIF DarwinCore download, 
# DWC translations in lmCompute/code/sdm/gbif/constants
# We are adding the 2 fields: LM_WKT_FIELD and GBIF_LINK_FIELD
GBIF_LINK_FIELD = 'gbifurl'
GBIF_OCCURRENCE_URL = 'http://www.gbif.org/occurrence'

# .............................................................................
# .                               BISON/ITIS constants                              .
# .............................................................................
# ......................................................
# For parsing BISON Solr API response, updated Feb 2015
# ......................................................
BISON_OCCURRENCE_URL = 'http://bison.usgs.ornl.gov/solrproduction/occurrences/select'
# For TSN query filtering on Binomial
BISON_NAME_KEY = 'ITISscientificName'
# For Occurrence query by TSN in hierarchy
BISON_HIERARCHY_KEY = 'hierarchy_homonym_string'
BISON_KINGDOM_KEY = 'kingdom'
BISON_TSN_KEY = 'TSNs'
# key = returned field name; val = (lmname, ogr type)
BISON_RESPONSE_FIELDS = {
                        'ITIScommonName': ('comname', OFTString),
                        BISON_NAME_KEY: (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                        'ITIStsn': ('itistsn', OFTInteger),
                        BISON_TSN_KEY: None,
                        'ambiguous': None,
                        DWCNames.BASIS_OF_RECORD['FULL']: 
                           (DWCNames.BASIS_OF_RECORD['SHORT'], OFTString),
                        'calculatedCounty': ('county', OFTString),
                        'calculatedState': ('state', OFTString),
                        DWCNames.CATALOG_NUMBER['FULL']: 
                           (DWCNames.CATALOG_NUMBER['SHORT'], OFTString),
                        'collectionID': ('coll_id', OFTString),
                        'computedCountyFips': None,
                        'computedStateFips': None,
                        DWCNames.COUNTRY_CODE['FULL']: 
                           (DWCNames.COUNTRY_CODE['SHORT'], OFTString),
                        DWCNames.DECIMAL_LATITUDE['FULL']: 
                           (DWCNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
                        DWCNames.DECIMAL_LONGITUDE['FULL']:
                           (DWCNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
                        'eventDate':('date', OFTString),
                        # Space delimited, same as latlon
                        'geo': None,
                        BISON_HIERARCHY_KEY: ('tsn_hier', OFTString),
                        'institutionID': ('inst_id', OFTString),
                        BISON_KINGDOM_KEY: ('kingdom', OFTString),
                        # Comma delimited, same as geo
                        'latlon': ('latlon', OFTString),
                        DWCNames.OCCURRENCE_ID['FULL']: 
                           (DWCNames.OCCURRENCE_ID['SHORT'], OFTInteger),
                        'ownerInstitutionCollectionCode': 
                           (DWCNames.INSTITUTION_CODE['SHORT'], OFTString),
                        'pointPath': None,
                        'providedCounty': None,
                        'providedScientificName': None,
                        'provider': ('provider', OFTString),
                        'providerID': None,
                        DWCNames.RECORDED_BY['FULL']: 
                           (DWCNames.RECORDED_BY['SHORT'], OFTString),
                        'resourceID': None,
                        # Use ITIS Scientific Name
                        'scientificName': None,
                        'stateProvince': ('stprov', OFTString),
                        DWCNames.YEAR['SHORT']: 
                           (DWCNames.YEAR['SHORT'], OFTInteger),
                        # Very long integer
                       '_version_': None
                        }

BISON_MIN_POINT_COUNT = 20
BISON_MAX_POINT_COUNT = 5000000
BISON_BBOX = (24, -125, 50, -66)

BISON_BINOMIAL_REGEX = '/[A-Za-z]*[ ]{1,1}[A-Za-z]*/'
BISON_TSN_FILTERS = {'facet': True,
                     'facet.limit': -1,
                     'facet.mincount': BISON_MIN_POINT_COUNT,
                     'facet.field': BISON_TSN_KEY, 
                     'rows': 0}

BISON_OCC_FILTERS = {'rows': BISON_MAX_POINT_COUNT}


# Common Q Filters
BISON_QFILTERS = {'decimalLatitude': (BISON_BBOX[0], BISON_BBOX[2]),
                   'decimalLongitude': (BISON_BBOX[1], BISON_BBOX[3]),
                   'basisOfRecord': [(False, 'living'), (False, 'fossil')]}
# Common Other Filters
BISON_FILTERS = {'wt': 'json', 
                 'json.nl': 'arrarr'}

# Expected Response Dictionary Keys
BISON_RECORD_KEYS = ['response', 'docs']
BISON_TSN_LIST_KEYS = ['facet_counts', 'facet_fields', BISON_TSN_KEY]

ITIS_DATA_NAMESPACE = 'http://data.itis_service.itis.usgs.gov/xsd'
# Basic Web Services
ITIS_TAXONOMY_HIERARCHY_URL = 'http://www.itis.gov/ITISWebService/services/ITISService/getFullHierarchyFromTSN'
# JSON Web Services
# ITIS_TAXONOMY_HIERARCHY_URL = 'http://www.itis.gov/ITISService/jsonservice/getFullHierarchyFromTSN'
ITIS_TAXONOMY_KEY = 'tsn'
ITIS_HIERARCHY_TAG = 'hierarchyList'
ITIS_RANK_TAG = 'rankName'
ITIS_TAXON_TAG = 'taxonName'
ITIS_KINGDOM_KEY = 'Kingdom'
ITIS_PHYLUM_DIVISION_KEY = 'Division'
ITIS_CLASS_KEY = 'Class'
ITIS_ORDER_KEY = 'Order'
ITIS_FAMILY_KEY = 'Family'
ITIS_GENUS_KEY = 'Genus'
ITIS_SPECIES_KEY = 'Species'

# .............................................................................
# .                           iDigBio constants                               .
# .............................................................................
IDIGBIO_URL_PREFIX = 'https://beta-search.idigbio.org/v2'
IDIGBIO_SEARCH_POSTFIX ='search'
IDIGBIO_OCCURRENCE_POSTFIX = 'records'
IDIGBIO_PUBLISHERS_POSTFIX = 'publishers'
IDIGBIO_RECORDSETS_POSTFIX = 'recordsets'
# IDIGBIO_RECORDSETS_SEARCH_URL_PREFIX="http://search.idigbio.org/idigbio/recordsets/_search"

IDIGBIO_SEARCH_LIMIT = 1000000

IDIGBIO_ID_FIELD = 'uuid'
IDIGBIO_LINK_FIELD = 'idigbiourl'
IDIGBIO_EXPORT_FIELDS = {0: (IDIGBIO_ID_FIELD, OFTString), 
                         1: (DWCNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
                         2: (DWCNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
                         3: (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                         4: ('provider', OFTString)
                         }
IDIGBIO_GBIFID_FIELD = 'taxonid'
# Geopoint.lat and Geopoint.lon are modified on return to short names
# Response record fields: https://beta-search.idigbio.org/v2/meta/fields/records
IDIGBIO_RETURN_FIELDS = {
                        IDIGBIO_ID_FIELD: (IDIGBIO_ID_FIELD, OFTString),
                        IDIGBIO_GBIFID_FIELD: (IDIGBIO_GBIFID_FIELD, OFTInteger),
                        'canonicalname': ('canonical', OFTString),
#                         'kingdom': ('kingdom', OFTString),
#                         'phylum': ('phylum', OFTString),
#                         'class': ('class', OFTString), 
#                         'order': ('order', OFTString),
#                         'family': ('family', OFTString),
                        'genus': ('genus', OFTString),
                        'scientificname': 
                           (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),                        
                        'basisofrecord':  
                           (DWCNames.BASIS_OF_RECORD['SHORT'], OFTString),
#                         'catalognumber': 
#                            (DWCNames.CATALOG_NUMBER['SHORT'], OFTString),
#                         'collectionid': ('coll_id', OFTString),
#                         'collectioncode': 
#                            (DWCNames.COLLECTION_CODE['SHORT'], OFTString),
#                         'collectionname': ('coll_name', OFTString),
#                         'collector': ('collector', OFTString),
#                         'commonname': ('comname', OFTString),
#                         'continent': ('continent', OFTString),
#                         'country': ('country', OFTString),
#                         'countrycode': 
#                            (DWCNames.COUNTRY_CODE['SHORT'], OFTString),
#                         'county': ('county', OFTString),
                        'datecollected':('date_coll', OFTString),
                        'eventdate':('date', OFTString),
                        'institutioncode': 
                           (DWCNames.INSTITUTION_CODE['SHORT'], OFTString),
                        'institutionid': ('inst_id', OFTString),
                        'institutionname': ('inst_name', OFTString),
                        'occurrenceid': 
                           (DWCNames.OCCURRENCE_ID['SHORT'], OFTInteger),
                        'stateprovince': ('stprov', OFTString),
#                         DWCNames.DECIMAL_LATITUDE['SHORT']: 
#                            (DWCNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
#                         DWCNames.DECIMAL_LONGITUDE['SHORT']:
#                            (DWCNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
                        }

IDIGBIO_BINOMIAL_REGEX = "(^[^ ]*) ([^ ]*)$"

IDIGBIO_OCCURRENCE_ITEMS_KEY = 'items'
IDIGBIO_RECORD_CONTENT_KEY = 'data'
IDIGBIO_RECORD_INDEX_KEY = 'indexTerms'

IDIGBIO_QFILTERS = {
                     'basisofrecord': 'preservedspecimen',
                    }
queryFlds = IDIGBIO_RETURN_FIELDS.keys()
queryFlds.append('geopoint')

IDIGBIO_FILTERS = {'fields': queryFlds,
                   'limit': IDIGBIO_SEARCH_LIMIT,
                   'offset': 0,
                   'no_attribution': True}

# .............................................................................
# .                  Provider/Local data fieldname constants                              .
# .............................................................................
LM_ID_FIELD = 'lmid'
LM_WKT_FIELD = 'geomwkt'

# .............................................................................
# .                              Other constants                              .
# .............................................................................
# TODO: replace hardcoded vars in code
DEFAULT_OGR_FORMAT = 'ESRI Shapefile'
DEFAULT_GDAL_FORMAT = 'GTiff'

DEFAULT_EPSG = 4326
DEFAULT_MAPUNITS = 'dd'
LegalMapUnits = ['feet', 'inches', 'kilometers', 'meters', 'miles', 'dd', 'ds']

URL_ESCAPES = [ [" ", "%20"], [",", "%2C"] ]

class HTTPStatus:
   """
   @summary: HTTP 1.1 Status Codes as defined by 
                http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
   """
   # Informational 1xx
   CONTINUE = 100
   SWITCHING_PROTOCOLS = 101
   
   # Successful 2xx
   OK = 200
   CREATED = 201
   ACCEPTED = 202
   NON_AUTHORITATIVE_INFORMATION = 203
   NO_CONTENT = 204
   RESET_CONTENT = 205
   PARTIAL_CONTENT = 206
   
   # Redirectional 3xx
   MULTIPLE_CHOICES = 300
   MOVED_PERMANENTLY = 301
   FOUND = 302
   SEE_OTHER = 303
   NOT_MODIFIED = 204
   USE_PROXY = 305
   TEMPORARY_REDIRECT = 307
   
   # Client Error 4xx
   BAD_REQUEST = 400
   UNAUTHORIZED = 401
   FORBIDDEN = 403
   NOT_FOUND = 404
   METHOD_NOT_ALLOWED = 405
   NOT_ACCEPTABLE = 406
   PROXY_AUTHENTICATION_REQUIRED = 407
   REQUEST_TIMEOUT = 408
   CONFLICT = 409
   GONE = 410
   LENGTH_REQUIRED = 411
   PRECONDITION_FAILED = 412
   REQUEST_ENTITY_TOO_LARGE = 413
   REQUEST_URI_TOO_LONG = 414
   UNSUPPORTED_MEDIA_TYPE = 415
   REQUEST_RANGE_NOT_SATISFIABLE = 416
   EXPECTATION_FAILED = 417
   
   # Server Error 5xx
   INTERNAL_SERVER_ERROR = 500
   NOT_IMPLEMENTED = 501
   BAD_GATEWAY = 502
   SERVICE_UNAVAILABLE = 503
   GATEWAY_TIMEOUT = 504
   HTTP_VERSION_NOT_SUPPORTED = 505
   
# .............................................................................
# .                            Namespace constants                            .
# .............................................................................
# Lifemapper Namespace constants
LM_NAMESPACE = "http://lifemapper.org"
LM_NS_PREFIX = "lm"
LM_RESPONSE_SCHEMA_LOCATION = "/schemas/serviceResponse.xsd"
LM_PROC_NAMESPACE = "http://lifemapper.org/process"
LM_PROC_NS_PREFIX = "lmProc"
LM_PROC_SCHEMA_LOCATION = "/schemas/lmProcess.xsd"

# .............................................................................
# .                             Logging constants                             .
# .............................................................................
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
