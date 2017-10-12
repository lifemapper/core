"""
@summary: Module containing common Lifemapper constants

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
try:
   from osgeo.ogr import OFTInteger, OFTReal, OFTString, OFTBinary
except:
   OFTInteger = 0 
   OFTReal = 2 
   OFTString = 4
   OFTBinary = 8
   
# .............................................................................
# .    Configuration file headings
# .............................................................................
SERVER_BOOM_HEADING = 'BOOM'
SERVER_ENV_HEADING = 'LmServer - environment'
SERVER_PIPELINE_HEADING = 'LmServer - pipeline'
SERVER_DB_HEADING = 'LmServer - dbserver'
SERVER_COMPUTE_HEADING = 'LmServer - registeredcompute'
SERVER_MATT_DAEMON_HEADING = 'LmServer - Matt Daemon'

COMPUTE_ENV_HEADING = 'LmCompute - environment'
COMPUTE_CMDS_HEADING = 'LmCompute - commands'
COMPUTE_CONTACT_HEADING = 'LmCompute - contact'
COMPUTE_OPTIONS_HEADING = 'LmCompute - options'
COMPUTE_METRICS_HEADING = 'LmCompute - metrics'
COMPUTE_ME_PLUGIN_HEADING = 'LmCompute - plugins - maxent'
   
# .............................................................................
# .    Normal user                              .
# .............................................................................
LM_USER = 'lmwriter'

# .............................................................................
# .    Directories shared between LmCompute and LmServer                              .
# .............................................................................
ENV_LAYER_DIR = 'layers'

# .............................................................................
# .                               File constants                              .
# .............................................................................
# DATA FORMATS
MASK_TYPECODE = 'MASK'
ENCODING =  'utf-8'

# .............................................................................
class FileFormat:
   """
   @summary: This class contains file format meta information
   @todo: Add GDAL / OGR type as optional parameters
   """
   # ...........................
   def __init__(self, extension, mimeType, allExtensions=None, driver=None,
                options=None, default=False):
      """
      @summary: Constructor
      @param extension: This is the primary extension if a format has multiple 
                        files
      @param mimeType: The MIME-Type for this format
      @param allExtensions: List of all possible extensions for this format
      @param driver: GDAL or OGR driver to use when reading this format
      """
      self._mimeType = mimeType
      self.ext = extension
      self.driver = driver
      self.options = options
      self.isDefault = default
      self._extensions = allExtensions
      if self._extensions is None:
         self._extensions = []
      # Add the default extension to the extensions list if not present
      if self.ext not in self._extensions:
         self._extensions.append(self.ext)
   
   # ...........................
   def getExtensions(self):
      return self._extensions
   
   # ...........................
   def getMimeType(self):
      return self._mimeType
   
# .............................................................................
class LMFormat:
   """
   @summary: Class containing known formats to Lifemapper
   @todo: Deprecate OutputFormat and instead use this
   """
   ASCII = FileFormat('.asc', 'text/plain', allExtensions=['.asc', '.prj'],
                      driver='AAIGrid',
                      options={'DECIMAL_PRECISION': 6, 
                               'FORCE_CELLSIZE':'YES'})
   CSV = FileFormat('.csv', 'text/csv',
                    driver='CSV')
   CONFIG = FileFormat('.ini', 'text/plain')
   GTIFF = FileFormat('.tif', 'image/tiff', driver='GTiff', default=True)
   HFA = FileFormat('.img', 'image/octet-stream', driver='HFA')
   JSON = FileFormat('.json', 'application/json')
   GEO_JSON = FileFormat('.geojson', 'application/vnd.geo+json')
   KML = FileFormat('.kml', 'application/vnd.google-earth.kml+xml')
   LOG = FileFormat('.log', 'text/plain')
   MAKEFLOW = FileFormat('.mf', 'text/plain')
   MAP = FileFormat('.map', 'text/plain')
   METADATA = FileFormat('.meta', 'text/plain')
   MXE = FileFormat('.mxe', 'application/octet-stream')
   NEWICK = FileFormat('.tre', 'text/plain', allExtensions=['.tre', '.nhx'])
   NUMPY = FileFormat('.npy', 'application/octet-stream')
   PICKLE = FileFormat('.pkl', 'application/octet-stream')
   PYTHON = FileFormat('.py', 'text/plain')
   SHAPE = FileFormat('.shp', 'application/x-gzip', 
                      allExtensions=[".shp", ".shx", ".dbf", ".prj", ".sbn", 
                                     ".sbx", ".fbn", ".fbx", ".ain", ".aih", 
                                     ".ixs", ".mxs", ".atx", ".shp.xml", 
                                     ".cpg", ".qix"],
                      driver='ESRI Shapefile',
                      default=True,
                      options={'MAX_STRLEN': 255})
   TAR_GZ = FileFormat('.tar.gz', 'application/x-gzip')
   TMP = FileFormat('.tmp', 'application/octet-stream')
   TXT = FileFormat('.txt', 'text/plain')
   XML = FileFormat('.xml', 'application/xml')
   ZIP = FileFormat('.zip', 'application/zip')
   
   @staticmethod
   def GDALFormats():
      return [LMFormat.ASCII, LMFormat.GTIFF, LMFormat.HFA]
   
   @staticmethod
   def getDefaultGDAL():
      for ff in LMFormat.GDALFormats():
         if ff.isDefault:
            return ff
      return None
   
   @staticmethod
   def OGRFormats():
      return [LMFormat.SHAPE, LMFormat.CSV, LMFormat.GEO_JSON]

   @staticmethod
   def getDefaultOGR():
      for ff in LMFormat.OGRFormats():
         if ff.isDefault:
            return ff
      return None
   
   @staticmethod
   def getStrlenForDefaultOGR():
      return LMFormat.getDefaultOGR().options['MAX_STRLEN']

   @staticmethod
   def SpatialFormats():
      spFormats = [ff for ff in LMFormat.GDALFormats()]
      spFormats.extend(LMFormat.OGRFormats())
      return spFormats

   @staticmethod
   def getFormatByExtension(ext):
      for ff in (LMFormat.ASCII, LMFormat.CSV, LMFormat.GEO_JSON, 
                 LMFormat.GTIFF, LMFormat.HFA, LMFormat.JSON, LMFormat.KML, 
                 LMFormat.LOG, LMFormat.MAKEFLOW, LMFormat.MAP, LMFormat.MXE, 
                 LMFormat.NEWICK, LMFormat.NUMPY, LMFormat.PICKLE, 
                 LMFormat.SHAPE, LMFormat.TAR_GZ, LMFormat.TMP, LMFormat.TXT, 
                 LMFormat.XML, LMFormat.ZIP):
         if ext == ff.ext:
            return ff
      return None

   @staticmethod
   def getFormatByDriver(driver):
      for ff in LMFormat.SpatialFormats():
         if driver == ff.driver:
            return ff
      return None

   @staticmethod
   def getExtensionByDriver(driver):
      ff = LMFormat.getFormatByDriver(driver)
      if ff is not None:
         return ff.ext
      return None

   @staticmethod
   def isGeo(ext=None, driver=None):
      for ff in LMFormat.SpatialFormats():
         if ext is not None and ext == ff.ext:
            return True
         elif driver is not None and driver == ff.driver:
            return True
      return False

   @staticmethod
   def isOGR(ext=None, driver=None):
      for ff in LMFormat.OGRFormats():
         if ext is not None and ext == ff.ext:
            return True
         elif driver is not None and driver == ff.driver:
            return True
      return False
      
   @staticmethod
   def OGRDrivers():
      return [ff.driver for ff in LMFormat.OGRFormats()]

   @staticmethod
   def isGDAL(ext=None, driver=None):
      for ff in LMFormat.GDALFormats():
         if ext is not None and ext == ff.ext:
            return True
         elif driver is not None and driver == ff.driver:
            return True
      return False
   
   @staticmethod
   def GDALDrivers():
      return [ff.driver for ff in LMFormat.GDALFormats()]

   @staticmethod
   def isJSON(ext):
      if ext in (LMFormat.GEO_JSON.ext, LMFormat.JSON.ext):
         return True
      return False
   
   @staticmethod
   def isTestable(ext):
      if ext in (LMFormat.ASCII.ext, LMFormat.GTIFF.ext,
                 LMFormat.SHAPE.ext, LMFormat.JSON.ext):
         return True
      return False

# # .............................................................................
# class OutputFormat:
#    TAR_GZ = '.tar.gz'
#    TXT = '.txt'
#    XML = '.xml'
#    ZIP = '.zip'
#    TMP = '.tmp'
#    MAP = '.map'
#    CSV = '.csv'
#    JSON = '.json'
#    MXE = '.mxe'
#    NUMPY = '.npy'
#    PICKLE = '.pkl'
#    GTIFF = '.tif'
#    ASCII = '.asc'
#    HFA = '.img'
#    SHAPE = '.shp'
#    LOG = '.log'
#    MAKEFLOW = '.mf'
#    METADATA = '.meta'
#    PYTHON = '.py'
#    CONFIG = '.ini'
#    
# SHAPEFILE_EXTENSIONS = [".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".fbn", 
#                         ".fbx", ".ain", ".aih", ".ixs", ".mxs", ".atx", 
#                         ".shp.xml", ".cpg", ".qix"]
# SHAPEFILE_MAX_STRINGSIZE = 255

# Web object interfaces
CSV_INTERFACE = 'csv'
GEO_JSON_INTERFACE = 'geojson'
GEOTIFF_INTERFACE = 'gtiff'
JSON_INTERFACE = 'json'
KML_INTERFACE = 'kml'
OGC_INTERFACE = 'ogc'
SHAPEFILE_INTERFACE = 'shapefile'

# .............................................................................
# .                               Job constants                               .
# .............................................................................
class InputDataType:
   LM_LOWRES_CLIMATE = 1
   LM_HIRES_CLIMATE = 2
   EDAC = 3
   
   USER_PRESENCE_ABSENCE = 11
   USER_ANCILLARY = 12
   
class MatrixType:
   """ 
   @summary: Constants to define the type of matrix (or set of matrix outputs)
   """
   # Inputs
   PAM = 1
   GRIM = 2
   BIOGEO_HYPOTHESES = 3
   ANC_PAM = 4
   ROLLING_PAM = 10
   # OUTPUTS
   OBSERVED_CALC = 101
   SITES_OBSERVED = 102
   SPECIES_OBSERVED = 103
   DIVERSITY_OBSERVED = 104
   SCHLUTER_OBSERVED = 105
   SPECIES_COV_OBSERVED = 106
   SITES_COV_OBSERVED = 107
   RANDOM_CALC = 150
   SITES_RANDOM = 151
   SPECIES_RANDOM = 152
   DIVERSITY_RANDOM = 153
   SCHLUTER_RANDOM = 154
   SPECIES_COV_RANDOM = 155
   SITES_COV_RANDOM = 156
   MCPA_OUTPUTS = 201 # Could be used for permutations
   MCPA_BG_F_GLOBAL = 202
   MCPA_BG_F_SEMI = 203
   MCPA_BG_OBS_ADJ_R_SQ = 204
   MCPA_BG_OBS_PARTIAL = 205
   MCPA_BG_RAND_F_GLOBAL = 206
   MCPA_BG_RAND_F_PARTIAL = 207
   MCPA_ENV_F_GLOBAL = 208
   MCPA_ENV_F_SEMI = 209
   MCPA_ENV_OBS_ADJ_R_SQ = 210
   MCPA_ENV_OBS_PARTIAL = 211
   MCPA_ENV_RAND_F_GLOBAL = 212
   MCPA_ENV_RAND_F_PARTIAL = 213
   


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
#    # SDMModelJob contains SDMModel 
#    MODEL = 110
   # SDMProjectionJob contains SDMProjection 
   PROJECT = 120
   # ==========================================================================   
   #           Valid Stage for Notification Jobs and Objects                     
   # ==========================================================================
   # This Job object is complete, and the user must be notified
   NOTIFY = 500

class JobStatus:
   """ 
   @summary: Constants to define the status of a job.
   @note: 0 - Not ready
   @note: 1 - Ready but not started
   @note: Greater than 1 but less than 300 - Running
   @note: 300 - Completed
   @note: 1000 or greater - Error
   """
   GENERAL = 0  ## Not ready
   INITIALIZE = 1 ## Ready to run
   PULL_REQUESTED = 90 ## Pull requested from job server
   PULL_COMPLETE = 100 ## Pulled from job server to compute
   ACQUIRING_INPUTS = 105 ## Acquiring inputs for computation
   COMPUTE_INITIALIZED = 110 ## Initialized for compute
   RUNNING = 120 ## LmCompute is working on it
   COMPUTED = 130 ## Finished computation
   PUSH_REQUESTED = 140 ## Waiting to push results back to server
   PUSHED = 150 ## Results pushed to server
   PUSH_COMPLETE = 200

   #NOTIFY_READY = 210

   COMPLETE = 300 ## Results computed and available
   
   # ==========================================================================   
   # =                             General Errors                             =
   # ==========================================================================
   # Not found in database, could be prior to insertion
   NOT_FOUND = 404

   GENERAL_ERROR = 1000 ## Any status greater than this is an error
   UNKNOWN_ERROR = 1001 ## Unknown error occurred
   DEPENDENCY_ERROR = 1002
   UNKNOWN_CLUSTER_ERROR = 1003
   PUSH_FAILED = 1100 ## Failed to push results to server
   
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
   # .........................
   # SDM
   # .........................
   # SDM model
   ATT_MODEL = 110
   OM_MODEL = 210
   # SDM project
   ATT_PROJECT = 120
   OM_PROJECT = 220
   PROJECT_REQUEST = 105
   # Occurrences
   GBIF_TAXA_OCCURRENCE = 405
   BISON_TAXA_OCCURRENCE = 410
   IDIGBIO_TAXA_OCCURRENCE = 415
   USER_TAXA_OCCURRENCE = 420
   OCC_BUCKETEER = 450
   OCC_SORTER = 460
   OCC_SPLITTER = 470
   # Intersect
   INTERSECT_RASTER = 230
   INTERSECT_VECTOR = 240
   INTERSECT_RASTER_GRIM = 250
   # .........................
   # RAD
   # .........................
   # RAD Prep
   RAD_BUILDGRID = 305
   RAD_CALCULATE = 340
   ENCODE_HYPOTHESES = 350
   ENCODE_PHYLOGENY = 360
   SQUID_INC = 365
   BUILD_ANC_PAM = 370
   # Randomize
   RAD_SWAP = 331
   RAD_SPLOTCH = 332
   RAD_GRADY = 333
   # MCPA
   MCPA_CORRECT_PVALUES = 530
   MCPA_OBSERVED = 540
   MCPA_RANDOM = 550  
   MCPA_ASSEMBLE = 560 # Assembles all MCPA outputs into a single matrix
   # .......... Notify ..........
   SMTP = 610
   CONCATENATE_MATRICES = 620
   UPDATE_OBJECT = 630
   MF_TRIAGE = 640
   TOUCH = 650
   SOLR_POST = 660
   SNIPPET_POST = 670
   
   # BOOM
   BOOM_INIT = 710
#    BOOM_DAEMON = 720
   BOOMER = 720
   
   # .........................
   # TODO: deleteMe
   # .........................
   RAD_INTERSECT = 310
   RAD_COMPRESS = 320

   @staticmethod
   def isSingle(ptype):
      if ptype in [ProcessType.SMTP, 
                   ProcessType.ATT_MODEL, ProcessType.ATT_PROJECT, 
                   ProcessType.OM_MODEL, ProcessType.OM_PROJECT, 
                   ProcessType.GBIF_TAXA_OCCURRENCE, 
                   ProcessType.BISON_TAXA_OCCURRENCE, 
                   ProcessType.IDIGBIO_TAXA_OCCURRENCE,
                   ProcessType.USER_TAXA_OCCURRENCE, 
                   ProcessType.INTERSECT_RASTER, 
                   ProcessType.INTERSECT_VECTOR, 
                   ProcessType.INTERSECT_RASTER_GRIM]:
         return True
      return False
      
   @staticmethod
   def isOccurrence(ptype):
      if ptype in [ProcessType.GBIF_TAXA_OCCURRENCE, 
                   ProcessType.BISON_TAXA_OCCURRENCE, 
                   ProcessType.IDIGBIO_TAXA_OCCURRENCE,
                   ProcessType.USER_TAXA_OCCURRENCE]:
         return True
      return False
      
   @staticmethod
   def isModel(ptype):
      if ptype in [ProcessType.ATT_MODEL, ProcessType.OM_MODEL]:
         return True
      return False
      
   @staticmethod
   def isProject(ptype):
      if ptype in [ProcessType.ATT_PROJECT, ProcessType.OM_PROJECT]:
         return True
      return False
      
   @staticmethod
   def isIntersect(ptype):
      if ptype in [ProcessType.INTERSECT_RASTER, ProcessType.INTERSECT_VECTOR, 
                   ProcessType.INTERSECT_RASTER_GRIM]:
         return True
      return False

   @staticmethod
   def isRAD(ptype):
      if ptype in [ProcessType.SMTP, ProcessType.RAD_BUILDGRID, 
                   ProcessType.RAD_INTERSECT, ProcessType.RAD_COMPRESS, 
                   ProcessType.RAD_SWAP, ProcessType.RAD_SPLOTCH, 
                   ProcessType.RAD_CALCULATE, ProcessType.RAD_GRADY,
                   ProcessType.MCPA_CORRECT_PVALUES, ProcessType.MCPA_OBSERVED,
                   ProcessType.MCPA_RANDOM, ProcessType.ENCODE_HYPOTHESES,
                   ProcessType.ENCODE_PHYLOGENY, ProcessType.MCPA_ASSEMBLE,
                   ProcessType.OCC_BUCKETEER, ProcessType.OCC_SORTER, 
                   ProcessType.OCC_SPLITTER, ProcessType.BUILD_ANC_PAM]:
         return True
      return False
   
   @staticmethod
   def isRADPrep(ptype):
      if ptype in [ProcessType.RAD_BUILDGRID, ProcessType.RAD_CALCULATE, 
              ProcessType.ENCODE_HYPOTHESES, ProcessType.ENCODE_PHYLOGENY,
              ProcessType.BUILD_ANC_PAM]:
         return True
      return False
   
   @staticmethod
   def isBoom(ptype):
      if ptype in [ProcessType.BOOM_INIT, ProcessType.BOOMER]:
         return True
      return False
   
   @staticmethod
   def isMatrix(ptype):
      if ptype in [ProcessType.CONCATENATE_MATRICES, ProcessType.RAD_CALCULATE, 
                   ProcessType.ENCODE_HYPOTHESES, ProcessType.ENCODE_PHYLOGENY, 
                   ProcessType.RAD_SWAP, ProcessType.RAD_SPLOTCH, 
                   ProcessType.RAD_GRADY, ProcessType.MCPA_ASSEMBLE,
                   ProcessType.MCPA_CORRECT_PVALUES, ProcessType.MCPA_OBSERVED, 
                   ProcessType.MCPA_RANDOM, ProcessType.BUILD_ANC_PAM]:
         return True
      return False  

   @staticmethod
   def isAggregate(ptype):
      return ptype in [ProcessType.OCC_BUCKETEER, ProcessType.OCC_SORTER, 
                       ProcessType.OCC_SPLITTER]

   @staticmethod
   def randomTypes():
      return [ProcessType.RAD_SWAP, ProcessType.RAD_SPLOTCH, 
              ProcessType.RAD_GRADY]
   
   @staticmethod
   def isRandom(ptype):
      if ptype in ProcessType.randomTypes():
         return True
      return False

   @staticmethod
   def encodeTypes():
      return [ProcessType.ENCODE_HYPOTHESES, ProcessType.ENCODE_PHYLOGENY]
   
   @staticmethod
   def isEncode(ptype):
      if ptype in ProcessType.encodeTypes():
         return True
      return False

   @staticmethod
   def mcpaTypes():
      return [ProcessType.MCPA_CORRECT_PVALUES, ProcessType.MCPA_OBSERVED, 
              ProcessType.MCPA_RANDOM, ProcessType.MCPA_ASSEMBLE]
   
   @staticmethod
   def isMCPA(ptype):
      if ptype in [ProcessType.MCPA_CORRECT_PVALUES, ProcessType.MCPA_OBSERVED, 
                   ProcessType.MCPA_RANDOM, ProcessType.MCPA_ASSEMBLE]:
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

# TODO: Remove?  Does not appear to be used
class API_SERVICE:
   BUCKETS = 'buckets' # TODO: Remove, obsolete
#    EXPERIMENTS = 'experiments' # TODO: Remove, obsolete
#    RAD_EXPERIMENTS = 'radexperiments' # TODO: Remove, obsolete
#    SDM_EXPERIMENTS = 'sdmexperiments' # TODO: Remove, obsolete
   LAYERS = 'layer'
#    LAYERTYPES = 'typecodes' # TODO: Remove, obsolete
#    MODELS = 'models' # TODO: Remove, obsolete
   OCCURRENCES = 'occurrence'
#    PAMSUMS = 'pamsums' # TODO: Remove, obsolete
#    ANCILLARY_LAYERS = 'anclayers' # TODO: Remove, obsolete
#    PRESENCEABSENCE_LAYERS = 'palayers' # TODO: Remove, obsolete
   MATRIX_LAYERS = 'column'
   PROJECTIONS = 'sdmproject'
   SCENARIOS = 'scenario'
   # Generic layersets, not Scenarios
   LAYERSETS = 'layersets' # TODO: Are we going to have this?
   # Biogeography Tools
   SHAPEGRIDS = 'shapegrid'


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
# .                               Geo constants                              .
# .............................................................................
DEFAULT_GLOBAL_EXTENT = (-180.0, -60.0, 180.0, 90.0)
DEFAULT_EPSG = 4326
DEFAULT_MAPUNITS = 'dd'
DEFAULT_CELLSIDES = 4
DEFAULT_CELLSIZE = 1.0

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
   INSTITUTION_ID = {'FULL': 'institutionID', 'SHORT': 'inst_id'}
   COLLECTION_CODE = {'FULL': 'collectionCode', 'SHORT': 'coll_code'}
   COLLECTION_ID = {'FULL': 'collectionID', 'SHORT': 'coll_id'}
   CONTINENT = {'FULL': 'continent', 'SHORT': 'continent'}
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
   STATE_PROVINCE = {'FULL': 'stateprovince', 'SHORT': 'stprov'}
   
   @staticmethod
   def _definedNames():
      return [DWCNames.OCCURRENCE_ID, DWCNames.INSTITUTION_CODE, 
              DWCNames.INSTITUTION_ID, DWCNames.COLLECTION_CODE, 
              DWCNames.COLLECTION_ID, DWCNames.CONTINENT, DWCNames.CATALOG_NUMBER,
              DWCNames.BASIS_OF_RECORD, DWCNames.DECIMAL_LATITUDE, 
              DWCNames.DECIMAL_LONGITUDE, DWCNames.SCIENTIFIC_NAME, DWCNames.DAY,
              DWCNames.MONTH, DWCNames.YEAR, DWCNames.RECORDED_BY, 
              DWCNames.COUNTRY_CODE, DWCNames.STATE_PROVINCE]
      
   @staticmethod
   def getShortName(fullname):
      if fullname.startswith(DWC_QUALIFIER):
         fullname = fullname[len(DWC_QUALIFIER):]
      for dn in DWCNames._definedNames():
         if dn['FULL'] == fullname:
            return dn['SHORT']
      return None

# ......................................................
PROVIDER_FIELD_COMMON = 'provider'
# # Bison
# BISON_COUNT_KEYS = ['response', 'numFound']

# PROVIDER_NAME_FIELD = 'provider'
# LINK_FIELD = 'point_url'

# ......................................................
# TODO: Replace individual GBIF_* constants with this class
class GBIF:
   TAXON_KEY = 'specieskey'
   TAXON_NAME = DWCNames.SCIENTIFIC_NAME['SHORT']
   PROVIDER = 'puborgkey'
   GBIFID = 'gbifid'
   WAIT_TIME = 3 * ONE_MIN
   LIMIT = 300
   REST_URL = 'http://api.gbif.org/v1'
   QUALIFIER = 'gbif:'
   
   SPECIES_SERVICE = 'species'
   OCCURRENCE_SERVICE = 'occurrence'
   DATASET_SERVICE = 'dataset'
   ORGANIZATION_SERVICE = 'organization'
   
   TAXONKEY_FIELD = 'specieskey'
   TAXONNAME_FIELD = DWCNames.SCIENTIFIC_NAME['SHORT']
   PROVIDER_FIELD = 'puborgkey'
   ID_FIELD = 'gbifid'

   REQUEST_SIMPLE_QUERY_KEY = 'q'
   REQUEST_NAME_QUERY_KEY = 'name'
   REQUEST_TAXON_KEY = 'TAXON_KEY'
   REQUEST_RANK_KEY = 'rank'
   REQUEST_DATASET_KEY = 'dataset_key'                
   
   DATASET_BACKBONE_VALUE = 'GBIF Backbone Taxonomy'
   
   SEARCH_COMMAND = 'search'
   COUNT_COMMAND = 'count'
   MATCH_COMMAND = 'match'
   DOWNLOAD_COMMAND = 'download'
   DOWNLOAD_REQUEST_COMMAND = 'request'
   RESPONSE_IDENTIFIER_KEY = 'key'
   RESPONSE_RESULT_KEY = 'results'
   RESPONSE_END_KEY = 'endOfRecords'
   RESPONSE_COUNT_KEY = 'count'
   RESPONSE_GENUS_ID_KEY = 'genusKey'
   RESPONSE_GENUS_KEY = 'genus'
   RESPONSE_SPECIES_ID_KEY = 'speciesKey'
   RESPONSE_SPECIES_KEY = 'species'
   RESPONSE_MATCH_KEY = 'matchType'
   RESPONSE_NOMATCH_VALUE = 'NONE'
   
   # For writing files from GBIF DarwinCore download, 
   # DWC translations in lmCompute/code/sdm/gbif/constants
   # We are adding the 2 fields: LM_WKT_FIELD and LINK_FIELD
   LINK_FIELD = 'gbifurl'
   # Ends in / to allow appending unique id
   LINK_PREFIX = 'http://www.gbif.org/occurrence/'

class GBIF_QUERY:
   TAXON_FIELDS = {0: ('taxonkey', OFTString), 
                   1: ('kingdom', OFTString),
                   2: ('phylum', OFTString),
                   3: ('class', OFTString), 
                   4: ('order', OFTString),
                   5: ('family', OFTString),
                   6: ('genus', OFTString),
                   7: (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                   8: ('genuskey', OFTInteger),
                   9: (GBIF.TAXONKEY_FIELD, OFTInteger),
                   10:('count', OFTInteger)}
   EXPORT_FIELDS = {0: (GBIF.ID_FIELD, OFTInteger), 
                    1: (DWCNames.OCCURRENCE_ID['SHORT'], OFTInteger), 
                    2: ('taxonkey', OFTInteger),
                    3: ('datasetkey', OFTString),
                    4: (GBIF.PROVIDER_FIELD, OFTString),
                    5: (DWCNames.BASIS_OF_RECORD['SHORT'], OFTString),
                    6: ('kingdomkey', OFTInteger),
                    7: ('phylumkey', OFTInteger),
                    8: ('classkey', OFTInteger),
                    9: ('orderkey', OFTInteger),
                    10: ('familykey', OFTInteger), 
                    11: ('genuskey', OFTInteger),
                    12: (GBIF.TAXONKEY_FIELD, OFTInteger),
                    13: (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                    14: (DWCNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
                    15: (DWCNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
                    16: (DWCNames.DAY['SHORT'], OFTInteger),
                    17: (DWCNames.MONTH['SHORT'], OFTInteger),
                    18: (DWCNames.YEAR['SHORT'], OFTInteger),
                    19: (DWCNames.RECORDED_BY['SHORT'], OFTString),
                    20: (DWCNames.INSTITUTION_CODE['SHORT'], OFTString),
                    21: (DWCNames.COLLECTION_CODE['SHORT'], OFTString),
                    22: (DWCNames.CATALOG_NUMBER['SHORT'], OFTString)}
   PARAMS = {GBIF.SPECIES_SERVICE: {'status': 'ACCEPTED',
                                    GBIF.REQUEST_RANK_KEY: None,
                                    GBIF.REQUEST_DATASET_KEY: None,
                                    GBIF.REQUEST_NAME_QUERY_KEY: None},
             GBIF.OCCURRENCE_SERVICE: {"GEOREFERENCED": True,
                                       "SPATIAL_ISSUES": False,
#                                     "BASIS_OF_RECORD": ["PRESERVED_SPECIMEN"],
                                       GBIF.REQUEST_TAXON_KEY: None},
             GBIF.DOWNLOAD_COMMAND: {"creator": "aimee",
                                     "notification_address": 
                                       ["lifemapper@mailinator.com"]}}

# .............................................................................
# .                               BISON/ITIS constants                              .
# .............................................................................
# ......................................................
# TODO: Replace individual BISON_* constants with this class
# For parsing BISON Solr API response, updated Feb 2015
class BISON:
   OCCURRENCE_URL = 'https://bison.usgs.gov/solr/occurrences/select'
   # Ends in : to allow appending unique id
   LINK_PREFIX = 'https://bison.usgs.gov/solr/occurrences/select/?q=occurrenceID:'
   LINK_FIELD = 'bisonurl'
   # For TSN query filtering on Binomial
   NAME_KEY = 'ITISscientificName'
   # For Occurrence query by TSN in hierarchy
   HIERARCHY_KEY = 'hierarchy_homonym_string'
   KINGDOM_KEY = 'kingdom'
   TSN_KEY = 'TSNs'
   # To limit query
   MIN_POINT_COUNT = 20
   MAX_POINT_COUNT = 5000000
   BBOX = (24, -125, 50, -66)
   BINOMIAL_REGEX = '/[A-Za-z]*[ ]{1,1}[A-Za-z]*/'
   
class BISON_QUERY:
   # Expected Response Dictionary Keys
   TSN_LIST_KEYS = ['facet_counts', 'facet_fields', BISON.TSN_KEY]
   RECORD_KEYS = ['response', 'docs']
   COUNT_KEYS = ['response', 'numFound']
   TSN_FILTERS = {'facet': True,
                  'facet.limit': -1,
                  'facet.mincount': BISON.MIN_POINT_COUNT,
                  'facet.field': BISON.TSN_KEY, 
                  'rows': 0}
   OCC_FILTERS = {'rows': BISON.MAX_POINT_COUNT}
   # Common Q Filters
   QFILTERS = {'decimalLatitude': (BISON.BBOX[0], BISON.BBOX[2]),
               'decimalLongitude': (BISON.BBOX[1], BISON.BBOX[3]),
               'basisOfRecord': [(False, 'living'), (False, 'fossil')]}
   # Common Other Filters
   FILTERS = {'wt': 'json', 
              'json.nl': 'arrarr'}
   RESPONSE_FIELDS = {'ITIScommonName': ('comname', OFTString),
                      BISON.NAME_KEY: (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                      'ITIStsn': ('itistsn', OFTInteger),
                      BISON.TSN_KEY: None,
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
                      BISON.HIERARCHY_KEY: ('tsn_hier', OFTString),
                      'institutionID': ('inst_id', OFTString),
                      BISON.KINGDOM_KEY: ('kingdom', OFTString),
                      # Comma delimited, same as geo
                      'latlon': ('latlon', OFTString),
                      DWCNames.OCCURRENCE_ID['FULL']: 
                         (DWCNames.OCCURRENCE_ID['SHORT'], OFTInteger),
                      'ownerInstitutionCollectionCode': 
                       (PROVIDER_FIELD_COMMON, OFTString),
                      'pointPath': None,
                      'providedCounty': None,
                      'providedScientificName': None,
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
                     '_version_': None }
   
# .............................................................................
# TODO: Replace individual ITIS_* constants with this class
class ITIS:
   DATA_NAMESPACE = 'http://data.itis_service.itis.usgs.gov/xsd'
   # Basic Web Services
   TAXONOMY_HIERARCHY_URL = 'http://www.itis.gov/ITISWebService/services/ITISService/getFullHierarchyFromTSN'
   # JSON Web Services
   # TAXONOMY_HIERARCHY_URL = 'http://www.itis.gov/ITISService/jsonservice/getFullHierarchyFromTSN'
   TAXONOMY_KEY = 'tsn'
   HIERARCHY_TAG = 'hierarchyList'
   RANK_TAG = 'rankName'
   TAXON_TAG = 'taxonName'
   KINGDOM_KEY = 'Kingdom'
   PHYLUM_DIVISION_KEY = 'Division'
   CLASS_KEY = 'Class'
   ORDER_KEY = 'Order'
   FAMILY_KEY = 'Family'
   GENUS_KEY = 'Genus'
   SPECIES_KEY = 'Species'
   
# .............................................................................
# .                           iDigBio constants                               .
# .............................................................................
# TODO: Replace individual ITIS_* constants with this class
class IDIGBIO:
   LINK_PREFIX = 'https://www.idigbio.org/portal/records/'
   SEARCH_PREFIX = 'https://search.idigbio.org/v2'
   SEARCH_POSTFIX ='search'
   OCCURRENCE_POSTFIX = 'records'
   PUBLISHERS_POSTFIX = 'publishers'
   RECORDSETS_POSTFIX = 'recordsets'
   SEARCH_LIMIT = 5000
   ID_FIELD = 'uuid'
   LINK_FIELD = 'idigbiourl'
   GBIFID_FIELD = 'taxonID'
   BINOMIAL_REGEX = "(^[^ ]*) ([^ ]*)$"
   OCCURRENCE_ITEMS_KEY = 'items'
   RECORD_CONTENT_KEY = 'data'
   RECORD_INDEX_KEY = 'indexTerms'
   QUALIFIER = 'idigbio:'
   
class IDIG_DUMP:
   EXPORT_FIELDS = {0: ('coreid', None),
                    1: ('idigbio:associatedsequences', None),
                    2: ('idigbio:barcodeValue', None),
                    3: ('dwc:basisOfRecord', None),
                    4: ('dwc:bed', None),
                    5: (GBIF.QUALIFIER + 'canonicalName', None),
                    6: ('dwc:catalogNumber', None),
                    7: ('dwc:class', None),
                    8: ('dwc:collectionCode', None),
                    9: ('dwc:collectionID', None),
                    10: ('idigbio:collectionName', None),
                    11: ('dwc:recordedBy', None),
                    12: ('dwc:vernacularName', None),
                    13: ('idigbio:commonnames', None),
                    14: ('dwc:continent', None),
                    15: ('dwc:coordinateUncertaintyInMeters', None),
                    16: ('dwc:country', None),
                    17: ('idigbio:isoCountryCode', None),
                    18: ('dwc:county', None),
                    19: ('idigbio:eventDate', None),
                    20: ('idigbio:dateModified', None),
                    21: ('idigbio:dataQualityScore', None),
                    22: ('dwc:earliestAgeOrLowestStage', None),
                    23: ('dwc:earliestEonOrLowestEonothem', None),
                    24: ('dwc:earliestEpochOrLowestSeries', None),
                    25: ('dwc:earliestEraOrLowestErathem', None),
                    26: ('dwc:earliestPeriodOrLowestSystem', None),
                    27: ('idigbio:etag', None),
                    28: ('dwc:eventDate', None),
                    29: ('dwc:family', None),
                    30: ('dwc:fieldNumber', None),
                    31: ('idigbio:flags', None),
                    32: ('dwc:formation', None),
                    33: ('dwc:genus', None),
                    34: ('dwc:geologicalContextID', None),
                    35: ('idigbio:geoPoint', None),
                    36: ('dwc:group', None),
                    37: ('idigbio:hasImage', None),
                    38: ('idigbio:hasMedia', None),
                    39: ('dwc:higherClassification', None),
                    40: ('dwc:highestBiostratigraphicZone', None),
                    41: ('dwc:individualCount', None),
                    42: ('dwc:infraspecificEpithet', None),
                    43: ('dwc:institutionCode', None),
                    44: ('dwc:institutionID', None),
                    45: ('idigbio:institutionName', None),
                    46: ('dwc:kingdom', None),
                    47: ('dwc:latestAgeOrHighestStage', None),
                    48: ('dwc:latestEonOrHighestEonothem', None),
                    49: ('dwc:latestEpochOrHighestSeries', None),
                    50: ('dwc:latestEraOrHighestErathem', None),
                    51: ('dwc:latestPeriodOrHighestSystem', None),
                    52: ('dwc:lithostratigraphicTerms', None),
                    53: ('dwc:locality', None),
                    54: ('dwc:lowestBiostratigraphicZone', None),
                    55: ('dwc:maximumDepthInMeters', None),
                    56: ('dwc:maximumElevationInMeters', None),
                    57: ('idigbio:mediarecords', None),
                    58: ('dwc:member', None),
                    59: ('dwc:minimumDepthInMeters', None),
                    60: ('dwc:minimumElevationInMeters', None),
                    61: ('dwc:municipality', None),
                    62: ('dwc:occurrenceID', None),
                    63: ('dwc:order', None),
                    64: ('dwc:phylum', None),
                    65: ('idigbio:recordIds', None),
                    66: ('dwc:recordNumber', None),
                    67: ('idigbio:recordset', None),
                    68: ('dwc:scientificName', None),
                    69: ('dwc:specificEpithet', None),
                    70: ('dwc:startDayOfYear', None),
                    71: ('dwc:stateProvince', None),
                    72: ('dwc:taxonID', None),
                    73: ('dwc:taxonomicStatus', None),
                    74: ('dwc:taxonRank', None),
                    75: ('dwc:typeStatus', None),
                    76: ('idigbio:uuid', None),
                    77: ('dwc:verbatimEventDate', None),
                    78: ('dwc:verbatimLocality', None),
                    79: ('idigbio:version', None),
                    80: ('dwc:waterBody', None)}
   METADATA = {0: {'name': 'coreid', 'type': OFTString},
               1: None,
               2: None,
               3: {'name': DWCNames.BASIS_OF_RECORD['SHORT'], 'type': OFTString},
               4: None,
               5: {'name': 'canonical', 'type': OFTString, 'role': 'taxaname'},
               6: {'name': DWCNames.CATALOG_NUMBER['SHORT'], 'type': OFTString},
               7: None,
               8: {'name': DWCNames.COLLECTION_CODE['SHORT'], 'type': OFTString},
               9: {'name': DWCNames.COLLECTION_ID['SHORT'], 'type': OFTString},
               10: None,
               11: {'name': DWCNames.RECORDED_BY['SHORT'], 'type': OFTString},
               12: None,
               13: None,
               14: None,
               15: None,
               16: None,
               17: {'name': DWCNames.COUNTRY_CODE['SHORT'], 'type': OFTString},
               18: None,
               19: None,
               20: None,
               21: None,
               22: None,
               23: None,
               24: None,
               25: None,
               26: None,
               27: None,
               28: None,
               29: None,
               30: None,
               31: None,
               32: None,
               33: None,
               34: None,
               35: {'name': 'geoPoint', 'type': OFTString, 'role': 'geopoint'},
               36: None,
               37: None,
               38: None,
               39: None,
               40: None,
               41: None,
               42: None,
               43: {'name': DWCNames.INSTITUTION_CODE['SHORT'], 'type': OFTString},
               44: {'name': DWCNames.INSTITUTION_ID['SHORT'], 'type': OFTString},
               45: None,
               46: None,
               47: None,
               48: None,
               49: None,
               50: None,
               51: None,
               52: None,
               53: None,
               54: None,
               55: None,
               56: None,
               57: None,
               58: None,
               59: None,
               60: None,
               61: None,
               62: {'name': DWCNames.OCCURRENCE_ID['SHORT'], 'type': OFTString},
               63: None,
               64: None,
               65: None,
               66: None,
               67: {'name': 'recordset', 'type': OFTString},
               68: {'name': DWCNames.SCIENTIFIC_NAME['SHORT'], 'type': OFTString},
               69: None,
               70: None,
               71: {'name': DWCNames.STATE_PROVINCE['SHORT'], 'type': OFTString},
               72: {'name': 'taxonID', 'type': OFTInteger, 'role': 'GroupBy'},
               73: None,
               74: {'name': 'rank', 'type': OFTString},
               75: None,
               76: {'name': 'uuid', 'type': OFTString},
               77: None,
               78: None,
               79: {'name': 'version', 'type': OFTString},
               80: None,
 }
   
class IDIGBIO_QUERY:
   EXPORT_FIELDS = {0: (IDIGBIO.ID_FIELD, OFTString), 
                    1: (DWCNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
                    2: (DWCNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
                    3: (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                    4: (PROVIDER_FIELD_COMMON, OFTString) }
   # Geopoint.lat and Geopoint.lon are modified on return to short names
   # Response record fields: https://search.idigbio.org/v2/meta/fields/records
   RETURN_FIELDS = {
      IDIGBIO.ID_FIELD: (IDIGBIO.ID_FIELD, OFTString),
      IDIGBIO.GBIFID_FIELD: ('taxonid', OFTString),
      DWC_QUALIFIER + DWCNames.SCIENTIFIC_NAME['FULL']: (DWCNames.SCIENTIFIC_NAME['SHORT'], 
                                                         OFTString),   
      DWC_QUALIFIER + DWCNames.BASIS_OF_RECORD['FULL']:  (DWCNames.BASIS_OF_RECORD['SHORT'], 
                                                          OFTString),
      DWC_QUALIFIER + DWCNames.CATALOG_NUMBER['FULL']: (DWCNames.CATALOG_NUMBER['SHORT'], 
                                                        OFTString),
      DWC_QUALIFIER + DWCNames.COLLECTION_ID['FULL']: 
                              (DWCNames.COLLECTION_ID['SHORT'], OFTString),
      DWC_QUALIFIER + DWCNames.COLLECTION_CODE['FULL']: 
                              (DWCNames.COLLECTION_CODE['SHORT'], OFTString),
      DWC_QUALIFIER + DWCNames.RECORDED_BY['FULL']: 
                              (DWCNames.RECORDED_BY['SHORT'], OFTString),
      'commonname': ('comname', OFTString),                  
      DWC_QUALIFIER + DWCNames.CONTINENT['FULL']: (DWCNames.CONTINENT['SHORT'], 
                                                   OFTString),
      DWC_QUALIFIER + DWCNames.COUNTRY_CODE['FULL']: (DWCNames.COUNTRY_CODE['SHORT'], 
                                                      OFTString),
      DWC_QUALIFIER + DWCNames.DAY['FULL']: (DWCNames.DAY['SHORT'], OFTString),
      DWC_QUALIFIER + DWCNames.MONTH['FULL']: (DWCNames.MONTH['SHORT'], OFTString),
      DWC_QUALIFIER + DWCNames.YEAR['FULL']: (DWCNames.YEAR['SHORT'], OFTString),
      DWC_QUALIFIER + DWCNames.INSTITUTION_CODE['FULL']: (DWCNames.INSTITUTION_CODE['SHORT'], 
                                                          OFTString),
      DWC_QUALIFIER + DWCNames.INSTITUTION_ID['FULL']: (DWCNames.INSTITUTION_ID['SHORT'], 
                                                        OFTString),
      DWC_QUALIFIER + DWCNames.OCCURRENCE_ID['FULL']: (DWCNames.OCCURRENCE_ID['SHORT'], 
                                                       OFTInteger),
      DWC_QUALIFIER + DWCNames.STATE_PROVINCE['FULL']: (DWCNames.STATE_PROVINCE['SHORT'], 
                                                        OFTString),
      DWC_QUALIFIER + DWCNames.DECIMAL_LATITUDE['FULL']: (DWCNames.DECIMAL_LATITUDE['SHORT'], 
                                                          OFTReal),
      DWC_QUALIFIER + DWCNames.DECIMAL_LONGITUDE['FULL']: (DWCNames.DECIMAL_LONGITUDE['SHORT'], 
                                                           OFTReal),
      }
   QFILTERS = {'basisofrecord': 'preservedspecimen' }
#    queryFlds = IDIGBIO_RETURN_FIELDS.keys()
#    queryFlds.append('geopoint')
   
   FILTERS = { # 'fields': queryFlds,
               'limit': IDIGBIO.SEARCH_LIMIT,
               'offset': 0,
               'no_attribution': False}
"""
"taxonid", "uuid", "scientificname", "canonicalname", 
"catalognumber", "collectionid", "collectioncode", "occurrenceid",
"country", "datecollected", 
"institutioncode", "institutionid", "institutionname"
"""


# .............................................................................
# .                  Provider/Local data fieldname constants                              .
# .............................................................................
LM_ID_FIELD = 'lmid'
LM_WKT_FIELD = 'geomwkt'

# .............................................................................
# .                              Other constants                              .
# .............................................................................
# TODO: replace hardcoded vars in code
# DEFAULT_OGR_FORMAT = 'ESRI Shapefile'
# DEFAULT_GDAL_FORMAT = 'GTiff'

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

# .............................................................................
# .                               PAM Statisitcs                              .
# .............................................................................
class PamStatKeys(object):
   """
   @summary: Class containing PAM statistics keys
   @todo: Link to literature
   """
   # Site statistics
   # ..................
   # Alpha is the species richness (number present) per site
   ALPHA = 'alpha'
   # Alpha prop. is the proportion of the entire set of species present per site
   ALPHA_PROP = 'alphaProp'
   # Phi is the range size per site
   PHI = 'phi'
   # PHI_AVG_PROP is the mean proportional range size per site
   PHI_AVG_PROP = 'phiProp'
   
   # Tree (site) statistics
   # ..................
   # MNTD is mean nearest taxon distance per site.  The average distance to the
   #    nearest taxon for every species present at a site
   MNTD = 'mntd'
   # MPD is the mean pairwise distance per site.  This is the average distance
   #    to all other species for each species at each site
   MPD = 'mpd'
   # PEARSON is Pearson's Correlation Coefficient 
   PEARSON = 'pearson'
   
   # Species statistics
   # ..................
   # OMEGA is the range size for each species
   OMEGA = 'omega'
   # OMEGA_PROP is the range size of each species as a proportion of the total 
   #    number of sites
   OMEGA_PROP = 'omegaProp'
   # PSI is the range richness of each species
   PSI = 'psi'
   # PSI_AVG_PROP is the mean proportional species diversity
   PSI_AVG_PROP = 'psiAvgProp'
   
   # Beta Diversity statistics
   # ..................
   WHITTAKERS_BETA = 'whittakersBeta'
   LANDES_ADDATIVE_BETA = 'landesAddativeBeta'
   LEGENDRES_BETA = 'legendresBeta'

   # Covariance matrices
   # ..................
   # Covariance matrices for the composition of sites and the range of species
   SITES_COVARIANCE = 'sitesCovariance'
   SPECIES_COVARIANCE = 'speciesCovariance'

   # Schluter's statistics
   # ..................
   # These are Schluter's statistics for site and species variance ratios in a PAM
   SPECIES_VARIANCE_RATIO = 'varSpeciesRatio'
   SITES_VARIANCE_RATIO = 'varSitesRatio'


# .............................................................................
# .                     Phylogenetic Tree Module Constants                    .
# .............................................................................
class PhyloTreeKeys(object):
   """
   @summary: Class containing keys for Jeff's Phylo Trees
   """
   CHILDREN = 'children' # Children of a node
   BRANCH_LENGTH = 'length' # Branch length for that node
   MTX_IDX = 'mx' # The matrix index for this node
   NAME = 'name' # Name of the node
   CLADE_ID = 'cladeId' # This is an identifier for the clade
   SQUID = 'squid' # This is the LM SQUID (species identifier) for the tip
