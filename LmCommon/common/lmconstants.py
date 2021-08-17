"""Module containing common Lifemapper constants
"""
try:
    from osgeo.ogr import OFTInteger, OFTReal, OFTString, OFTBinary
except ImportError:
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
SERVER_MATT_DAEMON_HEADING = 'LmServer - Matt Daemon'

SERVER_SDM_ALGORITHM_HEADING_PREFIX = 'ALGORITHM'
SERVER_SDM_MASK_HEADING_PREFIX = 'PREPROCESSING SDM_MASK'
SERVER_DEFAULT_HEADING_POSTFIX = 'DEFAULT'

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
# .                               File constants                              .
# .............................................................................
# DATA FORMATS
ENCODING = 'utf-8'


# ............................................................................
class BoomKeys:
    """Constants class for BOOM config document keys
    """
    # Algorithm
    ALG_CODE = 'ALG_CODE'

    # Masking
    BUFFER = 'BUFFER'
    MODEL_MASK_NAME = 'MODEL_MASK_NAME'
    PROJECTION_MASK_NAME = 'PROJECTION_MASK_NAME'
    REGION = 'REGION'

    # Server boom
    ARCHIVE_NAME = 'ARCHIVE_NAME'
    ARCHIVE_PRIORITY = 'ARCHIVE_PRIORITY'
    ARCHIVE_USER = 'ARCHIVE_USER'
    ARCHIVE_USER_EMAIL = 'ARCHIVE_USER_EMAIL'

    BIOGEO_HYPOTHESES_LAYERS = 'BIOGEO_HYPOTHESES_LAYERS'
    OTHER_LAYERS = 'OTHER_LAYERS'

    COMPUTE_PAM_STATS = 'COMPUTE_PAM_STATS'
    COMPUTE_MCPA = 'COMPUTE_MCPA'
    NUM_PERMUTATIONS = 'NUMBER_OF_PERMUTATIONS'

    DATA_SOURCE = 'DATA_SOURCE'

    EPSG = 'EPGS'

    INDEX_TAXONOMY = 'INDEX_TAXONOMY'

    ASSEMBLE_PAMS = 'ASSEMBLE_PAMS'
    GRID_BBOX = 'GRID_BBOX'
    GRID_CELL_SIZE = 'GRID_CELL_SIZE'
    GRID_NAME = 'GRID_NAME'
    GRID_NUM_SIDES = 'GRID_NUM_SIDES'

    # These are MatrixColumn class properties, prefixed by 'INTERSECT_'
    INTERSECT_FILTER_STRING = 'INTERSECT_FILTER_STRING'
    INTERSECT_MAX_PRESENCE = 'INTERSECT_MAX_PRESENCE'
    INTERSECT_MIN_PERCENT = 'INTERSECT_MIN_PERCENT'
    INTERSECT_MIN_PRESENCE = 'INTERSECT_MIN_PRESENCE'
    INTERSECT_VAL_NAME = 'INTERSECT_VAL_NAME'

    MAPUNITS = 'MAPUNITS'

    OCC_DATA_NAME = 'OCC_DATA_NAME'
    OCC_DATA_DELIMITER = 'OCC_DATA_DELIMITER'
    OCC_ID_FILENAME = 'OCC_ID_FILENAME'
    OCC_EXP_MJD = 'OCC_EXP_MJD'
    # TODO: Remove these later
    OCC_EXP_YEAR = 'OCC_EXP_YEAR'
    OCC_EXP_MONTH = 'OCC_EXP_MONTH'
    OCC_EXP_DAY = 'OCC_EXP_DAY'

    POINT_COUNT_MIN = 'POINT_COUNT_MIN'
    SCENARIO_PACKAGE = 'SCENARIO_PACKAGE'
    SCENARIO_PACKAGE_MODEL_SCENARIO = 'SCENARIO_PACKAGE_MODEL_SCENARIO'
    SCENARIO_PACKAGE_PROJECTION_SCENARIOS = 'SCENARIO_PACKAGE_PROJECTION_SCENARIOS'

    TAXON_ID_FILENAME = 'TAXON_ID_FILENAME'
    TAXON_NAME_FILENAME = 'TAXON_NAME_FILENAME'

    TREE = 'TREE'
    TROUBLESHOOTERS = 'TROUBLESHOOTERS'
    TAXONOMY_FILENAME = 'TAXONOMY_FILENAME'


# .............................................................................
class FileFormat:
    """
    @summary: This class contains file format meta information
    @todo: Add GDAL / OGR type as optional parameters
    """

    # ...........................
    def __init__(self, extension, mime_type, all_extensions=None, driver=None,
                 options=None, default=False):
        """Constructor

        Args:
            extension: This is the primary extension if a format has multiple
                files
            mime_type: The MIME-Type for this format
            all_extensions: List of all possible extensions for this format
            driver: GDAL or OGR driver to use when reading this format
        """
        self._mime_type = mime_type
        self.ext = extension
        self.driver = driver
        self.options = options
        self.is_default = default
        self._extensions = all_extensions
        if self._extensions is None:
            self._extensions = []
        # Add the default extension to the extensions list if not present
        if self.ext not in self._extensions:
            self._extensions.append(self.ext)

    # ..........................
    def get_extensions(self):
        """Get valid file extension for this file type
        """
        return self._extensions

    # ...........................
    def get_mime_type(self):
        """Get the mime type for this file type
        """
        return self._mime_type


# .............................................................................
class LMFormat:
    """Class containing known formats to Lifemapper
    """
    ASCII = FileFormat(
        '.asc', 'text/plain', all_extensions=['.asc', '.prj'],
        driver='AAIGrid', options={
            'DECIMAL_PRECISION': 6, 'FORCE_CELLSIZE': 'YES'})
    CSV = FileFormat('.csv', 'text/csv', driver='CSV')
    CONFIG = FileFormat('.ini', 'text/plain')
    EML = FileFormat('.eml', 'application/xml+eml')
    GTIFF = FileFormat('.tif', 'image/tiff', driver='GTiff', default=True)
    HFA = FileFormat('.img', 'image/octet-stream', driver='HFA')
    JSON = FileFormat('.json', 'application/json')
    GEO_JSON = FileFormat('.geojson', 'application/vnd.geo+json')
    KML = FileFormat('.kml', 'application/vnd.google-earth.kml+xml')
    LOG = FileFormat('.log', 'text/plain')
    MAKEFLOW = FileFormat('.mf', 'text/plain')
    MAP = FileFormat('.map', 'text/plain')
    MATRIX = FileFormat('.lmm', 'application/octet-stream')
    METADATA = FileFormat('.meta', 'text/plain')
    MXE = FileFormat('.mxe', 'application/octet-stream')
    NEXUS = FileFormat('.nex', 'text/plain', all_extensions=['.nex', '.nxs'])
    NEWICK = FileFormat('.tre', 'text/plain', all_extensions=['.tre', '.nhx'])
    NUMPY = FileFormat('.npy', 'application/octet-stream')
    PARAMS = FileFormat('.params', 'text/plain')
    PICKLE = FileFormat('.pkl', 'application/octet-stream')
    PROGRESS = FileFormat('.progress', 'application/progress+json')
    PYTHON = FileFormat('.py', 'text/plain')
    SHAPE = FileFormat(
        '.shp', 'application/x-gzip',
        all_extensions=['.shp', '.shx', '.dbf', '.prj', '.sbn', '.sbx', '.fbn',
                        '.fbx', '.ain', '.aih', '.ixs', '.mxs', '.atx',
                        '.shp.xml', '.cpg', '.qix'],
        driver='ESRI Shapefile', default=True, options={'MAX_STRLEN': 254})
    TAR_GZ = FileFormat('.tar.gz', 'application/x-gzip')
    TMP = FileFormat('.tmp', 'application/octet-stream')
    TXT = FileFormat('.txt', 'text/plain')
    XML = FileFormat('.xml', 'application/xml')
    ZIP = FileFormat('.zip', 'application/zip')

    @staticmethod
    def gdal_formats():
        """Returns a list of GDAL raster formats"""
        return [LMFormat.ASCII, LMFormat.GTIFF, LMFormat.HFA]

    @staticmethod
    def get_default_gdal():
        """Get the default gdal format"""
        for frmt in LMFormat.gdal_formats():
            if frmt.is_default:
                return frmt
        return None

    @staticmethod
    def ogr_formats():
        """Returns a list of OGR vector formats
        """
        return [LMFormat.SHAPE, LMFormat.CSV, LMFormat.GEO_JSON]

    @staticmethod
    def get_default_ogr():
        """Return the default OGR vector format
        """
        for frmt in LMFormat.ogr_formats():
            if frmt.is_default:
                return frmt
        return None

    @staticmethod
    def get_str_len_for_default_ogr():
        """Get the maximum string length for the default OGR vector format
        """
        return LMFormat.get_default_ogr().options['MAX_STRLEN']

    @staticmethod
    def spatial_formats():
        """Returns a list of spatial file formats
        """
        sp_formats = LMFormat.gdal_formats()
        sp_formats.extend(LMFormat.ogr_formats())
        return sp_formats

    @staticmethod
    def get_format_by_extension(ext):
        """Get a FileFormat object for the specified file extension
        """
        for frmt in (
                LMFormat.ASCII, LMFormat.CSV, LMFormat.GEO_JSON,
                LMFormat.GTIFF, LMFormat.HFA, LMFormat.JSON, LMFormat.KML,
                LMFormat.LOG, LMFormat.MAKEFLOW, LMFormat.MAP, LMFormat.MXE,
                LMFormat.NEWICK, LMFormat.NUMPY, LMFormat.PICKLE,
                LMFormat.SHAPE, LMFormat.TAR_GZ, LMFormat.TMP, LMFormat.TXT,
                LMFormat.XML, LMFormat.ZIP):
            if ext == frmt.ext:
                return frmt
        return None

    @staticmethod
    def get_format_by_driver(driver):
        """Get a FileFormat object that matches the specified driver
        """
        for frmt in LMFormat.spatial_formats():
            if driver == frmt.driver:
                return frmt
        return None

    @staticmethod
    def get_extension_by_driver(driver):
        """Get the appropriate file extension for the specified driver
        """
        frmt = LMFormat.get_format_by_driver(driver)
        if frmt is not None:
            return frmt.ext
        return None

    @staticmethod
    def is_geo(ext=None, driver=None):
        """Return boolean indicating if format is spatial
        """
        for format_ in LMFormat.spatial_formats():
            if ext is not None and ext == format_.ext:
                return True
            if driver is not None and driver == format_.driver:
                return True
        return False

    @staticmethod
    def is_ogr(ext=None, driver=None):
        """Return boolean indicating if format is OGR vector
        """
        for format_ in LMFormat.ogr_formats():
            if ext is not None and ext == format_.ext:
                return True
            if driver is not None and driver == format_.driver:
                return True
        return False

    @staticmethod
    def ogr_drivers():
        """Returns a list of OGR vector format drivers
        """
        return [format_.driver for format_ in LMFormat.ogr_formats()]

    @staticmethod
    def is_gdal(ext=None, driver=None):
        """Returns a boolean indication if the format is GDAL raster
        """
        for format_ in LMFormat.gdal_formats():
            if ext is not None and ext == format_.ext:
                return True
            if driver is not None and driver == format_.driver:
                return True
        return False

    @staticmethod
    def gdal_drivers():
        """Return a list of GDAL raster drivers
        """
        return [format_.driver for format_ in LMFormat.gdal_formats()]

    @staticmethod
    def is_json(ext):
        """Return a boolean indicating if the format is JSON
        """
        if ext in (LMFormat.GEO_JSON.ext, LMFormat.JSON.ext):
            return True
        return False

    @staticmethod
    def is_testable(ext):
        """Return a boolean indicating if the format is testable
        """
        if ext in (LMFormat.ASCII.ext, LMFormat.GTIFF.ext,
                   LMFormat.SHAPE.ext, LMFormat.JSON.ext):
            return True
        return False


# Web object interfaces
CSV_INTERFACE = 'csv'
EML_INTERFACE = 'eml'
GEO_JSON_INTERFACE = 'geojson'
GEOTIFF_INTERFACE = 'gtiff'
JSON_INTERFACE = 'json'
KML_INTERFACE = 'kml'
NEXUS_INTERFACE = 'nexus'
NEWICK_INTERFACE = 'newick'
PACKAGE_INTERFACE = 'package'
PROGRESS_INTERFACE = 'progress'
SHAPEFILE_INTERFACE = 'shapefile'


# .............................................................................
class MatrixType:
    """Constants to define the type of matrix (or set of matrix outputs)
    """
    # Inputs
    PAM = 1
    GRIM = 2
    BIOGEO_HYPOTHESES = 3
    ANC_PAM = 4
    ANC_STATE = 5
    ROLLING_PAM = 10
    # OUTPUTS
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
    MCPA_OUTPUTS = 201  # Could be used for permutations


class JobStatus:
    """Constants to define the status of a job.

    Note:
        0 - Not ready
        1 - Ready but not started
        Greater than 1 but less than 300 - Running
        300 - Completed
        1000 or greater - Error
    """
    GENERAL = 0  # Not ready
    INITIALIZE = 1  # Ready to run
    PULL_REQUESTED = 90  # Pull requested from job server
    COMPUTED = 130  # Finished computation

    # NOTIFY_READY = 210

    COMPLETE = 300  # Results computed and available

    # =========================================================================
    # =                            General Errors                             =
    # =========================================================================
    # Not found in database, could be prior to insertion
    NOT_FOUND = 1404

    GENERAL_ERROR = 1000  # Any status greater than this is an error
    UNKNOWN_ERROR = 1001  # Unknown error occurred

    # =========================================================================
    # =                              Common Errors                            =
    # =========================================================================
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

    # =========================================================================
    # =                            Lifemapper Errors                          =
    # =========================================================================
    # LM_GENERAL_ERROR = 2000 - conflicts with MODEL_ERROR and is not used.

    # Lifemapper data errors
    # ............................................
    LM_POINT_DATA_ERROR = 2301
    LM_RAW_POINT_DATA_ERROR = 2302

    LM_LONG_RUNNING_JOB_ERROR = 2499

    # =========================================================================
    # =                             SDM Errors                                =
    # =========================================================================

    # Mask error
    MASK_ERROR = 2500
    # openModeller errors
    # ............................................
    OM_GENERAL_ERROR = 3000

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
    ME_CORRUPTED_LAYER = 3602  # Could be issue with header or data
    ME_LAYER_MISSING = 3603
    ME_FILE_LOCK_ERROR = 3604

    # Maxent points issues
    # ............................................
    ME_POINTS_ERROR = 3740

    # Not enough points to trigger any feature classes
    ME_NO_FEATURES_CLASSES_AVAILABLE = 3751

    # Other Maxent problems
    # ............................................
    ME_HEAP_SPACE_ERROR = 3801
    ME_EXEC_MODEL_ERROR = 3802
    ME_EXEC_PROJECTION_ERROR = 3803

    # .......................................
    #  Occurrence set job errors
    # ...............................
    OCC_NO_POINTS_ERROR = 3901

    # =========================================================================
    # =                              HTTP Errors                              =
    # =========================================================================
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

    # =========================================================================
    # =                               IO Errors                               =
    # =========================================================================
    #   """
    #   Last digit meaning:
    #      0: General error
    #      1: Failed to read
    #      2: Failed to write
    #      3: Failed to delete
    #   """
    IO_GENERAL_ERROR = 6000

    # Matrix
    # ............................................
    IO_MATRIX_READ_ERROR = 6401

    # Occurrence Set jobs
    # ............................................
    IO_OCCURRENCE_SET_WRITE_ERROR = 6602

    # =========================================================================
    #                               Compute Status                            =
    # =========================================================================
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
    BLANK_PROJECTION_ERROR = 303100

    ENCODING_ERROR = 304100

    # ............................................
    @staticmethod
    def waiting(stat):
        """Return boolean indicating if status is a waiting status
        """
        return stat in (JobStatus.GENERAL, JobStatus.INITIALIZE)

    @staticmethod
    def incomplete(stat):
        """Return boolean indicating if the status is a non-terminal status
        """
        return stat < JobStatus.COMPLETE

    @staticmethod
    def finished(stat):
        """Return a boolean indicating if the status is a "finished" status
        """
        return stat >= JobStatus.COMPLETE

    @staticmethod
    def failed(stat):
        """Return boolean indication if the status is a failed status
        """
        return stat == JobStatus.NOT_FOUND or stat >= JobStatus.GENERAL_ERROR


# ............................................................................
# Aka reqSoftware in LmJob table
class ProcessType:
    """Process type enumeration
    """
    # .........................
    # SDM
    # .........................
    # SDM model
    ATT_MODEL = 110
    OM_MODEL = 210
    # SDM project
    ATT_PROJECT = 120
    OM_PROJECT = 220
    # Occurrences
    GBIF_TAXA_OCCURRENCE = 405
#     BISON_TAXA_OCCURRENCE = 410
#     IDIGBIO_TAXA_OCCURRENCE = 415
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
    BUILD_ANC_PAM = 370
    # Randomize
    RAD_SWAP = 331
    RAD_SPLOTCH = 332
    RAD_GRADY = 333
    # MCPA
    MCPA_CORRECT_PVALUES = 530
    MCPA_OBSERVED = 540
    MCPA_RANDOM = 550
    MCPA_ASSEMBLE = 560  # Assembles all MCPA outputs into a single matrix
    # .......... Notify ..........
    SMTP = 610
    CONCATENATE_MATRICES = 620

    # BOOM
    BOOM_INIT = 710
    #    BOOM_DAEMON = 720
    BOOMER = 720

    # .........................
    # TODO: deleteMe?
    # .........................
    RAD_INTERSECT = 310

    @staticmethod
    def is_occurrence(ptype):
        """Return boolean indicating if the process type is for occurrences
        """
        return ptype in (
            ProcessType.GBIF_TAXA_OCCURRENCE,
            # ProcessType.BISON_TAXA_OCCURRENCE,
            # ProcessType.IDIGBIO_TAXA_OCCURRENCE,
            ProcessType.USER_TAXA_OCCURRENCE)

    @staticmethod
    def is_project(ptype):
        """Return boolean indicating if the process type is for projections
        """
        return ptype in (ProcessType.ATT_PROJECT, ProcessType.OM_PROJECT)

    @staticmethod
    def is_intersect(ptype):
        """Return boolean indicating if the process type is for intersects
        """
        return ptype in (
            ProcessType.INTERSECT_RASTER, ProcessType.INTERSECT_VECTOR,
            ProcessType.INTERSECT_RASTER_GRIM)

    @staticmethod
    def is_rad(ptype):
        """Return boolean indicating if process type is for RAD process
        """
        return ptype in (
            ProcessType.SMTP, ProcessType.RAD_BUILDGRID,
            ProcessType.RAD_INTERSECT, ProcessType.RAD_SWAP,
            ProcessType.RAD_SPLOTCH, ProcessType.RAD_CALCULATE,
            ProcessType.RAD_GRADY, ProcessType.MCPA_CORRECT_PVALUES,
            ProcessType.MCPA_OBSERVED, ProcessType.MCPA_RANDOM,
            ProcessType.ENCODE_HYPOTHESES, ProcessType.ENCODE_PHYLOGENY,
            ProcessType.MCPA_ASSEMBLE, ProcessType.OCC_BUCKETEER,
            ProcessType.OCC_SORTER, ProcessType.OCC_SPLITTER,
            ProcessType.BUILD_ANC_PAM)

    @staticmethod
    def is_matrix(ptype):
        """Return boolean indicating if the process type generates a matrix
        """
        return ptype in (
            ProcessType.CONCATENATE_MATRICES, ProcessType.RAD_CALCULATE,
            ProcessType.ENCODE_HYPOTHESES, ProcessType.ENCODE_PHYLOGENY,
            ProcessType.RAD_SWAP, ProcessType.RAD_SPLOTCH,
            ProcessType.RAD_GRADY, ProcessType.MCPA_ASSEMBLE,
            ProcessType.MCPA_CORRECT_PVALUES, ProcessType.MCPA_OBSERVED,
            ProcessType.MCPA_RANDOM, ProcessType.BUILD_ANC_PAM)


# .............................................................................
# .                              Time constants                               .
# .............................................................................
# Time constants in Modified Julian Day (MJD) units
ONE_MONTH = 1.0 * 30
ONE_DAY = 1.0
ONE_HOUR = 1.0 / 24.0
ONE_MIN = 1.0 / 1440.0
ONE_SEC = 1.0 / 86400.0

# Time formats
ISO_8601_TIME_FORMAT_FULL = "%Y-%m-%dT%H:%M:%SZ"
ISO_8601_TIME_FORMAT_TRUNCATED = "%Y-%m-%d"
YMD_HH_MM_SS = "%Y-%m-%d %H:%M%S"

ONE_HOUR_SECONDS = 60 * 60

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
DEFAULT_NODATA = -9999


DWC_QUALIFIER = 'dwc:'


class DwcNames:
    """Darwin core names enumeration
    """
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
    STATE_PROVINCE = {'FULL': 'stateProvince', 'SHORT': 'stprov'}

    @staticmethod
    def _defined_names():
        return [
            DwcNames.OCCURRENCE_ID, DwcNames.INSTITUTION_CODE,
            DwcNames.INSTITUTION_ID, DwcNames.COLLECTION_CODE,
            DwcNames.COLLECTION_ID, DwcNames.CONTINENT,
            DwcNames.CATALOG_NUMBER, DwcNames.BASIS_OF_RECORD,
            DwcNames.DECIMAL_LATITUDE, DwcNames.DECIMAL_LONGITUDE,
            DwcNames.SCIENTIFIC_NAME, DwcNames.DAY, DwcNames.MONTH,
            DwcNames.YEAR, DwcNames.RECORDED_BY, DwcNames.COUNTRY_CODE,
            DwcNames.STATE_PROVINCE]


# ......................................................
PROVIDER_FIELD_COMMON = 'provider'


# ......................................................
class GBIF:
    """GBIF constants enumeration
    """
    DATA_DUMP_DELIMITER = '\t'
    TAXON_KEY = 'specieskey'
    TAXON_NAME = DwcNames.SCIENTIFIC_NAME['SHORT']
    PROVIDER = 'puborgkey'
    GBIFID = 'gbifid'
    WAIT_TIME = 3 * ONE_MIN
    LIMIT = 300
    REST_URL = 'http://api.gbif.org/v1'
    QUALIFIER = 'gbif:'

    SPECIES_SERVICE = 'species'
    PARSER_SERVICE = 'parser/name'
    OCCURRENCE_SERVICE = 'occurrence'
    DATASET_SERVICE = 'dataset'
    ORGANIZATION_SERVICE = 'organization'

    TAXONKEY_FIELD = 'specieskey'
    TAXONNAME_FIELD = DwcNames.SCIENTIFIC_NAME['SHORT']
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


class GbifQuery:
    """GBIF query constants
    """
    TAXON_FIELDS = {0: ('taxonkey', OFTString),
                    1: ('kingdom', OFTString),
                    2: ('phylum', OFTString),
                    3: ('class', OFTString),
                    4: ('order', OFTString),
                    5: ('family', OFTString),
                    6: ('genus', OFTString),
                    7: (DwcNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                    8: ('genuskey', OFTInteger),
                    9: (GBIF.TAXONKEY_FIELD, OFTInteger),
                    10: ('count', OFTInteger)}
    EXPORT_FIELDS = {0: (GBIF.ID_FIELD, OFTInteger),
                     1: (DwcNames.OCCURRENCE_ID['SHORT'], OFTInteger),
                     2: ('taxonkey', OFTInteger),
                     3: ('datasetkey', OFTString),
                     4: (GBIF.PROVIDER_FIELD, OFTString),
                     5: (DwcNames.BASIS_OF_RECORD['SHORT'], OFTString),
                     6: ('kingdomkey', OFTInteger),
                     7: ('phylumkey', OFTInteger),
                     8: ('classkey', OFTInteger),
                     9: ('orderkey', OFTInteger),
                     10: ('familykey', OFTInteger),
                     11: ('genuskey', OFTInteger),
                     12: (GBIF.TAXONKEY_FIELD, OFTInteger),
                     13: (DwcNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                     14: (DwcNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
                     15: (DwcNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
                     16: (DwcNames.DAY['SHORT'], OFTInteger),
                     17: (DwcNames.MONTH['SHORT'], OFTInteger),
                     18: (DwcNames.YEAR['SHORT'], OFTInteger),
                     19: (DwcNames.RECORDED_BY['SHORT'], OFTString),
                     20: (DwcNames.INSTITUTION_CODE['SHORT'], OFTString),
                     21: (DwcNames.COLLECTION_CODE['SHORT'], OFTString),
                     22: (DwcNames.CATALOG_NUMBER['SHORT'], OFTString)}
    PARAMS = {GBIF.SPECIES_SERVICE: {'status': 'ACCEPTED',
                                     GBIF.REQUEST_RANK_KEY: None,
                                     GBIF.REQUEST_DATASET_KEY: None,
                                     GBIF.REQUEST_NAME_QUERY_KEY: None},
              GBIF.OCCURRENCE_SERVICE: {
                  "GEOREFERENCED": True,
                  "SPATIAL_ISSUES": False,
                  # "BASIS_OF_RECORD": ["PRESERVED_SPECIMEN"],
                  GBIF.REQUEST_TAXON_KEY: None},
              GBIF.DOWNLOAD_COMMAND: {
                  "creator": "aimee",
                  "notification_address": ["lifemapper@mailinator.com"]}}


# .............................................................................
# .                               BISON/ITIS constants                        .
# .............................................................................
# ......................................................
# For parsing BISON Solr API response, updated Feb 2015
class BISON:
    """Bison constant enumeration
    """
    OCCURRENCE_URL = 'https://bison.usgs.gov/solr/occurrences/select'
    # Ends in : to allow appending unique id
    LINK_PREFIX = ('https://bison.usgs.gov/solr/occurrences/select/' +
                   '?q=occurrenceID:')
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


# .............................................................................
class BisonQuery:
    """BISON query constants enumeration
    """
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
    RESPONSE_FIELDS = {
        'ITIScommonName': ('comname', OFTString),
        BISON.NAME_KEY: (DwcNames.SCIENTIFIC_NAME['SHORT'], OFTString),
        'ITIStsn': ('itistsn', OFTInteger),
        BISON.TSN_KEY: None,
        'ambiguous': None,
        DwcNames.BASIS_OF_RECORD['FULL']: (
            DwcNames.BASIS_OF_RECORD['SHORT'], OFTString),
        'calculatedCounty': ('county', OFTString),
        'calculatedState': ('state', OFTString),
        DwcNames.CATALOG_NUMBER['FULL']: (
            DwcNames.CATALOG_NUMBER['SHORT'], OFTString),
        'collectionID': ('coll_id', OFTString),
        'computedCountyFips': None,
        'computedStateFips': None,
        DwcNames.COUNTRY_CODE['FULL']: (
            DwcNames.COUNTRY_CODE['SHORT'], OFTString),
        DwcNames.DECIMAL_LATITUDE['FULL']: (
            DwcNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
        DwcNames.DECIMAL_LONGITUDE['FULL']: (
            DwcNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
        'eventDate': ('date', OFTString),
        # Space delimited, same as latlon
        'geo': None,
        BISON.HIERARCHY_KEY: ('tsn_hier', OFTString),
        'institutionID': ('inst_id', OFTString),
        BISON.KINGDOM_KEY: ('kingdom', OFTString),
        # Comma delimited, same as geo
        'latlon': ('latlon', OFTString),
        DwcNames.OCCURRENCE_ID['FULL']: (
            DwcNames.OCCURRENCE_ID['SHORT'], OFTInteger),
        'ownerInstitutionCollectionCode': (PROVIDER_FIELD_COMMON, OFTString),
        'pointPath': None,
        'providedCounty': None,
        'providedScientificName': None,
        'providerID': None,
        DwcNames.RECORDED_BY['FULL']: (
            DwcNames.RECORDED_BY['SHORT'], OFTString),
        'resourceID': None,
        # Use ITIS Scientific Name
        'scientificName': None,
        'stateProvince': ('stprov', OFTString),
        DwcNames.YEAR['SHORT']: (DwcNames.YEAR['SHORT'], OFTInteger),
        # Very long integer
        '_version_': None
    }


# .............................................................................
class Itis:
    """ITIS constants enumeration
    """
    DATA_NAMESPACE = 'http://data.itis_service.itis.usgs.gov/xsd'
    # Basic Web Services
    TAXONOMY_HIERARCHY_URL = ('http://www.itis.gov/ITISWebService/services/' +
                              'ITISService/getFullHierarchyFromTSN')
    # JSON Web Services
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
class Idigbio:
    """iDigBio constants enumeration
    """
    LINK_PREFIX = 'https://www.idigbio.org/portal/records/'
    SEARCH_PREFIX = 'https://search.idigbio.org/v2'
    SEARCH_POSTFIX = 'search'
    OCCURRENCE_POSTFIX = 'records'
    PUBLISHERS_POSTFIX = 'publishers'
    RECORDSETS_POSTFIX = 'recordsets'
    SEARCH_LIMIT = 5000
    ID_FIELD = 'uuid'
    LINK_FIELD = 'idigbiourl'
    GBIFID_FIELD = 'taxonid'
    BINOMIAL_REGEX = "(^[^ ]*) ([^ ]*)$"
    OCCURRENCE_ITEMS_KEY = 'items'
    RECORD_CONTENT_KEY = 'data'
    RECORD_INDEX_KEY = 'indexTerms'
    QUALIFIER = 'idigbio:'


# .............................................................................
class IdigbioDump:
    """iDigBio dump constants enumeration
    """
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
    METADATA = {
        0: {'name': 'coreid', 'type': OFTString},
        1: None,
        2: None,
        3: {'name': DwcNames.BASIS_OF_RECORD['SHORT'], 'type': OFTString},
        4: None,
        5: {'name': 'canonical', 'type': OFTString, 'role': 'taxaname'},
        6: {'name': DwcNames.CATALOG_NUMBER['SHORT'], 'type': OFTString},
        7: None,
        8: {'name': DwcNames.COLLECTION_CODE['SHORT'], 'type': OFTString},
        9: {'name': DwcNames.COLLECTION_ID['SHORT'], 'type': OFTString},
        10: None,
        11: {'name': DwcNames.RECORDED_BY['SHORT'], 'type': OFTString},
        12: None,
        13: None,
        14: None,
        15: None,
        16: None,
        17: {'name': DwcNames.COUNTRY_CODE['SHORT'], 'type': OFTString},
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
        43: {'name': DwcNames.INSTITUTION_CODE['SHORT'], 'type': OFTString},
        44: {'name': DwcNames.INSTITUTION_ID['SHORT'], 'type': OFTString},
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
        62: {'name': DwcNames.OCCURRENCE_ID['SHORT'], 'type': OFTString},
        63: None,
        64: None,
        65: None,
        66: None,
        67: {'name': 'recordset', 'type': OFTString},
        68: {'name': DwcNames.SCIENTIFIC_NAME['SHORT'], 'type': OFTString},
        69: None,
        70: None,
        71: {'name': DwcNames.STATE_PROVINCE['SHORT'], 'type': OFTString},
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


# .............................................................................
class IdigbioQuery:
    """iDigBio query constants enumeration
    """
    EXPORT_FIELDS = {0: (Idigbio.ID_FIELD, OFTString),
                     1: (DwcNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
                     2: (DwcNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
                     3: (DwcNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                     4: (PROVIDER_FIELD_COMMON, OFTString)}
    # Geopoint.lat and Geopoint.lon are modified on return to short names
    # Response record fields: https://search.idigbio.org/v2/meta/fields/records
    RETURN_FIELDS = {
        Idigbio.QUALIFIER + Idigbio.ID_FIELD: (Idigbio.ID_FIELD, OFTString),
        Idigbio.GBIFID_FIELD: ('taxonid', OFTString),
        DWC_QUALIFIER + DwcNames.SCIENTIFIC_NAME['FULL']: (
            DwcNames.SCIENTIFIC_NAME['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.BASIS_OF_RECORD['FULL']: (
            DwcNames.BASIS_OF_RECORD['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.CATALOG_NUMBER['FULL']: (
            DwcNames.CATALOG_NUMBER['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.COLLECTION_ID['FULL']: (
            DwcNames.COLLECTION_ID['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.COLLECTION_CODE['FULL']: (
            DwcNames.COLLECTION_CODE['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.RECORDED_BY['FULL']: (
            DwcNames.RECORDED_BY['SHORT'], OFTString),
        'commonname': ('comname', OFTString),
        DWC_QUALIFIER + DwcNames.CONTINENT['FULL']: (
            DwcNames.CONTINENT['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.COUNTRY_CODE['FULL']: (
            DwcNames.COUNTRY_CODE['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.DAY['FULL']: (
            DwcNames.DAY['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.MONTH['FULL']: (
            DwcNames.MONTH['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.YEAR['FULL']: (
            DwcNames.YEAR['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.INSTITUTION_CODE['FULL']: (
            DwcNames.INSTITUTION_CODE['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.INSTITUTION_ID['FULL']: (
            DwcNames.INSTITUTION_ID['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.OCCURRENCE_ID['FULL']: (
            DwcNames.OCCURRENCE_ID['SHORT'], OFTInteger),
        DWC_QUALIFIER + DwcNames.STATE_PROVINCE['FULL']: (
            DwcNames.STATE_PROVINCE['SHORT'], OFTString),
        DWC_QUALIFIER + DwcNames.DECIMAL_LATITUDE['FULL']: (
            DwcNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
        DWC_QUALIFIER + DwcNames.DECIMAL_LONGITUDE['FULL']: (
            DwcNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
        }
    QFILTERS = {'basisofrecord': 'preservedspecimen'}
    #    queryFlds = IDIGBIO_RETURN_FIELDS.keys()
    #    queryFlds.append('geopoint')

    FILTERS = {  # 'fields': queryFlds,
        'limit': Idigbio.SEARCH_LIMIT,
        'offset': 0,
        'no_attribution': False}


# .............................................................................
# .                  Provider/Local data fieldname constants                  .
# .............................................................................
LM_WKT_FIELD = 'geomwkt'

# .............................................................................
# .                              Other constants                              .
# .............................................................................
LEGAL_MAP_UNITS = [
    'feet', 'inches', 'kilometers', 'meters', 'miles', 'dd', 'ds']

URL_ESCAPES = [[" ", "%20"], [",", "%2C"]]


class HTTPStatus:
    """HTTP 1.1 Status Codes

    See:
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
class PamStatKeys:
    """Class containing PAM statistics keys

    Todo:
        Link to literature
    """
    # Site statistics
    # ..................
    # Alpha is the species richness (number present) per site
    ALPHA = 'alpha'
    # Alpha prop. is the proportion of the entire set of species present per
    #    site
    ALPHA_PROP = 'alphaProp'
    # Phi is the range size per site
    PHI = 'phi'
    # PHI_AVG_PROP is the mean proportional range size per site
    PHI_AVG_PROP = 'phiProp'

    # Tree (site) statistics
    # ..................
    # MNTD is mean nearest taxon distance per site.  The average distance to
    #    the nearest taxon for every species present at a site
    MNTD = 'mntd'
    # MPD is the mean pairwise distance per site.  This is the average distance
    #    to all other species for each species at each site
    MPD = 'mpd'
    # PEARSON is Pearson's Correlation Coefficient
    PEARSON = 'pearson'
    # PD is Phylogenetic Diversity as defined by sum of the branch lengths for
    #    the minimum spanning path of the involved taxa
    PD = 'Phylogenetic Diversity'
    MNND = 'Mean Nearest Neighbor Distance'
    MPHYLODIST = 'Mean Phylogenetic Distance'
    SPD = 'Sum of Phylogenetic Distance'

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
    C_SCORE = 'c_score'

    # Covariance matrices
    # ..................
    # Covariance matrices for the composition of sites and the range of species
    SITES_COVARIANCE = 'sitesCovariance'
    SPECIES_COVARIANCE = 'speciesCovariance'

    # Schluter's statistics
    # ..................
    # These are Schluter's statistics for site and species variance ratios in
    #    a PAM
    SPECIES_VARIANCE_RATIO = 'varSpeciesRatio'
    SITES_VARIANCE_RATIO = 'varSitesRatio'


# .............................................................................
# .                     Phylogenetic Tree Module Constants                    .
# .............................................................................
DEFAULT_TREE_SCHEMA = 'nexus'


# .............................
class PhyloTreeKeys:
    """Keys for phylogenetic trees
    """
    MTX_IDX = 'mx'  # The matrix index for this node
    SQUID = 'squid'  # This is the LM SQUID (species identifier) for the tip
