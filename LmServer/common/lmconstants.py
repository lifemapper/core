"""Lifemapper server constants.
"""
import inspect
import os

from osgeo.gdalconst import (
    GDT_Byte, GDT_CInt16, GDT_CInt32, GDT_CFloat32, GDT_CFloat64, GDT_Float32,
    GDT_Float64, GDT_Int16, GDT_Int32, GDT_UInt16, GDT_UInt32, GDT_Unknown)
from osgeo.ogr import (
    wkbLineString, wkbMultiLineString, wkbMultiPoint, wkbMultiPolygon,
    wkbPoint, wkbPolygon)

from LmCommon.common.lmconstants import (
    DwcNames, JobStatus, LMFormat, MatrixType)
from LmServer.common.localconstants import (
    APP_PATH, CS_PORT, DATA_PATH, DEFAULT_EPSG, EXTRA_CS_OPTIONS,
    EXTRA_MAKEFLOW_OPTIONS, EXTRA_WORKER_FACTORY_OPTIONS, EXTRA_WORKER_OPTIONS,
    LM_DISK, MASTER_WORKER_PATH, MAX_WORKERS, PID_PATH, PUBLIC_FQDN,
    SCRATCH_PATH, SHARED_DATA_PATH, SPECIES_DIR, WEBSERVICES_ROOT, WORKER_PATH)

WEB_SERVICE_VERSION = 'v2'
API_PATH = 'api'
API_URL = '/'.join([WEBSERVICES_ROOT, API_PATH, WEB_SERVICE_VERSION])
OGC_SERVICE_URL = '/'.join([API_URL, 'ogc'])
DEFAULT_EMAIL_POSTFIX = '@nowhere.org'
BIN_PATH = os.path.join(APP_PATH, 'bin')
# Relative paths
USER_LAYER_DIR = 'Layers'
USER_MAKEFLOW_DIR = 'makeflow'
USER_TEMP_DIR = 'temp'
# On shared data directory (shared if lifemapper-compute is also installed)
ENV_DATA_PATH = os.path.join(SHARED_DATA_PATH, 'layers')
ARCHIVE_PATH = os.path.join(SHARED_DATA_PATH, 'archive')
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

CHERRYPY_CONFIG_FILE = os.path.join(APP_PATH, 'config', 'cherrypy.conf')
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

CS_OPTIONS = '-n {} -B {} -p {} -o {} -H {} {}'.format(
    PUBLIC_FQDN, CS_PID_FILE, CS_PORT, CS_LOG_FILE, CS_HISTORY_FILE,
    EXTRA_CS_OPTIONS)

# Worker options
WORKER_OPTIONS = '-C {}:{} -s {} {}'.format(
    PUBLIC_FQDN, CS_PORT, WORKER_PATH, EXTRA_WORKER_OPTIONS)

# Makeflow options
MAKEFLOW_OPTIONS = '-X {} -a -C {}:{} {}'.format(
    MASTER_WORKER_PATH, PUBLIC_FQDN, CS_PORT, EXTRA_MAKEFLOW_OPTIONS)

# Worker factory options
WORKER_FACTORY_OPTIONS = '-w {0} -W {0} -E "{1}" -S {2} {3}'.format(
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
    """Database user constant class."""
    Map = 'mapuser'
    WebService = 'wsuser'
    Pipeline = 'sdlapp'
    Job = 'jobuser'
    Anon = 'anon'


# ............................................................................
class PrimaryEnvironment:
    """Primary environment for an instance class."""
    TERRESTRIAL = 1
    MARINE = 2


# ............................................................................
class ReferenceType:
    """Reference type constant class."""
    SDMProjection = 102
    OccurrenceSet = 104
    MatrixColumn = 201
    ShapeGrid = 202
    Matrix = 301
    Gridset = 401

    @staticmethod
    def name(rt):
        attrs = inspect.getmembers(
            ReferenceType, lambda a: not inspect.isroutine(a))
        for att in attrs:
            if rt == att[1]:
                return att[0]
        return None

    @staticmethod
    def sdm_types():
        return [ReferenceType.OccurrenceSet, ReferenceType.SDMProjection]

    @staticmethod
    def is_sdm(rtype):
        if rtype in ReferenceType.sdm_types():
            return True
        return False

    @staticmethod
    def rad_types():
        return [
            ReferenceType.ShapeGrid, ReferenceType.MatrixColumn,
            ReferenceType.Matrix, ReferenceType.Gridset]

    @staticmethod
    def is_rad(rtype):
        if rtype in ReferenceType.rad_types():
            return True
        return False

    @staticmethod
    def boom_types():
        all_types = ReferenceType.sdm_types()
        all_types.extend(ReferenceType.rad_types())
        return all_types

    @staticmethod
    def is_boom(rtype):
        if rtype in ReferenceType.boom_types():
            return True
        return False

    @staticmethod
    def progress_types():
        return [
            ReferenceType.OccurrenceSet, ReferenceType.SDMProjection,
            ReferenceType.MatrixColumn, ReferenceType.Matrix]

    @staticmethod
    def status_types():
        tps = ReferenceType.progress_types()
        tps.append(ReferenceType.ShapeGrid)
        return tps


# .............................................................................
class OccurrenceFieldNames:
    """Occurrence filed names enumeration."""
    LOCAL_ID = ['localid', 'localId', 'occkey']
    UUID = ['uuid']
    LONGITUDE = ['longitude', 'x', 'lon', 'long',
                 DwcNames.DECIMAL_LONGITUDE['SHORT'],
                 DwcNames.DECIMAL_LONGITUDE['FULL']]
    LATITUDE = ['latitude', 'y', 'lat', DwcNames.DECIMAL_LATITUDE['SHORT'],
                DwcNames.DECIMAL_LATITUDE['FULL']]
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

WEB_MERCATOR_EPSG = '3857'
DEFAULT_SRS = 'epsg:{}'.format(DEFAULT_EPSG)
DEFAULT_WCS_FORMAT = 'image/tiff'
MAXENT_WCS_FORMAT = 'image/x-aaigrid'
DEFAULT_WMS_FORMAT = 'image/png'

# Map/data service parameters
MAP_TEMPLATE = 'template'
QUERY_TEMPLATE = 'query.html'
BLUE_MARBLE_IMAGE = 'BMNG.geotiff'
QUERY_TOLERANCE = 3
# POINT_SYMBOL and LINE_SYMBOL values are in symbols.txt, referenced in
#    mapfiles
POINT_SYMBOL = 'filledcircle'
LINE_SYMBOL = 'line'
POLYGON_SYMBOL = None
POINT_SIZE = 5
LINE_SIZE = 3
# Outline Width
POLYGON_SIZE = 0.91

# Gridset
RAD_EXPERIMENT_DIR_PREFIX = 'RAD'


# .............................................................................
class MapPrefix:
    """Map data prefix enumeration"""
    SDM = 'data'
    USER = 'usr'
    # Computed map products for bucket
    RAD = 'rad'
    ANC = 'anc'
    SCEN = 'scen'


# .............................................................................
class LMFileType:
    """Lifemapper file type enumeration"""
    # User level
    OTHER_MAP = 1
    TMP_JSON = 2
    ANCILLARY_MAP = 3
    TEMP_USER_DATA = 4
    # ..............................
    # Single species
    ENVIRONMENTAL_LAYER = 101
    SCENARIO_MAP = 102
    # Occurrence level (SDM data are organized by OccurrenceSets)
    SDM_MAP = 110
    OCCURRENCE_FILE = 111
    OCCURRENCE_RAW_FILE = 112
    OCCURRENCE_META_FILE = 113
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
    ATTR_MATRIX = 210  # not yet used
    SHAPEGRID = 201  # TODO: delete??
    PAM = 222
    GRIM = 223
    ANC_PAM = 224

    SUM_CALCS = 241
    SUM_SHAPE = 242
    CALCS = 243  # New rad calcs
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
    def is_sdm(rtype):
        if rtype in [
                LMFileType.MODEL_ATT_RESULT, LMFileType.MODEL_REQUEST,
                LMFileType.MODEL_RESULT, LMFileType.MODEL_STATS,
                LMFileType.OCCURRENCE_FILE, LMFileType.OCCURRENCE_LARGE_FILE,
                LMFileType.OCCURRENCE_META_FILE,
                LMFileType.OCCURRENCE_RAW_FILE, LMFileType.PROJECTION_LAYER,
                LMFileType.PROJECTION_PACKAGE, LMFileType.PROJECTION_REQUEST,
                LMFileType.SDM_MAP]:
            return True
        return False

    @staticmethod
    def is_rad(rtype):
        if rtype in [
                LMFileType.ANC_PAM, LMFileType.ATTR_MATRIX, LMFileType.CALCS,
                LMFileType.BIOGEO_HYPOTHESES, LMFileType.DIVERSITY_OBSERVED,
                LMFileType.DIVERSITY_RANDOM, LMFileType.GRIDSET_PACKAGE,
                LMFileType.GRIM, LMFileType.PAM, LMFileType.MCPA_OUTPUTS,
                LMFileType.RAD_MAP, LMFileType.RANDOM_CALC,
                LMFileType.SCHLUTER_OBSERVED, LMFileType.SCHLUTER_RANDOM,
                LMFileType.SITES_COV_OBSERVED, LMFileType.SITES_COV_RANDOM,
                LMFileType.SITES_OBSERVED, LMFileType.SITES_RANDOM,
                LMFileType.SPECIES_COV_OBSERVED, LMFileType.SPECIES_COV_RANDOM,
                LMFileType.SPECIES_OBSERVED, LMFileType.SPECIES_RANDOM,
                LMFileType.SUM_CALCS, LMFileType.SUM_SHAPE, LMFileType.TREE,
                LMFileType.UNSPECIFIED_RAD]:
            return True
        return False

    @staticmethod
    def is_user_space(rtype):
        if rtype in [
                LMFileType.BOOM_CONFIG, LMFileType.TMP_JSON, LMFileType.TREE,
                LMFileType.USER_ATTRIBUTE_MATRIX, LMFileType.USER_TREE]:
            return True
        return False

    @staticmethod
    def is_user_layer(rtype):
        if rtype in [LMFileType.ENVIRONMENTAL_LAYER, LMFileType.SHAPEGRID,
                     LMFileType.USER_LAYER, LMFileType.USER_SHAPEGRID]:
            return True
        return False

    @staticmethod
    def map_types():
        return [
            LMFileType.ANCILLARY_MAP, LMFileType.OTHER_MAP, LMFileType.RAD_MAP,
            LMFileType.SCENARIO_MAP, LMFileType.SDM_MAP]

    @staticmethod
    def is_map(rtype):
        if rtype in LMFileType.map_types():
            return True
        return False

    @staticmethod
    def is_matrix(rtype):
        if rtype in [
                LMFileType.ANC_PAM, LMFileType.BIOGEO_HYPOTHESES,
                LMFileType.CALCS, LMFileType.DIVERSITY_OBSERVED,
                LMFileType.DIVERSITY_RANDOM, LMFileType.GRIM,
                LMFileType.MCPA_OUTPUTS, LMFileType.PADDED_PAM, LMFileType.PAM,
                LMFileType.RANDOM_CALC, LMFileType.SCHLUTER_OBSERVED,
                LMFileType.SCHLUTER_RANDOM, LMFileType.SITES_COV_OBSERVED,
                LMFileType.SITES_COV_RANDOM, LMFileType.SITES_OBSERVED,
                LMFileType.SITES_RANDOM, LMFileType.SPECIES_COV_OBSERVED,
                LMFileType.SPECIES_COV_RANDOM, LMFileType.SPECIES_OBSERVED,
                LMFileType.SPECIES_RANDOM, LMFileType.TREE]:
            return True
        return False

    @staticmethod
    def get_matrix_filetype(mtype):
        if mtype in (MatrixType.PAM, MatrixType.ROLLING_PAM):
            return LMFileType.PAM
        if mtype == MatrixType.ANC_PAM:
            return LMFileType.ANC_PAM
        if mtype == MatrixType.GRIM:
            return LMFileType.GRIM
        if mtype == MatrixType.BIOGEO_HYPOTHESES:
            return LMFileType.BIOGEO_HYPOTHESES
        # if mtype == MatrixType.PADDED_PAM:
        #    return LMFileType.PADDED_PAM
        if mtype == MatrixType.MCPA_OUTPUTS:
            return LMFileType.MCPA_OUTPUTS
        if mtype == MatrixType.SITES_OBSERVED:
            return LMFileType.SITES_OBSERVED
        if mtype == MatrixType.SPECIES_OBSERVED:
            return LMFileType.SPECIES_OBSERVED
        if mtype == MatrixType.DIVERSITY_OBSERVED:
            return LMFileType.DIVERSITY_OBSERVED
        if mtype == MatrixType.SCHLUTER_OBSERVED:
            return LMFileType.SCHLUTER_OBSERVED
        if mtype == MatrixType.SPECIES_COV_OBSERVED:
            return LMFileType.SPECIES_COV_OBSERVED
        if mtype == MatrixType.SITES_COV_OBSERVED:
            return LMFileType.SITES_COV_OBSERVED
        if mtype == MatrixType.RANDOM_CALC:
            return LMFileType.RANDOM_CALC
        if mtype == MatrixType.SITES_RANDOM:
            return LMFileType.SITES_RANDOM
        if mtype == MatrixType.SPECIES_RANDOM:
            return LMFileType.SPECIES_RANDOM
        if mtype == MatrixType.DIVERSITY_RANDOM:
            return LMFileType.DIVERSITY_RANDOM
        if mtype == MatrixType.SCHLUTER_RANDOM:
            return LMFileType.SCHLUTER_RANDOM
        if mtype == MatrixType.SPECIES_COV_RANDOM:
            return LMFileType.SPECIES_COV_RANDOM
        if mtype == MatrixType.SITES_COV_RANDOM:
            return LMFileType.SITES_COV_RANDOM
        return None


NAME_SEPARATOR = '_'

# Mapfile layer name
OCC_NAME_PREFIX = 'occ'
GENERIC_LAYER_NAME_PREFIX = 'lyr'

OCC_PREFIX = 'pt'
PRJ_PREFIX = 'prj'
PAMSUM_PREFIX = 'pamsum'


class FileFix:
    PREFIX = {
        LMFileType.ANC_PAM: 'anc_pam',
        LMFileType.ANCILLARY_MAP: MapPrefix.ANC,
        LMFileType.ATTR_MATRIX: 'attrMtx',
        LMFileType.BIOGEO_HYPOTHESES: 'biogeo',
        LMFileType.BOOM_CONFIG: None,
        LMFileType.CALCS: 'calc',  # TODO: Add to this?
        LMFileType.DIVERSITY_OBSERVED: 'diversity',
        LMFileType.DIVERSITY_RANDOM: 'diversityRand',
        LMFileType.ENVIRONMENTAL_LAYER: None,
        LMFileType.GRIDSET_PACKAGE: 'gsPkg',
        LMFileType.GRIM: 'grim',
        LMFileType.MCPA_OUTPUTS: 'mcpa',
        LMFileType.MF_DOCUMENT: 'mf',
        LMFileType.MODEL_ATT_RESULT: None,
        LMFileType.MODEL_REQUEST: 'modReq',
        LMFileType.MODEL_RESULT: None,
        LMFileType.MODEL_STATS: None,
        LMFileType.OCCURRENCE_FILE: OCC_PREFIX,
        LMFileType.OCCURRENCE_LARGE_FILE: 'big' + OCC_PREFIX,
        LMFileType.OCCURRENCE_META_FILE: OCC_PREFIX,
        LMFileType.OCCURRENCE_RAW_FILE: OCC_PREFIX,
        LMFileType.OTHER_MAP: MapPrefix.USER,
        LMFileType.PADDED_PAM: 'ppam',
        LMFileType.PAM: 'pam',
        LMFileType.PROJECTION_LAYER: PRJ_PREFIX,
        LMFileType.PROJECTION_PACKAGE: PRJ_PREFIX,
        LMFileType.PROJECTION_REQUEST: 'projReq',
        LMFileType.RAD_MAP: MapPrefix.RAD,
        LMFileType.RANDOM_CALC: 'randCalc',
        LMFileType.SCENARIO_MAP: MapPrefix.SCEN,
        LMFileType.SCHLUTER_OBSERVED: 'schluter',
        LMFileType.SCHLUTER_RANDOM: 'schluterRand',
        LMFileType.SDM_MAP: MapPrefix.SDM,
        LMFileType.SHAPEGRID: 'shpgrid',
        LMFileType.SITES_COV_OBSERVED: 'siteCovObs',
        LMFileType.SITES_COV_RANDOM: 'siteCovRand',
        LMFileType.SITES_OBSERVED: 'sites',
        LMFileType.SITES_RANDOM: 'sitesRand',
        LMFileType.SPECIES_COV_OBSERVED: 'speciesCovObs',
        LMFileType.SPECIES_COV_RANDOM: 'speciesCovRand',
        LMFileType.SPECIES_OBSERVED: 'species',
        LMFileType.SPECIES_RANDOM: 'speciesRand',
        LMFileType.SUM_CALCS: PAMSUM_PREFIX,
        LMFileType.SUM_SHAPE: PAMSUM_PREFIX,
        LMFileType.TMP_JSON: None,
        LMFileType.TREE: 'tree',
        LMFileType.UNSPECIFIED_RAD: None,
        LMFileType.USER_ATTRIBUTE_MATRIX: 'attributes',
        LMFileType.USER_LAYER: GENERIC_LAYER_NAME_PREFIX,
        LMFileType.USER_SHAPEGRID: None,
        LMFileType.USER_TREE: 'tree'
    }

    # Postfix
    EXTENSION = {
        LMFileType.ANC_PAM: LMFormat.MATRIX.ext,
        LMFileType.ANCILLARY_MAP: LMFormat.MAP.ext,
        LMFileType.ATTR_MATRIX: LMFormat.NUMPY.ext,
        LMFileType.BIOGEO_HYPOTHESES: LMFormat.MATRIX.ext,
        LMFileType.BOOM_CONFIG: LMFormat.CONFIG.ext,
        LMFileType.CALCS: LMFormat.MATRIX.ext,
        LMFileType.DIVERSITY_OBSERVED: LMFormat.MATRIX.ext,
        LMFileType.DIVERSITY_RANDOM: LMFormat.MATRIX.ext,
        LMFileType.ENVIRONMENTAL_LAYER: LMFormat.GTIFF.ext,
        LMFileType.GRIDSET_PACKAGE: LMFormat.ZIP.ext,
        LMFileType.GRIM: LMFormat.MATRIX.ext,
        LMFileType.MCPA_OUTPUTS: LMFormat.MATRIX.ext,
        LMFileType.MF_DOCUMENT: LMFormat.MAKEFLOW.ext,
        LMFileType.MODEL_ATT_RESULT: LMFormat.TXT.ext,
        LMFileType.MODEL_REQUEST: LMFormat.XML.ext,
        LMFileType.MODEL_RESULT: LMFormat.XML.ext,
        LMFileType.MODEL_STATS: LMFormat.ZIP.ext,
        LMFileType.OCCURRENCE_FILE: LMFormat.SHAPE.ext,
        LMFileType.OCCURRENCE_LARGE_FILE: LMFormat.SHAPE.ext,
        LMFileType.OCCURRENCE_META_FILE: LMFormat.METADATA.ext,
        LMFileType.OCCURRENCE_RAW_FILE: LMFormat.CSV.ext,
        LMFileType.OTHER_MAP: LMFormat.MAP.ext,
        LMFileType.PADDED_PAM: LMFormat.MATRIX.ext,
        LMFileType.PAM: LMFormat.MATRIX.ext,
        LMFileType.PROJECTION_LAYER: LMFormat.GTIFF.ext,
        LMFileType.PROJECTION_PACKAGE: LMFormat.ZIP.ext,
        LMFileType.PROJECTION_REQUEST: LMFormat.XML.ext,
        LMFileType.RAD_MAP: LMFormat.MAP.ext,
        LMFileType.RANDOM_CALC: LMFormat.MATRIX.ext,
        LMFileType.SCENARIO_MAP: LMFormat.MAP.ext,
        LMFileType.SCHLUTER_OBSERVED: LMFormat.MATRIX.ext,
        LMFileType.SCHLUTER_RANDOM: LMFormat.MATRIX.ext,
        LMFileType.SDM_MAP: LMFormat.MAP.ext,
        LMFileType.SHAPEGRID:  LMFormat.SHAPE.ext,
        LMFileType.SITES_COV_OBSERVED: LMFormat.MATRIX.ext,
        LMFileType.SITES_COV_RANDOM: LMFormat.MATRIX.ext,
        LMFileType.SITES_OBSERVED: LMFormat.MATRIX.ext,
        LMFileType.SITES_RANDOM: LMFormat.MATRIX.ext,
        LMFileType.SPECIES_COV_OBSERVED: LMFormat.MATRIX.ext,
        LMFileType.SPECIES_COV_RANDOM: LMFormat.MATRIX.ext,
        LMFileType.SPECIES_OBSERVED: LMFormat.MATRIX.ext,
        LMFileType.SPECIES_RANDOM: LMFormat.MATRIX.ext,
        LMFileType.SUM_CALCS: LMFormat.PICKLE.ext,
        LMFileType.SUM_SHAPE: LMFormat.SHAPE.ext,
        LMFileType.TMP_JSON: LMFormat.JSON.ext,
        LMFileType.TREE: LMFormat.NEXUS.ext,
        LMFileType.UNSPECIFIED_RAD: None,
        LMFileType.USER_ATTRIBUTE_MATRIX: LMFormat.NUMPY.ext,
        LMFileType.USER_LAYER: None,
        LMFileType.USER_SHAPEGRID:  LMFormat.SHAPE.ext,
        LMFileType.USER_TREE: LMFormat.NEXUS.ext
    }

    @staticmethod
    def get_map_type_from_name(prefix=None, ext=None):
        if prefix is not None:
            for ftype in LMFileType.map_types():
                if FileFix.PREFIX[ftype] == prefix:
                    return ftype
        elif ext is not None:
            for ftype in LMFileType.map_types():
                if FileFix.EXTENSION[ftype] == ext:
                    return ftype
        return None


NAME_SEPARATOR = '_'

# Development desktops, debug users, and beta server names
DEBUG_USER_PREFIX = 'debug_'
HINT_PREFIX = 'hint'

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
    """Constants to define the priority of a Job"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    OBSOLETE = 3
    ABSENT = 4
    REQUESTED = 5


# ............................................................................
class LMServiceType:
    """Lifemapper web service type enum"""
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
    LAYERSETS = 'layersets'
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
OGRFormats = {
    'CSV': '.csv',
    'ESRI Shapefile': '.shp'
}

OGRDataTypes = (
    wkbPoint, wkbLineString, wkbPolygon, wkbMultiPoint, wkbMultiLineString,
    wkbMultiPolygon)

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
# TODO: Complete these lists, they may not include everything they need to
JSON_ESCAPES = [
    ('\n', 'None'),
    ('\t', '    '),
    ('"', '\'')
]

HTML_ESCAPES = [
    ('&', '&amp;'),
    ('\n', '<br />'),
    ('\t', '&nbsp;&nbsp;&nbsp;&nbsp;'),
    ('"', '&quot;')
]

SQL_ESCAPES = []

XML_ESCAPES = [
    ('&', '&amp;'),
    ('<', '&lt;'),
    ('>', '&gt;'),
    ('"', '&quot;')
]

STRING_ESCAPE_FORMATS = {
    'html': HTML_ESCAPES,
    'json': JSON_ESCAPES,
    'sql': SQL_ESCAPES,
    'xml': XML_ESCAPES
}

# .............................................................................
# Algorithm constants
BIOCLIM_PARAMS = {
    'StandardDeviationCutoff': {
        'type': float,
        'min': 0.0,
        'default': 0.674,
        'max': None
    }
}

CSMBS_PARAMS = {
    'Randomisations': {
        'type': int,
        'min': 1,
        'default': 8,
        'max': 1000
    },
    'StandardDeviations': {
        'type': float, 'min': -10.0, 'default': 2.0, 'max': 10.0},
    'MinComponents': {
        'type': int, 'min': 1, 'default': 1, 'max': 20},
    'VerboseDebugging': {
        'type': int, 'min': 0, 'default': 0, 'max': 1}}

ENVDIST_PARAMS = {
    'DistanceType': {
        'type': int, 'min': 1, 'default': 1, 'max': 4},
    'NearestPoints': {
        'type': int, 'min': 0, 'default': 1, 'max': None},
    'MaxDistance': {
        'type': float, 'min': 0.1, 'default': 0.1, 'max': 1.0}}

GARP_PARAMS = {
    'MaxGenerations': {
        'type': int, 'min': 1, 'default': 400, 'max': None},
    'ConvergenceLimit': {
        'type': float, 'min': 0.0, 'default': 0.01, 'max': 1.0},
    'PopulationSize': {
        'type': int, 'min': 1, 'default': 50, 'max': 500},
    'Resamples': {
        'type': int, 'min': 1, 'default': 2500, 'max': 100000}}

GARP_BS_PARAMS = {
    'TrainingProportion': {
        'type': float, 'min': 0, 'default': 50, 'max': 100},
    'TotalRuns': {
        'type': int, 'min': 0, 'default': 20, 'max': 10000},
    'HardOmissionThreshold': {
        'type': float, 'min': 0, 'default': 100, 'max': 100},
    'ModelsUnderOmissionThreshold': {
        'type': int, 'min': 0, 'default': 20, 'max': 10000},
    'CommissionThreshold': {
        'type': float, 'min': 0, 'default': 50, 'max': 100},
    'CommissionSampleSize': {
        'type': int, 'min': 1, 'default': 10000, 'max': None},
    'MaxThreads': {
        'type': int, 'min': 1, 'default': 1, 'max': 1024},
    'MaxGenerations': {
        'type': int, 'min': 1, 'default': 400, 'max': None},
    'ConvergenceLimit': {
        'type': float, 'min': 0.0, 'default': 0.1, 'max': 1.0},
    'PopulationSize': {
        'type': int, 'min': 1, 'default': 50, 'max': 500},
    'Resamples': {
        'type': int, 'min': 1, 'default': 2500, 'max': 10000}}

OM_MAXENT_PARAMS = {
    'NumberOfBackgroundPoints': {
        'type': int, 'min': 0, 'default': 10000, 'max': 10000},
    'UseAbsencesAsBackground': {
        'min': 0, 'max': 1, 'type': int, 'default': 0},
    'IncludePresencePointsInBackground': {
        'min': 0, 'max': 1, 'type': int, 'default': 1},
    'NumberOfIterations': {
        'min': 1, 'max': None, 'type': int, 'default': 500},
    'TerminateTolerance': {
        'min': 0, 'max': None, 'type': float, 'default': 0.00001},
    'OutputFormat': {
        'min': 1, 'max': 2, 'type': int, 'default': 2},
    'QuadraticFeatures': {
        'min': 0, 'max': 1, 'type': int, 'default': 1},
    'ProductFeatures': {
        'min': 0, 'max': 1, 'type': int, 'default': 1},
    'HingeFeatures': {
        'min': 0, 'max': 1, 'type': int, 'default': 1},
    'ThresholdFeatures': {
        'min': 0, 'max': 1, 'type': int, 'default': 1},
    'AutoFeatures': {
        'min': 0, 'max': 1, 'type': int, 'default': 1},
    'MinSamplesForProductThreshold': {
        'min': 1, 'max': None, 'type': int, 'default': 80},
    'MinSamplesForQuadratic': {
        'min': 1, 'max': None, 'type': int, 'default': 10},
    'MinSamplesForHinge': {
        'min': 1, 'max': None, 'type': int, 'default': 15}}

ATT_MAXENT_PARAMS = {
    'responsecurves': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'pictures': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'jackknife': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'outputformat': {  # 0 - raw, 1 - logistic, 2 - cumulative, 3 - cloglog
        'type': int, 'min': 0, 'max': 3, 'default': 3},
    'randomseed': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'logscale': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'removeduplicates': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'writeclampgrid': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'writemess': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'randomtestpoints': {'type': int, 'min': 0, 'max': 100, 'default': 0},
    'betamultiplier': {'type': float, 'min': 0, 'max': None, 'default': 1.0},
    'maximumbackground': {
        'type': int, 'min': 0, 'max': None, 'default': 10000},
    'replicates': {'type': int, 'min': 1, 'max': None, 'default': 1},
    'replicatetype': {  # 0 - cross validate, 1 - bootstrap, 2 - subsample
        'type': int, 'min': 0, 'max': 2, 'default': 0},
    'perspeciesresults': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'writebackgroundpredictions': {
        'type': int, 'min': 0, 'max': 1, 'default': 0},
    'responsecurvesexponent': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'linear': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'quadratic': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'product': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'threshold': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'hinge': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'writeplotdata': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'fadebyclamping': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'extrapolate': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'autofeature': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'doclamp': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'outputgrids': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'plots': {'type': int, 'min': 0, 'max': 1, 'default': 1},
    'appendtoresultsfile': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'maximumiterations': {'type': int, 'min': 0, 'max': None, 'default': 500},
    'convergencethreshold': {
        'type': float, 'min': 0, 'max': None, 'default': 0.00001},
    'adjustsampleradius': {'type': int, 'min': 0, 'max': None, 'default': 0},
    'lq2lqptthreshold': {'type': int, 'min': 0, 'max': None, 'default': 80},
    'l2lqthreshold': {'type': int, 'min': 0, 'max': None, 'default': 10},
    'hingethreshold': {'type': int, 'min': 0, 'max': None, 'default': 15},
    'beta_threshold': {
        'type': float, 'min': None, 'max': None, 'default': -1.0},
    'beta_categorical': {
        'type': float, 'min': None, 'max': None, 'default': -1.0},
    'beta_lqp': {'type': float, 'min': None, 'max': None, 'default': -1.0},
    'beta_hinge': {'type': float, 'min': None, 'max': None, 'default': -1.0},
    'defaultprevalence': {
        'type': float, 'min': 0.0, 'max': 1.0, 'default': 0.5},
    'addallsamplestobackground': {
        'type': int, 'min': 0, 'max': 1, 'default': 0},
    'addsamplestobackground': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'allowpartialdata': {'type': int, 'min': 0, 'max': 1, 'default': 0},
    'applythresholdrule': {
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
        'type': int, 'min': 0, 'max': 10, 'default': 0
        },
    'verbose': {'type': int, 'min': 0, 'max': 1, 'default': 0}
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

SVM_PARAMS = {
    'svmtype': {'type': int, 'min': 0, 'default': 0, 'max': 2},
    'KernelType': {'type': int, 'min': 0, 'default': 2, 'max': 4},
    'Degree': {'type': int, 'min': 0, 'default': 3, 'max': None},
    'Gamma': {'type': float, 'min': None, 'default': 0.0, 'max': None},
    'Coef0': {'type': float, 'min': None, 'default': 0.0, 'max': None},
    # Cost
    'C': {'type': float, 'min': 0.001, 'default': 1.0, 'max': None},
    'Nu': {'type': float, 'min': 0.001, 'default': 0.5, 'max': 1},
    'ProbabilisticOutput': {'type': int, 'min': 0, 'default': 1, 'max': 1},
    'NumberOfPseudoAbsences': {
        'type': int, 'min': 0, 'default': 0, 'max': None}}

ANN_PARAMS = {
    'HiddenLayerNeurons': {'type': int, 'min': 0, 'default': 8, 'max': None},
    'LearningRate': {'type': float, 'min': 0.0, 'default': 0.3, 'max': 1.0},
    'Momentum': {'type': float, 'min': 0.0, 'default': 0.05, 'max': 1.0},
    'Choice': {'type': int, 'min': 0, 'default': 0, 'max': 1},
    'Epoch': {'type': int, 'min': 1, 'default': 50000, 'max': None},
    # Note: There is a typo in openModeller so it is maintained here
    'MinimunError': {'type': float, 'min': 0.0, 'default': 0.01, 'max': 0.05}}

AQUAMAPS_PARAMS = {
    'UseSurfaceLayers': {'type': int, 'min': -1, 'default': -1, 'max': 1},
    'UseDepthRange': {'type': int, 'min': 0, 'default': 1, 'max': 1},
    'UseIceConcentration': {'type': int, 'min': 0, 'default': 1, 'max': 1},
    'UseDistanceToLand': {'type': int, 'min': 0, 'default': 1, 'max': 1},
    'UsePrimaryProduction': {'type': int, 'min': 0, 'default': 1, 'max': 1},
    'usesalinity': {'type': int, 'min': 0, 'default': 1, 'max': 1},
    'usetemperature': {'type': int, 'min': 0, 'default': 1, 'max': 1}}


# .............................................................................
class AlgQualities:
    """Algorithm qualities metadata class."""
    # ...........................
    def __init__(self, code, name, is_discrete_output=False,
                 output_format=LMFormat.get_default_gdal().driver,
                 accepts_categorical_maps=False, parameters=None):
        """Constructor.
        """
        if parameters is None:
            parameters = {}
        self.code = code
        self.name = name
        self.is_discrete_output = is_discrete_output
        self.output_format = output_format
        self.accepts_categorical_maps = accepts_categorical_maps
        self.parameters = parameters


# .............................................................................
class Algorithms:
    """Algorithms enumeration"""
    BIOCLIM = AlgQualities(
        'BIOCLIM', 'Bioclimatic Envelope Algorithm',
        # output is 0, 0.5, 1.0
        is_discrete_output=True, parameters=BIOCLIM_PARAMS)
    CSMBS = AlgQualities(
        'CSMBS', 'Climate Space Model - Broken-Stick Implementation',
        parameters=CSMBS_PARAMS)
    ENVDIST = AlgQualities(
        'ENVDIST', 'Environmental Distance', parameters=ENVDIST_PARAMS)
    ENVSCORE = AlgQualities('ENVSCORE', 'Envelope Score')
    GARP = AlgQualities(
        'GARP', 'GARP (single run) - new openModeller implementation',
        is_discrete_output=True, parameters=GARP_PARAMS)
    DG_GARP = AlgQualities(
        'DG_GARP', 'GARP (single run) - DesktopGARP implementation',
        is_discrete_output=True, parameters=GARP_PARAMS)
    GARP_BS = AlgQualities(
        'GARP_BS', 'GARP with Best Subsets - new openModeller implementation ',
        parameters=GARP_BS_PARAMS)
    DG_GARP_BS = AlgQualities(
        'DG_GARP_BS', 'GARP with Best Subsets - DesktopGARP implementation',
        parameters=GARP_BS_PARAMS)
    MAXENT = AlgQualities(
        'MAXENT', 'Maximum Entropy (openModeller Implementation)',
        parameters=OM_MAXENT_PARAMS)
    ATT_MAXENT = AlgQualities(
        'ATT_MAXENT', 'Maximum Entropy (ATT Implementation)',
        accepts_categorical_maps=True, parameters=ATT_MAXENT_PARAMS)
    SVM = AlgQualities(
        'SVM', 'SVM (Support Vector Machines)', parameters=SVM_PARAMS)
    ANN = AlgQualities(
        'ANN', 'Artificial Neural Network', parameters=ANN_PARAMS)
    AQUAMAPS = AlgQualities(
        'AQUAMAPS', 'AquaMaps (beta version)', parameters=AQUAMAPS_PARAMS)
    # Not yet implemented
    ENFA = AlgQualities('ENFA', 'Ecological-Niche Factor Analysis')
    # Not yet implemented
    RNDFOREST = AlgQualities('RNDFOREST', 'Random Forests')
    # Masking algorithm
    HULL_INTERSECT = AlgQualities(
        'hull_region_intersect', 'Convex Hull Region Intersect',
        is_discrete_output=True, parameters={
            'buffer': {'type': float, 'min': 0, 'default': 0.5, 'max': 2},
            # Region MUST be supplied by user
            'region': {'type': str, 'default': None}})

    @staticmethod
    def implemented():
        """Return a list of implemented algorithms"""
        return (Algorithms.BIOCLIM, Algorithms.CSMBS,
                Algorithms.ENVDIST, Algorithms.ENVSCORE, Algorithms.GARP,
                Algorithms.DG_GARP, Algorithms.GARP_BS, Algorithms.DG_GARP_BS,
                Algorithms.MAXENT, Algorithms.ATT_MAXENT, Algorithms.SVM,
                Algorithms.ANN, Algorithms.AQUAMAPS, Algorithms.HULL_INTERSECT)

    @staticmethod
    def codes():
        """Returns a list of implemented algorithm codes."""
        return [alg.code for alg in Algorithms.implemented()]

    @staticmethod
    def is_openModeller(code):
        """Return if algorithm is part of openModeller.

        Returns:
            bool - Inidcation if the provided algorithm code is in openModeller
        """
        atype = Algorithms.get(code)
        if atype == Algorithms.ATT_MAXENT:
            return False
        return True

    @staticmethod
    def is_att(code):
        """Returns a boolean if the provided algorithm code is in ATT Maxent"""
        atype = Algorithms.get(code)
        if atype == Algorithms.ATT_MAXENT:
            return True
        return False

    @staticmethod
    def get(code):
        """Gets an algorithm from the provided code."""
        for alg in Algorithms.implemented():
            if alg.code == code:
                return alg
        return None

    @staticmethod
    def returns_discrete_output(code):
        """Returns boolean if algorithm provides discrete output"""
        atype = Algorithms.get(code)
        return atype.is_discrete_output


# .............................................................................
class SdmMasks:
    """SDM mask methods enumeration."""
    HULL_INTERSECT = AlgQualities(
        'hull_region_intersect', 'Convex Hull Region Intersect',
        is_discrete_output=True,
        parameters={
            'buffer': {'type': float, 'min': 0, 'default': 0.5, 'max': 2},
            # Region MUST be supplied by user
            'region': {'type': str}})


# ============================================================================
# =                           Snippet Constants                              =
# ============================================================================
class SnippetOperations:
    """Class of available snippet operations"""
    DOWNLOADED = 'downloaded'
    VIEWED = 'viewed'
    ADDED_TO = 'addedTo'
    USED_IN = 'usedIn'


# =============================================================================
class SnippetFields:
    """Snippet fields in Solr index"""
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


class SOLR_FIELDS:
    """This class contains constants for SOLR index field names"""
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
    PAM_ID = 'pamId'
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
class SOLR_TAXONOMY_FIELDS:
    """Constants for Taxonomy Solr index fields"""
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


class SubsetMethod:
    """Subset method enumeration"""
    COLUMN = 0  # Same shapegrid, just cut out columns
    SPATIAL = 1  # Spatial subset of original shapegrid.  Column and row,
    #                cut out columns and remove sites
    REINTERSECT = 2  # New shapegrid with possibly different resolution,
    #                    reintersect all columns


# ============================================================================
# =                           Permutation constants                          =
# ============================================================================
DEFAULT_NUM_PERMUTATIONS = 1000
DEFAULT_RANDOM_GROUP_SIZE = 10

# ============================================================================
# =                           Open Tree Constants                            =
# ============================================================================
NONTREE_GBIF_IDS_KEY = 'nontree_ids'
TREE_DATA_KEY = 'tree_data'
TREE_FORMAT_KEY = 'tree_format'
TREE_NAME_KEY = 'tree_name'
UNMATCHED_GBIF_IDS_KEY = 'unmatched_ids'
