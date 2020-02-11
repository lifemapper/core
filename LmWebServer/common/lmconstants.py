"""This module contains constants used by the Lifemapper web services
"""
from enum import Enum
import os

from LmServer.base.utilities import get_mjd_time_from_iso_8601
from LmServer.common.lmconstants import SESSION_DIR
from LmServer.common.localconstants import SCRATCH_PATH, APP_PATH
from LmWebServer.common.localconstants import PACKAGING_DIR


# CherryPy constants
SESSION_PATH = os.path.join(SCRATCH_PATH, SESSION_DIR)
SESSION_KEY = '_cp_username'
REFERER_KEY = 'lm_referer'

# Results package constants
GRIDSET_DIR = 'gridset'
MATRIX_DIR = os.path.join(GRIDSET_DIR, 'matrix')
SDM_PRJ_DIR = os.path.join(GRIDSET_DIR, 'sdm')
DYN_PACKAGE_DIR = 'package'
STATIC_PACKAGE_PATH = os.path.join(APP_PATH, PACKAGING_DIR)
MAX_PROJECTIONS = 1000


# HTTP Methods
class HTTPMethod(Enum):
    DELETE = 'DELETE'
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'


def sci_name_prep(x):
    strip_chars = [' ', '+', '%20', ',', '%2C']
    for c in strip_chars:
        x = x.replace(c, '')
    return x[:20]


# This constant is used for processing query parameters.  If no 'processIn'
#     key, just take the parameter as it comes in
# Note: The dictionary keys are the .lower() version of the parameter names.
#             The 'name' value of each key is what it gets translated to
# The point of this structure is to allow query parameters to be
#    case-insensitive
QP_NAME_KEY = 'name'
QP_PROCESS_KEY = 'process_in'

QUERY_PARAMETERS = {
    'afterstatus': {
        QP_NAME_KEY: 'afterStatus',
        QP_PROCESS_KEY: int
    },
    'aftertime': {
        QP_NAME_KEY: 'afterTime',
        QP_PROCESS_KEY: get_mjd_time_from_iso_8601
    },
    'agent': {
        QP_NAME_KEY: 'agent'
    },
    'algorithmcode': {
        QP_NAME_KEY: 'algorithmCode',
    },
    'altpredcode': {
        QP_NAME_KEY: 'altPredCode'
    },
    'archivename': {
        QP_NAME_KEY: 'archiveName'
    },
    'beforestatus': {
        QP_NAME_KEY: 'beforeStatus',
        QP_PROCESS_KEY: int
    },
    'beforetime': {
        QP_NAME_KEY: 'beforeTime',
        QP_PROCESS_KEY: get_mjd_time_from_iso_8601
    },
    'bbox': {
        # Comes in as a comma separated list, turn it into a tuple of floats
        QP_NAME_KEY: 'bbox',
        # QP_PROCESS_KEY: lambda x: [float(i) for i in x.split(',')]
    },
    'bgcolor': {
        QP_NAME_KEY: 'bgcolor',
    },
    'canonicalname': {
        QP_NAME_KEY: 'canonicalName'
    },
    'catalognumber': {
        QP_NAME_KEY: 'catalogNumber'
    },
    'cellsides': {
        QP_NAME_KEY: 'cellSides',
        QP_PROCESS_KEY: int
    },
    'cellsize': {
        QP_NAME_KEY: 'cellSize',
        QP_PROCESS_KEY: float
    },
    'collection': {
        QP_NAME_KEY: 'collection'
    },
    'color': {
        QP_NAME_KEY: 'color',
    },
    'coverage': {
        QP_NAME_KEY: 'coverage'
    },
    'crs': {
        # TODO: Consider processing the EPSG here
        QP_NAME_KEY: 'crs'
    },
    'datecode': {
        QP_NAME_KEY: 'dateCode'
    },
    'detail': {
        QP_NAME_KEY: 'detail',
        QP_PROCESS_KEY: lambda x: bool(int(x))  # Zero is false, one is true
    },
    'displayname': {
        QP_NAME_KEY: 'displayName'
    },
    'docalc': {
        QP_NAME_KEY: 'doCalc',
        QP_PROCESS_KEY: lambda x: bool(int(x))  # Zero is false, one is true
    },
    'domcpa': {
        QP_NAME_KEY: 'doMcpa',
        QP_PROCESS_KEY: lambda x: bool(int(x))  # Zero is false, one is true
    },
    'envcode': {
        QP_NAME_KEY: 'envCode'
    },
    'envtypeid': {
        QP_NAME_KEY: 'envTypeId',
        QP_PROCESS_KEY: int
    },
    'epsgcode': {
        QP_NAME_KEY: 'epsgCode',
        QP_PROCESS_KEY: int
    },
    'exceptions': {
        QP_NAME_KEY: 'exceptions'
    },
    'filename': {
        QP_NAME_KEY: 'fileName'
    },
    'fillpoints': {
        QP_NAME_KEY: 'fillPoints',
        QP_PROCESS_KEY: lambda x: bool(int(x))  # Zero is false, one is true
    },
    'format': {
        # TODO: Forward to respFormat since format is reserved
        QP_NAME_KEY: 'respFormat',
    },
    'gcmcode': {
        QP_NAME_KEY: 'gcmCode',
    },
    'gridsetid': {
        QP_NAME_KEY: 'gridSetId',
        QP_PROCESS_KEY: int
    },
    'hasbranchlengths': {
        QP_NAME_KEY: 'hasBranchLengths',
        QP_PROCESS_KEY: lambda x: bool(int(x))  # Zero is false, one is true
    },
    'height': {
        QP_NAME_KEY: 'height',
        QP_PROCESS_KEY: int
    },
    'ident1': {
        QP_NAME_KEY: 'ident1'
    },
    'ident2': {
        QP_NAME_KEY: 'ident2'
    },
    'includecsvs': {
        QP_NAME_KEY: 'includeCSVs',
        QP_PROCESS_KEY: lambda x: bool(int(x))  # Zero is false, one is true
    },
    'includesdms': {
        QP_NAME_KEY: 'includeSDMs',
        QP_PROCESS_KEY: lambda x: bool(int(x))  # Zero is false, one is true
    },
    'isbinary': {
        QP_NAME_KEY: 'isBinary',
        QP_PROCESS_KEY: lambda x: bool(int(x))  # Zero is false, one is true
    },
    'isultrametric': {
        QP_NAME_KEY: 'isUltrametric',
        QP_PROCESS_KEY: lambda x: bool(int(x))  # Zero is false, one is true
    },
    'keyword': {
        QP_NAME_KEY: 'keyword',
        QP_PROCESS_KEY: lambda x: [float(x)]
    },
    'layer': {
        QP_NAME_KEY: 'layer'
    },
    'layers': {
        QP_NAME_KEY: 'layers',
        # QP_PROCESS_KEY: lambda x: [i for i in x.split(',')]
    },
    'layertype': {
        QP_NAME_KEY: 'layerType',
        QP_PROCESS_KEY: int
    },
    'limit': {
        QP_NAME_KEY: 'limit',
        QP_PROCESS_KEY: lambda x: max(1, int(x))  # Integer, minimum is one
    },
    'map': {
        QP_NAME_KEY: 'mapName'
    },
    'mapname': {
        QP_NAME_KEY: 'mapName'
    },
    'matrixtype': {
        QP_NAME_KEY: 'matrixType',
        QP_PROCESS_KEY: int
    },
    'metadata': {
        QP_NAME_KEY: 'metadata'
    },
    'metastring': {
        QP_NAME_KEY: 'metaString'
    },
    'modelscenariocode': {
        QP_NAME_KEY: 'modelScenarioCode'
    },
    'minimumnumberofpoints': {
        QP_NAME_KEY: 'minimumNumberOfPoints',
        QP_PROCESS_KEY: lambda x: max(1, int(x))  # Integer, minimum is one
    },
    'numpermutations': {
        QP_NAME_KEY: 'numPermutations',
        QP_PROCESS_KEY: int
    },
    'occurrencesetid': {
        QP_NAME_KEY: 'occurrenceSetId',
        QP_PROCESS_KEY: int
    },
    'operation': {
        QP_NAME_KEY: 'operation'
    },
    'offset': {
        QP_NAME_KEY: 'offset',
        QP_PROCESS_KEY: lambda x: max(0, int(x))  # Integer, minimum is zero
    },
    'pathbiogeoid': {
        QP_NAME_KEY: 'pathBioGeoId'
    },
    'pathgridsetid': {
        QP_NAME_KEY: 'pathGridSetId'
    },
    'pathlayerid': {
        QP_NAME_KEY: 'pathLayerId'
    },
    'pathmatrixid': {
        QP_NAME_KEY: 'pathMatrixId'
    },
    'pathoccsetid': {
        QP_NAME_KEY: 'pathOccSetId'
    },
    'pathprojectionid': {
        QP_NAME_KEY: 'pathProjectionId'
    },
    'pathscenarioid': {
        QP_NAME_KEY: 'pathScenarioId'
    },
    'pathscenariopackageid': {
        QP_NAME_KEY: 'pathScenarioPackageId'
    },
    'pathshapegridid': {
        QP_NAME_KEY: 'pathShapegridId'
    },
    'pathtreeid': {
        QP_NAME_KEY: 'pathTreeId'
    },
    'pointmax': {
        QP_NAME_KEY: 'pointMax',
        QP_PROCESS_KEY: int
    },
    'pointmin': {
        QP_NAME_KEY: 'pointMin',
        QP_PROCESS_KEY: int
    },
    'projectionscenariocode': {
        QP_NAME_KEY: 'projectionScenarioCode'
    },
    'provider': {
        QP_NAME_KEY: 'provider'
    },
    'request': {
        QP_NAME_KEY: 'request'
    },
    'resolution': {
        QP_NAME_KEY: 'resolution'
    },
    'scenariocode': {
        QP_NAME_KEY: 'scenarioCode'
    },
    'scenarioid': {
        QP_NAME_KEY: 'scenarioId',
        QP_PROCESS_KEY: int
    },
    'scientificname': {
        QP_NAME_KEY: 'scientificName',
        QP_PROCESS_KEY: sci_name_prep
    },
    'searchstring': {
        QP_NAME_KEY: 'searchString'
    },
    'service': {
        QP_NAME_KEY: 'service'
    },
    'shapegridid': {
        QP_NAME_KEY: 'shapegridId'
    },
    'sld': {
        QP_NAME_KEY: 'sld'
    },
    'sldbody': {
        QP_NAME_KEY: 'sld_body'
    },
    'squid': {
        QP_NAME_KEY: 'squid',
        # TODO: Evaluate what needs to be done to process into list
        QP_PROCESS_KEY: lambda x: x
    },
    'srs': {
        # TODO: Forward to crs for WMS 1.3.0?
        QP_NAME_KEY: 'srs'
    },
    'status': {
        QP_NAME_KEY: 'status',
        QP_PROCESS_KEY: int
    },
    'styles': {
        QP_NAME_KEY: 'styles',
        # QP_PROCESS_KEY: lambda x: [i for i in x.split(',')]
    },
    'taxonclass': {
        QP_NAME_KEY: 'taxonClass'
    },
    'taxonfamily': {
        QP_NAME_KEY: 'taxonFamily'
    },
    'taxongenus': {
        QP_NAME_KEY: 'taxonGenus'
    },
    'taxonkingdom': {
        QP_NAME_KEY: 'taxonKingdom'
    },
    'taxonorder': {
        QP_NAME_KEY: 'taxonOrder'
    },
    'taxonphylum': {
        QP_NAME_KEY: 'taxonPhylum'
    },
    'taxonspecies': {
        QP_NAME_KEY: 'taxonSpecies'
    },
    'time': {
        QP_NAME_KEY: 'time'
    },
    'transparent': {
        QP_NAME_KEY: 'transparent',
        # QP_PROCESS_KEY: lambda x: bool(x.lower() == 'true')
    },
    'treename': {
        QP_NAME_KEY: 'name'  # Map to 'name' for processing
    },
    'treeschema': {
        QP_NAME_KEY: 'treeSchema'
    },
    'file': {
        QP_NAME_KEY: 'file'
    },
    'uploadtype': {
        QP_NAME_KEY: 'uploadType'
    },
    'url': {
        QP_NAME_KEY: 'url'
    },
    'user': {
        QP_NAME_KEY: 'urlUser',
        QP_PROCESS_KEY: lambda x: x
    },
    'version': {
        QP_NAME_KEY: 'version'
    },
    'who': {
        QP_NAME_KEY: 'who'
    },
    'why': {
        QP_NAME_KEY: 'why'
    },
    'width': {
        QP_NAME_KEY: 'width',
        QP_PROCESS_KEY: int
    },
    # Authentication parameters
    'address1': {
        QP_NAME_KEY: 'address1'
    },
    'address2': {
        QP_NAME_KEY: 'address2'
    },
    'address3': {
        QP_NAME_KEY: 'address3'
    },
    'phone': {
        QP_NAME_KEY: 'phone'
    },
    'email': {
        QP_NAME_KEY: 'email'
    },
    'firstname': {
        QP_NAME_KEY: 'firstName'
    },
    'institution': {
        QP_NAME_KEY: 'institution'
    },
    'lastname': {
        QP_NAME_KEY: 'lastName'
    },
    'pword': {
        QP_NAME_KEY: 'pword'
    },
    'pword1': {
        QP_NAME_KEY: 'pword1'
    },
    'userid': {
        QP_NAME_KEY: 'userId'
    },
}

# Kml
KML_NAMESPACE = "http://earth.google.com/kml/2.2"
KML_NS_PREFIX = None


# API
class APIPostKeys(Enum):
    """This class contains constants for API JSON POST keys
    """
    ALGORITHM = 'algorithm'
    ALGORITHM_CODE = 'code'
    ALGORITHM_PARAMETERS = 'parameters'
    ARCHIVE_NAME = 'archive_name'
    BUFFER = 'buffer'
    CELL_SIDES = 'cell_sides'
    DELIMITER = 'delimiter'
    DO_PAM_STATS = 'compute_pam_stats'
    DO_MCPA = 'compute_mcpa'
    GLOBAL_PAM = 'global_pam'
    HULL_REGION = 'hull_region_intersect_mask'
    INTERSECT_PARAMETERS = 'intersect_parameters'
    MAX_PRESENCE = 'max_presence'
    MAX_X = 'maxx'
    MAX_Y = 'maxy'
    MCPA = 'mcpa'
    MIN_PERCENT = 'min_percent'
    MIN_POINTS = 'point_count_min'
    MIN_PRESENCE = 'min_presence'
    MIN_X = 'minx'
    MIN_Y = 'miny'
    MODEL_SCENARIO = 'model_scenario'
    NAME = 'name'
    OCCURRENCE = 'occurrence'
    OCCURRENCE_IDS = 'occurrence_ids'
    PACKAGE_FILENAME = 'scenario_package_filename'
    PACKAGE_NAME = 'scenario_package_name'
    PAM_STATS = 'pam_stats'
    POINTS_FILENAME = 'points_filename'
    PROJECTION_SCENARIO = 'projection_scenario'
    REGION = 'region'
    RESOLUTION = 'resolution'
    SCENARIO_CODE = 'scenario_code'
    SCENARIO_PACKAGE = 'scenario_package'
    SDM = 'sdm'
    SHAPEGRID = 'shapegrid'
    TAXON_IDS = 'taxon_ids'
    TAXON_NAMES = 'taxon_names'
    TREE = 'tree'
    TREE_FILENAME = 'tree_file_name'
    VALUE_NAME = 'value_name'
