"""This module contains constants used by the Lifemapper web services
"""
import os
import secrets

from LmServer.base.utilities import get_mjd_time_from_iso_8601
from LmServer.common.lmconstants import SESSION_DIR
from LmServer.common.localconstants import SCRATCH_PATH, APP_PATH
from LmWebServer.common.localconstants import PACKAGING_DIR

FALLBACK_SECRET_KEY = secrets.token_hex()

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


# .............................................................................
class HTTPMethod:
    """Constant class for HTTP methods
    """
    DELETE = 'DELETE'
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'


# .............................................................................
def sci_name_prep(name):
    """Prepare scientific name
    """
    strip_chars = [' ', '+', '%20', ',', '%2C']
    for strip_chr in strip_chars:
        name = name.replace(strip_chr, '')
    return name[:20]


# .............................................................................
def boolify_parameter(param, default=True):
    """Convert an input query parameter to boolean."""
    try:
        # If zero or one
        return bool(int(param))
    except ValueError:
        try:
            # Try processing a string
            str_val = param.lower().strip()
            if str_val in('false', 'f', 'no','n'):
                return False
            if str_val in ('true', 't', 'yes', 'y'):
                return True
        except Exception:
            pass
    # Return default if we can't figure it out
    return default


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
        QP_NAME_KEY: 'after_status',
        QP_PROCESS_KEY: int
    },
    'aftertime': {
        QP_NAME_KEY: 'after_time',
        QP_PROCESS_KEY: get_mjd_time_from_iso_8601
    },
    'agent': {
        QP_NAME_KEY: 'agent'
    },
    'algorithmcode': {
        QP_NAME_KEY: 'algorithm_code',
    },
    'altpredcode': {
        QP_NAME_KEY: 'alt_pred_code'
    },
    'archivename': {
        QP_NAME_KEY: 'archive_name'
    },
    'atom': {
        QP_NAME_KEY: 'atom',
        QP_PROCESS_KEY: lambda x: boolify_parameter(x, default=True)
    },
    'beforestatus': {
        QP_NAME_KEY: 'before_status',
        QP_PROCESS_KEY: int
    },
    'beforetime': {
        QP_NAME_KEY: 'before_time',
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
        QP_NAME_KEY: 'canonical_name'
    },
    'catalognumber': {
        QP_NAME_KEY: 'catalog_number'
    },
    'cellsides': {
        QP_NAME_KEY: 'cell_sides',
        QP_PROCESS_KEY: int
    },
    'cellsize': {
        QP_NAME_KEY: 'cell_size',
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
        QP_NAME_KEY: 'date_code'
    },
    'detail': {
        QP_NAME_KEY: 'detail',
        QP_PROCESS_KEY: lambda x: boolify_parameter(x, default=False)
    },
    'displayname': {
        QP_NAME_KEY: 'display_name'
    },
    'docalc': {
        QP_NAME_KEY: 'do_calc',
        QP_PROCESS_KEY: lambda x: boolify_parameter(x, default=False)
    },
    'domcpa': {
        QP_NAME_KEY: 'do_mcpa',
        QP_PROCESS_KEY: lambda x: boolify_parameter(x, default=False)
    },
    'envcode': {
        QP_NAME_KEY: 'env_code'
    },
    'envtypeid': {
        QP_NAME_KEY: 'env_type_id',
        QP_PROCESS_KEY: int
    },
    'epsgcode': {
        QP_NAME_KEY: 'epsg_code',
        QP_PROCESS_KEY: int
    },
    'exceptions': {
        QP_NAME_KEY: 'exceptions'
    },
    'filename': {
        QP_NAME_KEY: 'file_name'
    },
    'fillpoints': {
        QP_NAME_KEY: 'fill_points',
        QP_PROCESS_KEY: lambda x: boolify_parameter(x, default=False)
    },
    'format': {
        # TODO: Forward to respFormat since format is reserved
        QP_NAME_KEY: 'format_',
    },
    'gcmcode': {
        QP_NAME_KEY: 'gcm_code',
    },
    'gridsetid': {
        QP_NAME_KEY: 'gridset_id',
        QP_PROCESS_KEY: int
    },
    'hasbranchlengths': {
        QP_NAME_KEY: 'has_branch_lengths',
        QP_PROCESS_KEY: lambda x: boolify_parameter(x, default=True)
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
        QP_NAME_KEY: 'include_csvs',
        QP_PROCESS_KEY: lambda x: boolify_parameter(x, default=False)
    },
    'includesdms': {
        QP_NAME_KEY: 'include_sdms',
        QP_PROCESS_KEY: lambda x: boolify_parameter(x, default=False)
    },
    'isbinary': {
        QP_NAME_KEY: 'is_binary',
        QP_PROCESS_KEY: lambda x: boolify_parameter(x, default=True)
    },
    'isultrametric': {
        QP_NAME_KEY: 'is_ultrametric',
        QP_PROCESS_KEY: lambda x: boolify_parameter(x, default=True)
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
        QP_NAME_KEY: 'layer_type',
        QP_PROCESS_KEY: int
    },
    'limit': {
        QP_NAME_KEY: 'limit',
        QP_PROCESS_KEY: lambda x: max(1, int(x))  # min = 1
    },
    'map': {
        QP_NAME_KEY: 'map_name'
    },
    'mapname': {
        QP_NAME_KEY: 'map_name'
    },
    'matrixtype': {
        QP_NAME_KEY: 'matrix_type',
        QP_PROCESS_KEY: int
    },
    'metadata': {
        QP_NAME_KEY: 'metadata'
    },
    'metastring': {
        QP_NAME_KEY: 'meta_string'
    },
    'modelscenariocode': {
        QP_NAME_KEY: 'model_scenario_code'
    },
    'minimumnumberofpoints': {
        QP_NAME_KEY: 'minimum_number_of_points',
        QP_PROCESS_KEY: lambda x: max(1, int(x))  # min = 1
    },
    'numpermutations': {
        QP_NAME_KEY: 'num_permutations',
        QP_PROCESS_KEY: int
    },
    'occurrencesetid': {
        QP_NAME_KEY: 'occurrence_set_id',
        QP_PROCESS_KEY: int
    },
    'operation': {
        QP_NAME_KEY: 'operation'
    },
    'offset': {
        QP_NAME_KEY: 'offset',
        QP_PROCESS_KEY: lambda x: max(0, int(x))  # min = 0
    },
    'pathbiogeoid': {
        QP_NAME_KEY: 'path_biogeo_id'
    },
    'pathgridsetid': {
        QP_NAME_KEY: 'path_gridset_id'
    },
    'pathlayerid': {
        QP_NAME_KEY: 'path_layer_id'
    },
    'pathmatrixid': {
        QP_NAME_KEY: 'path_matrix_id'
    },
    'pathoccsetid': {
        QP_NAME_KEY: 'path_occset_id'
    },
    'pathprojectionid': {
        QP_NAME_KEY: 'path_projection_id'
    },
    'pathscenarioid': {
        QP_NAME_KEY: 'path_scenario_id'
    },
    'pathscenariopackageid': {
        QP_NAME_KEY: 'path_scenario_package_id'
    },
    'pathshapegridid': {
        QP_NAME_KEY: 'path_shapegrid_id'
    },
    'pathtreeid': {
        QP_NAME_KEY: 'path_tree_id'
    },
    'pointmax': {
        QP_NAME_KEY: 'point_max',
        QP_PROCESS_KEY: int
    },
    'pointmin': {
        QP_NAME_KEY: 'point_min',
        QP_PROCESS_KEY: int
    },
    'projectionscenariocode': {
        QP_NAME_KEY: 'projection_scenario_code'
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
        QP_NAME_KEY: 'scenario_code'
    },
    'scenarioid': {
        QP_NAME_KEY: 'scenario_id',
        QP_PROCESS_KEY: int
    },
    'scientificname': {
        QP_NAME_KEY: 'scientific_name',
        QP_PROCESS_KEY: sci_name_prep
    },
    'searchstring': {
        QP_NAME_KEY: 'search_string'
    },
    'service': {
        QP_NAME_KEY: 'service'
    },
    'shapegridid': {
        QP_NAME_KEY: 'shapegrid_id'
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
        QP_NAME_KEY: 'class_'
    },
    'taxonfamily': {
        QP_NAME_KEY: 'family'
    },
    'taxongenus': {
        QP_NAME_KEY: 'genus'
    },
    'taxonkingdom': {
        QP_NAME_KEY: 'kingdom'
    },
    'taxonorder': {
        QP_NAME_KEY: 'order_'
    },
    'taxonphylum': {
        QP_NAME_KEY: 'phylum'
    },
    'taxonspecies': {
        QP_NAME_KEY: 'species'
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
        QP_NAME_KEY: 'tree_schema'
    },
    'file': {
        QP_NAME_KEY: 'file'
    },
    'uploadtype': {
        QP_NAME_KEY: 'upload_type'
    },
    'url': {
        QP_NAME_KEY: 'url'
    },
    'user': {
        QP_NAME_KEY: 'url_user',
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
        QP_NAME_KEY: 'first_name'
    },
    'institution': {
        QP_NAME_KEY: 'institution'
    },
    'lastname': {
        QP_NAME_KEY: 'last_name'
    },
    'pword': {
        QP_NAME_KEY: 'pword'
    },
    'pword1': {
        QP_NAME_KEY: 'pword1'
    },
    'userid': {
        QP_NAME_KEY: 'user_id'
    },
}

# Kml
KML_NAMESPACE = "http://earth.google.com/kml/2.2"
KML_NS_PREFIX = None


# .............................................................................
class APIPostKeys:
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
