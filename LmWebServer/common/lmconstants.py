"""This module contains constants used by the Lifemapper web services
"""
import os
from LmServer.base.utilities import getColor, getMjdTimeFromISO8601
from LmServer.common.lmconstants import SESSION_DIR
from LmServer.common.localconstants import SCRATCH_PATH

# CherryPy constants
SESSION_PATH = os.path.join(SCRATCH_PATH, SESSION_DIR)
SESSION_KEY = '_cp_username'
REFERER_KEY = 'lm_referer'

# HTTP Methods
class HTTPMethod(object):
    DELETE = 'DELETE'
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'


# This constant is used for processing query parameters.  If no 'processIn' 
#     key, just take the parameter as it comes in
# Note: The dictionary keys are the .lower() version of the parameter names.
#             The 'name' value of each key is what it gets translated to
# The point of this structure is to allow query parameters to be case-insensitive
QUERY_PARAMETERS = {
    'afterstatus' : {
        'name' : 'afterStatus',
        'processIn' : int
    },
    'aftertime' : {
        'name' : 'afterTime',
        'processIn' : getMjdTimeFromISO8601
    },
    'agent' : {
        'name' : 'agent'
    },
    'algorithmcode' : {
        'name' : 'algorithmCode',
    },
    'altpredcode' : {
        'name' : 'altPredCode'
    },
    'archivename' : {
        'name' : 'archiveName'
    },
    'beforestatus' : {
        'name' : 'beforeStatus',
        'processIn' : int
    },
    'beforetime' : {
        'name' : 'beforeTime',
        'processIn' : getMjdTimeFromISO8601
    },
    'bbox' : {
        # Comes in as a comma separated list, turn it into a tuple of floats
        'name' : 'bbox',
        #'processIn' : lambda x: [float(i) for i in x.split(',')]
    },
    'bgcolor' : {
        'name' : 'bgcolor',
        'processIn' : lambda x: getColor(x, allowRamp=False)
    },
    'canonicalname' : {
        'name' : 'canonicalName'
    },
    'catalognumber' : {
        'name' : 'catalogNumber'
    },
    'cellsides' : {
        'name' : 'cellSides',
        'processIn' : int
    },
    'cellsize' : {
        'name' : 'cellSize',
        'processIn' : float
    },
    'collection' : {
        'name' : 'collection'
    },
    'color' : {
        'name' : 'color',
        'processIn' : lambda x: getColor(x, allowRamp=True)
    },
    'coverage' : {
        'name' : 'coverage'
    },
    'crs' : {
        # TODO: Consider processing the EPSG here
        'name' : 'crs'
    },
    'datecode' : {
        'name' : 'dateCode'
    },
    'displayname' : {
        'name' : 'displayName'
    },
    'docalc' : {
        'name' : 'doCalc',
        'processIn' : lambda x: bool(int(x)) # Zero is false, one is true
    },
    'domcpa' : {
        'name' : 'doMcpa',
        'processIn' : lambda x: bool(int(x)) # Zero is false, one is true
    },
    'envcode' : {
        'name' : 'envCode'
    },
    'envtypeid' : {
        'name' : 'envTypeId',
        'processIn' : int
    },
    'epsgcode' : {
        'name' : 'epsgCode',
        'processIn' : int
    },
    'exceptions' : {
        'name' : 'exceptions'
    },
    'filename' : {
        'name' : 'fileName'
    },
    'fillpoints' : {
        'name' : 'fillPoints',
        'processIn' : lambda x: bool(int(x)) # Zero is false, one is true
    },
    'format' : {
        # TODO: Forward to respFormat since format is reserved
        'name' : 'respFormat',
    },
    'gcmcode' : {
        'name' : 'gcmCode',
    },
    'gridsetid' : {
        'name' : 'gridSetId',
        'processIn' : int
    },
    'hasbranchlengths' : {
        'name' : 'hasBranchLengths',
        'processIn' : lambda x: bool(int(x)) # Zero is false, one is true
    },
    'height' : {
        'name' : 'height',
        'processIn' : int
    },
    'ident1' : {
        'name' : 'ident1'
    },
    'ident2' : {
        'name' : 'ident2'
    },
    'includecsvs' : {
        'name' : 'includeCSVs',
        'processIn' : lambda x: bool(int(x)) # Zero is false, one is true
    },
    'includesdms' : {
        'name' : 'includeSDMs',
        'processIn' : lambda x: bool(int(x)) # Zero is false, one is true
    },
    'isbinary' : {
        'name' : 'isBinary',
        'processIn' : lambda x: bool(int(x)) # Zero is false, one is true
    },
    'isultrametric' : {
        'name' : 'isUltrametric',
        'processIn' : lambda x: bool(int(x)) # Zero is false, one is true
    },
    'keyword' : {
        'name' : 'keyword',
        'processIn' : lambda x: [float(x)]
    },
    'layer' : {
        'name' : 'layer'
    },
    'layers' : {
        'name' : 'layers',
        #'processIn' : lambda x: [i for i in x.split(',')]
    },
    'layertype' : {
        'name' : 'layerType',
        'processIn' : int
    },
    'limit' : {
        'name' : 'limit',
        'processIn' : lambda x: max(1, int(x)) # Integer, minimum is one
    },
    'map' : {
        'name' : 'mapName'
    },
    'mapname' : {
        'name' : 'mapName'
    },
    'matrixtype' : {
        'name' : 'matrixType',
        'processIn' : int
    },
    'metadata' : {
        'name' : 'metadata'
    },
    'metastring' : {
        'name' : 'metaString'
    },
    'modelscenariocode' : {
        'name' : 'modelScenarioCode'
    },
    'minimumnumberofpoints' : {
        'name' : 'minimumNumberOfPoints',
        'processIn' : lambda x: max(1, int(x)) # Integer, minimum is one
    },
    'numpermutations' : {
        'name' : 'numPermutations',
        'processIn' : int
    },
    'occurrencesetid' : {
        'name' : 'occurrenceSetId',
        'processIn' : int
    },
    'operation' : {
        'name' : 'operation'
    },
    'offset' : {
        'name' : 'offset',
        'processIn' : lambda x: max(0, int(x)) # Integer, minimum is zero
    },
    'pathbiogeoid' : {
        'name' : 'pathBioGeoId'
    },
    'pathgridsetid' : {
        'name' : 'pathGridSetId'
    },
    'pathlayerid' : {
        'name' : 'pathLayerId'
    },
    'pathmatrixid' : {
        'name' : 'pathMatrixId'
    },
    'pathoccsetid' : {
        'name' : 'pathOccSetId'
    },
    'pathprojectionid' : {
        'name' : 'pathProjectionId'
    },
    'pathscenarioid' : {
        'name' : 'pathScenarioId'
    },
    'pathscenariopackageid' : {
        'name' : 'pathScenarioPackageId'
    },
    'pathshapegridid' : {
        'name' : 'pathShapegridId'
    },
    'pathtreeid' : {
        'name' : 'pathTreeId'
    },
    'pointmax' : {
        'name' : 'pointMax',
        'processIn' : int
    },
    'pointmin' : {
        'name' : 'pointMin',
        'processIn' : int
    },
    'projectionscenariocode' : {
        'name' : 'projectionScenarioCode'
    },
    'provider' : {
        'name' : 'provider'
    },
    'request' : {
        'name' : 'request'
    },
    'resolution' : {
        'name' : 'resolution'
    },
    'scenariocode' : {
        'name' : 'scenarioCode'
    },
    'scenarioid' : {
        'name' : 'scenarioId',
        'processIn' : int
    },
    'scientificname' : {
        'name' : 'scientificName' 
    },
    'searchstring' : {
        'name' : 'searchString'
    },
    'service' : {
        'name' : 'service'
    },
    'shapegridid' : {
        'name' : 'shapegridId'
    },
    'sld' : {
        'name' : 'sld'
    },
    'sldbody' : {
        'name' : 'sld_body'
    },
    'squid' : {
        'name' : 'squid',
        'processIn' : lambda x: x # TODO: Evaluate what needs to be done to process into list
    },
    'srs' : {
        # TODO: Forward to crs for WMS 1.3.0?
        'name' : 'srs'
    },
    'status' : {
        'name' : 'status',
        'processIn' : int
    },
    'styles' : {
        'name' : 'styles',
        #'processIn' : lambda x: [i for i in x.split(',')]
    },
    'taxonclass' : {
        'name' : 'taxonClass'
    },
    'taxonfamily' : {
        'name' : 'taxonFamily'
    },
    'taxongenus' : {
        'name' : 'taxonGenus'
    },
    'taxonkingdom' : {
        'name' : 'taxonKingdom'
    },
    'taxonorder' : {
        'name' : 'taxonOrder'
    },
    'taxonphylum' : {
        'name' : 'taxonPhylum'
    },
    'taxonspecies' : {
        'name' : 'taxonSpecies'
    },
    'time' : {
        'name' : 'time'
    },
    'transparent' : {
        'name' : 'transparent',
        #'processIn' : lambda x: bool(x.lower() == 'true')
    },
    'treename' : {
        'name' : 'name' # Map to 'name' for processing
    },
    'treeschema' : {
        'name' : 'treeSchema' 
    },
    'uploadtype' : {
        'name' : 'uploadType'
    },
    'url' : {
        'name' : 'url'
    },
    'user' : {
        'name' : 'urlUser',
        'processIn' : lambda x: x.lower()
    },
    'version' : {
        'name' : 'version'
    },
    'who' : {
        'name' : 'who'
    },
    'why' : {
        'name' : 'why'
    },
    'width' : {
        'name' : 'width',
        'processIn' : int
    },
    # Authentication parameters
    'address1' : {
        'name' : 'address1'
    },
    'address2' : {
        'name' : 'address2'
    },
    'address3' : {
        'name' : 'address3'
    },
    'phone' : {
        'name' : 'phone'
    },
    'email' : {
        'name' : 'email'
    },
    'firstname' : {
        'name' : 'firstName'
    },
    'institution' : {
        'name' : 'institution'
    },
    'lastname' : {
        'name' : 'lastName'
    },
    'pword' : {
        'name' : 'pword'
    },
    'pword1' : {
        'name' : 'pword1'
    },
    'userid' : {
        'name' : 'userId'
    },
}

# Kml
KML_NAMESPACE = "http://earth.google.com/kml/2.2"
KML_NS_PREFIX = None


# API
class APIPostKeys(object):
    """This class contains constants for API JSON POST keys
    """
    ALGORITHM = 'algorithm'
    ALGORITHM_CODE = 'code'
    ALGORITHM_PARAMETERS = 'parameters'
    ARCHIVE_NAME = 'archive_name'
    BUFFER = 'buffer'
    CELL_SIDES = 'cell_sides'
    DO_PAM_STATS = 'compute_pam_stats'
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
