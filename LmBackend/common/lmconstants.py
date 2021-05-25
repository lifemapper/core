"""Lifemapper backend constants module
"""
# Relative paths
# For LmCompute command construction by LmServer (for Makeflow)
SINGLE_SPECIES_SCRIPTS_DIR = 'LmCompute/tools/single'
MULTI_SPECIES_SCRIPTS_DIR = 'LmCompute/tools/multi'
COMMON_SCRIPTS_DIR = 'LmCompute/tools/common'
BACKEND_SCRIPTS_DIR = 'LmBackend/tools'
BOOM_SCRIPTS_DIR = 'LmDbServer/boom'
SERVER_SCRIPTS_DIR = 'LmServer/tools'
DB_SERVER_SCRIPTS_DIR = 'LmDbServer/tools'

# This is just for the command objects.  There is probably a better way
CMD_PYBIN = '$PYTHON'


# Constants for configuration and processing of single species processes
# .............................................................................
class MaskMethod:
    """Constants for SDM masking methods
    """
    HULL_REGION_INTERSECT = 'hull_region_intersect'
    BLANK_MASK = 'blank_mask'


# .............................................................................
class RegistryKey:
    """Constants for dictionary keys used when processing single species SDMs
    """
    ALGORITHM_CODE = 'algorithm_code'
    IDENTIFIER = 'identifier'
    METRICS = 'metrics'
    NAME = 'name'
    PARAMETER = 'parameter'
    PATH = 'path'
    PRIMARY_OUTPUT = 'primary'
    PROCESS_TYPE = 'process_type'
    SECONDARY_OUTPUTS = 'secondary'
    SNIPPETS = 'snippets'
    STATUS = 'status'
    VALUE = 'value'

    # TODO: Should these be somewhere else?  Used for config, not SDM process
    OCCURRENCE_SET_ID = 'occ_set_id'
    OCCURRENCE_SET_PATH = 'occ_set_path'
    ALGORITHM = 'algorithm'
    SCENARIO = 'scenario'
    MASK_ID = 'mask_id'
    PROJECTION_ID = 'projection_id'
    SCALE_PARAMETERS = 'scale_parameters'
    MULTIPLIER = 'multiplier'
    METHOD = 'method'
    REGION_LAYER_PATH = 'region_path'
    BUFFER = 'buffer'
    TEMPLATE_LAYER_PATH = 'template_path'
    WORK_DIR = 'work_dir'
    PACKAGE_PATH = 'package_path'
    RULESET_PATH = 'ruleset_path'
    LOG_PATH = 'log_path'
    PROJECTION_PATH = 'projection_path'
    COMPRESSED_PAV_DATA = 'compressed_data'

    # ..................
    # Types
    # ..................
    LAYER = 'layer'
    MASK = 'mask'
    MODEL = 'model'
    OCCURRENCE = 'occurrence'
    PAV = 'pav'
    PROJECTION = 'projection'

    # ........................................
    @staticmethod
    def group_keys():
        """Returns the group level keys
        """
        return [RegistryKey.MASK, RegistryKey.MODEL, RegistryKey.OCCURRENCE,
                RegistryKey.PAV, RegistryKey.PROJECTION]
