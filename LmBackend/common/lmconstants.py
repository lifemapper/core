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
class MaskMethod(object):
    """Constants for SDM masking methods
    """
    HULL_REGION_INTERSECT = 'hull_region_intersect'
    BLANK_MASK = 'blank_mask'

# .............................................................................
class RegistryKey(object):
    """Constants for dictionary keys used when processing single species SDMs
    """
    ID = 'id'
    METRICS = 'metrics'
    PRIMARY_OUTPUT = 'primary'
    PROCESS_TYPE = 'process_type'
    SECONDARY_OUTPUTS = 'secondary'
    SNIPPETS = 'snippets'
    STATUS = 'status'
    # ..................
    # Types
    # ..................
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
