"""Local constants for LmCompute
"""
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (
    COMPUTE_CMDS_HEADING, COMPUTE_CONTACT_HEADING, COMPUTE_ENV_HEADING,
    COMPUTE_ME_PLUGIN_HEADING)

_CFG = Config()

# Environment variables
LM_PATH = _CFG.get(COMPUTE_ENV_HEADING, 'LM_PATH')
LM_DISK = _CFG.get(COMPUTE_ENV_HEADING, 'LM_DISK')
SCRATCH_PATH = _CFG.get(COMPUTE_ENV_HEADING, 'SCRATCH_PATH')
SHARED_DATA_PATH = _CFG.get(COMPUTE_ENV_HEADING, 'SHARED_DATA_PATH')

JAVA_EXE = _CFG.get(COMPUTE_ME_PLUGIN_HEADING, 'JAVA_EXE')
JAVA_INIT_MEM_OPTION = _CFG.get(
    COMPUTE_ME_PLUGIN_HEADING, 'JAVA_INIT_MEM_OPTION')
JAVA_MAX_MEM_OPTION = _CFG.get(
    COMPUTE_ME_PLUGIN_HEADING, 'JAVA_MAX_MEM_OPTION')
CONVERT_JAVA_INIT_MEM_OPTION = _CFG.get(
    COMPUTE_ME_PLUGIN_HEADING, 'CONVERT_JAVA_INIT_MEM_OPTION')
CONVERT_JAVA_MAX_MEM_OPTION = _CFG.get(
    COMPUTE_ME_PLUGIN_HEADING, 'CONVERT_JAVA_MAX_MEM_OPTION')

PLUGINS_PATH = _CFG.get(COMPUTE_ENV_HEADING, 'PLUGINS_PATH')

# Commands
PYTHON_CMD = _CFG.get(COMPUTE_CMDS_HEADING, 'PYTHON_CMD')

# Contact information
INSTITUTION_NAME = _CFG.get(COMPUTE_CONTACT_HEADING, 'INSTITUTION_NAME')
ADMIN_NAME = _CFG.get(COMPUTE_CONTACT_HEADING, 'ADMIN_NAME')
ADMIN_EMAIL = _CFG.get(COMPUTE_CONTACT_HEADING, 'ADMIN_EMAIL')
LOCAL_MACHINE_ID = _CFG.get(COMPUTE_CONTACT_HEADING, 'LOCAL_MACHINE_ID')
