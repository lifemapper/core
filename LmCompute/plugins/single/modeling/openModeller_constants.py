"""Module containing Lifemapper constants for openModeller SDM jobs
"""
import os

from LmCompute.common.lmconstants import BIN_PATH

FILE_TYPES = ["FloatingHFA", "FloatingTiff", "GreyTiff"]
DEFAULT_FILE_TYPE = 'GreyTiff100'
OM_DEFAULT_LOG_LEVEL = 'debug'
OM_MODEL_CMD = 'om_model'
OM_PROJECT_CMD = 'om_project'
OM_VERSION = '1.5.0'
OM_MODEL_TOOL = os.path.join(BIN_PATH, OM_MODEL_CMD)
OM_PROJECT_TOOL = os.path.join(BIN_PATH, OM_PROJECT_CMD)
