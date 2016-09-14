"""
@summary: Local constants for LmCompute
@author: CJ Grady
@version: 3.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
import os
from LmCommon.common.config import Config

_cfg = Config()
_ENV_SECTION = 'LmCompute - environment'
_CMDS_SECTION = 'LmCompute - commands'
_CONTACT_SECTION = 'LmCompute - contact'
_OPTIONS_SECTION = 'LmCompute - options'
_METRICS_SECTION = 'LmCompute - metrics'
   

# Environment variables
PLUGINS_DIR = _cfg.get(_ENV_SECTION, 'PLUGINS_DIR')
BIN_PATH = _cfg.get(_ENV_SECTION, 'BIN_PATH')
JOB_DATA_PATH = _cfg.get(_ENV_SECTION, 'JOB_DATA_PATH')

TEMPORARY_FILE_PATH = _cfg.get(_ENV_SECTION, 'TEMPORARY_FILE_PATH')
SAMPLE_LAYERS_PATH = _cfg.get(_ENV_SECTION, 'SAMPLE_LAYERS_PATH')
SAMPLE_JOBS_PATH = _cfg.get(_ENV_SECTION, 'SAMPLE_JOBS_PATH')
SAMPLE_DATA_PATH = _cfg.get(_ENV_SECTION, 'SAMPLE_DATA_PATH')

INPUT_LAYER_DIR = _cfg.get(_ENV_SECTION, 'INPUT_LAYER_DIR')
INPUT_LAYER_DB = _cfg.get(_ENV_SECTION, 'INPUT_LAYER_DB')


# Commands
GDALINFO_CMD = _cfg.get(_CMDS_SECTION, 'GDALINFO_CMD')
PYTHON_CMD = _cfg.get(_CMDS_SECTION, 'PYTHON_CMD')

# Contact information
INSTITUTION_NAME = _cfg.get(_CONTACT_SECTION, 'INSTITUTION_NAME')
ADMIN_NAME = _cfg.get(_CONTACT_SECTION, 'ADMIN_NAME')
ADMIN_EMAIL = _cfg.get(_CONTACT_SECTION, 'ADMIN_EMAIL')
LOCAL_MACHINE_ID = _cfg.get(_CONTACT_SECTION, 'LOCAL_MACHINE_ID')


# Options
STORE_LOGS = _cfg.getboolean(_OPTIONS_SECTION, 'STORE_LOG_FILES')
LOG_LOCATION = _cfg.get(_OPTIONS_SECTION, 'LOG_STORAGE_LOCATION')

# Metrics
STORE_METRICS = _cfg.getboolean(_METRICS_SECTION, 'STORE_METRICS')
METRICS_LOCATION = _cfg.get(_METRICS_SECTION, 'METRICS_STORAGE_DIRECTORY')

