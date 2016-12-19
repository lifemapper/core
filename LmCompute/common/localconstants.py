"""
@summary: Local constants for LmCompute
@author: CJ Grady
@version: 3.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
LM_PATH = _cfg.get(_ENV_SECTION, 'LM_PATH')
LM_DISK = _cfg.get(_ENV_SECTION, 'LM_DISK')
SCRATCH_PATH = _cfg.get(_ENV_SECTION, 'SCRATCH_PATH')
SHARED_DATA_PATH = _cfg.get(_ENV_SECTION, 'SHARED_DATA_PATH')

JAVA_EXE = _cfg.get(_ME_PLUGIN_SECTION, 'JAVA_EXE')
JAVA_INIT_MEM_OPTION = _cfg.get(_ME_PLUGIN_SECTION, 'JAVA_INIT_MEM_OPTION')
JAVA_MAX_MEM_OPTION = _cfg.get(_ME_PLUGIN_SECTION, 'JAVA_MAX_MEM_OPTION')
CONVERT_JAVA_INIT_MEM_OPTION = _cfg.get(_ME_PLUGIN_SECTION, 'CONVERT_JAVA_INIT_MEM_OPTION')
CONVERT_JAVA_MAX_MEM_OPTION = _cfg.get(_ME_PLUGIN_SECTION, 'CONVERT_JAVA_MAX_MEM_OPTION')

PLUGINS_PATH = _cfg.get(_ENV_SECTION, 'PLUGINS_PATH')
JOB_REQUEST_PATH = _cfg.get(_ENV_SECTION, "JOB_REQUEST_PATH")

# Commands
PYTHON_CMD = _cfg.get(_CMDS_SECTION, 'PYTHON_CMD')

# Contact information
INSTITUTION_NAME = _cfg.get(_CONTACT_SECTION, 'INSTITUTION_NAME')
ADMIN_NAME = _cfg.get(_CONTACT_SECTION, 'ADMIN_NAME')
ADMIN_EMAIL = _cfg.get(_CONTACT_SECTION, 'ADMIN_EMAIL')
LOCAL_MACHINE_ID = _cfg.get(_CONTACT_SECTION, 'LOCAL_MACHINE_ID')
