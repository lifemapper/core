"""
@summary: Local constants for LmCompute
@author: CJ Grady
@version: 3.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (COMPUTE_ENV_HEADING, 
      COMPUTE_CMDS_HEADING, COMPUTE_CONTACT_HEADING, COMPUTE_ME_PLUGIN_HEADING)

_cfg = Config()
   
# Environment variables
LM_PATH = _cfg.get(COMPUTE_ENV_HEADING, 'LM_PATH')
LM_DISK = _cfg.get(COMPUTE_ENV_HEADING, 'LM_DISK')
SCRATCH_PATH = _cfg.get(COMPUTE_ENV_HEADING, 'SCRATCH_PATH')
SHARED_DATA_PATH = _cfg.get(COMPUTE_ENV_HEADING, 'SHARED_DATA_PATH')

JAVA_EXE = _cfg.get(COMPUTE_ME_PLUGIN_HEADING, 'JAVA_EXE')
JAVA_INIT_MEM_OPTION = _cfg.get(COMPUTE_ME_PLUGIN_HEADING, 'JAVA_INIT_MEM_OPTION')
JAVA_MAX_MEM_OPTION = _cfg.get(COMPUTE_ME_PLUGIN_HEADING, 'JAVA_MAX_MEM_OPTION')
CONVERT_JAVA_INIT_MEM_OPTION = _cfg.get(COMPUTE_ME_PLUGIN_HEADING, 'CONVERT_JAVA_INIT_MEM_OPTION')
CONVERT_JAVA_MAX_MEM_OPTION = _cfg.get(COMPUTE_ME_PLUGIN_HEADING, 'CONVERT_JAVA_MAX_MEM_OPTION')

PLUGINS_PATH = _cfg.get(COMPUTE_ENV_HEADING, 'PLUGINS_PATH')
# JOB_REQUEST_PATH = _cfg.get(COMPUTE_ENV_HEADING, "JOB_REQUEST_PATH")

# Commands
PYTHON_CMD = _cfg.get(COMPUTE_CMDS_HEADING, 'PYTHON_CMD')

# Contact information
INSTITUTION_NAME = _cfg.get(COMPUTE_CONTACT_HEADING, 'INSTITUTION_NAME')
ADMIN_NAME = _cfg.get(COMPUTE_CONTACT_HEADING, 'ADMIN_NAME')
ADMIN_EMAIL = _cfg.get(COMPUTE_CONTACT_HEADING, 'ADMIN_EMAIL')
LOCAL_MACHINE_ID = _cfg.get(COMPUTE_CONTACT_HEADING, 'LOCAL_MACHINE_ID')
