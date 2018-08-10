"""
@summary: Module containing the constants set by Lifemapper
@author: CJ Grady
@version: 3.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
from LmCompute.common.localconstants import (LM_PATH, LM_DISK, SCRATCH_PATH, 
                     JAVA_EXE, JAVA_INIT_MEM_OPTION, JAVA_MAX_MEM_OPTION,
                     CONVERT_JAVA_INIT_MEM_OPTION, CONVERT_JAVA_MAX_MEM_OPTION)
# ============================================================================
# =                             Client Constants                             =
# ============================================================================
CLIENT_VERSION = "3.0.0"

# ============================================================================
# =                             Paths/Directories                              =
# ============================================================================
BIN_PATH = os.path.join(LM_PATH, 'bin')
ME_CMD = os.path.join(LM_PATH, 'LmCompute/apps/maxent.jar')

# JOB_OUTPUT_PATH --> WORKSPACE_PATH
WORKSPACE_PATH = os.path.join(SCRATCH_PATH, 'work')
TEMPORARY_FILE_PATH = os.path.join(SCRATCH_PATH, 'temp')
COMPUTE_LOG_PATH = os.path.join(SCRATCH_PATH, 'log')

SAMPLE_JOBS_PATH = os.path.join(LM_DISK, 'tests/config/sampleJobs')
SAMPLE_LAYERS_PATH = os.path.join(LM_DISK, 'tests/data/layers/testLayers.txt')
SAMPLE_DATA_PATH = os.path.join(LM_DISK, 'tests/data')
METRICS_STORAGE_PATH = os.path.join(LM_DISK, 'metrics')
SGE_PATH = os.path.join(LM_DISK, 'sge')

JAVA_SYSTEM_ROOT = os.path.join(SCRATCH_PATH, '.java')
JAVA_USER_ROOT = os.path.join(JAVA_SYSTEM_ROOT, '.userPrefs')
JAVA_CMD = '{} -Xms{} -Xmx{} -Djava.util.prefs.systemRoot={} -Djava.util.prefs.userRoot={} -cp'.format(
           JAVA_EXE, JAVA_INIT_MEM_OPTION, JAVA_MAX_MEM_OPTION, JAVA_SYSTEM_ROOT,
           JAVA_USER_ROOT)
CONVERT_JAVA_CMD = '{} -Xms{} -Xmx{} -Djava.util.prefs.systemRoot={} -Djava.util.prefs.userRoot={} -cp'.format(
                    JAVA_EXE, CONVERT_JAVA_INIT_MEM_OPTION, 
                    CONVERT_JAVA_MAX_MEM_OPTION, JAVA_SYSTEM_ROOT, JAVA_USER_ROOT)
GDALINFO_CMD = os.path.join(BIN_PATH, 'gdalinfo')

