"""
@summary: Module containing the constants set by Lifemapper
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
from LmCompute.common.localconstants import (LM_PATH, LM_DISK, SCRATCH_PATH)
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

SAMPLE_JOBS_PATH = os.path.join(LM_DISK, 'tests/config/sampleJobs')
SAMPLE_LAYERS_PATH = os.path.join(LM_DISK, 'tests/data/layers/testLayers.txt')
SAMPLE_DATA_PATH = os.path.join(LM_DISK, 'tests/data')
METRICS_STORAGE_PATH = os.path.join(LM_DISK, 'metrics')
COMPUTE_LOG_PATH = os.path.join(LM_DISK, 'logs')

# ============================================================================
# =                             Commands                                     =
# ============================================================================
# Maxent
MDL_TOOL = 'density.MaxEnt'
PRJ_TOOL = 'density.Project'
CONVERT_TOOL ='density.Convert'
ME_VERSION = '3.3.3k'

# ============================================================================
# =                             Layer Constants                              =
# ============================================================================
RETRIEVED_LAYER_DIR = 'retrieved'
INPUT_LAYER_DB = 'layers.db'

class LayerStatus:
   """
   @summary: This class contains possible layer statuses for LmCompute layers
   """
   ABSENT = 0
   STORED = 1
   RETRIEVING = 2
   SEEDED = 3
   TIFF_AVAILABLE = 4
   
class LayerFormat:
   """
   @summary: Class containing LmCompute compatible layer formats
   """
   GTIFF = 0
   ASCII = 1
   MXE = 2
   SHAPE = 3

class LayerAttributes:
   """
   @summary: Compute layer db table attributes
   """
   LAYER_ID = 'layerid'
   FILE_PATH = 'filepath'
   FILE_TYPE = 'filetype'
   STATUS = 'status'
   CREATE_TIME = 'createdate'
   TOUCH_TIME = 'touchdate'
   
class SchemaMetadata:
   """
   @summary: Compute layers db schema metadata
   """
   TABLE_NAME = 'lmMetadata'
   VERSION = '2.0'
   VERSION_ATTRIBUTE = 'version'
   CREATE_TIME_ATTRIBUTE = 'createTime'
   