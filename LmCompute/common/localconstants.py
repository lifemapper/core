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
_JOB_PUSHER_SECTION = 'LmCompute - Job Pusher'
_MEDIATOR_SECTION = "LmCompute - Job Mediator"
_JOB_SUBMITTER_SECTION = "LmCompute - Job Submitter"
_JOB_RETRIEVERS_SECTION = "LmCompute - Job Retrievers"
   

# Environment variables
PLUGINS_PATH = _cfg.get(_ENV_SECTION, 'PLUGINS_PATH')
BIN_PATH = _cfg.get(_ENV_SECTION, 'BIN_PATH')
JOB_DATA_PATH = _cfg.get(_ENV_SECTION, 'JOB_DATA_PATH')
JOB_OUTPUT_PATH = _cfg.get(_ENV_SECTION, 'JOB_OUTPUT_PATH')
JOB_REQUEST_PATH = _cfg.get(_ENV_SECTION, "JOB_REQUEST_PATH")

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
METRICS_PATH = _cfg.get(_METRICS_SECTION, 'METRICS_STORAGE_PATH')

# Job pusher
PUSH_JOBS_PATH = _cfg.get(_JOB_PUSHER_SECTION, 'PUSH_JOBS_PATH')
LOCKFILE_NAME = _cfg.get(_JOB_PUSHER_SECTION, 'LOCKFILE_NAME')
METAFILE_NAME = _cfg.get(_JOB_PUSHER_SECTION, 'METAFILE_NAME')

# Job mediator Constants
JOB_MEDIATOR_PID_FILE = _cfg.get(_MEDIATOR_SECTION, 'PID_FILE')
JM_SLEEP_TIME = _cfg.getint(_MEDIATOR_SECTION, 'SLEEP_TIME')
# TODO: Remove JM_HOLD_PATH? This is unused
JM_HOLD_PATH = _cfg.get(_MEDIATOR_SECTION, 'HOLD_JOB_PATH')
JM_INACTIVE_TIME = _cfg.getint(_MEDIATOR_SECTION, 'INACTIVE_TIME')

# Job submitter
JOB_SUBMITTER_TYPE = _cfg.get(_JOB_SUBMITTER_SECTION, 'JOB_SUBMITTER_TYPE')
JOB_CAPACITY = _cfg.getint(_JOB_SUBMITTER_SECTION, 'CAPACITY')
try:
   LOCAL_SUBMIT_COMMAND = _cfg.get(_JOB_SUBMITTER_SECTION, 'LOCAL_SUBMIT_COMMAND')
except:
   LOCAL_SUBMIT_COMMAND = None

try:
   SGE_SUBMIT_COMMAND = _cfg.get(_JOB_SUBMITTER_SECTION, 'SGE_SUBMIT_COMMAND')
except:
   SGE_SUBMIT_COMMAND = None
try:
   SGE_COUNT_JOBS_COMMAND = _cfg.get(_JOB_SUBMITTER_SECTION, 'NUM_JOBS_COMMAND')
except:
   SGE_COUNT_JOBS_COMMAND = None


# Job retrievers constants
JOB_RETRIEVERS = {}
   
# Get retriever keys
_retrieverKeys = _cfg.getlist(_JOB_RETRIEVERS_SECTION, 'JOB_RETRIEVER_KEYS')

for retKey in _retrieverKeys:
   # Each retriever has its own section
   retSec = "%s - %s" % (_JOB_RETRIEVERS_SECTION, retKey)
      
   retrieverType = _cfg.get(retSec, 'RETRIEVER_TYPE')
      
   # TODO: CJ: directory option and JOB_DIR is not in the default config.lmcompute.ini
   #       If it is added, it should be JOB_PATH
   #  CJG 9/14/16: This will go away soon.  Won't fix
   if retrieverType.lower() == 'directory':
      from LmCompute.jobs.retrievers.directoryJobRetriever import DirectoryRetriever
      jobDir = _cfg.get(retSec, 'JOB_DIR')
      retriever = DirectoryRetriever(jobDir)
      JOB_RETRIEVERS[retKey] = {
                                "retrieverType" : "directory",
                                "jobDirectory" : jobDir
                               }
   elif retrieverType.lower() == 'server':
      jobDir = os.path.join(JM_HOLD_PATH, retKey)
      jobServer = _cfg.get(retSec, 'JOB_SERVER')
      numToPull = _cfg.getint(retSec, 'NUM_TO_PULL')
      threshold = _cfg.getint(retSec, 'PULL_THRESHOLD')
      
      # Create a dictionary with the optional parameters so they can be 
      #    passed in if provided but ignored if not
      optionalParameters = {}
      try:
         jobTypes = _cfg.getlist(retSec, 'JOB_TYPES')
         jobTypes = map(int, jobTypes)
         optionalParameters['jobTypes'] = jobTypes
      except:
         pass
      
      try:
         users = _cfg.get(retSec, 'USERS')
         optionalParameters['users'] = users
      except:
         pass
     
      JOB_RETRIEVERS[retKey] = {
                                "jobDirectory" : jobDir,
                                "retrieverType" : "server",
                                "jobServer" : jobServer,
                                "numToPull" : numToPull,
                                "threshold" : threshold,
                                "options" : optionalParameters
                               }
   else:
      raise Exception, "Unknown job retriever type for %s: %s" % (retKey, 
                                                           retrieverType)
