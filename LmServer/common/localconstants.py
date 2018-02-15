"""
@summary: Local configuration values for LmServer
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
from LmCommon.common.lmconstants import (SERVER_ENV_HEADING, 
      SERVER_PIPELINE_HEADING, SERVER_DB_HEADING, 
      SERVER_COMPUTE_HEADING, SERVER_MATT_DAEMON_HEADING)

cfg = Config()

# LmServer (LmDbServer and LmWebServer)
PUBLIC_FQDN = cfg.get(SERVER_ENV_HEADING, 'PUBLIC_FQDN')
PUBLIC_USER = cfg.get(SERVER_ENV_HEADING, 'PUBLIC_USER')
APP_PATH = cfg.get(SERVER_ENV_HEADING, 'APP_PATH')
DATA_PATH = cfg.get(SERVER_ENV_HEADING, 'DATA_PATH')
SHARED_DATA_PATH = cfg.get(SERVER_ENV_HEADING, "SHARED_DATA_PATH")
SCRATCH_PATH = cfg.get(SERVER_ENV_HEADING, 'SCRATCH_PATH')
PID_PATH = cfg.get(SERVER_ENV_HEADING, 'PID_PATH')
LM_DISK = cfg.get(SERVER_ENV_HEADING, 'LM_DISK')

# TEMP_PATH = cfg.get(SERVER_ENV_HEADING, 'TEMP_PATH')

SMTP_SERVER = cfg.get(SERVER_ENV_HEADING, 'SMTP_SERVER')
SMTP_SENDER = cfg.get(SERVER_ENV_HEADING, 'SMTP_SENDER')
WEBSERVICES_ROOT = cfg.get(SERVER_ENV_HEADING, 'WEBSERVICES_ROOT')
 
COMPUTE_NAME = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_NAME')
COMPUTE_IP = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_IP')
COMPUTE_IP_MASK = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_IP_MASK')
COMPUTE_CONTACT_USERID = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_CONTACT_USERID')
COMPUTE_CONTACT_EMAIL = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_CONTACT_EMAIL')
COMPUTE_CONTACT_FIRSTNAME = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_CONTACT_FIRSTNAME')
COMPUTE_CONTACT_LASTNAME = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_CONTACT_LASTNAME')
COMPUTE_INSTITUTION = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_INSTITUTION')
COMPUTE_ADDR1 = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_ADDR1')
COMPUTE_ADDR2 = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_ADDR2')
COMPUTE_ADDR3 = cfg.get(SERVER_COMPUTE_HEADING, 'COMPUTE_ADDR3')

DATASOURCE = cfg.get(SERVER_PIPELINE_HEADING, 'DATASOURCE')
# TODO: move this to LmCommon, include BISON, add to BISON API query params
POINT_COUNT_MIN = cfg.getint(SERVER_PIPELINE_HEADING, 'POINT_COUNT_MIN')
POINT_COUNT_MAX = cfg.getint(SERVER_PIPELINE_HEADING, 'POINT_COUNT_MAX')
TROUBLESHOOTERS = cfg.getlist(SERVER_PIPELINE_HEADING, 'TROUBLESHOOTERS')
DEFAULT_EPSG = cfg.getint(SERVER_PIPELINE_HEADING, 'DEFAULT_EPSG')

CONNECTION_PORT = int(cfg.get(SERVER_DB_HEADING, 'CONNECTION_PORT'))
DB_HOSTNAME = cfg.get(SERVER_DB_HEADING, 'DB_HOSTNAME')

# Makeflow
MAX_MAKEFLOWS = cfg.getint(SERVER_MATT_DAEMON_HEADING, 'MAX_MAKEFLOWS')
MAX_WORKERS = cfg.getint(SERVER_MATT_DAEMON_HEADING, 'MAX_WORKERS')
try:
   WORKER_PATH = cfg.get(SERVER_MATT_DAEMON_HEADING, 'WORKER_PATH')
except:
   WORKER_PATH = os.path.join(SCRATCH_PATH, 'worker')

try:
   MASTER_WORKER_PATH = cfg.get(SERVER_MATT_DAEMON_HEADING, 'MASTER_WORKER_PATH')
except:
   MASTER_WORKER_PATH = os.path.join(SCRATCH_PATH, 'worker')
   
