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
from LmCommon.common.config import Config

cfg = Config()
_ENV_CONFIG_HEADING = "LmServer - environment"
_PIPELINE_CONFIG_HEADING = "LmServer - pipeline"
_DB_CONFIG_HEADING = "LmServer - dbserver"
_COMPUTE_HEADING = "LmServer - registeredcompute"
_MATT_DAEMON_HEADING = "LmServer - Matt Daemon"

# LmServer (LmDbServer and LmWebServer)
PUBLIC_FQDN = cfg.get(_ENV_CONFIG_HEADING, 'PUBLIC_FQDN')
ARCHIVE_USER = cfg.get(_ENV_CONFIG_HEADING, 'ARCHIVE_USER')
APP_PATH = cfg.get(_ENV_CONFIG_HEADING, 'APP_PATH')
DATA_PATH = cfg.get(_ENV_CONFIG_HEADING, 'DATA_PATH')
SHARED_DATA_PATH = cfg.get(_ENV_CONFIG_HEADING, "SHARED_DATA_PATH")
SCRATCH_PATH = cfg.get(_ENV_CONFIG_HEADING, 'SCRATCH_PATH')
PID_PATH = cfg.get(_ENV_CONFIG_HEADING, 'PID_PATH')
TEMP_PATH = cfg.get(_ENV_CONFIG_HEADING, 'TEMP_PATH')

SMTP_SERVER = cfg.get(_ENV_CONFIG_HEADING, 'SMTP_SERVER')
SMTP_SENDER = cfg.get(_ENV_CONFIG_HEADING, 'SMTP_SENDER')
OGC_SERVICE_URL = cfg.get(_ENV_CONFIG_HEADING, 'OGC_SERVICE_URL')
WEBSERVICES_ROOT = cfg.get(_ENV_CONFIG_HEADING, 'WEBSERVICES_ROOT')
 
COMPUTE_NAME = cfg.get(_COMPUTE_HEADING, 'COMPUTE_NAME')
COMPUTE_IP = cfg.get(_COMPUTE_HEADING, 'COMPUTE_IP')
COMPUTE_IP_MASK = cfg.get(_COMPUTE_HEADING, 'COMPUTE_IP_MASK')
COMPUTE_CONTACT_USERID = cfg.get(_COMPUTE_HEADING, 'COMPUTE_CONTACT_USERID')
COMPUTE_CONTACT_EMAIL = cfg.get(_COMPUTE_HEADING, 'COMPUTE_CONTACT_EMAIL')
COMPUTE_CONTACT_FIRSTNAME = cfg.get(_COMPUTE_HEADING, 'COMPUTE_CONTACT_FIRSTNAME')
COMPUTE_CONTACT_LASTNAME = cfg.get(_COMPUTE_HEADING, 'COMPUTE_CONTACT_LASTNAME')
COMPUTE_INSTITUTION = cfg.get(_COMPUTE_HEADING, 'COMPUTE_INSTITUTION')
COMPUTE_ADDR1 = cfg.get(_COMPUTE_HEADING, 'COMPUTE_ADDR1')
COMPUTE_ADDR2 = cfg.get(_COMPUTE_HEADING, 'COMPUTE_ADDR2')
COMPUTE_ADDR3 = cfg.get(_COMPUTE_HEADING, 'COMPUTE_ADDR3')

DATASOURCE = cfg.get(_PIPELINE_CONFIG_HEADING, 'DATASOURCE')
# TODO: move this to LmCommon, include BISON, add to BISON API query params
POINT_COUNT_MIN = cfg.getint(_PIPELINE_CONFIG_HEADING, 'POINT_COUNT_MIN')
POINT_COUNT_MAX = cfg.getint(_PIPELINE_CONFIG_HEADING, 'POINT_COUNT_MAX')
TROUBLESHOOTERS = cfg.getlist(_PIPELINE_CONFIG_HEADING, 'TROUBLESHOOTERS')
DEFAULT_EPSG = cfg.getint(_PIPELINE_CONFIG_HEADING, 'DEFAULT_EPSG')
DEFAULT_MAPUNITS = cfg.get(_PIPELINE_CONFIG_HEADING, 'DEFAULT_MAPUNITS')

CONNECTION_PORT = int(cfg.get(_DB_CONFIG_HEADING, 'CONNECTION_PORT'))
DB_HOSTNAME = cfg.get(_DB_CONFIG_HEADING, 'DB_HOSTNAME')

# Catalog Server
CATALOG_SERVER_PID_FILE = cfg.get(_MATT_DAEMON_HEADING, 'CS_PID_FILE')
CATALOG_SERVER_OPTIONS = cfg.get(_MATT_DAEMON_HEADING, 'CS_OPTIONS')

# Worker Factory
WORKER_FACTORY_OPTIONS = cfg.get(_MATT_DAEMON_HEADING, 'WF_OPTIONS')

# Makeflow
MAX_MAKEFLOWS = cfg.getint(_MATT_DAEMON_HEADING, 'MAX_MAKEFLOWS')
MAKEFLOW_OPTIONS = cfg.get(_MATT_DAEMON_HEADING, 'MAKEFLOW_OPTIONS')
