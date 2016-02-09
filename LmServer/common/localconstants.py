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

_ENV_CONFIG_HEADING = "LmServer - environment"
_PIPELINE_CONFIG_HEADING = "LmServer - pipeline"
_DB_CONFIG_HEADING = "LmServer - dbserver"
_COMPUTE_HEADING = "LmServer - registeredcompute"

# LmServer (LmDbServer and LmWebServer)
ARCHIVE_USER = Config().get(_ENV_CONFIG_HEADING, 'ARCHIVE_USER')
APP_PATH = Config().get(_ENV_CONFIG_HEADING, 'APP_PATH')
DATA_PATH = Config().get(_ENV_CONFIG_HEADING, 'DATA_PATH')
TEMP_PATH = Config().get(_ENV_CONFIG_HEADING, 'TEMP_PATH')
MAPSERVER_ROOT = Config().get(_ENV_CONFIG_HEADING, 'MAPSERVER_ROOT')
DATASOURCE = Config().get(_ENV_CONFIG_HEADING, 'DATASOURCE')

SMTP_SERVER = Config().get(_ENV_CONFIG_HEADING, 'SMTP_SERVER')
SMTP_SENDER = Config().get(_ENV_CONFIG_HEADING, 'SMTP_SENDER')

COMPUTE_NAME = Config().get(_COMPUTE_HEADING, 'COMPUTE_NAME')
COMPUTE_IP = Config().get(_COMPUTE_HEADING, 'COMPUTE_IP')
COMPUTE_IP_MASK = Config().get(_COMPUTE_HEADING, 'COMPUTE_IP_MASK')
COMPUTE_CONTACT_USERID = Config().get(_COMPUTE_HEADING, 'COMPUTE_CONTACT_USERID')
COMPUTE_CONTACT_EMAIL = Config().get(_COMPUTE_HEADING, 'COMPUTE_CONTACT_EMAIL')
COMPUTE_CONTACT_FIRSTNAME = Config().get(_COMPUTE_HEADING, 'COMPUTE_CONTACT_FIRSTNAME')
COMPUTE_CONTACT_LASTNAME = Config().get(_COMPUTE_HEADING, 'COMPUTE_CONTACT_LASTNAME')
COMPUTE_INSTITUTION = Config().get(_COMPUTE_HEADING, 'COMPUTE_INSTITUTION')
COMPUTE_ADDR1 = Config().get(_COMPUTE_HEADING, 'COMPUTE_ADDR1')
COMPUTE_ADDR2 = Config().get(_COMPUTE_HEADING, 'COMPUTE_ADDR2')
COMPUTE_ADDR3 = Config().get(_COMPUTE_HEADING, 'COMPUTE_ADDR3')

# TODO: move this to LmCommon, include BISON, add to BISON API query params
POINT_COUNT_MIN = Config().getint(_PIPELINE_CONFIG_HEADING, 'POINT_COUNT_MIN')
POINT_COUNT_MAX = Config().getint(_PIPELINE_CONFIG_HEADING, 'POINT_COUNT_MAX')
TROUBLESHOOTERS = Config().getlist(_PIPELINE_CONFIG_HEADING, 'TROUBLESHOOTERS')

CONNECTION_PORT = int(Config().get(_DB_CONFIG_HEADING, 'CONNECTION_PORT'))
DB_HOSTNAME = Config().get(_DB_CONFIG_HEADING, 'DB_HOSTNAME')
