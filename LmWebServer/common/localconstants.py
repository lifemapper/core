"""
@summary: Local configuration constants for LmWebServer
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
from LmCommon.common.lmconstants import (SERVER_ENV_HEADING)

cfg = Config()

# Relative Home of Results packaging code
PACKAGING_DIR = cfg.get(SERVER_ENV_HEADING, 'PACKAGING_DIR')

# Max size for anonymous uploads (in lines)
try:
    MAX_ANON_UPLOAD_SIZE = cfg.get(SERVER_ENV_HEADING, 'MAX_ANON_UPLOAD_SIZE')
except:
    MAX_ANON_UPLOAD_SIZE = 20000
