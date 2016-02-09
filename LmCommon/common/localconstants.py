"""
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

_CONFIG_HEADING = "LmCommon - common"
# LmCommon (LmClient, LmCompute, LmDbServer, LmWebServer) 
OGC_SERVICE_URL = Config().get(_CONFIG_HEADING, 'OGC_SERVICE_URL')
WEBSERVICES_ROOT = Config().get(_CONFIG_HEADING, 'WEBSERVICES_ROOT')

try:
   CONCURRENT_PROCESSES = Config().getint(_CONFIG_HEADING, 'CONCURRENT_PROCESSES')
except:
   # If the parameter has not been set, set the number to the number of CPUs
   #    in the machine minus 2, or 1, whichever is larger
   import multiprocessing
   CONCURRENT_PROCESSES = max(1, multiprocessing.cpu_count() - 2)
   