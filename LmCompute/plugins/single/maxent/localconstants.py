"""
@summary: MaxEnt plugin local constants
@author: CJ Grady
@version: 4.0.0
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
from LmCommon.common.config import Config

_cfg = Config()
_ME_PLUGIN_SECTION = 'LmCompute - plugins - maxent'

JAVA_CMD = _cfg.get(_ME_PLUGIN_SECTION, 'JAVA_CMD')
CONVERT_JAVA_CMD = _cfg.get(_ME_PLUGIN_SECTION, 'CONVERT_JAVA_CMD')
ME_CMD = _cfg.get(_ME_PLUGIN_SECTION, 'ME_CMD')
MDL_TOOL = _cfg.get(_ME_PLUGIN_SECTION, 'MDL_TOOL')
PRJ_TOOL = _cfg.get(_ME_PLUGIN_SECTION, 'PRJ_TOOL')
CONVERT_TOOL = _cfg.get(_ME_PLUGIN_SECTION, 'CONVERT_TOOL')
ME_VERSION = _cfg.get(_ME_PLUGIN_SECTION, 'ME_VERSION')

