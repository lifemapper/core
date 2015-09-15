"""
@summary: openModeller plugin local constants
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
from LmCommon.common.config import Config

_cfg = Config()
_OM_PLUGIN_SECTION = 'LmCompute - plugins - openModeller'

DEFAULT_FILE_TYPE = _cfg.get(_OM_PLUGIN_SECTION, 'DEFAULT_FILE_TYPE')
DEFAULT_LOG_LEVEL = _cfg.get(_OM_PLUGIN_SECTION, 'DEFAULT_LOG_LEVEL')
OM_MODEL_CMD = _cfg.get(_OM_PLUGIN_SECTION, 'OM_MODEL_CMD')
OM_PROJECT_CMD = _cfg.get(_OM_PLUGIN_SECTION, 'OM_PROJECT_CMD')
OM_VERSION = _cfg.get(_OM_PLUGIN_SECTION, 'OM_VERSION')
