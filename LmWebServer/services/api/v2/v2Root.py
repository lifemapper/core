#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
@summary: This module provides REST services for service objects

@author: CJ Grady
@version: 2.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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

import cherrypy

from LmWebServer.services.api.v2.envLayer import EnvLayerService
from LmWebServer.services.api.v2.globalPam import GlobalPAMService
from LmWebServer.services.api.v2.gridset import GridSetService
from LmWebServer.services.api.v2.layer import LayerService
from LmWebServer.services.api.v2.occurrence import OccurrenceLayerService
from LmWebServer.services.api.v2.ogc import MapService
from LmWebServer.services.api.v2.scenario import ScenarioService
from LmWebServer.services.api.v2.sdmProject import SdmProjectService
from LmWebServer.services.api.v2.shapegrid import ShapeGridService
from LmWebServer.services.api.v2.tree import TreeService

# .............................................................................
@cherrypy.expose
class ApiRootV2(object):
   """
   @summary: Top level class containing Lifemapper services V2
   """
   envlayer = EnvLayerService()
   globalpam = GlobalPAMService()
   gridset = GridSetService()
   layer = LayerService()
   occurrence = OccurrenceLayerService()
   scenario = ScenarioService()
   sdmproject = SdmProjectService()
   shapegrid = ShapeGridService()
   tree = TreeService()
   
   ogc = MapService()

   # ................................
   def __init__(self):
      pass
   
   # ................................
   def index(self):
      return "Index of v2 root"

# .............................................................................
#if __name__ == '__main__':
#conf = {
#      '/v2/' : {
#         'request.dispatch' : cherrypy.dispatch.MethodDispatcher(),
#      }
#}
#cherrypy.quickstart(ApiRootV2(), '/v2/', conf)
   