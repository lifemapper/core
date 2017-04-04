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

from LmWebServer.services.api.v2.layer import Layer
from LmWebServer.services.api.v2.occurrenceSet import OccurrenceSet
from LmWebServer.services.api.v2.projection import Projection
from LmWebServer.services.api.v2.scenario import Scenario

# .............................................................................
@cherrypy.expose
class ApiRootV2(object):
   """
   @summary: Top level class containing Lifemapper services V2
   """
   layer = Layer()
   occurrenceSet = OccurrenceSet()
   projection = Projection()
   scenario = Scenario()

   # ................................
   def __init__(self):
      pass
   
   # ................................
   def index(self):
      pass

# .............................................................................
if __name__ == '__main__':
   conf = {
      '/v2/' : {
         'request.dispatch' : cherrypy.dispatch.MethodDispatcher(),
      }
   }
   cherrypy.quickstart(ApiRootV2(), '/v2/', conf)
   