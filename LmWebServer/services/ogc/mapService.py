"""
@summary: Module containing classes for Lifemapper Species Distribution 
             Modeling Experiments service and object
@author: CJ Grady
@version: 2.0
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
from LmServer.common.log import MapLogger
from LmWebServer.base.servicesBaseClass import OGCService
from LmWebServer.services.ogc.ancMapper import MapConstructor2

# =============================================================================
# =============================================================================
class StaticMapService(OGCService):
   """
   @summary: Lifemapper SDM Experiments Service
   @see: RestService
   """
   identifier = "maps"
   version = "2.0"
   summary = "Static Maps Service"
   description = """Static Maps Service"""
   
   queryParameters = [
                  ]
   processTypes = {}
   subServices = [
                 ]
   # Variables established in constructor:
   #  self.method - HTTP method used for service access
   #  self.user - User id to use
   #  self.body - body of HTTP message
   #  self.vpath - List of path variables
   #  self.parameters - Dictionary of url query parametes
   
   # ............................................
   def doAction(self):
      logger = MapLogger(isDev=True)
      mapper = MapConstructor2(logger)
      params = dict(self.parameters)
      templ = None
      mapFn = None
      mapper.assembleMap(params)
      #contentType, content = mapper.returnResponse()
      return mapper.returnResponse()
      
   # ............................................
   def help(self):
      pass
