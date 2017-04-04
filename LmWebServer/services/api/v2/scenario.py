"""
@summary: This module provides REST services for Scenario

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

from LmWebServer.services.api.v2.base import LmService

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('scenarioId')
class Scenario(LmService):
   """
   @summary: This class is for the scenarios service.  The dispatcher is
                responsible for calling the correct method
   """
   # ................................
   def DELETE(self, scenarioId):
      """
      @summary: Attempts to delete a scenario
      @param projectionId: The id of the scenario to delete
      @todo: Need user id
      """
      pass

   # ................................
   def GET(self, scenarioId=None, afterTime=None, beforeTime=None, 
           epsgCode=None, limit=100, offset=0, public=None):
      """
      @summary: Performs a GET request.  If a scenario id is provided,
                   attempt to return that item.  If not, return a list of 
                   scenarios that match the provided parameters
      """
      if scenarioId is None:
         return self._listScenarios(afterTime=afterTime, beforeTime=beforeTime, 
                  epsgCode=epsgCode, limit=limit, offset=offset, public=public)
      else:
         return self._getScenario(scenarioId)
   
   # ................................
   @cherrypy.json_in
   @cherrypy.json_out
   def POST(self):
      """
      @summary: Posts a new scenario
      @todo: User id
      """
      pass
   
   # ................................
   @cherrypy.json_in
   @cherrypy.json_out
   def PUT(self, scenarioId):
      pass
   
   # ................................
   def _getScenario(self, scenarioId):
      """
      @summary: Attempt to get a scenario
      """
      pass
   
   # ................................
   def _listScenarios(self, afterTime=None, beforeTime=None, epsgCode=None, 
                      limit=100, offset=0, public=None):
      """
      @summary: Return a list of scenarios matching the specified criteria
      """
      pass
   