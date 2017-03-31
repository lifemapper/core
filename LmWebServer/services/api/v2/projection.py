"""
@summary: This module provides REST services for Projections

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
@cherrypy.popargs('projectionId')
class Projection(LmService):
   """
   @summary: This class is for the projections service.  The dispatcher is
                responsible for calling the correct method
   """
   # ................................
   def DELETE(self, projectionId):
      """
      @summary: Attempts to delete a projection
      @param projectionId: The id of the projection to delete
      @todo: Need user id
      """
      pass

   # ................................
   def GET(self, projectionId=None, afterTime=None, beforeTime=None, 
           displayName=None, epsgCode=None, limit=100, occurrenceSetId=None,
           offset=0, public=None, scenarioId=None, status=None):
      """
      @summary: Performs a GET request.  If a projection id is provided,
                   attempt to return that item.  If not, return a list of 
                   projections that match the provided parameters
      """
      if projectionId is None:
         return self._listProjections(afterTime=afterTime, 
            beforeTime=beforeTime, displayName=displayName, epsgCode=epsgCode, 
            limit=limit, occurrenceSetId=occurrenceSetId, offset=offset, 
            public=public, scenarioId=scenarioId, status=status)
      else:
         return self._getProjection(projectionId)
   
   # ................................
   @cherrypy.json_in
   @cherrypy.json_out
   def POST(self):
      """
      @summary: Posts a new projection
      @todo: User id
      """
      projectionData = cherrypy.request.json
   
   # ................................
   @cherrypy.json_in
   @cherrypy.json_out
   def PUT(self, projectionId):
      pass
   
   # ................................
   def _getProjection(self, projectionId):
      """
      @summary: Attempt to get a projection
      """
      pass
   
   # ................................
   def _listProjections(self, afterTime=None, beforeTime=None, displayName=None, 
                        epsgCode=None, limit=100, occurrenceSetId=None, 
                        offset=0, public=None, scenarioId=None, status=None):
      """
      @summary: Return a list of projections matching the specified criteria
      """
      pass
   