"""
@summary: This module provides REST services for Occurrence sets

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
from LmServer.common.localconstants import PUBLIC_USER

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('occSetId')
class OccurrenceSet(LmService):
   """
   @summary: This class is for the occurrence sets service.  The dispatcher is
                responsible for calling the correct method
   """
   # ................................
   def DELETE(self, occSetId):
      """
      @summary: Attempts to delete an occurrence set
      @param occSetId: The id of the occurrence set to delete
      """
      occ = self.scribe.getOccurrenceSet(occid=int(occSetId))
      
      if occ is None:
         raise cherrypy.HTTPError(404, "Occurrence set not found")
      
      # If allowed to, delete
      if occ.getUserId() == self.userId:
         success = self.scribe.deleteObject(occ)
         if success:
            cherrypy.response.status = 204
            return
         else:
            raise cherrypy.HTTPError(500, 
                        "Failed to delete occurrence set")
      else:
         raise cherrypy.HTTPError(403, 
                 "User does not have permission to delete this occurrence set")
      
   # ................................
   def GET(self, occSetId=None, afterTime=None, beforeTime=None, 
           displayName=None, epsgCode=None, minimumNumberOfPoints=1, 
           limit=100, offset=0, public=None, status=None):
      """
      @summary: Performs a GET request.  If an occurrence set id is provided,
                   attempt to return that item.  If not, return a list of 
                   occurrence sets that match the provided parameters
      """
      if occSetId is None:
         return self._listOccurrenceSets(afterTime=afterTime, 
                beforeTime=beforeTime, displayName=displayName, 
                epsgCode=epsgCode, minimumNumberOfPoints=minimumNumberOfPoints, 
                limit=limit, offset=offset, public=public)
      else:
         return self._getOccurrenceSet(occSetId)
   
   # ................................
   @cherrypy.json_out
   def POST(self, displayName, epsgCode, additionalMetadata=None):
      """
      @summary: Posts a new occurrence set
      @param displayName: The display name for the new occurrence set
      @param epsgCode: The EPSG code for the new occurrence set
      @param additionalMetadata: Additional JSON metadata to add to this 
                                    occurrence set
      @todo: Add file type parameter or look at headers?
      @todo: User id
      """
      pass
   
   # ................................
   @cherrypy.json_out
   def PUT(self, occSetId, occSetModel):
      pass
   
   # ................................
   def _getOccurrenceSet(self, occSetId):
      """
      @summary: Attempt to get an occurrence set
      """
      occ = self.scribe.getOccurrenceSet(occid=int(occSetId))
      
      if occ is None:
         raise cherrypy.HTTPError(404, "Occurrence set not found")
      
      # If allowed to, delete
      if occ.getUserId() == self.userId:
         return occ
      else:
         raise cherrypy.HTTPError(403, 
                 "User does not have permission to delete this occurrence set")
   
   # ................................
   def _listOccurrenceSets(self, afterTime=None, beforeTime=None, 
                           displayName=None, epsgCode=None, 
                           minimumNumberOfPoints=1, limit=100, offset=0, 
                           public=None, status=None):
      """
      @summary: Return a list of occurrence sets matching the specified 
                   criteria
      """
      if public:
         queryUser = PUBLIC_USER
      else:
         queryUser = self.userId
         
      return self.scribe.listOccurrenceSets(offset, limit, userId=queryUser,
                     minOccurrenceCount=minimumNumberOfPoints, 
                     displayName=displayName, afterTime=afterTime, 
                     beforeTime=beforeTime, epsg=epsgCode, status=status)
   