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
import json
from mx.DateTime import gmt

from LmCommon.common.lmconstants import JobStatus, DEFAULT_POST_USER
from LmServer.base.atom import Atom
from LmServer.common.localconstants import PUBLIC_USER
#from LmServer.legion.occlayer import OccurrenceLayer
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.common.boomPost import BoomPoster
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathOccSetId')
class OccurrenceLayerService(LmService):
   """
   @summary: This class is for the occurrence sets service.  The dispatcher is
                responsible for calling the correct method
   """
   # ................................
   def DELETE(self, pathOccSetId):
      """
      @summary: Attempts to delete an occurrence set
      @param pathOccSetId: The id of the occurrence set to delete
      """
      occ = self.scribe.getOccurrenceSet(occId=int(pathOccSetId))
      
      if occ is None:
         raise cherrypy.HTTPError(404, "Occurrence set not found")
      
      # If allowed to, delete
      if checkUserPermission(self.getUserId(), occ, HTTPMethod.DELETE):
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
   @lmFormatter
   def GET(self, pathOccSetId=None, afterTime=None, beforeTime=None, 
           displayName=None, epsgCode=None, minimumNumberOfPoints=1, 
           limit=100, offset=0, public=None, status=None):
      """
      @summary: Performs a GET request.  If an occurrence set id is provided,
                   attempt to return that item.  If not, return a list of 
                   occurrence sets that match the provided parameters
      """
      if public:
         userId = PUBLIC_USER
      else:
         userId = self.getUserId()

      if pathOccSetId is None:
         return self._listOccurrenceSets(userId, afterTime=afterTime, 
                beforeTime=beforeTime, displayName=displayName, 
                epsgCode=epsgCode, minimumNumberOfPoints=minimumNumberOfPoints, 
                limit=limit, offset=offset)
      elif pathOccSetId.lower() == 'count':
         return self._countOccurrenceSets(userId, afterTime=afterTime, 
                beforeTime=beforeTime, displayName=displayName, 
                epsgCode=epsgCode, minimumNumberOfPoints=minimumNumberOfPoints)
      else:
         return self._getOccurrenceSet(pathOccSetId)
   
   # ................................
   #@cherrypy.tools.json_out
   @lmFormatter
   def POST(self):
      """
      @summary: Posts a new BOOM archive
      @todo: Do we want to enable single occurrence set posts still?
      """
      #projectionData = cherrypy.request.json
      projectionData = json.loads(cherrypy.request.body.read())
      
      if self.getUserId() == PUBLIC_USER:
         usr = self.scribe.findUser(DEFAULT_POST_USER)
      else:
         usr = self.scribe.findUser(self.getUserId())
      
      archiveName = '{}_{}'.format(usr.userid, gmt().mjd)
      
      bp = BoomPoster(usr.userid, usr.email, archiveName, projectionData)
      gridset = bp.initBoom()

      # TODO: What do we return?
      cherrypy.response.status = 202
      return Atom(gridset.getId(), gridset.name, gridset.metadataUrl, 
                  gridset.modTime, epsg=gridset.epsgcode)

   #@lmFormatter
   #def POST(self, displayName, epsgCode, squid=None, additionalMetadata=None):
   #   """
   #   @summary: Posts a new occurrence set
   #   @param displayName: The display name for the new occurrence set
   #   @param epsgCode: The EPSG code for the new occurrence set
   #   @param additionalMetadata: Additional JSON metadata to add to this 
   #                                 occurrence set
   #   """
   #   contentType = cherrypy.request.headers['Content-Type']
   #   #features = None
   #   
   #   # Get features
   #   if contentType == 'application/octet-stream':
   #      uploadType = 'shapefile'
   #   elif contentType == 'text/csv':
   #      uploadType = 'csv'
   #   #elif contentType == 'application/json':
   #   #   uploadType = None
   #   #   features = json.loads(cherrypy.request.body)
   #   
   #   occ = OccurrenceLayer(displayName, self.getUserId(), epsgCode, -1,
   #                         squid=squid, lyrMetadata=additionalMetadata)
   #   occ.readFromUploadedData(cherrypy.request.body, uploadType)
   #   #if features:
   #   #   occ.setFeatures(features, featureAttributes, featureCount)
   #   newOcc = self.scribe.findOrInsertOccurrenceSet(occ)
   #   
   #   return newOcc
   
   # ................................
   #@cherrypy.tools.json_out
   #def PUT(self, pathOccSetId, occSetModel):
   #   pass
   
   # ................................
   def _countOccurrenceSets(self, userId, afterTime=None, beforeTime=None, 
                           displayName=None, epsgCode=None, 
                           minimumNumberOfPoints=1, status=None):
      """
      @summary: Return a count of occurrence sets matching the specified 
                   criteria
      """
      afterStatus = None
      beforeStatus = None

      # Process status parameter
      if status:
         if status < JobStatus.COMPLETE:
            beforeStatus = JobStatus.COMPLETE - 1
         elif status == JobStatus.COMPLETE:
            beforeStatus = JobStatus.COMPLETE + 1
            afterStatus = JobStatus.COMPLETE - 1
         else:
            afterStatus = status - 1
      
      occCount = self.scribe.countOccurrenceSets(userId=userId,
                     minOccurrenceCount=minimumNumberOfPoints, 
                     displayName=displayName, afterTime=afterTime, 
                     beforeTime=beforeTime, epsg=epsgCode, 
                     beforeStatus=beforeStatus, afterStatus=afterStatus)
      return {'count' : occCount}

   # ................................
   def _getOccurrenceSet(self, pathOccSetId):
      """
      @summary: Attempt to get an occurrence set
      """
      occ = self.scribe.getOccurrenceSet(occId=int(pathOccSetId))
      
      if occ is None:
         raise cherrypy.HTTPError(404, "Occurrence set not found")
      
      # If allowed to, return
      if checkUserPermission(self.getUserId(), occ, HTTPMethod.GET):
         return occ
      else:
         raise cherrypy.HTTPError(403, 
               'User {} does not have permission to delete this occurrence set'.format(
                  self.getUserId()))
   
   # ................................
   def _listOccurrenceSets(self, userId, afterTime=None, beforeTime=None, 
                           displayName=None, epsgCode=None, 
                           minimumNumberOfPoints=1, limit=100, offset=0, 
                           status=None):
      """
      @summary: Return a list of occurrence sets matching the specified 
                   criteria
      """
      afterStatus = None
      beforeStatus = None
      
      # Process status parameter
      if status:
         if status < JobStatus.COMPLETE:
            beforeStatus = JobStatus.COMPLETE - 1
         elif status == JobStatus.COMPLETE:
            beforeStatus = JobStatus.COMPLETE + 1
            afterStatus = JobStatus.COMPLETE - 1
         else:
            afterStatus = status - 1
      
      occAtoms = self.scribe.listOccurrenceSets(offset, limit, userId=userId,
                     minOccurrenceCount=minimumNumberOfPoints, 
                     displayName=displayName, afterTime=afterTime, 
                     beforeTime=beforeTime, epsg=epsgCode, 
                     beforeStatus=beforeStatus, afterStatus=afterStatus)
      return occAtoms
   
   
