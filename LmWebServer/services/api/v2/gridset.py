"""
@summary: This module provides REST services for grid sets
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

from LmServer.base.atom import Atom
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.api.v2.matrix import MatrixService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.common.boomPost import BoomPoster
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathGridSetId')
class GridSetService(LmService):
   """
   @summary: This class is for the grid set service.  The dispatcher is 
                responsible for calling the correct method.
   """
   matrix = MatrixService()
   
   # ................................
   def DELETE(self, pathGridSetId):
      """
      @summary: Attempts to delete a grid set
      @param pathGridSetId: The id of the grid set to delete
      """
      gs = self.scribe.getGridset(gridsetId=pathGridSetId)

      if gs is None:
         raise cherrypy.HTTPError(404, "Grid set not found")
      
      # If allowed to, delete
      if checkUserPermission(self.getUserId(), gs, HTTPMethod.DELETE):
         success = self.scribe.deleteObject(gs)
         if success:
            cherrypy.response.status = 204
            return 
         else:
            # TODO: How can this happen?  Make sure we catch those cases and 
            #          respond appropriately.  We don't want 500 errors
            raise cherrypy.HTTPError(500, 
                        "Failed to delete grid set")
      else:
         raise cherrypy.HTTPError(403, 
                 "User does not have permission to delete this grid set")

   # ................................
   @lmFormatter
   def GET(self, pathGridSetId=None, afterTime=None, beforeTime=None, 
           epsgCode=None, limit=100, metaString=None, offset=0, urlUser=None, 
           shapegridId=None):
      """
      @summary: Performs a GET request.  If a grid set id is provided,
                   attempt to return that item.  If not, return a list of 
                   grid sets that match the provided parameters
      """
      if pathGridSetId is None:
         return self._listGridSets(self.getUserId(urlUser=urlUser), 
                                   afterTime=afterTime, beforeTime=beforeTime, 
                                   epsgCode=epsgCode, limit=limit, 
                                   metaString=metaString, offset=offset, 
                                   shapegridId=shapegridId)
      elif pathGridSetId.lower() == 'count':
         return self._countGridSets(self.getUserId(urlUser=urlUser), 
                                   afterTime=afterTime, beforeTime=beforeTime, 
                                   epsgCode=epsgCode, metaString=metaString, 
                                   shapegridId=shapegridId)
      else:
         return self._getGridSet(pathGridSetId)
      
   # ................................
   def POST(self):
      """
      @summary: Posts a new grid set
      """
      gridsetData = json.loads(cherrypy.request.body.read())
      
      usr = self.scribe.findUser(self.getUserId())
      
      archiveName = '{}_{}'.format(usr.userid, gmt().mjd)
      
      bp = BoomPoster(usr.userid, usr.email, archiveName, gridsetData)
      gridset = bp.initBoom()

      # TODO: What do we return?
      cherrypy.response.status = 202
      return Atom(gridset.getId(), gridset.name, gridset.metadataUrl, 
                  gridset.modTime, epsg=gridset.epsgcode)
      
   # ................................
   def _countGridSets(self, userId, afterTime=None, beforeTime=None, 
                        epsgCode=None, metaString=None, shapegridId=None):
      """
      @summary: Count GridSet objects matching the specified criteria
      @param userId: The user to count GridSets for.  Note that this may not 
                        be the same user logged into the system
      @param afterTime: (optional) Return GridSets modified after this time 
                           (Modified Julian Day)
      @param beforeTime: (optional) Return GridSets modified before this time 
                            (Modified Julian Day)
      @param epsgCode: (optional) Return GridSets with this EPSG code
      """
      gsCount = self.scribe.countGridsets(userId=userId, 
                              shpgrdLyrid=shapegridId, metastring=metaString,
                               afterTime=afterTime, beforeTime=beforeTime, 
                               epsg=epsgCode)
      # Format return
      # Set headers
      return {"count" : gsCount}

   # ................................
   def _getGridSet(self, pathGridSetId):
      """
      @summary: Attempt to get a GridSet
      """
      gs = self.scribe.getGridset(gridsetId=pathGridSetId, fillMatrices=True)
      if gs is None:
         raise cherrypy.HTTPError(404, 
                        'GridSet {} was not found'.format(pathGridSetId))
      if checkUserPermission(self.getUserId(), gs, HTTPMethod.GET):
         return gs
      else:
         raise cherrypy.HTTPError(403, 
              'User {} does not have permission to access GridSet {}'.format(
                     self.getUserId(), pathGridSetId))
   
   # ................................
   def _listGridSets(self, userId, afterTime=None, beforeTime=None, 
                              epsgCode=None, limit=100, metaString=None,
                              offset=0, shapegridId=None):
      """
      @summary: Count GridSet objects matching the specified criteria
      @param userId: The user to count GridSets for.  Note that this may not 
                        be the same user logged into the system
      @param afterTime: (optional) Return GridSets modified after this time 
                           (Modified Julian Day)
      @param beforeTime: (optional) Return GridSets modified before this time 
                            (Modified Julian Day)
      @param epsgCode: (optional) Return GridSets with this EPSG code
      @param limit: (optional) Return this number of GridSets, at most
      @param offset: (optional) Offset the returned GridSets by this number
      """
      gsAtoms = self.scribe.listGridsets(offset, limit, userId=userId, 
                               shpgrdLyrid=shapegridId, metastring=metaString,
                               afterTime=afterTime, beforeTime=beforeTime, 
                               epsg=epsgCode)

      return gsAtoms
