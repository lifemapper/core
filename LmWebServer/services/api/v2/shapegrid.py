"""
@summary: This module provides REST services for shapegrids
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

from LmServer.legion.shapegrid import ShapeGrid
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathShapegridId')
class ShapeGridService(LmService):
   """
   @summary: This class is for the shapegrid service.  The dispatcher is 
                responsible for calling the correct method.
   """
   # ................................
   def DELETE(self, pathShapegridId):
      """
      @summary: Attempts to delete a shapegrid
      @param pathShapegridId: The id of the shapegrid to delete
      """
      sg = self.scribe.getShapeGrid(lyrId=pathShapegridId)

      if sg is None:
         raise cherrypy.HTTPError(404, "Shapegrid not found")
      
      # If allowed to, delete
      if checkUserPermission(self.getUserId(), sg, HTTPMethod.DELETE):
         success = self.scribe.deleteObject(sg)
         if success:
            cherrypy.response.status = 204
            return 
         else:
            # TODO: How can this happen?  Make sure we catch those cases and 
            #          respond appropriately.  We don't want 500 errors
            raise cherrypy.HTTPError(500, 
                        "Failed to delete shapegrid")
      else:
         raise cherrypy.HTTPError(403, 
                 "User does not have permission to delete this shapegrid")

   # ................................
   @lmFormatter
   def GET(self, pathShapegridId=None, afterTime=None, beforeTime=None, 
           cellSides=None, cellSize=None, epsgCode=None, limit=100, offset=0, 
           urlUser=None):
      """
      @summary: Performs a GET request.  If a shapegrid id is provided,
                   attempt to return that item.  If not, return a list of 
                   shapegrids that match the provided parameters
      """
      if pathShapegridId is None:
         return self._listShapegrids(self.getUserId(urlUser=urlUser), 
                                     afterTime=afterTime, 
                                     beforeTime=beforeTime, cellSides=cellSides,
                                     cellSize=cellSize, epsgCode=epsgCode, 
                                     limit=limit, offset=offset)
      elif pathShapegridId.lower() == 'count':
         return self._countShapegrids(self.getUserId(urlUser=urlUser), 
                                      afterTime=afterTime, 
                                     beforeTime=beforeTime, cellSides=cellSides,
                                     cellSize=cellSize, epsgCode=epsgCode)
      else:
         return self._getShapegrid(pathShapegridId)
      
   # ................................
   @lmFormatter
   def POST(self, name, epsgCode, cellSides, cellSize, mapUnits, bbox, cutout):
      """
      @summary: Posts a new shapegrid
      @todo: Add cutout
      @todo: Take a completed shapegrid?
      """
      sg = ShapeGrid(name, self.getUserId(), epsgCode, cellSides, cellSize, 
                     mapUnits, bbox)
      updatedSg = self.scribe.findOrInsertShapeGrid(sg, cutout=cutout)
      return updatedSg
   
   # ................................
   def _countShapegrids(self, userId, afterTime=None, beforeTime=None, 
                        cellSides=None, cellSize=None, epsgCode=None):
      """
      @summary: Count shapegrid objects matching the specified criteria
      @param userId: The user to count shapegrids for.  Note that this may not 
                        be the same user logged into the system
      @param afterTime: (optional) Return shapegrids modified after this time 
                           (Modified Julian Day)
      @param beforeTime: (optional) Return shapegrids modified before this time 
                            (Modified Julian Day)
      @param epsgCode: (optional) Return shapegrids with this EPSG code
      """
      sgCount = self.scribe.countShapegrids(userId=userId, cellsides=cellSides,
                                            cellsize=cellSize, 
                                            afterTime=afterTime, 
                                            beforeTime=beforeTime, 
                                            epsg=epsgCode)
      # Format return
      # Set headers
      return {"count" : sgCount}

   # ................................
   def _getShapegrid(self, pathShapegridId):
      """
      @summary: Attempt to get a shapegrid
      """
      sg = self.scribe.getShapeGrid(lyrId=pathShapegridId)
      if sg is None:
         raise cherrypy.HTTPError(404, 
                        'Shapegrid {} was not found'.format(pathShapegridId))
      if checkUserPermission(self.getUserId(), sg, HTTPMethod.GET):
         return sg
      else:
         raise cherrypy.HTTPError(403, 
              'User {} does not have permission to access shapegrid {}'.format(
                     self.getUserId(), pathShapegridId))
   
   # ................................
   def _listShapegrids(self, userId, afterTime=None, beforeTime=None, 
                        cellSides=None, cellSize=None, epsgCode=None, 
                        limit=100, offset=0):
      """
      @summary: Count shapegrid objects matching the specified criteria
      @param userId: The user to count shapegrids for.  Note that this may not 
                        be the same user logged into the system
      @param afterTime: (optional) Return shapegrids modified after this time 
                           (Modified Julian Day)
      @param beforeTime: (optional) Return shapegrids modified before this time 
                            (Modified Julian Day)
      @param epsgCode: (optional) Return shapegrids with this EPSG code
      @param limit: (optional) Return this number of shapegrids, at most
      @param offset: (optional) Offset the returned shapegrids by this number
      """
      sgAtoms = self.scribe.listShapeGrids(offset, limit, userId=userId, 
                                    cellsides=cellSides, cellsize=cellSize,
                                    afterTime=afterTime, beforeTime=beforeTime, 
                                    epsg=epsgCode)
      # Format return
      # Set headers
      return sgAtoms
