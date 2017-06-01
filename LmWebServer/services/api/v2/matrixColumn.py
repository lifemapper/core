"""
@summary: This module provides REST services for matrix columns
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

from LmCommon.common.lmconstants import JobStatus
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.legion.mtxcolumn import MatrixColumn
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathMatrixColumnId')
class MatrixColumnService(LmService):
   """
   @summary: This class is for the matrix column service.  The dispatcher is 
                responsible for calling the correct method.
   """
   # ................................
   def DELETE(self, pathGridSetId, pathMatrixId, pathMatrixColumnId):
      """
      @summary: Attempts to delete a matrix column
      """
      mc = self.scribe.getMatrixColumn(mtxcolId=pathMatrixColumnId)

      if mc is None:
         raise cherrypy.HTTPError(404, "Matrix column not found")
      
      # If allowed to, delete
      if checkUserPermission(self.getUserId(), mc, HTTPMethod.DELETE):
         success = self.scribe.deleteObject(mc)
         if success:
            cherrypy.response.status = 204
            return 
         else:
            # TODO: How can this happen?  Make sure we catch those cases and 
            #          respond appropriately.  We don't want 500 errors
            raise cherrypy.HTTPError(500, 
                        "Failed to delete matrix column")
      else:
         raise cherrypy.HTTPError(403, 
                 "User does not have permission to delete this matrix")

   # ................................
   @lmFormatter
   def GET(self, pathGridSetId, pathMatrixId, pathMatrixColumnId=None, 
           afterTime=None, beforeTime=None, epsgCode=None, ident=None, 
           layerId=None, limit=100, offset=0, public=None, squid=None, 
           status=None):
      """
      @summary: Performs a GET request.  If a matrix id is provided,
                   attempt to return that item.  If not, return a list of 
                   matrices that match the provided parameters
      """
      if public:
         userId = PUBLIC_USER
      else:
         userId = self.getUserId()
      
      if pathMatrixColumnId is None:
         return self._listMatrixColumns(pathGridSetId, pathMatrixId, userId, 
                     afterTime=afterTime, beforeTime=beforeTime, 
                     epsgCode=epsgCode, ident=ident, layerId=layerId, 
                     limit=limit, offset=offset, squid=squid, status=status)
      elif pathMatrixColumnId.lower() == 'count':
         return self._countMatrixColumns(pathGridSetId, pathMatrixId, userId, 
                     afterTime=afterTime, beforeTime=beforeTime, 
                     epsgCode=epsgCode, ident=ident, layerId=layerId, 
                     squid=squid, status=status)
      else:
         return self._getMatrixColumn(pathGridSetId, pathMatrixId, 
                                      pathMatrixColumnId)
      
   # ................................
   @lmFormatter
   def POST(self, name, epsgCode, cellSides, cellSize, mapUnits, bbox, cutout):
      """
      @summary: Posts a new layer
      @todo: Add cutout
      @todo: Take a completed matrix?
      """
      sg = MatrixColumn(name, self.getUserId(), epsgCode, cellSides, cellSize, 
                     mapUnits, bbox)
      updatedSg = self.scribe.findOrInsertmatrix(sg, cutout=cutout)
      return updatedSg
   
   # ................................
   def _countMatrixColumns(self, pathGridSetId, pathMatrixId, userId, 
                     afterTime=None, beforeTime=None, epsgCode=None, 
                     ident=None, layerId=None, squid=None, status=None):

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

      mtxColCount = self.scribe.countMatrixColumns(userId=userId, squid=squid,
                       ident=ident, afterTime=afterTime, beforeTime=beforeTime, 
                       epsg=epsgCode, afterStatus=afterStatus, 
                       beforeStatus=beforeStatus, matrixId=pathMatrixId, 
                       layerId=layerId)
      return {'count' : mtxColCount}

   # ................................
   def _getMatrixColumn(self, pathGridSetId, pathMatrixId, pathMatrixColumnId):
      """
      @summary: Attempt to get a matrix column
      """
      mtxCol = self.scribe.getMatrixColumn(mtxcolId=pathMatrixColumnId)
      if mtxCol is None:
         raise cherrypy.HTTPError(404, 
                        'matrix column {} was not found'.format(pathMatrixColumnId))
      if checkUserPermission(self.getUserId(), mtxCol, HTTPMethod.GET):
         return mtxCol
      else:
         raise cherrypy.HTTPError(403, 
              'User {} does not have permission to access matrix {}'.format(
                     self.getUserId(), pathMatrixId))

   # ................................
   def _listMatrixColumns(self, pathGridSetId, pathMatrixId, userId, 
                     afterTime=None, beforeTime=None, epsgCode=None, 
                     ident=None, layerId=None, limit=100, offset=0, 
                     squid=None, status=None):
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

      mtxAtoms = self.scribe.listMatrixColumns(offset, limit, userId=userId, 
                           squid=squid, ident=ident, afterTime=afterTime, 
                           beforeTime=beforeTime, epsg=epsgCode, 
                           afterStatus=afterStatus, beforeStatus=beforeStatus, 
                           matrixId=pathMatrixId, layerId=layerId)
      return mtxAtoms

