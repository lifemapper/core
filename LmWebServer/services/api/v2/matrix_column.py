#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for matrix columns
"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus, JobStatus
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.legion.mtxcolumn import MatrixColumn
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter

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
            raise cherrypy.HTTPError(
                 HTTPStatus.NOT_FOUND, 'Matrix column not found')
        
        # If allowed to, delete
        if check_user_permission(self.get_user_id(), mc, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(mc)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return 
            else:
                # TODO: How can this happen?  Make sure we catch those cases and 
                #             respond appropriately.  We don't want 500 errors
                raise cherrypy.HTTPError(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    'Failed to delete matrix column')
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User does not have permission to delete this matrix')

    # ................................
    @lm_formatter
    def GET(self, pathGridSetId, pathMatrixId, pathMatrixColumnId=None,
            afterTime=None, beforeTime=None, epsgCode=None, ident=None,
            layerId=None, limit=100, offset=0, urlUser=None, squid=None,
            status=None, **params):
        """
        @summary: Performs a GET request.  If a matrix id is provided,
                         attempt to return that item.  If not, return a list of 
                         matrices that match the provided parameters
        """
        if pathMatrixColumnId is None:
            return self._listMatrixColumns(
                pathGridSetId, pathMatrixId, self.get_user_id(urlUser=urlUser),
                afterTime=afterTime, beforeTime=beforeTime, epsgCode=epsgCode,
                ident=ident, layerId=layerId, limit=limit, offset=offset,
                squid=squid, status=status)
        elif pathMatrixColumnId.lower() == 'count':
            return self._countMatrixColumns(
                pathGridSetId, pathMatrixId, self.get_user_id(urlUser=urlUser),
                afterTime=afterTime, beforeTime=beforeTime, epsgCode=epsgCode,
                ident=ident, layerId=layerId, squid=squid, status=status)
        else:
            return self._getMatrixColumn(
                pathGridSetId, pathMatrixId, pathMatrixColumnId)
        
    # ................................
    @lm_formatter
    def POST(self, name, epsgCode, cellSides, cellSize, mapUnits, bbox, cutout,
             **params):
        """
        @summary: Posts a new layer
        @todo: Add cutout
        @todo: Take a completed matrix?
        """
        sg = MatrixColumn(
            name, self.get_user_id(), epsgCode, cellSides, cellSize, mapUnits,
            bbox)
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

        mtxColCount = self.scribe.countMatrixColumns(
            userId=userId, squid=squid, ident=ident, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, afterStatus=afterStatus,
            beforeStatus=beforeStatus, matrixId=pathMatrixId, layerId=layerId)
        return {'count' : mtxColCount}

    # ................................
    def _getMatrixColumn(self, pathGridSetId, pathMatrixId, pathMatrixColumnId):
        """
        @summary: Attempt to get a matrix column
        """
        mtxCol = self.scribe.getMatrixColumn(mtxcolId=pathMatrixColumnId)
        if mtxCol is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Matrix column {} was not found'.format(pathMatrixColumnId))
        if check_user_permission(self.get_user_id(), mtxCol, HTTPMethod.GET):
            return mtxCol
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User {} does not have permission to access matrix {}'.format(
                    self.get_user_id(), pathMatrixId))

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

        mtxAtoms = self.scribe.listMatrixColumns(
            offset, limit, userId=userId, squid=squid, ident=ident,
            afterTime=afterTime, beforeTime=beforeTime, epsg=epsgCode,
            afterStatus=afterStatus, beforeStatus=beforeStatus,
            matrixId=pathMatrixId, layerId=layerId)
        return mtxAtoms

