#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for matrix columns
"""
from LmCommon.common.lmconstants import HTTPStatus, JobStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter
import cherrypy


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathMatrixColumnId')
class MatrixColumnService(LmService):
    """Matrix column service class.
    """

    # ................................
    def DELETE(self, pathGridSetId, pathMatrixId, pathMatrixColumnId):
        """Attempts to delete a matrix column
        """
        mtx_col = self.scribe.getMatrixColumn(mtxcolId=pathMatrixColumnId)

        if mtx_col is None:
            raise cherrypy.HTTPError(
                 HTTPStatus.NOT_FOUND, 'Matrix column not found')

        # If allowed to, delete
        if check_user_permission(
                self.get_user_id(), mtx_col, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(mtx_col)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return

            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Failed to delete matrix column')

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User does not have permission to delete this matrix')

    # ................................
    @lm_formatter
    def GET(self, pathGridSetId, pathMatrixId, pathMatrixColumnId=None,
            afterTime=None, beforeTime=None, epsgCode=None, ident=None,
            layerId=None, limit=100, offset=0, urlUser=None, squid=None,
            status=None, **params):
        """GET request.  Individual, count, or list
        """
        if pathMatrixColumnId is None:
            return self._list_matrix_columns(
                pathGridSetId, pathMatrixId, self.get_user_id(urlUser=urlUser),
                afterTime=afterTime, beforeTime=beforeTime, epsgCode=epsgCode,
                ident=ident, layerId=layerId, limit=limit, offset=offset,
                squid=squid, status=status)
        if pathMatrixColumnId.lower() == 'count':
            return self._count_matrix_columns(
                pathGridSetId, pathMatrixId, self.get_user_id(urlUser=urlUser),
                afterTime=afterTime, beforeTime=beforeTime, epsgCode=epsgCode,
                ident=ident, layerId=layerId, squid=squid, status=status)

        return self._get_matrix_column(
            pathGridSetId, pathMatrixId, pathMatrixColumnId)

    # ................................
    def _count_matrix_columns(self, pathGridSetId, pathMatrixId, userId,
                              afterTime=None, beforeTime=None, epsgCode=None,
                              ident=None, layerId=None, squid=None,
                              status=None):

        after_status = None
        before_status = None

        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE + 1
                after_status = JobStatus.COMPLETE - 1
            else:
                after_status = status - 1

        mtx_col_count = self.scribe.countMatrixColumns(
            userId=userId, squid=squid, ident=ident, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, afterStatus=after_status,
            beforeStatus=before_status, matrixId=pathMatrixId, layerId=layerId)
        return {'count': mtx_col_count}

    # ................................
    def _get_matrix_column(self, pathGridSetId, pathMatrixId,
                           pathMatrixColumnId):
        """Attempt to get a matrix column
        """
        mtx_col = self.scribe.getMatrixColumn(mtxcolId=pathMatrixColumnId)
        if mtx_col is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Matrix column {} was not found'.format(pathMatrixColumnId))
        if check_user_permission(self.get_user_id(), mtx_col, HTTPMethod.GET):
            return mtx_col

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access matrix {}'.format(
                self.get_user_id(), pathMatrixId))

    # ................................
    def _list_matrix_columns(self, pathGridSetId, pathMatrixId, userId,
                             afterTime=None, beforeTime=None, epsgCode=None,
                             ident=None, layerId=None, limit=100, offset=0,
                             squid=None, status=None):
        after_status = None
        before_status = None

        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE + 1
                after_status = JobStatus.COMPLETE - 1
            else:
                after_status = status - 1

        mtx_atoms = self.scribe.listMatrixColumns(
            offset, limit, userId=userId, squid=squid, ident=ident,
            afterTime=afterTime, beforeTime=beforeTime, epsg=epsgCode,
            afterStatus=after_status, beforeStatus=before_status,
            matrixId=pathMatrixId, layerId=layerId)
        return mtx_atoms
