#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for matrices
"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus, JobStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.api.v2.matrix_column import MatrixColumnService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathMatrixId')
class MatrixService(LmService):
    """This class is reponsible for matrix services.
    """
    column = MatrixColumnService()

    # ................................
    def DELETE(self, pathGridSetId, pathMatrixId):
        """Attempts to delete a matrix

        Args:
            pathMatrixId: The id of the matrix to delete
        """
        mtx = self.scribe.getMatrix(mtxId=pathMatrixId)

        if mtx is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Matrix not found')

        # If allowed to, delete
        if check_user_permission(self.get_user_id(), mtx, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(mtx)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return

            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR, 'Failed to delete matrix')

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User does not have permission to delete this matrix')

    # ................................
    @lm_formatter
    def GET(self, pathGridSetId, pathMatrixId=None, afterTime=None,
            altPredCode=None, beforeTime=None, dateCode=None, epsgCode=None,
            gcmCode=None, keyword=None, limit=100, matrixType=None, offset=0,
            urlUser=None, status=None, **params):
        """GET a matrix object, list, or count
        """
        if pathMatrixId is None:
            return self._list_matrices(
                self.get_user_id(urlUser=urlUser), gridset_id=pathGridSetId,
                after_time=afterTime, alt_pred_code=altPredCode,
                before_time=beforeTime, date_code=dateCode, epsg_code=epsgCode,
                gcm_code=gcmCode, keyword=keyword, limit=limit,
                matrix_type=matrixType, offset=offset, status=status)

        if pathMatrixId.lower() == 'count':
            return self._count_matrices(
                self.get_user_id(urlUser=urlUser), gridset_id=pathGridSetId,
                after_time=afterTime, alt_pred_code=altPredCode,
                before_time=beforeTime, date_code=dateCode, epsg_code=epsgCode,
                gcm_code=gcmCode, keyword=keyword, matrix_type=matrixType,
                status=status)

        return self._get_matrix(pathGridSetId, pathMatrixId)

    # ................................
    def _count_matrices(self, user_id, gridset_id, after_time=None,
                        alt_pred_code=None, before_time=None, date_code=None,
                        epsg_code=None, gcm_code=None, keyword=None,
                        matrix_type=None, status=None):
        """Count matrix objects matching the specified criteria

        Args:
            userId: The user to count matrices for.  Note that this may not be
                the same user logged into the system
            afterTime: Return matrices modified after this time (Modified
                Julian Day)
            beforeTime: Return matrices modified before this time (Modified
                Julian Day)
            epsgCode: (optional) Return matrices with this EPSG code
        """
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

        mtx_count = self.scribe.countMatrices(
            userId=user_id, matrixType=matrix_type, gcmCode=gcm_code,
            altpredCode=alt_pred_code, dateCode=date_code, keyword=keyword,
            gridsetId=gridset_id, afterTime=after_time, beforeTime=before_time,
            epsg=epsg_code, afterStatus=after_status,
            beforeStatus=before_status)

        return {'count': mtx_count}

    # ................................
    def _get_matrix(self, path_gridset_id, path_matrix_id):
        """Attempt to get a matrix
        """
        mtx = self.scribe.getMatrix(
            gridsetId=path_gridset_id, mtxId=path_matrix_id)
        if mtx is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'matrix {} was not found'.format(path_matrix_id))
        if check_user_permission(self.get_user_id(), mtx, HTTPMethod.GET):
            return mtx

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access matrix {}'.format(
                self.get_user_id(), path_matrix_id))

    # ................................
    def _list_matrices(self, user_id, gridset_id, after_time=None,
                       alt_pred_code=None, before_time=None, date_code=None,
                       epsg_code=None, gcm_code=None, keyword=None, limit=100,
                       matrix_type=None, offset=0, status=None):
        """Count matrix objects matching the specified criteria

        Args:
            userId: The user to count matrices for.  Note that this may not be
                the same user logged into the system
            afterTime: Return matrices modified after this time (Modified
                Julian Day)
            beforeTime: Return matrices modified before this time (Modified
                Julian Day)
            epsgCode: Return matrices with this EPSG code
            limit: Return this number of matrices, at most
            offset: Offset the returned matrices by this number
        """
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

        mtx_atoms = self.scribe.listMatrices(
            offset, limit, userId=user_id, matrixType=matrix_type,
            gcmCode=gcm_code, altpredCode=alt_pred_code, dateCode=date_code,
            keyword=keyword, gridsetId=gridset_id, afterTime=after_time,
            beforeTime=before_time, epsg=epsg_code, afterStatus=after_status,
            beforeStatus=before_status)

        return mtx_atoms
