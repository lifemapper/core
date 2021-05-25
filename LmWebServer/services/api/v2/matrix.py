"""This module provides REST services for matrices"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus, JobStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.api.v2.matrix_column import MatrixColumnService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('path_matrix_id')
class MatrixService(LmService):
    """This class is reponsible for matrix services.
    """
    column = MatrixColumnService()

    # ................................
    def DELETE(self, path_gridset_id, path_matrix_id):
        """Attempts to delete a matrix

        Args:
            path_matrix_id: The id of the matrix to delete
        """
        mtx = self.scribe.get_matrix(mtx_id=path_matrix_id)

        if mtx is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Matrix not found')

        # If allowed to, delete
        if check_user_permission(self.get_user_id(), mtx, HTTPMethod.DELETE):
            success = self.scribe.delete_object(mtx)
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
    def GET(self, path_gridset_id, path_matrix_id=None, after_time=None,
            alt_pred_code=None, before_time=None, date_code=None,
            epsg_code=None, gcm_code=None, keyword=None, limit=100,
            matrix_type=None, offset=0, url_user=None, status=None, **params):
        """GET a matrix object, list, or count
        """
        if path_matrix_id is None:
            return self._list_matrices(
                self.get_user_id(url_user=url_user),
                gridset_id=path_gridset_id, after_time=after_time,
                alt_pred_code=alt_pred_code, before_time=before_time,
                date_code=date_code, epsg_code=epsg_code, gcm_code=gcm_code,
                keyword=keyword, limit=limit, matrix_type=matrix_type,
                offset=offset, status=status)

        if path_matrix_id.lower() == 'count':
            return self._count_matrices(
                self.get_user_id(url_user=url_user),
                gridset_id=path_gridset_id, after_time=after_time,
                alt_pred_code=alt_pred_code, before_time=before_time,
                date_code=date_code, epsg_code=epsg_code, gcm_code=gcm_code,
                keyword=keyword, matrix_type=matrix_type, status=status)

        return self._get_matrix(path_gridset_id, path_matrix_id)

    # ................................
    def _count_matrices(self, user_id, gridset_id, after_time=None,
                        alt_pred_code=None, before_time=None, date_code=None,
                        epsg_code=None, gcm_code=None, keyword=None,
                        matrix_type=None, status=None):
        """Count matrix objects matching the specified criteria

        Args:
            user_id: The user to count matrices for.  Note that this may not be
                the same user logged into the system
            after_time: Return matrices modified after this time (Modified
                Julian Day)
            before_time: Return matrices modified before this time (Modified
                Julian Day)
            epsg_code: (optional) Return matrices with this EPSG code
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

        mtx_count = self.scribe.count_matrices(
            user_id=user_id, matrix_type=matrix_type, gcm_code=gcm_code,
            alt_pred_code=alt_pred_code, date_code=date_code, keyword=keyword,
            gridset_id=gridset_id, after_time=after_time,
            before_time=before_time, epsg=epsg_code, after_status=after_status,
            before_status=before_status)

        return {'count': mtx_count}

    # ................................
    def _get_matrix(self, path_gridset_id, path_matrix_id):
        """Attempt to get a matrix
        """
        mtx = self.scribe.get_matrix(
            gridset_id=path_gridset_id, mtx_id=path_matrix_id)
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
            user_id: The user to count matrices for.  Note that this may not be
                the same user logged into the system
            after_time: Return matrices modified after this time (Modified
                Julian Day)
            before_time: Return matrices modified before this time (Modified
                Julian Day)
            epsg_code: Return matrices with this EPSG code
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

        mtx_atoms = self.scribe.list_matrices(
            offset, limit, user_id=user_id, matrix_type=matrix_type,
            gcm_code=gcm_code, alt_pred_code=alt_pred_code,
            date_code=date_code, keyword=keyword, gridset_id=gridset_id,
            after_time=after_time, before_time=before_time, epsg=epsg_code,
            after_status=after_status, before_status=before_status)

        return mtx_atoms
