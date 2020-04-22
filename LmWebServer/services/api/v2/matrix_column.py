"""This module provides REST services for matrix columns"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus, JobStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('path_matrix_column_id')
class MatrixColumnService(LmService):
    """Matrix column service class.
    """

    # ................................
    def DELETE(self, path_gridset_id, path_matrix_id, path_matrix_column_id):
        """Attempts to delete a matrix column
        """
        mtx_col = self.scribe.get_matrix_column(
            mtx_col_id=path_matrix_column_id)

        if mtx_col is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Matrix column not found')

        # If allowed to, delete
        if check_user_permission(
                self.get_user_id(), mtx_col, HTTPMethod.DELETE):
            success = self.scribe.delete_object(mtx_col)
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
    def GET(self, path_gridset_id, path_matrix_id, path_matrix_column_id=None,
            after_time=None, before_time=None, epsg_code=None, ident=None,
            layer_id=None, limit=100, offset=0, url_user=None, squid=None,
            status=None, **params):
        """GET request.  Individual, count, or list
        """
        if path_matrix_column_id is None:
            return self._list_matrix_columns(
                path_gridset_id, path_matrix_id,
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, epsg_code=epsg_code, ident=ident,
                layer_id=layer_id, limit=limit, offset=offset, squid=squid,
                status=status)
        if path_matrix_column_id.lower() == 'count':
            return self._count_matrix_columns(
                path_gridset_id, path_matrix_id,
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, epsg_code=epsg_code, ident=ident,
                layer_id=layer_id, squid=squid, status=status)

        return self._get_matrix_column(
            path_gridset_id, path_matrix_id, path_matrix_column_id)

    # ................................
    def _count_matrix_columns(self, path_gridset_id, path_matrix_id, user_id,
                              after_time=None, before_time=None,
                              epsg_code=None, ident=None, layer_id=None,
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

        mtx_col_count = self.scribe.count_matrix_columns(
            user_id=user_id, squid=squid, ident=ident, after_time=after_time,
            before_time=before_time, epsg=epsg_code, after_status=after_status,
            before_status=before_status, matrix_id=path_matrix_id,
            layer_id=layer_id)
        return {'count': mtx_col_count}

    # ................................
    def _get_matrix_column(self, path_gridset_id, path_matrix_id,
                           path_matrix_column_id):
        """Attempt to get a matrix column
        """
        mtx_col = self.scribe.get_matrix_column(
            mtx_col_id=path_matrix_column_id)
        if mtx_col is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Matrix column {} was not found'.format(path_matrix_column_id))
        if check_user_permission(self.get_user_id(), mtx_col, HTTPMethod.GET):
            return mtx_col

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access matrix {}'.format(
                self.get_user_id(), path_matrix_id))

    # ................................
    def _list_matrix_columns(self, path_gridset_id, path_matrix_id, user_id,
                             after_time=None, before_time=None, epsg_code=None,
                             ident=None, layer_id=None, limit=100, offset=0,
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

        mtx_atoms = self.scribe.list_matrix_columns(
            offset, limit, user_id=user_id, squid=squid, ident=ident,
            after_time=after_time, before_time=before_time, epsg=epsg_code,
            after_status=after_status, before_status=before_status,
            matrix_id=path_matrix_id, layer_id=layer_id)
        return mtx_atoms
