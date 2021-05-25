"""This module provides REST services for shapegrids"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus
from LmServer.legion.shapegrid import Shapegrid
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('path_shapegrid_id')
class ShapegridService(LmService):
    """Class for shapegrid service.
    """

    # ................................
    def DELETE(self, path_shapegrid_id):
        """Attempts to delete a shapegrid

        Args:
            path_shapegrid_id: The id of the shapegrid to delete
        """
        shapegrid = self.scribe.get_shapegrid(lyr_id=path_shapegrid_id)
        if shapegrid is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Shapegrid not found')

        # If allowed to, delete
        if check_user_permission(
                self.get_user_id(), shapegrid, HTTPMethod.DELETE):
            success = self.scribe.delete_object(shapegrid)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return

            # How can this happen?  Make sure we catch those cases and
            #     respond appropriately.  We don't want 500 errors
            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR, 'Failed to delete shapegrid')

        # If request is not permitted, raise exception
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User does not have permission to delete this shapegrid')

    # ................................
    @lm_formatter
    def GET(self, path_shapegrid_id=None, after_time=None, before_time=None,
            cell_sides=None, cell_size=None, epsg_code=None, limit=100,
            offset=0, url_user=None, **params):
        """Perform a GET request, either list, count, or return individual.
        """
        if path_shapegrid_id is None:
            return self._list_shapegrids(
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, cell_sides=cell_sides,
                cell_size=cell_size, epsg_code=epsg_code, limit=limit,
                offset=offset)
        if path_shapegrid_id.lower() == 'count':
            return self._count_shapegrids(
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, cell_sides=cell_sides,
                cell_size=cell_size, epsg_code=epsg_code)

        # Fallback to return individual
        return self._get_shapegrid(path_shapegrid_id)

    # ................................
    @lm_formatter
    def POST(self, name, epsg_code, cell_sides, cell_size, map_units, bbox,
             cutout, **params):
        """Posts a new shapegrid
        """
        shapegrid = Shapegrid(
            name, self.get_user_id(), epsg_code, cell_sides, cell_size,
            map_units, bbox)
        updated_shapegrid = self.scribe.find_or_insert_shapegrid(
            shapegrid, cutout=cutout)
        return updated_shapegrid

    # ................................
    def _count_shapegrids(self, user_id, after_time=None, before_time=None,
                          cell_sides=None, cell_size=None, epsg_code=None):
        """Count shapegrid objects matching the specified criteria

        Args:
            user_id (str): The user to count shapegrids for.  Note that this
                may not be the same user logged into the system
            after_time: Return shapegrids modified after this time (Modified
                Julian Day)
            before_time: Return shapegrids modified before this time (Modified
                Julian Day)
            epsg_code: Return shapegrids with this EPSG code
        """
        shapegrid_count = self.scribe.count_shapegrids(
            user_id=user_id, cell_sides=cell_sides, cell_size=cell_size,
            after_time=after_time, before_time=before_time, epsg=epsg_code)
        # Format return
        # Set headers
        return {'count': shapegrid_count}

    # ................................
    def _get_shapegrid(self, path_shapegrid_id):
        """Attempt to get a shapegrid
        """
        shapegrid = self.scribe.get_shapegrid(lyr_id=path_shapegrid_id)
        if shapegrid is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Shapegrid {} was not found'.format(path_shapegrid_id))
        if check_user_permission(
                self.get_user_id(), shapegrid, HTTPMethod.GET):
            return shapegrid

        # HTTP 403 error if no permission to get
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access shapegrid {}'.format(
                self.get_user_id(), path_shapegrid_id))

    # ................................
    def _list_shapegrids(self, user_id, after_time=None, before_time=None,
                         cell_sides=None, cell_size=None, epsg_code=None,
                         limit=100, offset=0):
        """Count shapegrid objects matching the specified criteria

        Args:
            user_id: The user to count shapegrids for.  Note that this may not
                be the same user logged into the system
            after_time: Return shapegrids modified after this time (Modified
                Julian Day)
            before_time: Return shapegrids modified before this time (Modified
                Julian Day)
            epsg_code: Return shapegrids with this EPSG code
            limit: Return this number of shapegrids, at most
            offset: Offset the returned shapegrids by this number
        """
        shapegrid_atoms = self.scribe.list_shapegrids(
            offset, limit, user_id=user_id, cell_sides=cell_sides,
            cell_size=cell_size, after_time=after_time,
            before_time=before_time, epsg=epsg_code)
        # Format return
        # Set headers
        return shapegrid_atoms
