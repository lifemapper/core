"""This module provides REST services for shapegrids"""
from flask import make_response, Response
from http import HTTPStatus
import werkzeug.exceptions as WEXC

from LmServer.legion.shapegrid import Shapegrid
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.flask_app.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.flask_tools.lm_format import lm_formatter


# .............................................................................
class ShapegridService(LmService):
    """Class for shapegrid service."""

    # ................................
    def delete_shapegrid(self, user_id, shapegrid_id):
        """Attempts to delete a shapegrid

        Args:
            shapegrid_id: The id of the shapegrid to delete
        """
        shapegrid = self.scribe.get_shapegrid(lyr_id=shapegrid_id)
        if shapegrid is None:
            raise WEXC.NotFound('Shapegrid not found')

        # If allowed to, delete
        if check_user_permission(user_id, shapegrid, HTTPMethod.DELETE):
            success = self.scribe.delete_object(shapegrid)
            if success:
                return Response(status=HTTPStatus.NO_CONTENT)

            # How can this happen?  Catch and respond appropriately
            raise WEXC.InternalServerError('Failed to delete shapegrid')

        # If request is not permitted, raise exception
        raise WEXC.Forbidden('User does not have permission to delete this shapegrid')

    # ................................
    @lm_formatter
    def post_shapegrid(
        self, user_id, name, epsg_code, cell_sides, cell_size, map_units, bbox, cutout, **params):
        """Posts a new shapegrid"""
        shapegrid = Shapegrid(name, user_id, epsg_code, cell_sides, cell_size,map_units, bbox)
        updated_shapegrid = self.scribe.find_or_insert_shapegrid(shapegrid, cutout=cutout)
        return updated_shapegrid

    # ................................
    def count_shapegrids(
        self, user_id, after_time=None, before_time=None, cell_sides=None, cell_size=None, epsg_code=None):
        """Count shapegrid objects matching the specified criteria

        Args:
            user_id (str): The user to count shapegrids for.  Note that this may not be the 
                same user logged into the system
            after_time (float): Return shapegrids modified after this time (Modified Julian Day)
            before_time (float): Return shapegrids modified before this time (Modified Julian Day)
            cell_sides (omt): Number of sides for shapegrid cells, 4 for square cells, 6 for hexagonal cells.
            cell_size (float): Size of cells in mapunits
            epsg_code (str): Return shapegrids with this EPSG code
        """
        shapegrid_count = self.scribe.count_shapegrids(
            user_id=user_id, cell_sides=cell_sides, cell_size=cell_size, after_time=after_time, before_time=before_time, epsg=epsg_code)
        return {'count': shapegrid_count}

    # ................................
    def get_shapegrid(self, user_id, shapegrid_id):
        """Return a shapegrid
        
        Args:
            user_id (str): The user to return a shapegrid for.  Note that this may not be the 
                same user logged into the system
            shapegrid_id (int): Database key for the shapegrid object to return
        """
        shapegrid = self.scribe.get_shapegrid(lyr_id=shapegrid_id)
        if shapegrid is None:
            raise WEXC.NotFound('Shapegrid {} was not found'.format(shapegrid_id))
        
        if check_user_permission(user_id, shapegrid, HTTPMethod.GET):
            return shapegrid
        else:
            raise WEXC.Forbidden('User {} does not have permission to access shapegrid {}'.format(
                user_id, shapegrid_id))

    # ................................
    def list_shapegrids(
        self, user_id, after_time=None, before_time=None, cell_sides=None, cell_size=None, epsg_code=None,
        limit=100, offset=0):
        """List shapegrid objects matching the specified criteria

        Args:
            user_id (str): The user to count shapegrids for.  Note that this may not be the 
                same user logged into the system
            after_time (float): Return shapegrids modified after this time (Modified Julian Day)
            before_time (float): Return shapegrids modified before this time (Modified Julian Day)
            cell_sides (omt): Number of sides for shapegrid cells, 4 for square cells, 6 for hexagonal cells.
            cell_size (float): Size of cells in mapunits
            epsg_code (str): Return shapegrids with this EPSG code
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
