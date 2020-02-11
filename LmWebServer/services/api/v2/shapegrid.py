#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for shapegrids
"""
from LmCommon.common.lmconstants import HTTPStatus
from LmServer.legion.shapegrid import ShapeGrid
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter
import cherrypy


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathShapegridId')
class ShapeGridService(LmService):
    """Class for shapegrid service.
    """
    # ................................
    def DELETE(self, pathShapegridId):
        """Attempts to delete a shapegrid

        Args:
            pathShapegridId: The id of the shapegrid to delete
        """
        shapegrid = self.scribe.getShapeGrid(lyrId=pathShapegridId)

        if shapegrid is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Shapegrid not found')

        # If allowed to, delete
        if check_user_permission(
                self.get_user_id(), shapegrid, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(shapegrid)
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
    def GET(self, pathShapegridId=None, afterTime=None, beforeTime=None,
            cellSides=None, cellSize=None, epsgCode=None, limit=100, offset=0,
            urlUser=None, **params):
        """Perform a GET request, either list, count, or return individual.
        """
        if pathShapegridId is None:
            return self._list_shapegrids(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, cellSides=cellSides, cellSize=cellSize,
                epsgCode=epsgCode, limit=limit, offset=offset)
        if pathShapegridId.lower() == 'count':
            return self._count_shapegrids(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, cellSides=cellSides, cellSize=cellSize,
                epsgCode=epsgCode)

        # Fallback to return individual
        return self._get_shapegrid(pathShapegridId)

    # ................................
    @lm_formatter
    def POST(self, name, epsgCode, cellSides, cellSize, mapUnits, bbox, cutout,
             **params):
        """Posts a new shapegrid
        """
        shapegrid = ShapeGrid(
            name, self.get_user_id(), epsgCode, cellSides, cellSize, mapUnits,
            bbox)
        updated_shapegrid = self.scribe.findOrInsertShapeGrid(
            shapegrid, cutout=cutout)
        return updated_shapegrid

    # ................................
    def _count_shapegrids(self, userId, afterTime=None, beforeTime=None,
                          cellSides=None, cellSize=None, epsgCode=None):
        """Count shapegrid objects matching the specified criteria

        Args:
            userId (str): The user to count shapegrids for.  Note that this may
                not be the same user logged into the system
            afterTime: Return shapegrids modified after this time (Modified
                Julian Day)
            beforeTime: Return shapegrids modified before this time (Modified
                Julian Day)
            epsgCode: Return shapegrids with this EPSG code
        """
        shapegrid_count = self.scribe.countShapeGrids(
            userId=userId, cellsides=cellSides, cellsize=cellSize,
            afterTime=afterTime, beforeTime=beforeTime, epsg=epsgCode)
        # Format return
        # Set headers
        return {'count': shapegrid_count}

    # ................................
    def _get_shapegrid(self, pathShapegridId):
        """Attempt to get a shapegrid
        """
        shapegrid = self.scribe.getShapeGrid(lyrId=pathShapegridId)
        if shapegrid is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Shapegrid {} was not found'.format(pathShapegridId))
        if check_user_permission(
                self.get_user_id(), shapegrid, HTTPMethod.GET):
            return shapegrid

        # HTTP 403 error if no permission to get
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access shapegrid {}'.format(
                self.get_user_id(), pathShapegridId))

    # ................................
    def _list_shapegrids(self, userId, afterTime=None, beforeTime=None,
                         cellSides=None, cellSize=None, epsgCode=None,
                         limit=100, offset=0):
        """Count shapegrid objects matching the specified criteria

        Args:
            userId: The user to count shapegrids for.  Note that this may not
                be the same user logged into the system
            afterTime: Return shapegrids modified after this time (Modified
                Julian Day)
            beforeTime: Return shapegrids modified before this time (Modified
                Julian Day)
            epsgCode: Return shapegrids with this EPSG code
            limit: Return this number of shapegrids, at most
            offset: Offset the returned shapegrids by this number
        """
        shapegrid_atoms = self.scribe.listShapeGrids(
            offset, limit, userId=userId, cellsides=cellSides,
            cellsize=cellSize, afterTime=afterTime, beforeTime=beforeTime,
            epsg=epsgCode)
        # Format return
        # Set headers
        return shapegrid_atoms
