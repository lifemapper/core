#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for shapegrids
"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus
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
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Shapegrid not found')
        
        # If allowed to, delete
        if checkUserPermission(self.getUserId(), sg, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(sg)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return 
            else:
                # TODO: How can this happen?  Make sure we catch those cases and 
                #             respond appropriately.  We don't want 500 errors
                raise cherrypy.HTTPError(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    'Failed to delete shapegrid')
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User does not have permission to delete this shapegrid')

    # ................................
    @lmFormatter
    def GET(self, pathShapegridId=None, afterTime=None, beforeTime=None,
            cellSides=None, cellSize=None, epsgCode=None, limit=100, offset=0,
            urlUser=None, **params):
        """
        @summary: Performs a GET request.  If a shapegrid id is provided,
                         attempt to return that item.  If not, return a list of 
                         shapegrids that match the provided parameters
        """
        if pathShapegridId is None:
            return self._listShapegrids(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, cellSides=cellSides, cellSize=cellSize,
                epsgCode=epsgCode, limit=limit, offset=offset)
        elif pathShapegridId.lower() == 'count':
            return self._countShapegrids(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, cellSides=cellSides, cellSize=cellSize,
                epsgCode=epsgCode)
        else:
            return self._getShapegrid(pathShapegridId)
        
    # ................................
    @lmFormatter
    def POST(self, name, epsgCode, cellSides, cellSize, mapUnits, bbox, cutout,
             **params):
        """
        @summary: Posts a new shapegrid
        @todo: Add cutout
        @todo: Take a completed shapegrid?
        """
        sg = ShapeGrid(
            name, self.getUserId(), epsgCode, cellSides, cellSize, mapUnits,
            bbox)
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
        sgCount = self.scribe.countShapeGrids(
            userId=userId, cellsides=cellSides, cellsize=cellSize,
            afterTime=afterTime, beforeTime=beforeTime, epsg=epsgCode)
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
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Shapegrid {} was not found'.format(pathShapegridId))
        if checkUserPermission(self.getUserId(), sg, HTTPMethod.GET):
            return sg
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
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
        sgAtoms = self.scribe.listShapeGrids(
            offset, limit, userId=userId, cellsides=cellSides,
            cellsize=cellSize, afterTime=afterTime, beforeTime=beforeTime,
            epsg=epsgCode)
        # Format return
        # Set headers
        return sgAtoms
