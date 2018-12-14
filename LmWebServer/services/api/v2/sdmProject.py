#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Projections
"""
import cherrypy
import json

from LmCommon.common.lmconstants import (
    DEFAULT_POST_USER, HTTPStatus, JobStatus)
from LmServer.base.atom import Atom
from LmServer.common.localconstants import PUBLIC_USER
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.common.boomPost import BoomPoster
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathProjectionId')
class SdmProjectService(LmService):
    """Class responsible for SDM Projection services
    """
    # ................................
    def DELETE(self, pathProjectionId):
        """
        @summary: Attempts to delete a projection
        @param pathProjectionId: The id of the projection to delete
        """
        prj = self.scribe.getSDMProject(int(pathProjectionId))
        
        if prj is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Projection {} not found'.format(
                    pathProjectionId))
        
        if checkUserPermission(self.getUserId(), prj, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(prj)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return
            else:
                raise cherrypy.HTTPError(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    'Failed to delete projection {}'.format(pathProjectionId))
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN, 
                'User {} does not have permission to delete projection {}'.format(
                    self.getUserId(), pathProjectionId))

    # ................................
    @lmFormatter
    def GET(self, pathProjectionId=None, afterStatus=None, afterTime=None,
            algorithmCode=None, beforeStatus=None, beforeTime=None,
            displayName=None, epsgCode=None, limit=100, modelScenarioCode=None,
            occurrenceSetId=None, offset=0, projectionScenarioCode=None,
            urlUser=None, scenarioId=None, status=None, gridSetId=None,
            **params):
        """
        @summary: Performs a GET request.  If a projection id is provided,
                         attempt to return that item.  If not, return a list of 
                         projections that match the provided parameters
        """
        if pathProjectionId is None:
            return self._listProjections(
                self.getUserId(urlUser=urlUser), afterStatus=afterStatus,
                afterTime=afterTime, algCode=algorithmCode,
                beforeStatus=beforeStatus, beforeTime=beforeTime,
                displayName=displayName, epsgCode=epsgCode, limit=limit,
                mdlScnCode=modelScenarioCode, occurrenceSetId=occurrenceSetId,
                offset=offset, prjScnCode=projectionScenarioCode,
                status=status, gridSetId=gridSetId)
        elif pathProjectionId.lower() == 'count':
            return self._countProjections(
                self.getUserId(urlUser=urlUser), afterStatus=afterStatus,
                afterTime=afterTime, algCode=algorithmCode,
                beforeStatus=beforeStatus, beforeTime=beforeTime,
                displayName=displayName, epsgCode=epsgCode,
                mdlScnCode=modelScenarioCode, occurrenceSetId=occurrenceSetId,
                prjScnCode=projectionScenarioCode, status=status,
                gridSetId=gridSetId)
        else:
            return self._getProjection(pathProjectionId)
    
    # ................................
    #@cherrypy.tools.json_in
    #@cherrypy.tools.json_out
    @lmFormatter
    def POST(self, **params):
        """
        @summary: Posts a new projection
        """
        #projectionData = cherrypy.request.json
        projectionData = json.loads(cherrypy.request.body.read())
        
        if self.getUserId() == PUBLIC_USER:
            usr = self.scribe.findUser(DEFAULT_POST_USER)
        else:
            usr = self.scribe.findUser(self.getUserId())
        
        bp = BoomPoster(usr.userid, usr.email, projectionData, self.scribe)
        gridset = bp.init_boom()

        # TODO: What do we return?
        cherrypy.response.status = HTTPStatus.ACCEPTED
        return Atom(gridset.getId(), gridset.name, gridset.metadataUrl, 
                        gridset.modTime, epsg=gridset.epsgcode)
    
    # ................................
    #@cherrypy.json_in
    #@cherrypy.json_out
    #def PUT(self, pathProjectionId):
    #    pass
    
    # ................................
    def _countProjections(self, userId, afterStatus=None, afterTime=None,
                          algCode=None, beforeStatus=None, beforeTime=None,
                          displayName=None, epsgCode=None, mdlScnCode=None,
                          occurrenceSetId=None, prjScnCode=None, status=None,
                          gridSetId=None):
        """
        @summary: Return a count of projections matching the specified criteria
        """

        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE + 1
                afterStatus = JobStatus.COMPLETE - 1
            else:
                afterStatus = status - 1
    
        prjCount = self.scribe.countSDMProjects(
            userId=userId, displayName=displayName, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, afterStatus=afterStatus,
            beforeStatus=beforeStatus, occsetId=occurrenceSetId,
            algCode=algCode, mdlscenCode=mdlScnCode, prjscenCode=prjScnCode,
            gridsetId=gridSetId)
        return {"count" : prjCount}

    # ................................
    def _getProjection(self, pathProjectionId):
        """
        @summary: Attempt to get a projection
        """
        prj = self.scribe.getSDMProject(int(pathProjectionId))
        
        if prj is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Projection {} not found'.format(pathProjectionId))
        
        if checkUserPermission(self.getUserId(), prj, HTTPMethod.GET):
            return prj
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN, 
                'User {} does not have permission to delete projection {}'.format(
                    self.getUserId(), pathProjectionId))

    # ................................
    def _listProjections(self, userId, afterStatus=None, afterTime=None,
                         algCode=None, beforeStatus=None, beforeTime=None,
                         displayName=None, epsgCode=None, limit=100,
                         mdlScnCode=None, occurrenceSetId=None, offset=0,
                         prjScnCode=None, status=None, gridSetId=None):
        """
        @summary: Return a list of projections matching the specified criteria
        """
        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE + 1
                afterStatus = JobStatus.COMPLETE - 1
            else:
                afterStatus = status - 1
    
        prjAtoms = self.scribe.listSDMProjects(
            offset, limit, userId=userId, displayName=displayName,
            afterTime=afterTime, beforeTime=beforeTime, epsg=epsgCode,
            afterStatus=afterStatus, beforeStatus=beforeStatus,
            occsetId=occurrenceSetId, algCode=algCode, mdlscenCode=mdlScnCode,
            prjscenCode=prjScnCode, gridsetId=gridSetId)
        return prjAtoms
