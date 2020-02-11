#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Projections
"""
import json

import cherrypy

from LmCommon.common.lmconstants import (
    DEFAULT_POST_USER, HTTPStatus, JobStatus)
from LmServer.base.atom import Atom
from LmServer.common.localconstants import PUBLIC_USER
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.common.boom_post import BoomPoster
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathProjectionId')
class SdmProjectService(LmService):
    """Class responsible for SDM Projection services
    """
    # ................................
    def DELETE(self, pathProjectionId):
        """Attempts to delete a projection

        Args:
            pathProjectionId: The id of the projection to delete
        """
        prj = self.scribe.getSDMProject(int(pathProjectionId))

        if prj is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Projection {} not found'.format(
                    pathProjectionId))

        if check_user_permission(self.get_user_id(), prj, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(prj)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return

            # If we have permission but cannot delete, error
            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Failed to delete projection {}'.format(pathProjectionId))

        # HTTP 403 if no permission to delete
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to delete projection {}'.format(
                self.get_user_id(), pathProjectionId))

    # ................................
    @lm_formatter
    def GET(self, pathProjectionId=None, afterStatus=None, afterTime=None,
            algorithmCode=None, beforeStatus=None, beforeTime=None,
            displayName=None, epsgCode=None, limit=100, modelScenarioCode=None,
            occurrenceSetId=None, offset=0, projectionScenarioCode=None,
            urlUser=None, scenarioId=None, status=None, gridSetId=None,
            **params):
        """Perform a GET request. List, count, or get individual projection.
        """
        if pathProjectionId is None:
            return self._list_projections(
                self.get_user_id(urlUser=urlUser), afterStatus=afterStatus,
                afterTime=afterTime, algCode=algorithmCode,
                beforeStatus=beforeStatus, beforeTime=beforeTime,
                displayName=displayName, epsgCode=epsgCode, limit=limit,
                mdlScnCode=modelScenarioCode, occurrenceSetId=occurrenceSetId,
                offset=offset, prjScnCode=projectionScenarioCode,
                status=status, gridSetId=gridSetId)

        if pathProjectionId.lower() == 'count':
            return self._count_projections(
                self.get_user_id(urlUser=urlUser), afterStatus=afterStatus,
                afterTime=afterTime, algCode=algorithmCode,
                beforeStatus=beforeStatus, beforeTime=beforeTime,
                displayName=displayName, epsgCode=epsgCode,
                mdlScnCode=modelScenarioCode, occurrenceSetId=occurrenceSetId,
                prjScnCode=projectionScenarioCode, status=status,
                gridSetId=gridSetId)

        # Get individual as fall back
        return self._get_projection(pathProjectionId)

    # ................................
    @lm_formatter
    def POST(self, **params):
        """Posts a new projection
        """
        projection_data = json.loads(cherrypy.request.body.read())

        if self.get_user_id() == PUBLIC_USER:
            usr = self.scribe.findUser(DEFAULT_POST_USER)
        else:
            usr = self.scribe.findUser(self.get_user_id())

        boom_post = BoomPoster(
            usr.userid, usr.email, projection_data, self.scribe)
        gridset = boom_post.init_boom()

        cherrypy.response.status = HTTPStatus.ACCEPTED
        return Atom(
            gridset.get_id(), gridset.name, gridset.metadataUrl,
            gridset.modTime, epsg=gridset.epsgcode)

    # ................................
    def _count_projections(self, userId, afterStatus=None, afterTime=None,
                           algCode=None, beforeStatus=None, beforeTime=None,
                           displayName=None, epsgCode=None, mdlScnCode=None,
                           occurrenceSetId=None, prjScnCode=None, status=None,
                           gridSetId=None):
        """Return a count of projections matching the specified criteria
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

        prj_count = self.scribe.countSDMProjects(
            userId=userId, displayName=displayName, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, afterStatus=afterStatus,
            beforeStatus=beforeStatus, occsetId=occurrenceSetId,
            algCode=algCode, mdlscenCode=mdlScnCode, prjscenCode=prjScnCode,
            gridsetId=gridSetId)
        return {'count': prj_count}

    # ................................
    def _get_projection(self, pathProjectionId):
        """Attempt to get a projection
        """
        prj = self.scribe.getSDMProject(int(pathProjectionId))

        if prj is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Projection {} not found'.format(pathProjectionId))

        if check_user_permission(self.get_user_id(), prj, HTTPMethod.GET):
            return prj

        # If no permission, HTTP 403
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to delete projection {}'.format(
                self.get_user_id(), pathProjectionId))

    # ................................
    def _list_projections(self, userId, afterStatus=None, afterTime=None,
                          algCode=None, beforeStatus=None, beforeTime=None,
                          displayName=None, epsgCode=None, limit=100,
                          mdlScnCode=None, occurrenceSetId=None, offset=0,
                          prjScnCode=None, status=None, gridSetId=None):
        """Return a list of projections matching the specified criteria
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

        prj_atoms = self.scribe.listSDMProjects(
            offset, limit, userId=userId, displayName=displayName,
            afterTime=afterTime, beforeTime=beforeTime, epsg=epsgCode,
            afterStatus=afterStatus, beforeStatus=beforeStatus,
            occsetId=occurrenceSetId, algCode=algCode, mdlscenCode=mdlScnCode,
            prjscenCode=prjScnCode, gridsetId=gridSetId)
        return prj_atoms
