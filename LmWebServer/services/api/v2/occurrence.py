#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Occurrence sets
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
@cherrypy.popargs('pathOccSetId')
class OccurrenceLayerService(LmService):
    """
    @summary: This class is for the occurrence sets service.  The dispatcher is
                     responsible for calling the correct method
    """
    # ................................
    def DELETE(self, pathOccSetId):
        """Attempts to delete an occurrence set

        Args:
            pathOccSetId (int): The id of the occurrence set to delete.
        """
        occ = self.scribe.getOccurrenceSet(occId=int(pathOccSetId))

        if occ is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Occurrence set not found')

        # If allowed to, delete
        if check_user_permission(self.get_user_id(), occ, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(occ)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return

            # If unsuccessful, fail
            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Failed to delete occurrence set')

        # If no permission to delete, raise HTTP 403
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User does not have permission to delete this occurrence set')

    # ................................
    @lm_formatter
    def GET(self, pathOccSetId=None, afterTime=None, beforeTime=None,
            displayName=None, epsgCode=None, minimumNumberOfPoints=1,
            limit=100, offset=0, urlUser=None, status=None, gridSetId=None,
            fillPoints=False, **params):
        """GET request.  Either an occurrence set or list of them.
        """
        if pathOccSetId is None:
            return self._list_occurrence_sets(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, displayName=displayName,
                epsgCode=epsgCode, minimumNumberOfPoints=minimumNumberOfPoints,
                limit=limit, offset=offset, gridSetId=gridSetId, status=status)

        if pathOccSetId.lower() == 'count':
            return self._count_occurrence_sets(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, displayName=displayName,
                epsgCode=epsgCode, minimumNumberOfPoints=minimumNumberOfPoints,
                gridSetId=gridSetId, status=status)

        if pathOccSetId.lower() == 'web':
            return self._list_web_occurrence_sets(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, displayName=displayName,
                epsgCode=epsgCode, minimumNumberOfPoints=minimumNumberOfPoints,
                limit=limit, offset=offset, gridSetId=gridSetId, status=status)

        # Fallback to just get an individual occurrence set
        return self._get_occurrence_set(pathOccSetId, fill_points=fillPoints)

    # ................................
    # @cherrypy.tools.json_out
    @lm_formatter
    def POST(self, **params):
        """Posts a new BOOM archive
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
            gridset.mod_time, epsg=gridset.epsgcode)

    # ................................
    def _count_occurrence_sets(self, userId, afterTime=None, beforeTime=None,
                               displayName=None, epsgCode=None,
                               minimumNumberOfPoints=1, status=None,
                               gridSetId=None):
        """Return a count of occurrence sets matching the specified criteria
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

        occ_count = self.scribe.countOccurrenceSets(
            userId=userId, minOccurrenceCount=minimumNumberOfPoints,
            displayName=displayName, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, beforeStatus=before_status,
            afterStatus=after_status, gridsetId=gridSetId)
        return {'count': occ_count}

    # ................................
    def _get_occurrence_set(self, pathOccSetId, fill_points=False):
        """Attempt to get an occurrence set
        """
        occ = self.scribe.getOccurrenceSet(occId=int(pathOccSetId))

        if occ is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Occurrence set not found')

        # If allowed to, return
        if check_user_permission(self.get_user_id(), occ, HTTPMethod.GET):
            if fill_points:
                occ.readShapefile()
            return occ

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to GET occurrence set'.format(
                self.get_user_id()))

    # ................................
    def _list_occurrence_sets(self, userId, afterTime=None, beforeTime=None,
                              displayName=None, epsgCode=None,
                              minimumNumberOfPoints=1, limit=100, offset=0,
                              status=None, gridSetId=None):
        """Return a list of occurrence sets matching the specified criteria
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

        occ_atoms = self.scribe.listOccurrenceSets(
            offset, limit, userId=userId,
            minOccurrenceCount=minimumNumberOfPoints, displayName=displayName,
            afterTime=afterTime, beforeTime=beforeTime, epsg=epsgCode,
            beforeStatus=before_status, afterStatus=after_status,
            gridsetId=gridSetId)
        return occ_atoms

    # ................................
    def _list_web_occurrence_sets(
            self, userId, afterTime=None, beforeTime=None, displayName=None,
            epsgCode=None, minimumNumberOfPoints=1, limit=100, offset=0,
            status=None, gridSetId=None):
        """Return a list of occurrence set web objects matching criteria
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

        occs = self.scribe.listOccurrenceSets(
            offset, limit, userId=userId,
            minOccurrenceCount=minimumNumberOfPoints, displayName=displayName,
            afterTime=afterTime, beforeTime=beforeTime, epsg=epsgCode,
            beforeStatus=before_status, afterStatus=after_status,
            gridsetId=gridSetId, atom=False)
        occ_objs = []
        for occ in occs:
            occ_objs.append(
                {
                    'id': occ.get_id(),
                    'metadata_url': occ.metadataUrl,
                    'name': occ.displayName,
                    'modification_time': occ.statusModTime,
                    'epsg': occ.epsgcode,
                    'status': occ.status,
                    'count': occ.queryCount
                }
            )
        return occ_objs
