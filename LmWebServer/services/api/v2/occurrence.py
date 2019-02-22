#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Occurrence sets
"""
import cherrypy
import json

from LmCommon.common.lmconstants import (
    DEFAULT_POST_USER, HTTPStatus, JobStatus)
from LmServer.base.atom import Atom
from LmServer.common.localconstants import PUBLIC_USER
#from LmServer.legion.occlayer import OccurrenceLayer
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.common.boomPost import BoomPoster
from LmWebServer.services.cpTools.lmFormat import lmFormatter

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
        """
        @summary: Attempts to delete an occurrence set
        @param pathOccSetId: The id of the occurrence set to delete
        """
        occ = self.scribe.getOccurrenceSet(occId=int(pathOccSetId))
        
        if occ is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Occurrence set not found')
        
        # If allowed to, delete
        if checkUserPermission(self.getUserId(), occ, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(occ)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return
            else:
                raise cherrypy.HTTPError(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    'Failed to delete occurrence set')
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN, 
                'User does not have permission to delete this occurrence set')
        
    # ................................
    @lmFormatter
    def GET(self, pathOccSetId=None, afterTime=None, beforeTime=None,
            displayName=None, epsgCode=None, minimumNumberOfPoints=1,
            limit=100, offset=0, urlUser=None, status=None, gridSetId=None,
            fillPoints=False, **params):
        """
        @summary: Performs a GET request.  If an occurrence set id is provided,
                         attempt to return that item.  If not, return a list of 
                         occurrence sets that match the provided parameters
        """
        if pathOccSetId is None:
            return self._listOccurrenceSets(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, displayName=displayName,
                epsgCode=epsgCode, minimumNumberOfPoints=minimumNumberOfPoints,
                limit=limit, offset=offset, gridSetId=gridSetId, status=status)
        elif pathOccSetId.lower() == 'count':
            return self._countOccurrenceSets(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, displayName=displayName,
                epsgCode=epsgCode, minimumNumberOfPoints=minimumNumberOfPoints,
                gridSetId=gridSetId, status=status)
        elif pathOccSetId.lower() == 'web':
            return self._list_web_occurrence_sets(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, displayName=displayName,
                epsgCode=epsgCode, minimumNumberOfPoints=minimumNumberOfPoints,
                limit=limit, offset=offset, gridSetId=gridSetId, status=status)
        else:
            return self._getOccurrenceSet(pathOccSetId, fillPoints=fillPoints)
    
    # ................................
    #@cherrypy.tools.json_out
    @lmFormatter
    def POST(self, **params):
        """
        @summary: Posts a new BOOM archive
        @todo: Do we want to enable single occurrence set posts still?
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
    def _countOccurrenceSets(self, userId, afterTime=None, beforeTime=None,
                             displayName=None, epsgCode=None,
                             minimumNumberOfPoints=1, status=None,
                             gridSetId=None):
        """
        @summary: Return a count of occurrence sets matching the specified 
                         criteria
        """
        afterStatus = None
        beforeStatus = None

        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE + 1
                afterStatus = JobStatus.COMPLETE - 1
            else:
                afterStatus = status - 1
        
        occCount = self.scribe.countOccurrenceSets(
            userId=userId, minOccurrenceCount=minimumNumberOfPoints,
            displayName=displayName, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, beforeStatus=beforeStatus,
            afterStatus=afterStatus, gridsetId=gridSetId)
        return {'count' : occCount}

    # ................................
    def _getOccurrenceSet(self, pathOccSetId, fillPoints=False):
        """
        @summary: Attempt to get an occurrence set
        """
        occ = self.scribe.getOccurrenceSet(occId=int(pathOccSetId))
        
        if occ is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Occurrence set not found')
        
        # If allowed to, return
        if checkUserPermission(self.getUserId(), occ, HTTPMethod.GET):
            if fillPoints:
                occ.readShapefile()
            return occ
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN, 
                'User {} does not have permission to delete this occurrence set'.format(
                    self.getUserId()))
    
    # ................................
    def _listOccurrenceSets(self, userId, afterTime=None, beforeTime=None,
                            displayName=None, epsgCode=None,
                            minimumNumberOfPoints=1, limit=100, offset=0,
                            status=None, gridSetId=None):
        """Return a list of occurrence sets matching the specified criteria
        """
        afterStatus = None
        beforeStatus = None
        
        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE + 1
                afterStatus = JobStatus.COMPLETE - 1
            else:
                afterStatus = status - 1
        
        occAtoms = self.scribe.listOccurrenceSets(offset, limit, userId=userId,
                            minOccurrenceCount=minimumNumberOfPoints, 
                            displayName=displayName, afterTime=afterTime, 
                            beforeTime=beforeTime, epsg=epsgCode, 
                            beforeStatus=beforeStatus, afterStatus=afterStatus,
                            gridsetId=gridSetId)
        return occAtoms

    # ................................
    def _list_web_occurrence_sets(
            self, userId, afterTime=None, beforeTime=None, displayName=None,
            epsgCode=None, minimumNumberOfPoints=1, limit=100, offset=0,
            status=None, gridSetId=None):
        """Return a list of occurrence set web objects matching criteria
        """
        afterStatus = None
        beforeStatus = None
        
        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE + 1
                afterStatus = JobStatus.COMPLETE - 1
            else:
                afterStatus = status - 1
        
        occs = self.scribe.listOccurrenceSets(
            offset, limit, userId=userId,
            minOccurrenceCount=minimumNumberOfPoints, displayName=displayName,
            afterTime=afterTime, beforeTime=beforeTime, epsg=epsgCode,
            beforeStatus=beforeStatus, afterStatus=afterStatus,
            gridsetId=gridSetId, atom=False)
        occ_objs = []
        for occ in occs:
            occ_objs.append(
                {
                    'id': occ.getId(),
                    'metadata_url': occ.metadataUrl,
                    'name' : occ.displayName,
                    'modification_time' : occ.statusModTime,
                    'epsg' : occ.epsgcode,
                    'status' : occ.status,
                    'count' : occ.queryCount
                }
            )
        return occ_objs
