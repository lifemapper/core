#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Layers
"""

import cherrypy

from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathLayerId')
class LayerService(LmService):
    """
    @summary: This class is for the layers service.  The dispatcher is
                     responsible for calling the correct method
    """
    # ................................
    @lmFormatter
    def GET(self, pathLayerId=None, afterTime=None, altPredCode=None,
            beforeTime=None, dateCode=None, epsgCode=None, envCode=None,
            envTypeId=None, gcmCode=None, layerType=None, limit=100, offset=0,
            urlUser=None, scenarioId=None, squid=None, **params):
        """
        @summary: Performs a GET request.  If a layer id is provided,
                         attempt to return that item.  If not, return a list of 
                         layers that match the provided parameters
        """
        # Layer type:
        #    0 - Anything
        #    1 - Environmental layer
        #    2 - ? (Not implemented yet)
        if layerType is None or layerType == 0:
            if pathLayerId is None:
                return self._listLayers(
                    self.getUserId(urlUser=urlUser), afterTime=afterTime,
                    beforeTime=beforeTime, epsgCode=epsgCode, limit=limit,
                    offset=offset, squid=squid)
            elif pathLayerId.lower() == 'count':
                return self._countLayers(
                    self.getUserId(urlUser=urlUser), afterTime=afterTime,
                    beforeTime=beforeTime, epsgCode=epsgCode, squid=squid)
            else:
                return self._getLayer(pathLayerId, envLayer=False)
        else:
            if pathLayerId is None:
                return self._listEnvLayers(
                    self.getUserId(urlUser=urlUser), afterTime=afterTime,
                    altPredCode=altPredCode, beforeTime=beforeTime,
                    dateCode=dateCode, envCode=envCode, envTypeId=envTypeId,
                    epsgCode=epsgCode, gcmCode=gcmCode, limit=limit,
                    offset=offset, scenarioId=scenarioId)
            elif pathLayerId.lower() == 'count':
                return self._countEnvLayers(
                    self.getUserId(urlUser=urlUser), afterTime=afterTime,
                    altPredCode=altPredCode, beforeTime=beforeTime,
                    dateCode=dateCode, envCode=envCode, envTypeId=envTypeId,
                    epsgCode=epsgCode, gcmCode=gcmCode, scenarioId=scenarioId)
            else:
                return self._getLayer(pathLayerId, envLayer=True)
        
    # ................................
    def _countEnvLayers(self, userId, afterTime=None, altPredCode=None,
                        beforeTime=None, dateCode=None, envCode=None,
                        envTypeId=None, epsgCode=None, gcmCode=None,
                        scenarioId=None):
        """
        @summary: Count environmental layer objects matching the specified 
                         criteria
        @param userId: The user to list environmental layers for.  Note that this
                                may not be the same user logged into the system
        @param afterTime: (optional) Return layers modified after this time 
                                    (Modified Julian Day)
        @param altPredCode: (optional) Return layers with this alternate 
                                      prediction code
        @param beforeTime: (optional) Return layers modified before this time 
                                     (Modified Julian Day)
        @param dateCode: (optional) Return layers with this date code
        @param envCode: (optional) Return layers with this environment code
        @param envTypeId: (optional) Return layers with this environmental type
        @param epsgCode: (optional) Return layers with this EPSG code
        @param gcmCode: (optional) Return layers with this GCM code
        @param scenarioId: (optional) Return layers from this scenario
        """
        lyrCount = self.scribe.countEnvLayers(
            userId=userId, envCode=envCode, gcmcode=gcmCode,
            altpredCode=altPredCode, dateCode=dateCode, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, envTypeId=envTypeId,
            scenarioId=scenarioId)
        # Format return
        # Set headers
        return {"count" : lyrCount}

    # ................................
    def _countLayers(self, userId, afterTime=None, beforeTime=None,
                     epsgCode=None, squid=None):
        """
        @summary: Return a count of layers matching the specified criteria
        @param userId: The user to list layers for.  Note that this may not be
                                the same user that is logged into the system
        @param afterTime: (optional) List layers modified after this time 
                                    (Modified Julian Day)
        @param beforeTime: (optional) List layers modified before this time
                                     (Modified Julian Day)
        @param epsgCode: (optional) Return layers that have this EPSG code
        @param limit: (optional) Return this number of layers, at most
        @param offset: (optional) Offset the returned layers by this number
        @param squid: (optional) Return layers with this species identifier
        """
        lyrCount = self.scribe.countLayers(
            userId=userId, squid=squid, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode)
        # Format return
        # Set headers
        return {"count" : lyrCount}

    # ................................
    def _getLayer(self, pathLayerId, envLayer=False):
        """
        @summary: Attempt to get a layer
        """
        try:
            _ = int(pathLayerId)
        except ValueError:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                '{} is not a valid layer ID'.format(pathLayerId))

        if envLayer:
            lyr = self.scribe.getEnvLayer(lyrId=pathLayerId)
        else:
            lyr = self.scribe.getLayer(lyrId=pathLayerId)
        if lyr is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Environmental layer {} was not found'.format(pathLayerId))
        if checkUserPermission(self.getUserId(), lyr, HTTPMethod.GET):
            return lyr
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User {} does not have permission to access layer {}'.format(
                    self.getUserId(), pathLayerId))
    
    # ................................
    def _listEnvLayers(self, userId, afterTime=None, altPredCode=None,
                       beforeTime=None, dateCode=None, envCode=None,
                       envTypeId=None, epsgCode=None, gcmCode=None, limit=100,
                       offset=0, scenarioId=None):
        """
        @summary: List environmental layer objects matching the specified 
                         criteria
        @param userId: The user to list environmental layers for.  Note that this
                                may not be the same user logged into the system
        @param afterTime: (optional) Return layers modified after this time 
                                    (Modified Julian Day)
        @param altPredCode: (optional) Return layers with this alternate 
                                      prediction code
        @param beforeTime: (optional) Return layers modified before this time 
                                     (Modified Julian Day)
        @param dateCode: (optional) Return layers with this date code
        @param envCode: (optional) Return layers with this environment code
        @param envTypeId: (optional) Return layers with this environmental type
        @param epsgCode: (optional) Return layers with this EPSG code
        @param gcmCode: (optional) Return layers with this GCM code
        @param limit: (optional) Return this number of layers, at most
        @param offset: (optional) Offset the returned layers by this number
        @param scenarioId: (optional) Return layers from this scenario
        """
        lyrAtoms = self.scribe.listEnvLayers(
            offset, limit, userId=userId, envCode=envCode, gcmcode=gcmCode,
            altpredCode=altPredCode, dateCode=dateCode, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, envTypeId=envTypeId,
            scenarioId=scenarioId)
        # Format return
        # Set headers
        return lyrAtoms
    
    # ................................
    def _listLayers(self, userId, afterTime=None, beforeTime=None,
                    epsgCode=None, limit=100, offset=0, squid=None):
        """
        @summary: Return a list of layers matching the specified criteria
        @param userId: The user to list layers for.  Note that this may not be
                                the same user that is logged into the system
        @param afterTime: (optional) List layers modified after this time 
                                    (Modified Julian Day)
        @param beforeTime: (optional) List layers modified before this time
                                     (Modified Julian Day)
        @param epsgCode: (optional) Return layers that have this EPSG code
        @param limit: (optional) Return this number of layers, at most
        @param offset: (optional) Offset the returned layers by this number
        @param squid: (optional) Return layers with this species identifier
        """
        lyrAtoms = self.scribe.listLayers(
            offset, limit, userId=userId, squid=squid, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode)
        # Format return
        # Set headers
        return lyrAtoms
    