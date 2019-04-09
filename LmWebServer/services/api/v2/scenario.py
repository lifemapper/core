#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Scenario
"""

import cherrypy

from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter
from LmCommon.common.lmconstants import HTTPStatus

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathScenarioId')
class ScenarioService(LmService):
    """
    @summary: This class is for the scenarios service.  The dispatcher is
                     responsible for calling the correct method
    """
    # ................................
    @lmFormatter
    def GET(self, pathScenarioId=None, afterTime=None,
            altPredCode=None, beforeTime=None, dateCode=None,
            epsgCode=None, gcmCode=None, limit=100, offset=0, urlUser=None,
            **params):
        """
        @summary: Performs a GET request.  If a scenario id is provided,
                         attempt to return that item.  If not, return a list of 
                         scenarios that match the provided parameters
        """
        if pathScenarioId is None:
            return self._listScenarios(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                altPredCode=altPredCode, beforeTime=beforeTime,
                dateCode=dateCode, epsgCode=epsgCode, gcmCode=gcmCode,
                limit=limit, offset=offset)
        elif pathScenarioId.lower() == 'count':
            return self._countScenarios(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                altPredCode=altPredCode, beforeTime=beforeTime,
                dateCode=dateCode, epsgCode=epsgCode, gcmCode=gcmCode)
        else:
            return self._getScenario(pathScenarioId)
    
    # ................................
    def _countScenarios(self, userId, afterTime=None, altPredCode=None,
                        beforeTime=None, dateCode=None, epsgCode=None,
                        gcmCode=None):
        """
        @summary: Return a list of scenarios matching the specified criteria
        """
        scnCount = self.scribe.countScenarios(
            userId=userId, beforeTime=beforeTime, afterTime=afterTime,
            epsg=epsgCode, gcmCode=gcmCode, altpredCode=altPredCode,
            dateCode=dateCode)
        return {'count' : scnCount}

    # ................................
    def _getScenario(self, pathScenarioId):
        """
        @summary: Attempt to get a scenario
        """
        scn = self.scribe.getScenario(int(pathScenarioId), fillLayers=True)
        
        if scn is None:
            raise cherrypy.HTTPError(404, 'Scenario {} not found'.format(
                                                                                        pathScenarioId))
        
        if checkUserPermission(self.getUserId(), scn, HTTPMethod.GET):
            return scn

        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User {} does not have permission to get scenario {}'.format(
                    self.getUserId(), pathScenarioId))
    
    # ................................
    def _listScenarios(self, userId, afterTime=None, altPredCode=None,
                       beforeTime=None, dateCode=None, epsgCode=None,
                       gcmCode=None, limit=100, offset=0):
        """Return a list of scenarios matching the specified criteria
        """
        scnAtoms = self.scribe.listScenarios(
            offset, limit, userId=userId, beforeTime=beforeTime,
            afterTime=afterTime, epsg=epsgCode, gcmCode=gcmCode,
            altpredCode=altPredCode, dateCode=dateCode)
        return scnAtoms

