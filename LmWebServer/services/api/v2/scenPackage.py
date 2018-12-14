#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Scenario packages
"""

import cherrypy

#from LmServer.legion.scenario import Scenario
from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathScenarioPackageId')
class ScenarioPackageService(LmService):
    """Class for scenario packages web services
    """

    # ................................
    @lmFormatter
    def GET(self, pathScenarioPackageId=None, afterTime=None, beforeTime=None,
            epsgCode=None, limit=100, offset=0, urlUser=None, scenarioId=None,
            **params):
        """
        @summary: Performs a GET request.  If a scenario package id is
            provided, attempt to return that item.  If not, return a list of
            scenario packagess that match the provided parameters
        """
        if pathScenarioPackageId is None:
            return self._listScenarioPackages(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                scenarioId=scenarioId, limit=limit, offset=offset,
                epsgCode=epsgCode)
        elif pathScenarioPackageId.lower() == 'count':
            return self._countScenarioPackages(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, scenarioId=scenarioId,
                epsgCode=epsgCode)
        else:
            return self._getScenarioPackage(pathScenarioPackageId)
    
    # ................................
    def _countScenarioPackages(self, userId, afterTime=None, beforeTime=None,
                               epsgCode=None, scenarioId=None):
        """Return the number of scenario packages that match the parameters
        """
        scnPkgCount = self.scribe.countScenPackages(
            userId=userId, beforeTime=beforeTime, afterTime=afterTime,
            epsg=epsgCode, scenId=scenarioId)
        return {'count' : scnPkgCount}

    # ................................
    def _getScenarioPackage(self, pathScenarioPackageId):
        """Attempt to get a scenario
        """
        scnPkg = self.scribe.getScenPackage(scenPkgId=pathScenarioPackageId)
        
        if scnPkg is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Scenario package{} not found'.format(pathScenarioPackageId))
        
        if checkUserPermission(self.getUserId(), scnPkg, HTTPMethod.GET):
            return scnPkg

        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User {} does not have permission to get scenario package {}'.format(
                    self.getUserId(), pathScenarioPackageId))
    
    # ................................
    def _listScenarioPackages(self, userId, afterTime=None, beforeTime=None,
                              epsgCode=None, scenarioId=None, limit=100,
                              offset=0):
        """Return a list of scenarios matching the specified criteria
        """
        scnPkgAtoms = self.scribe.listScenPackages(
            offset, limit, userId=userId, beforeTime=beforeTime,
            afterTime=afterTime, scenId=scenarioId, epsg=epsgCode)
        return scnPkgAtoms

