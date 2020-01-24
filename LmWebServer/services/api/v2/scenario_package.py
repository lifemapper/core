#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Scenario packages
"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathScenarioPackageId')
class ScenarioPackageService(LmService):
    """Class for scenario packages web services
    """

    # ................................
    @lm_formatter
    def GET(self, pathScenarioPackageId=None, afterTime=None, beforeTime=None,
            epsgCode=None, limit=100, offset=0, urlUser=None, scenarioId=None,
            **params):
        """Perform a GET request.  A list, count, or individual.
        """
        # List
        if pathScenarioPackageId is None:
            return self._list_scenario_packages(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                scenarioId=scenarioId, limit=limit, offset=offset,
                epsgCode=epsgCode)

        # Count
        if pathScenarioPackageId.lower() == 'count':
            return self._count_scenario_packages(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, scenarioId=scenarioId,
                epsgCode=epsgCode)

        # Get an individual scenario package
        return self._get_scenario_package(pathScenarioPackageId)

    # ................................
    def _count_scenario_packages(self, userId, afterTime=None, beforeTime=None,
                                 epsgCode=None, scenarioId=None):
        """Return the number of scenario packages that match the parameters
        """
        scen_package_count = self.scribe.countScenPackages(
            userId=userId, beforeTime=beforeTime, afterTime=afterTime,
            epsg=epsgCode, scenId=scenarioId)
        return {'count': scen_package_count}

    # ................................
    def _get_scenario_package(self, pathScenarioPackageId):
        """Attempt to get a scenario
        """
        scen_package = self.scribe.getScenPackage(
            scenPkgId=pathScenarioPackageId)

        if scen_package is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Scenario package{} not found'.format(pathScenarioPackageId))

        if check_user_permission(
                self.get_user_id(), scen_package, HTTPMethod.GET):
            return scen_package

        # 403 if no permission
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission for scenario package {}'.format(
                self.get_user_id(), pathScenarioPackageId))

    # ................................
    def _list_scenario_packages(self, userId, afterTime=None, beforeTime=None,
                                epsgCode=None, scenarioId=None, limit=100,
                                offset=0):
        """Return a list of scenarios matching the specified criteria
        """
        scen_package_atoms = self.scribe.listScenPackages(
            offset, limit, userId=userId, beforeTime=beforeTime,
            afterTime=afterTime, scenId=scenarioId, epsg=epsgCode)
        return scen_package_atoms
