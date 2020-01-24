#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Scenario
"""
import cherrypy

from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter
from LmCommon.common.lmconstants import HTTPStatus


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathScenarioId')
class ScenarioService(LmService):
    """Scenarios service class.
    """
    # ................................
    @lm_formatter
    def GET(self, pathScenarioId=None, afterTime=None,
            altPredCode=None, beforeTime=None, dateCode=None,
            epsgCode=None, gcmCode=None, limit=100, offset=0, urlUser=None,
            **params):
        """GET request.  Individual, list, count
        """
        if pathScenarioId is None:
            return self._list_scenarios(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                altPredCode=altPredCode, beforeTime=beforeTime,
                dateCode=dateCode, epsgCode=epsgCode, gcmCode=gcmCode,
                limit=limit, offset=offset)

        if pathScenarioId.lower() == 'count':
            return self._count_scenarios(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                altPredCode=altPredCode, beforeTime=beforeTime,
                dateCode=dateCode, epsgCode=epsgCode, gcmCode=gcmCode)

        return self._get_scenario(pathScenarioId)

    # ................................
    def _count_scenarios(self, userId, afterTime=None, altPredCode=None,
                         beforeTime=None, dateCode=None, epsgCode=None,
                         gcmCode=None):
        """Return a list of scenarios matching the specified criteria
        """
        scen_count = self.scribe.countScenarios(
            userId=userId, beforeTime=beforeTime, afterTime=afterTime,
            epsg=epsgCode, gcmCode=gcmCode, altpredCode=altPredCode,
            dateCode=dateCode)
        return {'count': scen_count}

    # ................................
    def _get_scenario(self, pathScenarioId):
        """Attempt to get a scenario
        """
        scn = self.scribe.getScenario(int(pathScenarioId), fillLayers=True)

        if scn is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Scenario {} not found'.format(
                    pathScenarioId))

        if check_user_permission(self.get_user_id(), scn, HTTPMethod.GET):
            return scn

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to get scenario {}'.format(
                self.get_user_id(), pathScenarioId))

    # ................................
    def _list_scenarios(self, userId, afterTime=None, altPredCode=None,
                        beforeTime=None, dateCode=None, epsgCode=None,
                        gcmCode=None, limit=100, offset=0):
        """Return a list of scenarios matching the specified criteria
        """
        scn_atoms = self.scribe.listScenarios(
            offset, limit, userId=userId, beforeTime=beforeTime,
            afterTime=afterTime, epsg=epsgCode, gcmCode=gcmCode,
            altpredCode=altPredCode, dateCode=dateCode)
        return scn_atoms
