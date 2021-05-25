"""This module provides REST services for Scenario packages"""
import cherrypy
from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('path_scenario_package_id')
class ScenarioPackageService(LmService):
    """Class for scenario packages web services
    """

    # ................................
    @lm_formatter
    def GET(self, path_scenario_package_id=None, after_time=None,
            before_time=None, epsg_code=None, limit=100, offset=0,
            url_user=None, scenario_id=None, **params):
        """Perform a GET request.  A list, count, or individual.
        """
        # List
        if path_scenario_package_id is None:
            return self._list_scenario_packages(
                self.get_user_id(url_user=url_user), after_time=after_time,
                scenario_id=scenario_id, limit=limit, offset=offset,
                epsg_code=epsg_code)

        # Count
        if path_scenario_package_id.lower() == 'count':
            return self._count_scenario_packages(
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, scenario_id=scenario_id,
                epsg_code=epsg_code)

        # Get an individual scenario package
        return self._get_scenario_package(path_scenario_package_id)

    # ................................
    def _count_scenario_packages(self, user_id, after_time=None,
                                 before_time=None, epsg_code=None,
                                 scenario_id=None):
        """Return the number of scenario packages that match the parameters
        """
        scen_package_count = self.scribe.count_scen_packages(
            user_id=user_id, before_time=before_time, after_time=after_time,
            epsg=epsg_code, scen_id=scenario_id)
        return {'count': scen_package_count}

    # ................................
    def _get_scenario_package(self, path_scenario_package_id):
        """Attempt to get a scenario
        """
        scen_package = self.scribe.get_scen_package(
            scen_package_id=path_scenario_package_id)

        if scen_package is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Scenario package{} not found'.format(
                    path_scenario_package_id))

        if check_user_permission(
                self.get_user_id(), scen_package, HTTPMethod.GET):
            return scen_package

        # 403 if no permission
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission for scenario package {}'.format(
                self.get_user_id(), path_scenario_package_id))

    # ................................
    def _list_scenario_packages(self, user_id, after_time=None,
                                before_time=None, epsg_code=None,
                                scenario_id=None, limit=100, offset=0):
        """Return a list of scenarios matching the specified criteria
        """
        scen_package_atoms = self.scribe.list_scen_packages(
            offset, limit, user_id=user_id, before_time=before_time,
            after_time=after_time, scen_id=scenario_id, epsg=epsg_code)
        return scen_package_atoms
