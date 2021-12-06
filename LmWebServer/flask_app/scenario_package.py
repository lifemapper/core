"""This module provides REST services for Scenario packages"""
import werkzeug.exceptions as WEXC

from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.flask_app.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.flask_tools.lm_format import lm_formatter


# .............................................................................
class ScenarioPackageService(LmService):
    """Class for scenario packages web services"""

    # ................................
    @lm_formatter
    def count_scenario_packages(
            self, user_id, after_time=None, before_time=None, epsg_code=None, scenario_id=None):
        """Return the number of scenario packages that match the parameters"""
        scen_package_count = self.scribe.count_scen_packages(
            user_id=user_id, before_time=before_time, after_time=after_time, epsg=epsg_code, 
            scen_id=scenario_id)
        return {'count': scen_package_count}

    # ................................
    @lm_formatter
    def get_scenario_package(self, user_id, scenario_package_id):
        """Attempt to get a scenario"""
        scen_package = self.scribe.get_scen_package(scen_package_id=scenario_package_id)

        if scen_package is None:
            raise WEXC.NotFound('Scenario package{} not found'.format(scenario_package_id))

        if check_user_permission(user_id, scen_package, HTTPMethod.GET):
            return scen_package

        # 403 if no permission
        raise WEXC.Forbidden('User {} does not have permission for scenario package {}'.format(
            user_id, scenario_package_id))

    # ................................
    @lm_formatter
    def list_scenario_packages(
            self, user_id, after_time=None, before_time=None, epsg_code=None, scenario_id=None, 
            limit=100, offset=0):
        """Return a list of scenarios matching the specified criteria"""
        scen_package_atoms = self.scribe.list_scen_packages(
            offset, limit, user_id=user_id, before_time=before_time, after_time=after_time, 
            scen_id=scenario_id, epsg=epsg_code)
        
        return scen_package_atoms
