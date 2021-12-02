"""This module provides REST services for Scenario"""
import werkzeug.exceptions as WEXC

from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
class ScenarioService(LmService):
    """Scenarios service class.
    """

    # ................................
    # @lm_formatter
    # def GET(self, scenario_id=None, after_time=None,
    #         alt_pred_code=None, before_time=None, date_code=None,
    #         epsg_code=None, gcm_code=None, limit=100, offset=0, url_user=None,
    #         **params):
    #     """GET request.  Individual, list, count
    #     """
    #     if scenario_id is None:
    #         return self._list_scenarios(
    #             self.get_user_id(url_user=url_user), after_time=after_time,
    #             alt_pred_code=alt_pred_code, before_time=before_time,
    #             date_code=date_code, epsg_code=epsg_code, gcm_code=gcm_code,
    #             limit=limit, offset=offset)
    #
    #     if scenario_id.lower() == 'count':
    #         return self._count_scenarios(
    #             self.get_user_id(url_user=url_user), after_time=after_time,
    #             alt_pred_code=alt_pred_code, before_time=before_time,
    #             date_code=date_code, epsg_code=epsg_code, gcm_code=gcm_code)
    #
    #     return self._get_scenario(scenario_id)

    # ................................
    @lm_formatter
    def count_scenarios(
        self, user_id, after_time=None, before_time=None, alt_pred_code=None, date_code=None, 
        gcm_code=None, epsg_code=None):
        """Return a list of scenarios matching the specified criteria"""
        scen_count = self.scribe.count_scenarios(
            user_id=user_id, before_time=before_time, after_time=after_time,
            epsg=epsg_code, gcm_code=gcm_code, alt_pred_code=alt_pred_code,
            date_code=date_code)
        return {'count': scen_count}

    # ................................
    @lm_formatter
    def get_scenario(self, user_id, scenario_id):
        """Return a scenario"""
        scn = self.scribe.get_scenario(int(scenario_id), fill_layers=True)

        if scn is None:
            raise WEXC.NotFound('Scenario {} not found'.format(scenario_id))

        if check_user_permission(user_id, scn, HTTPMethod.GET):
            return scn
        else:
            raise WEXC.Forbidden('User {} does not have permission to get scenario {}'.format(
                user_id, scenario_id))

    # ................................
    @lm_formatter
    def list_scenarios(
        self, user_id, after_time=None, before_time=None, alt_pred_code=None, date_code=None, 
        gcm_code=None, epsg_code=None, limit=100, offset=0):
        """Return a list of scenarios matching the specified criteria"""
        
        scn_atoms = self.scribe.list_scenarios(
            offset, limit, user_id=user_id, before_time=before_time, after_time=after_time, 
            epsg=epsg_code, gcm_code=gcm_code, alt_pred_code=alt_pred_code, date_code=date_code)
        
        return scn_atoms
