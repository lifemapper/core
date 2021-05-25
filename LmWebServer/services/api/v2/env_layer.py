"""This module provides REST services for environmental layers

Todo:
    * Fill in documentation
    * Use constants for 'count'
"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('path_layer_id')
class EnvLayerService(LmService):
    """This class is responsible for the layers service.

    Note:
        * The dispatcher is responsible for calling the correct method
    """

    # ................................
    @lm_formatter
    def GET(self, path_layer_id=None, after_time=None, alt_pred_code=None,
            before_time=None, date_code=None, epsg_code=None, env_code=None,
            env_type_id=None, gcm_code=None, limit=100, offset=0,
            url_user=None, scenario_code=None, squid=None, **params):
        """Gets a layer, count, or list of layers

        Performs a GET request.  If a layer id is provided, attempt to return
        that item.  If not, return a list of layers that match the provided
        parameters
        """
        if path_layer_id is None:
            return self._list_env_layers(
                self.get_user_id(url_user=url_user), after_time=after_time,
                alt_pred_code=alt_pred_code, before_time=before_time,
                date_code=date_code, env_code=env_code,
                env_type_id=env_type_id, epsg_code=epsg_code,
                gcm_code=gcm_code, limit=limit, offset=offset,
                scenario_code=scenario_code)

        if path_layer_id.lower() == 'count':
            return self._count_env_layers(
                self.get_user_id(url_user=url_user), after_time=after_time,
                alt_pred_code=alt_pred_code, before_time=before_time,
                date_code=date_code, env_code=env_code,
                env_type_id=env_type_id, epsg_code=epsg_code,
                gcm_code=gcm_code, scenario_code=scenario_code)

        try:
            _ = int(path_layer_id)
        except ValueError:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                '{} is not a valid environmental layer ID'.format(
                    path_layer_id))
        return self._get_env_layer(path_layer_id)

    # ................................
    def _count_env_layers(self, user_id, after_time=None, alt_pred_code=None,
                          before_time=None, date_code=None, env_code=None,
                          env_type_id=None, epsg_code=None, gcm_code=None,
                          scenario_code=None):
        """Count environmental layer objects matching the specified criteria
        """
        layer_count = self.scribe.count_env_layers(
            user_id=user_id, env_code=env_code, gcm_code=gcm_code,
            alt_pred_code=alt_pred_code, date_code=date_code,
            after_time=after_time, before_time=before_time, epsg=epsg_code,
            env_type_id=env_type_id, scenario_code=scenario_code)

        return {'count': layer_count}

    # ................................
    def _get_env_layer(self, path_layer_id):
        """Attempt to get a layer
        """
        lyr = self.scribe.get_env_layer(env_lyr_id=path_layer_id)
        if lyr is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Environmental layer {} was not found'.format(path_layer_id))
        if check_user_permission(self.get_user_id(), lyr, HTTPMethod.GET):
            return lyr

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access layer {}'.format(
                self.get_user_id(), path_layer_id))

    # ................................
    def _list_env_layers(self, user_id, after_time=None, alt_pred_code=None,
                         before_time=None, date_code=None, env_code=None,
                         env_type_id=None, epsg_code=None, gcm_code=None,
                         limit=100, offset=0, scenario_code=None):
        """List environmental layer objects matching the specified criteria
        """
        layer_atoms = self.scribe.list_env_layers(
            offset, limit, user_id=user_id, env_code=env_code,
            gcm_code=gcm_code, alt_pred_code=alt_pred_code,
            date_code=date_code, after_time=after_time,
            before_time=before_time, epsg=epsg_code, env_type_id=env_type_id,
            scen_code=scenario_code)

        return layer_atoms
