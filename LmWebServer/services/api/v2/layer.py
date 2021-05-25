"""This module provides REST services for Layers"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('path_layer_id')
class LayerService(LmService):
    """Class for layers service.
    """

    # ................................
    @lm_formatter
    def GET(self, path_layer_id=None, after_time=None, alt_pred_code=None,
            before_time=None, date_code=None, epsg_code=None, env_code=None,
            env_type_id=None, gcm_code=None, layerType=None, limit=100,
            offset=0, url_user=None, scenario_id=None, squid=None, **params):
        """GET request.  Individual layer, count, or list.
        """
        # Layer type:
        #    0 - Anything
        #    1 - Environmental layer
        #    2 - ? (Not implemented yet)
        if layerType is None or layerType == 0:
            if path_layer_id is None:
                return self._list_layers(
                    self.get_user_id(url_user=url_user), after_time=after_time,
                    before_time=before_time, epsg_code=epsg_code, limit=limit,
                    offset=offset, squid=squid)

            if path_layer_id.lower() == 'count':
                return self._count_layers(
                    self.get_user_id(url_user=url_user), after_time=after_time,
                    before_time=before_time, epsg_code=epsg_code, squid=squid)

            return self._get_layer(path_layer_id, env_layer=False)

        if path_layer_id is None:
            return self._list_env_layers(
                self.get_user_id(url_user=url_user), after_time=after_time,
                alt_pred_code=alt_pred_code, before_time=before_time,
                date_code=date_code, env_code=env_code,
                env_type_id=env_type_id, epsg_code=epsg_code,
                gcm_code=gcm_code, limit=limit, offset=offset,
                scenario_id=scenario_id)

        if path_layer_id.lower() == 'count':
            return self._count_env_layers(
                self.get_user_id(url_user=url_user), after_time=after_time,
                alt_pred_code=alt_pred_code, before_time=before_time,
                date_code=date_code, env_code=env_code,
                env_type_id=env_type_id, epsg_code=epsg_code,
                gcm_code=gcm_code, scenario_code=scenario_id)

        return self._get_layer(path_layer_id, env_layer=True)

    # ................................
    def _count_env_layers(self, user_id, after_time=None, alt_pred_code=None,
                          before_time=None, date_code=None, env_code=None,
                          env_type_id=None, epsg_code=None, gcm_code=None,
                          scenario_code=None):
        """Count environmental layer objects matching the specified criteria

        Args:
            user_id: The user to list environmental layers for.  Note that this
                                may not be the same user logged into the system
            after_time: Return layers modified after this time (Modified Julian
                Day)
            alt_pred_code: Return layers with this alternate prediction code
            before_time: Return layers modified before this time (Modified
                Julian Day)
            date_code: Return layers with this date code
            env_code: Return layers with this environment code
            env_type_id: Return layers with this environmental type
            epsg_code: Return layers with this EPSG code
            gcm_code: Return layers with this GCM code
            scenario_id: Return layers from this scenario
        """
        layer_count = self.scribe.count_env_layers(
            user_id=user_id, env_code=env_code, gcm_code=gcm_code,
            alt_pred_code=alt_pred_code, date_code=date_code,
            after_time=after_time, before_time=before_time, epsg=epsg_code,
            env_type_id=env_type_id, scenario_code=scenario_code)

        return {'count': layer_count}

    # ................................
    def _count_layers(self, user_id, after_time=None, before_time=None,
                      epsg_code=None, squid=None):
        """Return a count of layers matching the specified criteria

        Args:
            user_id: The user to list layers for.  Note that this may not be
                the same user that is logged into the system
            after_time: List layers modified after this time (Modified Julian
                Day)
            before_time: List layers modified before this time (Modified Julian
                Day)
            epsg_code: Return layers that have this EPSG code
            limit: Return this number of layers, at most
            offset: Offset the returned layers by this number
            squid: Return layers with this species identifier
        """
        layer_count = self.scribe.count_layers(
            user_id=user_id, squid=squid, after_time=after_time,
            before_time=before_time, epsg=epsg_code)

        return {'count': layer_count}

    # ................................
    def _get_layer(self, path_layer_id, env_layer=False):
        """Attempt to get a layer
        """
        try:
            _ = int(path_layer_id)
        except ValueError:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                '{} is not a valid layer ID'.format(path_layer_id))

        if env_layer:
            lyr = self.scribe.get_env_layer(lyr_id=path_layer_id)
        else:
            lyr = self.scribe.get_layer(lyr_id=path_layer_id)
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
                         limit=100, offset=0, scenario_id=None):
        """List environmental layer objects matching the specified criteria

        Args:
            user_id: The user to list environmental layers for.  Note that this
                may not be the same user logged into the system
            after_time: (optional) Return layers modified after this time
                (Modified Julian Day)
            alt_pred_code: (optional) Return layers with this alternate
                prediction code
            before_time: (optional) Return layers modified before this time
                (Modified Julian Day)
            date_code: (optional) Return layers with this date code
            env_code: (optional) Return layers with this environment code
            env_type_id: (optional) Return layers with this environmental type
            epsg_code: (optional) Return layers with this EPSG code
            gcm_code: (optional) Return layers with this GCM code
            limit: (optional) Return this number of layers, at most
            offset: (optional) Offset the returned layers by this number
            scenario_id: (optional) Return layers from this scenario
        """
        lyr_atoms = self.scribe.list_env_layers(
            offset, limit, user_id=user_id, env_code=env_code,
            gcm_code=gcm_code, alt_pred_code=alt_pred_code,
            date_code=date_code, after_time=after_time,
            before_time=before_time, epsg=epsg_code, env_type_id=env_type_id)

        return lyr_atoms

    # ................................
    def _list_layers(self, user_id, after_time=None, before_time=None,
                     epsg_code=None, limit=100, offset=0, squid=None):
        """Return a list of layers matching the specified criteria

        Args:
            user_id: The user to list layers for.  Note that this may not be
                                the same user that is logged into the system
            after_time: List layers modified after this time (Modified Julian
                Day)
            before_time: List layers modified before this time (Modified Julian
                Day)
            epsg_code: Return layers that have this EPSG code
            limit: Return this number of layers, at most
            offset: Offset the returned layers by this number
            squid: Return layers with this species identifier
        """
        layer_atoms = self.scribe.list_layers(
            offset, limit, user_id=user_id, squid=squid, after_time=after_time,
            before_time=before_time, epsg=epsg_code)

        return layer_atoms
