#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Layers
"""

from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter
import cherrypy


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathLayerId')
class LayerService(LmService):
    """Class for layers service.
    """
    # ................................
    @lm_formatter
    def GET(self, pathLayerId=None, afterTime=None, altPredCode=None,
            beforeTime=None, dateCode=None, epsgCode=None, envCode=None,
            envTypeId=None, gcmCode=None, layerType=None, limit=100, offset=0,
            urlUser=None, scenarioId=None, squid=None, **params):
        """GET request.  Individual layer, count, or list.
        """
        # Layer type:
        #    0 - Anything
        #    1 - Environmental layer
        #    2 - ? (Not implemented yet)
        if layerType is None or layerType == 0:
            if pathLayerId is None:
                return self._list_layers(
                    self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                    beforeTime=beforeTime, epsgCode=epsgCode, limit=limit,
                    offset=offset, squid=squid)

            if pathLayerId.lower() == 'count':
                return self._count_layers(
                    self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                    beforeTime=beforeTime, epsgCode=epsgCode, squid=squid)

            return self._get_layer(pathLayerId, envLayer=False)

        if pathLayerId is None:
            return self._list_env_layers(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                altPredCode=altPredCode, beforeTime=beforeTime,
                dateCode=dateCode, envCode=envCode, envTypeId=envTypeId,
                epsgCode=epsgCode, gcmCode=gcmCode, limit=limit,
                offset=offset, scenarioId=scenarioId)

        if pathLayerId.lower() == 'count':
            return self._count_env_layers(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                altPredCode=altPredCode, beforeTime=beforeTime,
                dateCode=dateCode, envCode=envCode, envTypeId=envTypeId,
                epsgCode=epsgCode, gcmCode=gcmCode, scenarioId=scenarioId)

        return self._get_layer(pathLayerId, envLayer=True)

    # ................................
    def _count_env_layers(self, userId, afterTime=None, altPredCode=None,
                          beforeTime=None, dateCode=None, envCode=None,
                          envTypeId=None, epsgCode=None, gcmCode=None,
                          scenarioId=None):
        """Count environmental layer objects matching the specified criteria

        Args:
            userId: The user to list environmental layers for.  Note that this
                                may not be the same user logged into the system
            afterTime: Return layers modified after this time (Modified Julian
                Day)
            altPredCode: Return layers with this alternate prediction code
            beforeTime: Return layers modified before this time (Modified
                Julian Day)
            dateCode: Return layers with this date code
            envCode: Return layers with this environment code
            envTypeId: Return layers with this environmental type
            epsgCode: Return layers with this EPSG code
            gcmCode: Return layers with this GCM code
            scenarioId: Return layers from this scenario
        """
        layer_count = self.scribe.countEnvLayers(
            userId=userId, envCode=envCode, gcmcode=gcmCode,
            altpredCode=altPredCode, dateCode=dateCode, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, envTypeId=envTypeId,
            scenarioId=scenarioId)

        return {'count': layer_count}

    # ................................
    def _count_layers(self, userId, afterTime=None, beforeTime=None,
                      epsgCode=None, squid=None):
        """Return a count of layers matching the specified criteria

        Args:
            userId: The user to list layers for.  Note that this may not be the
                same user that is logged into the system
            afterTime: List layers modified after this time (Modified Julian
                Day)
            beforeTime: List layers modified before this time (Modified Julian
                Day)
            epsgCode: Return layers that have this EPSG code
            limit: Return this number of layers, at most
            offset: Offset the returned layers by this number
            squid: Return layers with this species identifier
        """
        layer_count = self.scribe.countLayers(
            userId=userId, squid=squid, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode)

        return {'count': layer_count}

    # ................................
    def _get_layer(self, pathLayerId, envLayer=False):
        """Attempt to get a layer
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
        if check_user_permission(self.get_user_id(), lyr, HTTPMethod.GET):
            return lyr

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access layer {}'.format(
                self.get_user_id(), pathLayerId))

    # ................................
    def _list_env_layers(self, userId, afterTime=None, altPredCode=None,
                         beforeTime=None, dateCode=None, envCode=None,
                         envTypeId=None, epsgCode=None, gcmCode=None,
                         limit=100, offset=0, scenarioId=None):
        """List environmental layer objects matching the specified criteria

        Args:
            userId: The user to list environmental layers for.  Note that this
                may not be the same user logged into the system
            afterTime: (optional) Return layers modified after this time
                (Modified Julian Day)
            altPredCode: (optional) Return layers with this alternate
                prediction code
            beforeTime: (optional) Return layers modified before this time
                (Modified Julian Day)
            dateCode: (optional) Return layers with this date code
            envCode: (optional) Return layers with this environment code
            envTypeId: (optional) Return layers with this environmental type
            epsgCode: (optional) Return layers with this EPSG code
            gcmCode: (optional) Return layers with this GCM code
            limit: (optional) Return this number of layers, at most
            offset: (optional) Offset the returned layers by this number
            scenarioId: (optional) Return layers from this scenario
        """
        lyr_atoms = self.scribe.listEnvLayers(
            offset, limit, userId=userId, envCode=envCode, gcmcode=gcmCode,
            altpredCode=altPredCode, dateCode=dateCode, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, envTypeId=envTypeId)

        return lyr_atoms

    # ................................
    def _list_layers(self, userId, afterTime=None, beforeTime=None,
                     epsgCode=None, limit=100, offset=0, squid=None):
        """Return a list of layers matching the specified criteria

        Args:
            userId: The user to list layers for.  Note that this may not be
                                the same user that is logged into the system
            afterTime: List layers modified after this time (Modified Julian
                Day)
            beforeTime: List layers modified before this time (Modified Julian
                Day)
            epsgCode: Return layers that have this EPSG code
            limit: Return this number of layers, at most
            offset: Offset the returned layers by this number
            squid: Return layers with this species identifier
        """
        layer_atoms = self.scribe.listLayers(
            offset, limit, userId=userId, squid=squid, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode)

        return layer_atoms
