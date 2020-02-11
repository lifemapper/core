#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for environmental layers

Todo:
    * Fill in documentation
    * Use constants for 'count'
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
class EnvLayerService(LmService):
    """This class is responsible for the layers service.

    Note:
        * The dispatcher is responsible for calling the correct method
    """
    # ................................
    @lm_formatter
    def GET(self, pathLayerId=None, afterTime=None, altPredCode=None,
            beforeTime=None, dateCode=None, epsgCode=None, envCode=None,
            envTypeId=None, gcmCode=None, limit=100, offset=0, urlUser=None,
            scenarioCode=None, squid=None, **params):
        """Gets a layer, count, or list of layers

        Performs a GET request.  If a layer id is provided, attempt to return
        that item.  If not, return a list of layers that match the provided
        parameters
        """
        if pathLayerId is None:
            return self._list_env_layers(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                altPredCode=altPredCode, beforeTime=beforeTime,
                dateCode=dateCode, envCode=envCode, envTypeId=envTypeId,
                epsgCode=epsgCode, gcmCode=gcmCode, limit=limit, offset=offset,
                scenarioCode=scenarioCode)

        if pathLayerId.lower() == 'count':
            return self._count_env_layers(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                altPredCode=altPredCode, beforeTime=beforeTime,
                dateCode=dateCode, envCode=envCode, envTypeId=envTypeId,
                epsgCode=epsgCode, gcmCode=gcmCode, scenarioCode=scenarioCode)

        try:
            _ = int(pathLayerId)
        except ValueError:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                '{} is not a valid environmental layer ID'.format(
                    pathLayerId))
        return self._get_env_layer(pathLayerId)

    # ................................
    def _count_env_layers(self, userId, afterTime=None, altPredCode=None,
                          beforeTime=None, dateCode=None, envCode=None,
                          envTypeId=None, epsgCode=None, gcmCode=None,
                          scenarioCode=None):
        """Count environmental layer objects matching the specified criteria
        """
        layer_count = self.scribe.countEnvLayers(
            userId=userId, envCode=envCode, gcmcode=gcmCode,
            altpredCode=altPredCode, dateCode=dateCode, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, envTypeId=envTypeId,
            scenarioCode=scenarioCode)

        return {'count': layer_count}

    # ................................
    def _get_env_layer(self, pathLayerId):
        """Attempt to get a layer
        """
        lyr = self.scribe.getEnvLayer(envlyrId=pathLayerId)
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
                         limit=100, offset=0, scenarioCode=None):
        """List environmental layer objects matching the specified criteria
        """
        layer_atoms = self.scribe.listEnvLayers(
            offset, limit, userId=userId, envCode=envCode, gcmcode=gcmCode,
            altpredCode=altPredCode, dateCode=dateCode, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, envTypeId=envTypeId,
            scenCode=scenarioCode)

        return layer_atoms
