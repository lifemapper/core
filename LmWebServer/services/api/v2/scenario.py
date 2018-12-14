#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for Scenario
"""

import cherrypy

from LmServer.legion.scenario import Scenario
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter
from LmCommon.common.lmconstants import HTTPStatus

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathScenarioId')
class ScenarioService(LmService):
    """
    @summary: This class is for the scenarios service.  The dispatcher is
                     responsible for calling the correct method
    """
    # ................................
    def DELETE(self, pathScenarioId):
        """
        @summary: Attempts to delete a scenario
        @param projectionId: The id of the scenario to delete
        """
        scn = self.scribe.getScenario(int(pathScenarioId))
        
        if scn is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Scenario {} not found'.format(
                    pathScenarioId))
        
        if checkUserPermission(self.getUserId(), scn, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(scn)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return
            else:
                raise cherrypy.HTTPError(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    'Failed to delete scenario {}'.format(pathScenarioId))
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User {} does not have permission to delete scenario {}'.format(
                    self.getUserId(), pathScenarioId))

    # ................................
    @lmFormatter
    def GET(self, pathScenarioId=None, afterTime=None,
            altPredCode=None, beforeTime=None, dateCode=None,
            epsgCode=None, gcmCode=None, limit=100, offset=0, urlUser=None,
            **params):
        """
        @summary: Performs a GET request.  If a scenario id is provided,
                         attempt to return that item.  If not, return a list of 
                         scenarios that match the provided parameters
        """
        if pathScenarioId is None:
            return self._listScenarios(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                altPredCode=altPredCode, beforeTime=beforeTime,
                dateCode=dateCode, epsgCode=epsgCode, gcmCode=gcmCode,
                limit=limit, offset=offset)
        elif pathScenarioId.lower() == 'count':
            return self._countScenarios(
                self.getUserId(urlUser=urlUser), afterTime=afterTime,
                altPredCode=altPredCode, beforeTime=beforeTime,
                dateCode=dateCode, epsgCode=epsgCode, gcmCode=gcmCode)
        else:
            return self._getScenario(pathScenarioId)
    
    # ................................
    #@cherrypy.tools.json_in
    #@cherrypy.tools.json_out
    @lmFormatter
    def POST(self, **params):
        """
        @summary: Posts a new scenario
        """
        layers = []
        scnModel = cherrypy.request.json

        try:
            code = scnModel['code']
            epsgCode = int(scnModel['epsgCode'])
            rawLayers = scnModel['layers']
        except KeyError, ke:
            # If one of these is missing, we have a bad request
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST, 
                'code, epsgCode, and layers are required parameters for scenarios')
        except Exception, e:
            # TODO: Log error
            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Unknown error: {}'.format(str(e)))
        
        metadata = scnModel.get('metadata', {})
        units = scnModel.get('units', None)
        resolution = scnModel.get('resolution', None)
        gcmCode = scnModel.get('gcmCode', None)
        altPredCode = scnModel.get('altPredCode', None)
        dateCode = scnModel.get('dateCode', None)

        # Process layers, assume they are Lifemapper IDs for now
        for lyrId in rawLayers:
            layers.append(int(lyrId))
        
        scn = Scenario(
            code, self.getUserId(), epsgCode, metadata=metadata, units=units,
            res=resolution, gcmCode=gcmCode, altpredCode=altPredCode,
            dateCode=dateCode, layers=layers)
        newScn = self.scribe.findOrInsertScenario(scn)
        
        return newScn
    
    # ................................
    #@cherrypy.tools.json_in
    #@cherrypy.tools.json_out
    #def PUT(self, pathScenarioId):
    #    pass
    
    # ................................
    def _countScenarios(self, userId, afterTime=None, altPredCode=None,
                        beforeTime=None, dateCode=None, epsgCode=None,
                        gcmCode=None):
        """
        @summary: Return a list of scenarios matching the specified criteria
        """
        scnCount = self.scribe.countScenarios(
            userId=userId, beforeTime=beforeTime, afterTime=afterTime,
            epsg=epsgCode, gcmCode=gcmCode, altpredCode=altPredCode,
            dateCode=dateCode)
        return {'count' : scnCount}

    # ................................
    def _getScenario(self, pathScenarioId):
        """
        @summary: Attempt to get a scenario
        """
        scn = self.scribe.getScenario(int(pathScenarioId), fillLayers=True)
        
        if scn is None:
            raise cherrypy.HTTPError(404, 'Scenario {} not found'.format(
                                                                                        pathScenarioId))
        
        if checkUserPermission(self.getUserId(), scn, HTTPMethod.GET):
            return scn

        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User {} does not have permission to get scenario {}'.format(
                    self.getUserId(), pathScenarioId))
    
    # ................................
    def _listScenarios(self, userId, afterTime=None, altPredCode=None,
                       beforeTime=None, dateCode=None, epsgCode=None,
                       gcmCode=None, limit=100, offset=0):
        """Return a list of scenarios matching the specified criteria
        """
        scnAtoms = self.scribe.listScenarios(
            offset, limit, userId=userId, beforeTime=beforeTime,
            afterTime=afterTime, epsg=epsgCode, gcmCode=gcmCode,
            altpredCode=altPredCode, dateCode=dateCode)
        return scnAtoms

