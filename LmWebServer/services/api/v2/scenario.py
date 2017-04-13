"""
@summary: This module provides REST services for Scenario

@author: CJ Grady
@version: 2.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""

import cherrypy

from LmServer.common.localconstants import PUBLIC_USER
from LmServer.legion.scenario import Scenario
from LmWebServer.formatters.jsonFormatter import objectFormatter
from LmWebServer.services.api.v2.base import LmService

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('scenarioId')
class Scenario(LmService):
   """
   @summary: This class is for the scenarios service.  The dispatcher is
                responsible for calling the correct method
   """
   # ................................
   def DELETE(self, scenarioId):
      """
      @summary: Attempts to delete a scenario
      @param projectionId: The id of the scenario to delete
      """
      scn = self.scribe.getScenario(scenarioId)
      
      if scn is None:
         raise cherrypy.HTTPError(404, 'Scenario {} not found'.format(
                                                                  scenarioId))
      
      if scn.getUserId() == self.getUserId():
         success = self.scribe.deleteObject(scn)
         if success:
            cherrypy.response.status = 204
            return
         else:
            raise cherrypy.HTTPError(500, 
                             'Failed to delete scenario {}'.format(scenarioId))
      else:
         raise cherrypy.HTTPError(403,
               'User {} does not have permission to delete scenario {}'.format(
                  self.getUserId(), scenarioId))

   # ................................
   def GET(self, scenarioId=None, afterTime=None, alternatePredictionCode=None,
           beforeTime=None, dateCode=None, epsgCode=None, gcmCode=None, 
           limit=100, offset=0, public=None):
      """
      @summary: Performs a GET request.  If a scenario id is provided,
                   attempt to return that item.  If not, return a list of 
                   scenarios that match the provided parameters
      """
      if scenarioId is None:
         if public:
            userId = PUBLIC_USER
         else:
            userId = self.getUserId()
            
         return self._listScenarios(userId, afterTime=afterTime,
                      altPredCode=alternatePredictionCode, 
                      beforeTime=beforeTime, dateCode=dateCode, 
                      epsgCode=epsgCode, gcmCode=gcmCode, limit=limit, 
                      offset=offset)
      else:
         return self._getScenario(scenarioId)
   
   # ................................
   #@cherrypy.tools.json_in
   #@cherrypy.tools.json_out
   def POST(self):
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
         raise cherrypy.HTTPError(400, 
            'code, epsgCode, and layers are required parameters for scenarios')
      except Exception, e:
         # TODO: Log error
         raise cherrypy.HTTPError(500, 'Unknown error: {}'.format(str(e)))
      
      metadata = scnModel.get('metadata', {})
      units = scnModel.get('units', None)
      resolution = scnModel.get('resolution', None)
      gcmCode = scnModel.get('gcmCode', None)
      altPredCode = scnModel.get('altPredCode', None)
      dateCode = scnModel.get('dateCode', None)

      # Process layers, assume they are Lifemapper IDs for now
      for lyrId in rawLayers:
         layers.append(int(lyrId))
      
      scn = Scenario(code, self.getUserId(), epsgCode, metadata=metadata, 
                     units=units, res=resolution, gcmCode=gcmCode, 
                     altpredCode=altPredCode, dateCode=dateCode, layers=layers)
      newScn = self.scribe.findOrInsertScenario(scn)
      
      # TODO: Return or format
      return objectFormatter(newScn)
   
   # ................................
   #@cherrypy.tools.json_in
   #@cherrypy.tools.json_out
   #def PUT(self, scenarioId):
   #   pass
   
   # ................................
   def _getScenario(self, scenarioId):
      """
      @summary: Attempt to get a scenario
      """
      scn = self.scribe.getScenario(scenarioId)
      
      if scn is None:
         raise cherrypy.HTTPError(404, 'Scenario {} not found'.format(
                                                                  scenarioId))
      
      if scn.getUserId() in [self.getUserId(), PUBLIC_USER]:
         
         # TODO: Return or format
         return objectFormatter(scn)

      else:
         raise cherrypy.HTTPError(403,
               'User {} does not have permission to get scenario {}'.format(
                  self.getUserId(), scenarioId))
   
   # ................................
   def _listScenarios(self, userId, afterTime=None, altPredCode=None,  
                      beforeTime=None, dateCode=None, epsgCode=None, 
                      gcmCode=None, limit=100, offset=0):
      """
      @summary: Return a list of scenarios matching the specified criteria
      """
      scnAtoms = self.scribe.listScenarios(offset, limit, userId=userId, 
                                    beforeTime=beforeTime, afterTime=afterTime,
                                    epsg=epsgCode, gcmCode=gcmCode,
                                    altpredCode=altPredCode, dateCode=dateCode)
      # TODO: Return or format
      return objectFormatter(scnAtoms)

