"""
@summary: This module provides REST services for Scenario packages

@author: CJ Grady
@version: 2.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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

#from LmServer.legion.scenario import Scenario
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathScenarioPackageId')
class ScenarioPackageService(LmService):
   """
   @summary: This class is for the scenario packagess service.  The dispatcher is
                responsible for calling the correct method
   """
   # ................................
   #def DELETE(self, pathScenarioId):
   #   """
   #   @summary: Attempts to delete a scenario
   #   @param projectionId: The id of the scenario to delete
   #   """
   #   scn = self.scribe.getScenario(int(pathScenarioId))
   #   
   #   if scn is None:
   #      raise cherrypy.HTTPError(404, 'Scenario {} not found'.format(
   #                                                               pathScenarioId))
   #   
   #   if checkUserPermission(self.getUserId(), scn, HTTPMethod.DELETE):
   #      success = self.scribe.deleteObject(scn)
   #      if success:
   #         cherrypy.response.status = 204
   #         return
   #      else:
   #         raise cherrypy.HTTPError(500, 
   #                          'Failed to delete scenario {}'.format(pathScenarioId))
   #   else:
   #      raise cherrypy.HTTPError(403,
   #            'User {} does not have permission to delete scenario {}'.format(
   #               self.getUserId(), pathScenarioId))

   # ................................
   @lmFormatter
   def GET(self, pathScenarioPackageId=None, afterTime=None, beforeTime=None,  
                 epsgCode=None, limit=100, offset=0, urlUser=None, 
                 scenarioId=None):
      """
      @summary: Performs a GET request.  If a scenario package id is provided,
                   attempt to return that item.  If not, return a list of 
                   scenario packagess that match the provided parameters
      """
      if pathScenarioPackageId is None:
         return self._listScenarioPackages(self.getUserId(urlUser=urlUser), 
                       afterTime=afterTime, scenarioId=scenarioId, limit=limit,
                       offset=offset, epsgCode=epsgCode)
      elif pathScenarioPackageId.lower() == 'count':
         return self._countScenarioPackages(self.getUserId(urlUser=urlUser), 
                                 afterTime=afterTime, beforeTime=beforeTime, 
                                 scenarioId=scenarioId, epsgCode=epsgCode)
      else:
         return self._getScenarioPackage(pathScenarioPackageId)
   
   # ................................
   #@cherrypy.tools.json_in
   #@cherrypy.tools.json_out
   #@lmFormatter
   #def POST(self):
   #   """
   #   @summary: Posts a new scenario
   #   """
   #   layers = []
   #   scnModel = cherrypy.request.json
   #
   #   try:
   #      code = scnModel['code']
   #      epsgCode = int(scnModel['epsgCode'])
   #      rawLayers = scnModel['layers']
   #   except KeyError, ke:
   #      # If one of these is missing, we have a bad request
   #      raise cherrypy.HTTPError(400, 
   #         'code, epsgCode, and layers are required parameters for scenarios')
   #   except Exception, e:
   #      # TODO: Log error
   #      raise cherrypy.HTTPError(500, 'Unknown error: {}'.format(str(e)))
      
   #   metadata = scnModel.get('metadata', {})
   #   units = scnModel.get('units', None)
   #   resolution = scnModel.get('resolution', None)
   #   gcmCode = scnModel.get('gcmCode', None)
   #   altPredCode = scnModel.get('altPredCode', None)
   #   dateCode = scnModel.get('dateCode', None)
   #
   #   # Process layers, assume they are Lifemapper IDs for now
   #   for lyrId in rawLayers:
   #      layers.append(int(lyrId))
   #   
   #   scn = Scenario(code, self.getUserId(), epsgCode, metadata=metadata, 
   #                  units=units, res=resolution, gcmCode=gcmCode, 
   #                  altpredCode=altPredCode, dateCode=dateCode, layers=layers)
   #   newScn = self.scribe.findOrInsertScenario(scn)
   #   
   #   return newScn
   
   # ................................
   #@cherrypy.tools.json_in
   #@cherrypy.tools.json_out
   #def PUT(self, pathScenarioId):
   #   pass
   
   # ................................
   def _countScenarioPackages(self, userId, afterTime=None, beforeTime=None,  
                             epsgCode=None, scenarioId=None):
      """
      @summary: Return the number of scenario packages that match the specified
                   criteria
      """
      scnPkgCount = self.scribe.countScenPackages(userId=userId, 
                                    beforeTime=beforeTime, afterTime=afterTime,
                                    epsg=epsgCode, scenId=scenarioId)
      return {'count' : scnPkgCount}

   # ................................
   def _getScenarioPackage(self, pathScenarioPackageId):
      """
      @summary: Attempt to get a scenario
      """
      scnPkg = self.scribe.getScenPackage(scenPkgId=pathScenarioPackageId)
      
      if scnPkg is None:
         raise cherrypy.HTTPError(404, 'Scenario package{} not found'.format(
                                                        pathScenarioPackageId))
      
      if checkUserPermission(self.getUserId(), scnPkg, HTTPMethod.GET):
         return scnPkg

      else:
         raise cherrypy.HTTPError(403,
               'User {} does not have permission to get scenario package {}'.format(
                  self.getUserId(), pathScenarioPackageId))
   
   # ................................
   def _listScenarioPackages(self, userId, afterTime=None, beforeTime=None,  
                             epsgCode=None, scenarioId=None, limit=100, offset=0):
      """
      @summary: Return a list of scenarios matching the specified criteria
      """
      scnPkgAtoms = self.scribe.listScenPackages(offset, limit, userId=userId, 
                                    beforeTime=beforeTime, afterTime=afterTime,
                                    scenId=scenarioId, epsg=epsgCode)
      return scnPkgAtoms

