"""
@summary: This module provides REST services for Projections

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

from LmCommon.common.lmconstants import JobStatus
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.processchain import MFChain
from LmServer.legion.sdmproj import SDMProjection
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathProjectionId')
class SdmProjectService(LmService):
   """
   @summary: This class is for the projections service.  The dispatcher is
                responsible for calling the correct method
   """
   # ................................
   def DELETE(self, pathProjectionId):
      """
      @summary: Attempts to delete a projection
      @param pathProjectionId: The id of the projection to delete
      """
      prj = self.scribe.getSDMProject(int(pathProjectionId))
      
      if prj is None:
         raise cherrypy.HTTPError(404, 'Projection {} not found'.format(
                                                                 pathProjectionId))
      
      if checkUserPermission(self.getUserId(), prj, HTTPMethod.DELETE):
         success = self.scribe.deleteObject(prj)
         if success:
            cherrypy.response.status = 204
            return
         else:
            raise cherrypy.HTTPError(500, 
                         'Failed to delete projection {}'.format(pathProjectionId))
      else:
         raise cherrypy.HTTPError(403, 
            'User {} does not have permission to delete projection {}'.format(
               self.getUserId(), pathProjectionId))

   # ................................
   @lmFormatter
   def GET(self, pathProjectionId=None, afterTime=None, algorithmCode=None, 
                 beforeTime=None, displayName=None, epsgCode=None, limit=100, 
                 modelScenarioCode=None, occurrenceSetId=None, offset=0, 
                 projectionScenarioCode=None, public=None, scenarioId=None, 
                 status=None):
      """
      @summary: Performs a GET request.  If a projection id is provided,
                   attempt to return that item.  If not, return a list of 
                   projections that match the provided parameters
      """
      if public:
         userId = PUBLIC_USER
      else:
         userId = self.getUserId()

      if pathProjectionId is None:
         return self._listProjections(userId, afterTime=afterTime, 
                                 algCode=algorithmCode, beforeTime=beforeTime, 
                                 displayName=displayName, epsgCode=epsgCode, 
                                 limit=limit, mdlScnCode=modelScenarioCode, 
                                 occurrenceSetId=occurrenceSetId, offset=offset,
                                 prjScnCode=projectionScenarioCode, 
                                 status=status)
      elif pathProjectionId.lower() == 'count':
         return self._countProjections(userId, afterTime=afterTime, 
                                 algCode=algorithmCode, beforeTime=beforeTime, 
                                 displayName=displayName, epsgCode=epsgCode, 
                                 mdlScnCode=modelScenarioCode, 
                                 occurrenceSetId=occurrenceSetId, 
                                 prjScnCode=projectionScenarioCode, 
                                 status=status)
      else:
         return self._getProjection(pathProjectionId)
   
   # ................................
   #@cherrypy.tools.json_in
   #@cherrypy.tools.json_out
   @lmFormatter
   def POST(self):
      """
      @summary: Posts a new projection
      @todo: User id
      """
      projectionData = cherrypy.request.json
      
      try:
         occSetId = int(projectionData['occurrenceSetId'])
         occ = self.scribe.getOccurrenceSet(occId=occSetId)
         algoCode = projectionData['algorithmCode']
         modelScenarioId = projectionData['modelScenario']
         mdlScn = self.scribe.getScenario(modelScenarioId)
         prjScns = []
         for prjScn in projectionData['projectionScenario']:
            prjScns.append(self.scribe.getScenario(int(prjScn)))
      except KeyError, ke:
         raise cherrypy.HTTPError(400, 
                            'Missing projection parameter: {}'.format(str(ke)))

      # TODO: Process masks and maybe others like metadata

      chain = MFChain(self.getUserId(), status=JobStatus.GENERAL)
      insMFChain = self.scribe.insertMFChain(chain)
      
      rules = []
      
      algo = Algorithm(algoCode)
      
      for prjScn in prjScns:
         prj = SDMProjection(occ, algo, mdlScn, prjScn)
         newPrj = self.scribe.findOrInsertSDMProject(prj)
         rules.extend(newPrj.computeMe())
         
      insMFChain.addCommands(rules)
      insMFChain.write()
      insMFChain.updateStatus(JobStatus.INITIALIZE)
      self.scribe.updateObject(insMFChain)
         
      # TODO: What do we return?
      cherrypy.response.status = 202
      return insMFChain
   
   
   # ................................
   #@cherrypy.json_in
   #@cherrypy.json_out
   #def PUT(self, pathProjectionId):
   #   pass
   
   # ................................
   def _countProjections(self, userId, afterTime=None, algCode=None, 
                        beforeTime=None, displayName=None, epsgCode=None,
                        mdlScnCode=None, occurrenceSetId=None, prjScnCode=None, 
                        status=None):
      """
      @summary: Return a count of projections matching the specified criteria
      """
      afterStatus = None
      beforeStatus = None

      # Process status parameter
      if status:
         if status < JobStatus.COMPLETE:
            beforeStatus = JobStatus.COMPLETE - 1
         elif status == JobStatus.COMPLETE:
            beforeStatus = JobStatus.COMPLETE + 1
            afterStatus = JobStatus.COMPLETE - 1
         else:
            afterStatus = status - 1
   
      prjCount = self.scribe.countSDMProjects(userId=userId,
                           displayName=displayName, afterTime=afterTime, 
                           beforeTime=beforeTime, epsg=epsgCode, 
                           afterStatus=afterStatus, beforeStatus=beforeStatus, 
                           occsetId=occurrenceSetId, algCode=algCode, 
                           mdlscenCode=mdlScnCode, prjscenCode=prjScnCode)
      return {"count" : prjCount}

   # ................................
   def _getProjection(self, pathProjectionId):
      """
      @summary: Attempt to get a projection
      """
      prj = self.scribe.getSDMProject(int(pathProjectionId))
      
      if prj is None:
         raise cherrypy.HTTPError(404, 'Projection {} not found'.format(
                                                                 pathProjectionId))
      
      if checkUserPermission(self.getUserId(), prj, HTTPMethod.GET):
         return prj
      else:
         raise cherrypy.HTTPError(403, 
            'User {} does not have permission to delete projection {}'.format(
               self.getUserId(), pathProjectionId))

   # ................................
   def _listProjections(self, userId, afterTime=None, algCode=None, 
                        beforeTime=None, displayName=None, epsgCode=None,
                        limit=100, mdlScnCode=None, occurrenceSetId=None, 
                        offset=0, prjScnCode=None, status=None):
      """
      @summary: Return a list of projections matching the specified criteria
      """
      afterStatus = None
      beforeStatus = None

      # Process status parameter
      if status:
         if status < JobStatus.COMPLETE:
            beforeStatus = JobStatus.COMPLETE - 1
         elif status == JobStatus.COMPLETE:
            beforeStatus = JobStatus.COMPLETE + 1
            afterStatus = JobStatus.COMPLETE - 1
         else:
            afterStatus = status - 1
   
      prjAtoms = self.scribe.listSDMProjects(offset, limit, userId=userId,
                           displayName=displayName, afterTime=afterTime, 
                           beforeTime=beforeTime, epsg=epsgCode, 
                           afterStatus=afterStatus, beforeStatus=beforeStatus, 
                           occsetId=occurrenceSetId, algCode=algCode, 
                           mdlscenCode=mdlScnCode, prjscenCode=prjScnCode)
      return prjAtoms
   