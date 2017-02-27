"""
@summary: Module containing classes for Lifemapper Species Distribution 
             Modeling Projections service and object
@author: CJ Grady
@version: 2.0
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
from LmServer.base.utilities import formatTimeUrl, getMjdTimeFromISO8601
from LmServer.common.localconstants import PUBLIC_USER

from LmWebServer.base.servicesBaseClass import (buildAttListResponse, 
                                     getQueryParameters, RestService, WebObject)
from LmWebServer.common.lmconstants import QueryParamNames
                                          
# =============================================================================
class SDMProjectionWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper SDM Experiment
   """
   subObjects = [
                 {
                  "names" : ["scenario", "scen", "scn"],
                  "func" : lambda x: x.getScenario()
                 }
                ]
   subServices = [
                 ]
   interfaces = ['atom', 'eml', 'html', 'json', 'kml', 'wcs', 'wms', 'xml']
   
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: Deletes a projection from the Lifemapper system
      """
      success = self.conn.deleteProjection(item)
      return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      exp = self.conn.getProjectionById(self.id)
      return exp

 # =============================================================================
class SDMProjectionsRestService(RestService):
   """
   @summary: Lifemapper SDM Projections Service
   @see: Service
   """
   identifier = "projections"
   version = "2.0"
   summary = "SDM projections Service"
   description = """SDM Projections Service"""
   WebObjectConstructor = SDMProjectionWebObject
   
   queryParameters = [
                   {
                    "name" : "afterTime",
                    "process" : lambda x: getMjdTimeFromISO8601(x)
                   },
                   {
                    "name" : "algorithmCode",
                    "process" : lambda x: x
                   },
                   {
                    "name" : "beforeTime",
                    "process" : lambda x: getMjdTimeFromISO8601(x)
                   },
                   {
                    "name" : "displayName",
                    "process" : lambda x: x
                   },
                   {
                    "name" : "epsgCode",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "occurrenceSetId",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "page",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "perPage",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "status",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "experimentId",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "scenarioId",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "public",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "fullObjects",
                    "process" : lambda x: bool(int(x))
                   }

                  ]
   
   # ............................................
   def count(self):
      """
      @summary: Counts the number of projections that match the provided query 
                   parameters
      """
      afterTime, algCode, beforeTime, displayName, epsg, occSetId, \
         page, perPage, status, expId, scnId, public, fullObjs = getQueryParameters(
                                                      self.queryParameters, 
                                                      self.parameters)

      if public:
         user = PUBLIC_USER
      else:
         user = self.user
      count = self.conn.countProjections(userId=user, 
                                       displayName=displayName, 
                                       beforeTime=beforeTime, 
                                       afterTime=afterTime, 
                                       epsg=epsg,
                                       status=status, 
                                       occSetId=occSetId, 
                                       mdlId=expId, 
                                       algCode=algCode, 
                                       scenarioId=scnId)
      return count
   
   # ............................................
   def help(self):
      pass
   
   # ............................................
   def list(self):
      """
      @summary: Returns a list of projections that match the provided query
                   parameters
      """
      afterTime, algCode, beforeTime, displayName, epsg, occSetId, \
         page, perPage, status, expId, scnId, public, fullObjs = getQueryParameters(
                                                      self.queryParameters, 
                                                      self.parameters)
      if page is None:
         page = 0
      if perPage is None:
         perPage = 100
      startRec = page * perPage

      if fullObjs is None:
         fullObjs = False

      if public:
         user = PUBLIC_USER
      else:
         user = self.user
         
      items = self.conn.listProjections(startRec, 
                                      perPage, 
                                      userId=user, 
                                      displayName=displayName, 
                                      beforeTime=beforeTime, 
                                      afterTime=afterTime, 
                                      epsg=epsg,
                                      status=status, 
                                      occSetId=occSetId, 
                                      mdlId=expId, 
                                      algCode=algCode, 
                                      scenarioId=scnId,
                                      atom=not fullObjs)
      for item in items:
         item.url = "%s/%s" % (self.basePath, item.id)
      return buildAttListResponse(items,
                                  self.count(),
                                  self.user,
                                  [
                                   (QueryParamNames.PAGE, page),
                                   (QueryParamNames.PER_PAGE, perPage),
                                   (QueryParamNames.BEFORE_TIME, formatTimeUrl(beforeTime)),
                                   (QueryParamNames.AFTER_TIME, formatTimeUrl(afterTime)),
                                   (QueryParamNames.DISPLAY_NAME, displayName),
                                   (QueryParamNames.EPSG_CODE, epsg),
                                   (QueryParamNames.PROJECTION_STATUS, status),
                                   (QueryParamNames.OCCURRENCE_SET_ID, occSetId),
                                   (QueryParamNames.MODEL_ID, expId),
                                   (QueryParamNames.ALGO_CODE, algCode),
                                   (QueryParamNames.SCENARIO_ID, scnId),
                                   (QueryParamNames.PUBLIC, public),
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
