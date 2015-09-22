"""
@summary: Module containing classes for Lifemapper Species Distribution 
             Modeling Scenarios service and object
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
from types import ListType, StringType, UnicodeType

from LmCommon.common.localconstants import ARCHIVE_USER

from LmServer.base.utilities import formatTimeUrl, getMjdTimeFromISO8601
from LmServer.common.log import LmPublicLogger

from LmWebServer.base.servicesBaseClass import buildAttListResponse, \
                                     getQueryParameters, RestService, WebObject
from LmWebServer.common.lmconstants import QueryParamNames, SERVICE_MOUNTS
from LmWebServer.services.common.userdata import DataPoster
from LmWebServer.services.sdm.layers import SDMLayersRestService

# =============================================================================
class SDMScenarioWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper SDM Scenario
   """
   subObjects = [
                ]
   subServices = [
                  {
                   "names" : SERVICE_MOUNTS["sdm"]["layers"],
                   "constructor" : SDMLayersRestService,
                   "idParameter" : "scenarioid"
                  }
                 ]
   interfaces = ['atom', 'eml', 'html', 'json', 'xml']
   
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: Deletes a scenario from the Lifemapper system
      """
      success = self.conn.deleteScenario(item)
      return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      scn = self.conn.getScenario(self.id)
      return scn
   
   # ............................................
   def processId(self, id):
      """
      @summary: Process the id given to the scenario
      """
      try:
         return int(id)
      except:
         return id

# =============================================================================
class SDMScenariosRestService(RestService):
   """
   @summary: Lifemapper SDM Scenarios Service
   @see: RestService
   """
   identifier = "scenarios"
   version = "2.0"
   summary = "SDM Scenarios Service"
   description = """SDM Scenarios Service"""
   WebObjectConstructor = SDMScenarioWebObject
   
   queryParameters = [
                   {
                    "name" : "afterTime",
                    "process" : lambda x: getMjdTimeFromISO8601(x)
                   },
                   {
                    "name" : "beforeTime",
                    "process" : lambda x: getMjdTimeFromISO8601(x)
                   },
                   {
                    "name" : "epsgCode",
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
                    "name" : "keyword",
                    "process" : lambda x: x
                   },
                   {
                    "name" : "matchingScenario",
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
      @summary: Counts the number of scenarios that match the provided query 
                   parameters
      """
      afterTime, beforeTime, epsg, page, perPage, kws, matchScn, public, \
           fullObjs = getQueryParameters(self.queryParameters, self.parameters)
      if kws is None:
         kws = []
      if isinstance(kws, StringType):
         kws = [kws]
      if public:
         user = ARCHIVE_USER
      else:
         user = self.user
      count = self.conn.countScenarios(userId=user,
                                     beforeTime=beforeTime, 
                                     afterTime=afterTime, 
                                     epsg=epsg,
                                     matchingId=matchScn, 
                                     kywdLst=kws)
      return count
   
   # ............................................
   def help(self):
      pass
   
   # ............................................
   def list(self):
      """
      @summary: Returns a list of scenarios that match the provided query
                   parameters
      """
      afterTime, beforeTime, epsg, page, perPage, kws, matchScn, public, \
          fullObjs = getQueryParameters(self.queryParameters, self.parameters)
      if page is None:
         page = 0
      if perPage is None:
         perPage = 100
      startRec = page * perPage
      if kws is None:
         kws = []
      if isinstance(kws, (StringType, UnicodeType)):
         kws = [kws]
      
      if fullObjs is None:
         fullObjs = False

      if public:
         user = ARCHIVE_USER
      else:
         user = self.user

      items = self.conn.listScenarios(startRec, 
                                    perPage, 
                                    userId=user, 
                                    beforeTime=beforeTime, 
                                    afterTime=afterTime,
                                    epsg=epsg,
                                    matchingId=matchScn, 
                                    kywdLst=kws,
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
                                   (QueryParamNames.EPSG_CODE, epsg),
                                   (QueryParamNames.MATCHING_SCENARIO, matchScn),
                                   (QueryParamNames.KEYWORD, kws),
                                   (QueryParamNames.PUBLIC, public),
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new SDM scenario from the inputs provided
      """
      log = LmPublicLogger()
      with DataPoster(self.user, log) as dp:
         ret = dp.postSDMScenario(self.parameters, self.body)
      return ret

   # ............................................
   def processTriggers(self, obj=None):
      """
      @summary: Process triggers passed as url parameters to the service
      """
      triggers = []
      if self.parameters.has_key("trigger"):
         triggers = self.parameters["trigger"]
         if not isinstance(triggers, ListType):
            triggers = [triggers]
         
      for trigger in triggers:
         pass
   
