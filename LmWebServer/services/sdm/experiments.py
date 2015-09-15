"""
@summary: Module containing classes for Lifemapper Species Distribution 
             Modeling Experiments service and object
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
from types import ListType

from LmCommon.common.localconstants import ARCHIVE_USER

from LmServer.base.utilities import formatTimeUrl, getMjdTimeFromISO8601
from LmServer.common.log import LmPublicLogger

from LmWebServer.base.servicesBaseClass import buildAttListResponse, \
                                     getQueryParameters, RestService, WebObject
from LmWebServer.common.lmconstants import QueryParamNames, SERVICE_MOUNTS

from LmWebServer.services.common.userdata import DataPoster
from LmWebServer.services.sdm.processes import SDMExperiment
from LmWebServer.services.sdm.projections import SDMProjectionsRestService
from LmWebServer.services.triggers.email import TriggerEmail
                                          
# =============================================================================
class SDMExpWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper SDM Experiment
   """
   subObjects = [
                 {
                  "names" : ["algorithm", "algo"],
                  "func" : lambda x: x.algorithm
                 },
                 {
                  "names" : ["occurrences", "occ"],
                  "func" : lambda x: x.model.occurrenceSet
                 },
                 {
                  "names" : ["scenario", "scen", "scn"],
                  "func" : lambda x: x.model._scenario
                 }
                ]
   subServices = [
                  {
                   "names" : SERVICE_MOUNTS["sdm"]["projections"],
                   "constructor" : SDMProjectionsRestService,
                   "idParameter" : "experimentid"
                  }
                 ]
   interfaces = ['atom', 'eml', 'html', 'json', 'model', 'narrative', 'xml']
   
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: Deletes an experiment from the Lifemapper system
      """
      success = self.conn.deleteExperiment(item.model)
      return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      exp = self.conn.getExperimentForModel(self.id)
      # Commented this out.  We don't really want to return the points
      #exp.model.occurrenceSet.readShapefile()
      return exp
   
# =============================================================================
class SDMExpRestService(RestService):
   """
   @summary: Lifemapper SDM Experiments Service
   @see: RestService
   """
   identifier = "experiments"
   version = "2.0"
   summary = "SDM Experiments Service"
   description = """SDM Experiments Service"""
   WebObjectConstructor = SDMExpWebObject
   
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
                    "name" : "public",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "fullObjects",
                    "process" : lambda x: bool(int(x))
                   }

                  ]
   #processes = ["omexperiment"]
   processTypes = {SDMExperiment.identifier : SDMExperiment}
   subServices = [
                  {
                   "names" : [SDMExperiment.identifier], 
                   "constructor" : SDMExperiment,
                   "idParameter" : "experimentId"
                  }
                 ]
   # Variables established in constructor:
   #  self.method - HTTP method used for service access
   #  self.user - User id to use
   #  self.body - body of HTTP message
   #  self.vpath - List of path variables
   #  self.parameters - Dictionary of url query parametes
   
   # ............................................
   def count(self):
      """
      @summary: Counts the number of experiments that match the provided query 
                   parameters
      """
      afterTime, algCode, beforeTime, displayName, epsg, occSetId, \
              page, perPage, status, public, fullObjs = \
                                      getQueryParameters(self.queryParameters, 
                                                         self.parameters)
      if public:
         user = ARCHIVE_USER
      else:
         user = self.user
      count = self.conn.countModels(userId=user, 
                                  displayName=displayName, 
                                  beforeTime=beforeTime, 
                                  afterTime=afterTime, 
                                  epsg=epsg, 
                                  status=status, 
                                  occSetId=occSetId, 
                                  algCode=algCode)
      return count
   
   # ............................................
   def help(self):
      pass
   
   # ............................................
   def list(self):
      """
      @summary: Returns a list of experiments that match the provided query
                   parameters
      """
      afterTime, algCode, beforeTime, displayName, epsg, occSetId, \
              page, perPage, status, public, fullObjs = \
                                      getQueryParameters(self.queryParameters, 
                                                         self.parameters)
      if page is None:
         page = 0
      if perPage is None:
         perPage = 100
      if public:
         user = ARCHIVE_USER
      else:
         user = self.user
      
      if fullObjs is None:
         fullObjs = False

      startRec = page * perPage

      items = self.conn.listModels(startRec, 
                                 perPage, 
                                 userId=user, 
                                 displayName=displayName, 
                                 beforeTime=beforeTime, 
                                 afterTime=afterTime, 
                                 epsg=epsg,
                                 status=status, 
                                 occSetId=occSetId, 
                                 algCode=algCode,
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
                                   (QueryParamNames.MODEL_STATUS, status),
                                   (QueryParamNames.OCCURRENCE_SET_ID, occSetId),
                                   (QueryParamNames.ALGO_CODE, algCode),
                                   (QueryParamNames.PUBLIC, public),
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new SDM experiment from the inputs provided
      """
      log = LmPublicLogger()
      with DataPoster(self.user, log) as dp:
         ret = dp.postSDMExperimentRest(self.parameters, self.body)
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
         if trigger.lower().startswith("email:") and obj is not None:
            addrs = trigger.lower().split("email:")[1].split(",")
            emailer = TriggerEmail()
            emailer.postSDMExperiment(obj, addrs)
   
#    # ............................................
#    def wps(self):
#       """
#       @summary: End-point for wps services
#       """
#       pass
#       #pTypes = {
#       #            OMExperiment.identifier : OMExperiment
#       #         }
#       #s = WPSService(pTypes, self.method, self.user, self.body, self.vpath, 
#       #                                                         self.parameters)
#       #return s.doAction()

