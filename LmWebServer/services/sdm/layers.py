"""
@summary: Module containing classes for Lifemapper Species Distribution 
             Modeling Layers service and object
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


from LmServer.base.utilities import formatTimeUrl, getMjdTimeFromISO8601
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import LmPublicLogger

from LmWebServer.base.servicesBaseClass import buildAttListResponse, \
                                     getQueryParameters, RestService, WebObject
from LmWebServer.common.lmconstants import QueryParamNames
from LmWebServer.services.common.userdata import DataPoster
                                          
# =============================================================================
class SDMLayerWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper SDM Layer
   """
   subObjects = [
                ]
   subServices = [
                 ]
   interfaces = ['atom', 'eml', 'html', 'json', 'kml', 'wcs', 'wms', 'xml']
   
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: Deletes a layer from the Lifemapper system
      """
      # At some point this should bubble up more information if the layer is 
      #    connected to a scenario
      success = self.conn.deleteEnvLayer(item)
      return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      exp = self.conn.getLayer(self.id)
      return exp

# =============================================================================
class SDMLayersRestService(RestService):
   """
   @summary: Lifemapper SDM Layers Service
   @see: Service
   """
   identifier = "layers"
   version = "2.0"
   summary = "SDM Layers Service"
   description = """SDM Layers Service"""
   WebObjectConstructor = SDMLayerWebObject
   
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
                    "name" : "isCategorical",
                    "process" : lambda x: bool(int(x))
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
                    "name" : "scenarioId",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "public",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "typeCode",
                    "process" : lambda x: x
                   },
                   {
                    "name" : "fullObjects",
                    "process" : lambda x: bool(int(x))
                   }
                  ]
   
   # ............................................
   def count(self):
      """
      @summary: Counts the number of layers that match the provided query 
                   parameters
      """
      afterTime, beforeTime, epsg, isCategorical, page, perPage, scnId, \
            public, typeCode, fullObjs = \
                      getQueryParameters(self.queryParameters, self.parameters)
      if public:
         user = PUBLIC_USER
      else:
         user = self.user
      count = self.conn.countLayers(userId=user, 
                                    typecode=typeCode,
                                  beforeTime=beforeTime, 
                                  afterTime=afterTime,
                                  epsg=epsg, 
                                  isCategorical=isCategorical,
                                  scenarioId=scnId)
      return count
   
   # ............................................
   def help(self):
      pass
   
   # ............................................
   def list(self):
      """
      @summary: Returns a list of layers that match the provided query
                   parameters
      """
      afterTime, beforeTime, epsg, isCategorical, page, perPage, scnId, \
              public, typeCode, fullObjs = \
                      getQueryParameters(self.queryParameters, self.parameters)
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
      items = self.conn.listLayers(startRec, 
                                 perPage, 
                                 userId=user, 
                                 typecode=typeCode,
                                 beforeTime=beforeTime, 
                                 afterTime=afterTime,
                                 epsg=epsg,
                                 isCategorical=isCategorical,
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
                                   (QueryParamNames.EPSG_CODE, epsg),
                                   (QueryParamNames.TYPE_CODE, typeCode),
                                   (QueryParamNames.BEFORE_TIME, formatTimeUrl(beforeTime)),
                                   (QueryParamNames.AFTER_TIME, formatTimeUrl(afterTime)),
                                   (QueryParamNames.IS_CATEGORICAL, int(isCategorical) if isCategorical is not None else None),
                                   (QueryParamNames.SCENARIO_ID, scnId),
                                   (QueryParamNames.PUBLIC, public),
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new SDM layer from the inputs provided
      """
      log = LmPublicLogger()
      with DataPoster(self.user, log) as dp:
         ret = dp.postSDMLayer(self.parameters, self.body)
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
   
