"""
@summary: Module containing classes for Lifemapper Range and Diversity 
             Layers service and object
@author: CJ Grady
@version: 1.0
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
from LmServer.common.log import LmPublicLogger

from LmWebServer.base.servicesBaseClass import buildAttListResponse, \
                                     getQueryParameters, RestService, WebObject
from LmWebServer.common.lmconstants import QueryParamNames
from LmWebServer.services.common.userdata import DataPoster
from LmServer.base.layer import Vector

# =============================================================================
class RADLayerWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper RAD Layer
   """
   subObjects = [
                ]
   subServices = [
                 ]
   interfaces = ['atom', 'eml', 'html', 'json', 'narrative', 'xml']
   
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: Deletes a RAD layer from the Lifemapper system
      """
      success = self.conn.deleteRADLayer(item)
      return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      layer = self.conn.getRADLayer(lyrid=self.id)
      return layer
   
# =============================================================================
class RADLayersRestService(RestService):
   """
   @summary: Lifemapper RAD Layers Service
   @see: RestService
   """
   identifier = "layers"
   version = "1.0"
   summary = "RAD Layers Service"
   description = """RAD Layers Service"""
   WebObjectConstructor = RADLayerWebObject
   
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
                    "name" : "layerId",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "layerName",
                    "process" : lambda x: x.strip()
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
                    "name" : "fullObjects",
                    "process" : lambda x: bool(int(x))
                   }
                  ]
   processes = []
   
   # ............................................
   def count(self):
      """
      @summary: Counts the number of layers that match the provided query 
                   parameters
      """
      afterTime, beforeTime, epsg, layerId, layerName, page, perPage, \
           fullObjs = getQueryParameters(self.queryParameters, self.parameters)
      count = self.conn.countRADLayers(self.user, 
                                     beforeTime=beforeTime,
                                     afterTime=afterTime,
                                     epsg=epsg,
                                     lyrname=layerName)
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
      afterTime, beforeTime, epsg, layerId, layerName, page, perPage, \
           fullObjs = getQueryParameters(self.queryParameters, self.parameters)
      if page is None:
         page = 0
      if perPage is None:
         perPage = 100
      startRec = page * perPage

      if fullObjs is None:
         fullObjs = False

      items = self.conn.listRADLayers(startRec, 
                                    perPage, 
                                    self.user, 
                                    beforeTime=beforeTime,
                                    afterTime=afterTime, 
                                    epsg=epsg,
                                    layerId=layerId,
                                    layerName=layerName,
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
                                   (QueryParamNames.LAYER_ID, layerId),
                                   (QueryParamNames.LAYER_NAME, layerName),
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new RAD layer from the inputs provided
      """
      log = LmPublicLogger()
      dp = DataPoster(self.user, log)
      return dp.postRADLayer(self.parameters, self.body)

