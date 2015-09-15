"""
@summary: Module containing classes for Lifemapper Range and Diversity 
             Ancillary Layers service and object
@author: CJ Grady
@version: 1.0
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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

from LmWebServer.base.servicesBaseClass import buildAttListResponse, \
                                     getQueryParameters, RestService, WebObject
from LmWebServer.common.lmconstants import QueryParamNames, SERVICE_MOUNTS

from LmWebServer.services.common.userdata import DataPoster

# =============================================================================
class RADAncLayerWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper RAD Ancillary Layer
   """
   subObjects = []
   subServices = [
                 ]
   interfaces = ['atom', 'html', 'json', 'xml']
   
#    # ............................................
#    def _deleteItem(self, item):
#       """
#       @summary: Deletes a RAD ancillary layer from an experiment
#       """
#       experimentId = int(self.parameters["experimentid"])
#       success = self.conn.deleteAncillaryLayer(item, experimentId)
#       return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      # Need function to get a PA layer
      bucket = self.conn.getRADLayer(anclyrid=self.id)
      return bucket
   
# =============================================================================
class RADAncLayersRestService(RestService):
   """
   @summary: Lifemapper RAD Ancillary Layers Service
   @see: RestService
   """
   identifier = "anclayers"
   version = "1.0"
   summary = "RAD Ancillary Layers Service"
   description = """RAD Ancillary Layers Service"""
   WebObjectConstructor = RADAncLayerWebObject
   
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
                    "name" : "experimentId",
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
                    "name" : "ancillaryValueId",
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
      @summary: Counts the number of buckets that match the provided query 
                   parameters
      """
      afterTime, ancValueId, beforeTime, epsgCode, experimentId, layerId, \
                      layerName, page, perPage, fullObjs = \
                      getQueryParameters(self.queryParameters, self.parameters)
      count = self.conn.countAncillaryLayers(self.user, 
                                           beforeTime=beforeTime,
                                           afterTime=afterTime,
                                           epsg=epsgCode,
                                           layerId=layerId,
                                           layerName=layerName,
                                           ancillaryValueId=ancValueId,
                                           expId=experimentId)
      return count
   
   # ............................................
   def help(self):
      pass
   
   # ............................................
   def list(self):
      """
      @summary: Returns a list of buckets that match the provided query
                   parameters
      """
      afterTime, ancValueId, beforeTime, epsgCode, experimentId, layerId, \
                      layerName, page, perPage, fullObjs = \
                      getQueryParameters(self.queryParameters, self.parameters)
      if page is None:
         page = 0
      if perPage is None:
         perPage = 100
      startRec = page * perPage

      if fullObjs is None:
         fullObjs = False

      items = self.conn.listAncillaryLayers(
                                           startRec, 
                                           perPage,
                                           self.user,
                                           beforeTime=beforeTime,
                                           afterTime=afterTime,
                                           epsg=epsgCode,
                                           layerId=layerId,
                                           layerName=layerName,
                                           ancillaryValueId=ancValueId,
                                           expId=experimentId,
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
                                   (QueryParamNames.EPSG_CODE, epsgCode),
                                   (QueryParamNames.LAYER_ID, layerId),
                                   (QueryParamNames.LAYER_NAME, layerName),
                                   (QueryParamNames.ANCILLARY_VALUE_ID, ancValueId),
                                   (QueryParamNames.EXPERIMENT_ID, experimentId),
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new RAD bucket from the inputs provided
      """
      with DataPoster(self.user, self.conn.log) as dp:
         ret = db.postRADAncLayer(self.parameters, self.body)
      return ret

