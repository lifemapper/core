"""
@summary: Module containing classes for Lifemapper Range and Diversity 
             Buckets service and object
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
from LmWebServer.common.lmconstants import QueryParamNames, SERVICE_MOUNTS

from LmWebServer.services.common.userdata import DataPoster
from LmWebServer.services.rad.pamsums import RADPamSumsRestService
from LmWebServer.services.rad.processes import IntersectProcess, RandomizeProcess

# =============================================================================
class RADBucketWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper RAD Bucket
   """
   subObjects = [
                 {
                  "names" : ["shapegrid"],
                  "func" : lambda x: x.shapegrid
                 }
                ]
   subServices = [
                  {
                   "names" : SERVICE_MOUNTS["rad"]['pamsums'],
                   "constructor" : RADPamSumsRestService,
                   "idParameter" : "bucketId"
                  },
                  {
                   "names" : [IntersectProcess.identifier],
                   "constructor" : IntersectProcess,
                   "idParameter" : "bucketId"
                  },
                  {
                   "names" : [RandomizeProcess.identifier],
                   "constructor" : RandomizeProcess,
                   "idParameter" : "bucketId"
                  }
                 ]
   interfaces = ['atom', 'eml', 'html', 'json', 'narrative', 'xml']
   
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: Deletes a RAD bucket from the Lifemapper system
      """
      success = self.conn.deleteBucket(item)
      return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      bucket = self.conn.getRADBucket(self.user, bucketId=self.id, fillRandoms=True)
      return bucket
   
# =============================================================================
class RADBucketsRestService(RestService):
   """
   @summary: Lifemapper RAD Buckets Service
   @see: RestService
   """
   identifier = "buckets"
   version = "1.0"
   summary = "RAD Buckets Service"
   description = """RAD Buckets Service"""
   WebObjectConstructor = RADBucketWebObject
   
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
                    "name" : "experimentId",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "experimentName",
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
                    "name" : "shapegridId",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "shapegridName",
                    "process" : lambda x: x.strip()
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
      afterTime, beforeTime, experimentId, experimentName, page, perPage, \
                                       shapegridId, shapegridName, fullObjs = \
                      getQueryParameters(self.queryParameters, self.parameters)
      count = self.conn.countRADBuckets(self.user, 
                                      beforeTime=beforeTime,
                                      afterTime=afterTime, 
                                      experimentId=experimentId, 
                                      experimentName=experimentName, 
                                      shapegridId=shapegridId, 
                                      shapegridName=shapegridName)
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
      afterTime, beforeTime, experimentId, experimentName, page, perPage, \
                                       shapegridId, shapegridName, fullObjs = \
                      getQueryParameters(self.queryParameters, self.parameters)
      if page is None:
         page = 0
      if perPage is None:
         perPage = 100
      startRec = page * perPage

      if fullObjs is None:
         fullObjs = False

      items = self.conn.listRADBuckets(startRec, 
                                     perPage, 
                                     self.user, 
                                     beforeTime=beforeTime,
                                     afterTime=afterTime, 
                                     experimentId=experimentId, 
                                     experimentName=experimentName, 
                                     shapegridId=shapegridId, 
                                     shapegridName=shapegridName,
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
                                   (QueryParamNames.EXPERIMENT_ID, experimentId),
                                   (QueryParamNames.EXPERIMENT_NAME, experimentName),
                                   (QueryParamNames.SHAPEGRID_ID, shapegridId),
                                   (QueryParamNames.SHAPEGRID_NAME, shapegridName),
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new RAD bucket from the inputs provided
      """
      log = LmPublicLogger()
      dp = DataPoster(self.user, log)
      return dp.postRADBucket(self.parameters, self.body)
