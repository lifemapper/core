"""
@summary: Module containing classes for Lifemapper Range and Diversity 
             PamSums service and object
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

from LmWebServer.base.servicesBaseClass import buildAttListResponse, \
                                     getQueryParameters, RestService, WebObject
from LmWebServer.common.lmconstants import QueryParamNames

# =============================================================================
class RADPamSumWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper RAD PAM Sum
   """
   subObjects = [
                ]
   subServices = [
                 ]
   interfaces = ['atom', 'eml', 'html', 'json', 'narrative', 'xml']
   
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: Deletes a PAMSum from the Lifemapper system
      """
      success = self.conn.deletePamSum(item)
      return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      if self.id.lower() == "original":
         bucket = self.conn.getRADBucket(self.user, 
                                       bucketId=self.parameters["bucketid"])
         pamsum = bucket.pamSum
      else:
         pamsum = self.conn.getPamSum(self.id)
      return pamsum
   
# =============================================================================
class RADPamSumsRestService(RestService):
   """
   @summary: Lifemapper RAD PAM Sums Service
   @see: RestService
   """
   identifier = "pamsums"
   version = "1.0"
   summary = "RAD PAM Sums Service"
   description = """RAD PAM Sums Service"""
   WebObjectConstructor = RADPamSumWebObject
   
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
                    "name" : "bucketId",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "experimentId",
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
                    "name" : "randomized",
                    "process" : lambda x: bool(int(x))
                   },
                   {
                    "name" : "randomMethod",
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
      @summary: Counts the number of pamsums that match the provided query 
                   parameters
      """
      afterTime, beforeTime, bucketId, experimentId, page, perPage, \
           randomized, randomMethod, fullObjs = \
                      getQueryParameters(self.queryParameters, self.parameters)
      if randomized is None:
         randomized = False
         
      count = self.conn.countPamSums(self.user, 
                                   beforeTime=beforeTime, 
                                   afterTime=afterTime, 
                                   experimentId=experimentId, 
                                   bucketId=bucketId, 
                                   isRandomized=randomized,
                                   randomMethod=randomMethod)
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
      afterTime, beforeTime, bucketId, experimentId, page, perPage, \
           randomized, randomMethod, fullObjs = \
                      getQueryParameters(self.queryParameters, self.parameters)
      if randomized is None:
         randomized = True
         
      if page is None:
         page = 0
      if perPage is None:
         perPage = 100
      startRec = page * perPage

      if fullObjs is None:
         fullObjs = False

      items = self.conn.listPamSums(startRec, 
                                  perPage, 
                                  self.user,
                                  beforeTime=beforeTime, 
                                  afterTime=afterTime, 
                                  experimentId=experimentId, 
                                  bucketId=bucketId, 
                                  isRandomized=randomized,
                                  randomMethod=randomMethod,
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
                                   (QueryParamNames.BUCKET_ID, bucketId),
                                   (QueryParamNames.IS_RANDOMIZED, randomized),
                                   (QueryParamNames.RANDOM_METHOD, randomMethod),
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new RAD pamsum from the inputs provided
      """
      pass
      #log = LmPublicLogger()
      #dp = DataPoster(self.user, log)
      #return dp.postMatrix(self.parameters, self.body)[0]

