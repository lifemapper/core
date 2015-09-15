"""
@summary: Module containing classes for Lifemapper Range and Diversity 
             Experiments service and object
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
import os

from LmCommon.common.lmconstants import HTTPStatus

from LmServer.base.lmobj import LmHTTPError
from LmServer.base.utilities import formatTimeUrl, getMjdTimeFromISO8601
from LmServer.common.log import LmPublicLogger

from LmWebServer.base.servicesBaseClass import buildAttListResponse, \
                                     getQueryParameters, RestService, WebObject
from LmWebServer.common.lmconstants import QueryParamNames, SERVICE_MOUNTS

from LmWebServer.services.common.userdata import DataPoster
from LmWebServer.services.rad.anclayers import RADAncLayersRestService
from LmWebServer.services.rad.buckets import RADBucketsRestService
from LmWebServer.services.rad.palayers import RADPALayersRestService
from LmWebServer.services.rad.processes import AddAncLayerProcess, \
                          AddBucketProcess, AddPALayerProcess, \
                          AddTreeProcess, IntersectProcess

# =============================================================================
class RADExpWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper RAD Experiment
   """
   subObjects = [
                 # Consider removing in the future in favor of the listing service
                 {
                  "names" : ["anc", "env"],
                  "func" : lambda x: x.envLayerset
                 },
                 # Consider removing in the future in favor of the listing service
                 {
                  "names" : ["pa", "org"],
                  "func" : lambda x: x.orgLayerset
                 },
                 {
                  "names" : ["tree"],
                  "func" : lambda x: getTreeForExperiment(x)
                 }
                ]
   subServices = [
                  {
                   "names" : SERVICE_MOUNTS["rad"]["anclayers"],
                   "constructor" : RADAncLayersRestService,
                   "idParameter" : "experimentId"
                  },
                  {
                   "names" : SERVICE_MOUNTS["rad"]["buckets"],
                   "constructor" : RADBucketsRestService,
                   "idParameter" : "experimentId"
                  },
                  {
                   "names" : SERVICE_MOUNTS["rad"]["palayers"],
                   "constructor" : RADPALayersRestService,
                   "idParameter" : "experimentId"
                  },
                  {
                   "names" : [AddAncLayerProcess.identifier],
                   "constructor" : AddAncLayerProcess,
                   "idParameter" : "experimentId"
                  },
                  {
                   "names" : [IntersectProcess.identifier],
                   "constructor" : IntersectProcess,
                   "idParameter" : "experimentId"
                  },
                  {
                   "names" : [AddBucketProcess.identifier],
                   "constructor" : AddBucketProcess,
                   "idParameter" : "experimentId"
                  },
                  {
                   "names" : [AddPALayerProcess.identifier],
                   "constructor" : AddPALayerProcess,
                   "idParameter" : "experimentId"
                  },
                  {
                   "names" : [AddTreeProcess.identifier],
                   "constructor" : AddTreeProcess,
                   "idParameter" : "experimentId"
                  }
                 ]
   interfaces = ['atom', 'eml', 'html', 'json', 'narrative', 'xml']
   
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: Deletes an experiment from the Lifemapper system
      """
      success = self.conn.deleteRADExperiment(item)
      return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      exp = self.conn.getRADExperiment(self.user, expid=self.id)
      self.conn.fillRADLayersetForExperiment(exp)
      return exp
   
# =============================================================================
class RADExpRestService(RestService):
   """
   @summary: Lifemapper RAD Experiments Service
   @see: RestService
   """
   identifier = "experiments"
   version = "1.0"
   summary = "RAD Experiments Service"
   description = """RAD Experiments Service"""
   WebObjectConstructor = RADExpWebObject
   
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
                    "default" : 0,
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "perPage",
                    "default" : 100,
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
      @summary: Counts the number of experiments that match the provided query 
                   parameters
      """
      afterTime, beforeTime, epsgCode, page, perPage, \
           fullObjs = getQueryParameters(self.queryParameters, self.parameters)
      count = self.conn.countRADExperiments(self.user, 
                                          beforeTime=beforeTime, 
                                          afterTime=afterTime, 
                                          epsg=epsgCode)
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
      afterTime, beforeTime, epsgCode, page, perPage, \
           fullObjs = getQueryParameters(self.queryParameters, self.parameters)
      startRec = page * perPage

      if fullObjs is None:
         fullObjs = False

      items = self.conn.listRADExperiments(startRec, 
                                         perPage, 
                                         self.user,
                                         beforeTime=beforeTime, 
                                         afterTime=afterTime,
                                         epsg=epsgCode,
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
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new RAD experiment from the inputs provided
      """
      log = LmPublicLogger()
      dp = DataPoster(self.user, log)
      return dp.postRADExperiment(self.parameters, self.body)

   # ............................................
   def wps(self):
      """
      @summary: End-point for wps services
      """
      pass

# .............................................................................
def getTreeForExperiment(exp):
   """
   @summary: Attempts to return a tree for an experiment
   """
   if os.path.exists(exp.attrTreeDLocation):
      return open(exp.attrTreeDLocation).read()
   else:
      raise LmHTTPError(HTTPStatus.NOT_FOUND, 
              "Experiment %s does not have a tree associated with it" % exp.id)