"""
@summary: Module containing classes for Lifemapper Species Distribution 
             Modeling Occurrence Sets service and object
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

from LmCommon.common.lmconstants import JobStatus

from LmServer.base.utilities import formatTimeUrl, getMjdTimeFromISO8601
from LmServer.common.localconstants import ARCHIVE_USER, POINT_COUNT_MAX
from LmServer.common.log import LmPublicLogger

from LmWebServer.base.servicesBaseClass import buildAttListResponse, \
                                     getQueryParameters, RestService, WebObject
from LmWebServer.common.lmconstants import QueryParamNames
from LmWebServer.services.common.userdata import DataPoster
                                          
# =============================================================================
class SDMOccurrenceSetWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper SDM Occurrence Set
   """
   subObjects = [
                ]
   subServices = [
                 ]
   interfaces = ['atom', 'csv', 'eml', 'html', 'json', 'kml', 'shapefile', #'wfs', 
                 'wms', 'xml']
   
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: Deletes an occurrence set from the Lifemapper system
      """
      success = self.conn.completelyRemoveOccurrenceSet(item)
      return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      occ = self.conn.getOccurrenceSet(self.id)
      if occ.status == JobStatus.COMPLETE:
         if self.parameters.has_key("fillpoints") and self.parameters["fillpoints"].lower() == "false":
            #log.debug("Fill points was present and false")
            pass
         else:
            maxPoints = None
            try:
               if self.vpath[1].lower() == 'kml':
                  maxPoints = POINT_COUNT_MAX
            except:
               pass

            if maxPoints is not None and occ.queryCount > POINT_COUNT_MAX:
               subset = True
            else:
               subset = False
            
            occ.readShapefile(subset=subset)
      return occ

# =============================================================================
class SDMOccurrenceSetsRestService(RestService):
   """
   @summary: Lifemapper SDM Occurrence Sets Service
   @see: Service
   """
   identifier = "occurrences"
   version = "2.0"
   summary = "SDM Occurrence Sets Service"
   description = """SDM Occurrence Sets Service"""
   WebObjectConstructor = SDMOccurrenceSetWebObject
   
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
                    "name" : "hasProjections",
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
                    "name" : "displayName",
                    "process" : lambda x: x
                   },
                   {
                    "name" : "minimumNumberOfPoints",
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
      @summary: Counts the number of occurrence sets that match the provided query 
                   parameters
      """
      afterTime, beforeTime, epsg, hasPrj, page, perPage, displayName, \
               minPoints, public, fullObjs = getQueryParameters(self.queryParameters, 
                                           self.parameters)
      if minPoints is None or minPoints < 0:
         minPoints = 1
      if public:
         user = ARCHIVE_USER
      else:
         user = self.user
      #TODO: Make status an optional parameter
      count = self.conn.countOccurrenceSets(minOccurrenceCount=minPoints, 
                                          hasProjections=hasPrj,
                                          userId=user, 
                                          displayName=displayName,
                                          beforeTime=beforeTime, 
                                          afterTime=afterTime,
                                          epsg=epsg,
                                          status=JobStatus.COMPLETE)
      return count
   
   # ............................................
   def help(self):
      pass
   
   # ............................................
   def list(self):
      """
      @summary: Returns a list of occurrence sets that match the provided query
                   parameters
      """
      afterTime, beforeTime, epsg, hasPrj, page, perPage, displayName, \
               minPoints, public, fullObjs = getQueryParameters(
                                         self.queryParameters, self.parameters)
      if page is None:
         page = 0
      if perPage is None:
         perPage = 100
      startRec = page * perPage

      if hasPrj is None:
         hasPrj = False
      
      if fullObjs is None:
         fullObjs = False

      if minPoints is None or minPoints < 0:
         minPoints = 1
      if public:
         user = ARCHIVE_USER
      else:
         user = self.user
      #TODO: Make status an optional parameter
      items = self.conn.listOccurrenceSets(startRec, 
                                         perPage, 
                                         minOccurrenceCount=minPoints, 
                                         hasProjections=hasPrj,
                                         userId=user, 
                                         displayName=displayName, 
                                         beforeTime=beforeTime, 
                                         afterTime=afterTime,
                                         epsg=epsg,
                                         status=JobStatus.COMPLETE,
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
                                   (QueryParamNames.MIN_POINTS, minPoints),
                                   (QueryParamNames.DISPLAY_NAME, displayName),
                                   (QueryParamNames.PUBLIC, public),
                                   (QueryParamNames.HAS_PROJECTIONS, int(hasPrj)),
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new SDM occurrence set from the inputs provided
      """
      log = LmPublicLogger()
      with DataPoster(self.user, log) as dp:
         occ = dp.postSDMOccurrenceSet(self.parameters, self.body)
         occ._features = []
      return occ

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
   
