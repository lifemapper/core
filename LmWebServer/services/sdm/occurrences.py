"""
@summary: Module containing classes for Lifemapper Species Distribution 
             Modeling Occurrence Sets service and object
@author: CJ Grady
@version: 2.0
@status: release
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
from LmServer.common.localconstants import PUBLIC_USER, POINT_COUNT_MAX
from LmServer.common.log import LmPublicLogger

from LmWebServer.base.servicesBaseClass import buildAttListResponse, \
                                     getQueryParameters, RestService, WebObject
from LmWebServer.common.lmconstants import (ATOM_INTERFACE, CSV_INTERFACE, 
      EML_INTERFACE, HTML_INTERFACE, JSON_INTERFACE, KML_INTERFACE, 
      QueryParamNames, SHAPEFILE_INTERFACE, WMS_INTERFACE, XML_INTERFACE)
from LmWebServer.services.common.userdata import DataPoster

# TODO: Integrate maxCount and max returned
# TODO: Reinstate fill points
                                          
# =============================================================================
class SDMOccurrenceSetWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper SDM Occurrence Set
   """
   subObjects = [
                ]
   subServices = [
                 ]
   interfaces = [ATOM_INTERFACE, CSV_INTERFACE, EML_INTERFACE, HTML_INTERFACE,
                 JSON_INTERFACE, KML_INTERFACE, SHAPEFILE_INTERFACE,  
                 WMS_INTERFACE, XML_INTERFACE]
   
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
         if self.parameters.has_key(QueryParamNames.FILL_POINTS['name']) and \
             int(self.parameters[QueryParamNames.FILL_POINTS['name']]) == 0:
            pass # Skip if we shouldn't fill points
         else:
            maxPoints = None
            try:
               if self.vpath[1].lower() == KML_INTERFACE.lower():
                  maxPoints = POINT_COUNT_MAX
            except:
               pass
            
            if self.parameters.has_key(QueryParamNames.MAX_RETURNED['name'].lower()):
               temp = int(self.parameters[QueryParamNames.MAX_RETURNED['name'].lower()])
               if temp < maxPoints:
                  maxPoints = temp
                  
            # If we have a maximum and that maximum is less than or equal to the
            #    limit for subsetting and the occurrence set has more than the
            #    subset limit
            if maxPoints is not None \
                and occ.queryCount > POINT_COUNT_MAX \
                and maxPoints <= POINT_COUNT_MAX:
               subset = True
            else:
               subset = False
            
            occ.readShapefile(subset=subset)
               
            # If we have more points than maxPoints
            if maxPoints is not None and len(occ.features) > maxPoints:
               try:
                  # Get A list of dictionary entries, slice the list, and 
                  #   convert back to dictionary
                  feats = dict(occ.getFeatures().items()[:int(maxPoints)])
                  atts = occ.getFeatureAttributes()
                  occ.clearFeatures()
                  occ.setFeatures(feats, atts)
               except:
                  pass
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
                    "name" : QueryParamNames.AFTER_TIME['name'],
                    "process" : lambda x: getMjdTimeFromISO8601(x)
                   },
                   {
                    "name" : QueryParamNames.BEFORE_TIME['name'],
                    "process" : lambda x: getMjdTimeFromISO8601(x)
                   },
                   {
                    "name" : QueryParamNames.EPSG_CODE['name'],
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : QueryParamNames.HAS_PROJECTIONS['name'],
                    "process" : lambda x: bool(int(x))
                   },
                   {
                    "name" : QueryParamNames.PAGE['name'],
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : QueryParamNames.PER_PAGE['name'],
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : QueryParamNames.DISPLAY_NAME['name'],
                    "process" : lambda x: x
                   },
                   {
                    "name" : QueryParamNames.MIN_POINTS['name'],
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : QueryParamNames.PUBLIC['name'],
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : QueryParamNames.FULL_OBJECTS['name'],
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
         user = PUBLIC_USER
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
         user = PUBLIC_USER
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
   
