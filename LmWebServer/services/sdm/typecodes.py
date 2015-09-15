"""
@summary: Module containing classes for Lifemapper Species Distribution 
             Modeling Layer Type Codes service and object
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
from LmCommon.common.localconstants import ARCHIVE_USER
from LmServer.base.utilities import formatTimeUrl, getMjdTimeFromISO8601
from LmServer.common.log import LmPublicLogger


from LmWebServer.base.servicesBaseClass import buildAttListResponse, \
                                     getQueryParameters, RestService, WebObject
from LmWebServer.common.lmconstants import QueryParamNames
from LmWebServer.services.common.userdata import DataPoster
                                          
# =============================================================================
class SDMTypeCodeWebObject(WebObject):
   """
   @summary: Service object for a Lifemapper SDM Layer Type Code
   """
   subObjects = [
                ]
   subServices = [
                 ]
   interfaces = ['atom', 'html', 'json', 'xml']
   
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: Deletes a layer type code from the Lifemapper system
      """
      success = self.conn.deleteLayerType(item)
      return success
   
   # ............................................
   def _getItem(self):
      """
      @summary: Gets the item from the database
      """
      exp = self.conn.getLayerTypeCode(userid=self.user, typeid=self.id)
      return exp

# =============================================================================
class SDMTypeCodesRestService(RestService):
   """
   @summary: Lifemapper SDM Layer Type Codes Service
   @see: Service
   """
   identifier = "typecodes"
   version = "2.0"
   summary = "SDM Layer Type Codes Service"
   description = """SDM Layer Type Codes Service"""
   WebObjectConstructor = SDMTypeCodeWebObject
   
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
                    "name" : "page",
                    "process" : lambda x: int(x)
                   },
                   {
                    "name" : "perPage",
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
      @summary: Counts the number of type codes that match the provided query 
                   parameters
      """
      afterTime, beforeTime, page, perPage, public, fullObjs = \
                      getQueryParameters(self.queryParameters, self.parameters)
      if public:
         user = ARCHIVE_USER
      else:
         user = self.user
      count = self.conn.countLayerTypeCodes(userId=user, 
                                            beforeTime=beforeTime, 
                                            afterTime=afterTime)
      return count
   
   # ............................................
   def help(self):
      pass
   
   # ............................................
   def list(self):
      """
      @summary: Returns a list of type codes that match the provided query
                   parameters
      """
      afterTime, beforeTime, page, perPage, public, fullObjs = \
                      getQueryParameters(self.queryParameters, self.parameters)
      if page is None:
         page = 0
      if perPage is None:
         perPage = 100
      startRec = page * perPage

      if fullObjs is None:
         fullObjs = False

      if public:
         user = ARCHIVE_USER
      else:
         user = self.user
      items = self.conn.listLayerTypeCodes(startRec, 
                                           perPage, 
                                           userId=user, 
                                           beforeTime=beforeTime, 
                                           afterTime=afterTime,
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
                                   (QueryParamNames.PUBLIC, public),
                                   (QueryParamNames.FULL_OBJECTS, int(fullObjs))
                                  ])
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new SDM type code from the inputs provided
      """
      log = LmPublicLogger()
      with DataPoster(self.user, log) as dp:
         ret = dp.postSDMLayerTypeCode(self.parameters, self.body)
      return ret

   
