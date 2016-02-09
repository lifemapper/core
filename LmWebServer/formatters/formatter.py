"""
@summary: Base class for object formatters
@author: CJ Grady
@version: 1.0
@status: beta
@note: Part of the Factory pattern
@see: formatterFactory
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
from LmCommon.common.lmconstants import ENCODING

from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ServiceObject
from LmServer.base.utilities import escapeString

# .............................................................................
class Formatter(object):
   """
   @summary: Base class for Formatter objects
   """
   # ..................................
   def __init__(self, obj, parameters={}):
      """
      @summary: Constructor
      @param obj: The object to format
      @param parameters: (optional) A dictionary of parameters that might be
                            useful for the Formatter
      """
      if self.__class__ == Formatter:
         raise LMError(["Formatter base class should not be instantiated."])
      
      self.obj = obj
      self.parameters = parameters
   
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      """
      raise LMError("Format function not implemented")

   # ..................................
   def _getMetadataProperties(self):
      """
      @summary: Gets the properties required by metadata formatters 
                   (atom, json, xml, etc)
      """
      try:
         self.url = self.parameters["url"]
      except:
         try:
            self.url = self.obj.metadataUrl
         except:
            self.url = ""

      self.updateTime = self.obj.modTime \
                               if isinstance(self.obj, ServiceObject) else None
      self.title = None
      try:
         self.title = self.obj.title
      except:
         try:
            self.title = "Lifemapper %s %s" % (self.obj.serviceType[:-1], self.obj.id)
         except:
            self.title = "Lifemapper List Service"
      self.name = "items"
      self.pages = []
      count = 0
      params = []
      try:
         count = self.obj.itemCount
         perPage = self.obj.queryParameters["perPage"]["value"]
         page = self.obj.queryParameters["page"]["value"]
         for key in self.obj.queryParameters.keys():
            try:
               value = self.obj.queryParameters[key]["value"]
               if value is not None and key not in ["perPage", "page"]:
                  params.append((key, value))
            except:
               pass
         params = '&'.join(["%s=%s" % (k, v) for k, v in params])
         params = "&%s" % params if len(params) > 0 else params
         
         firstUrl = escapeString("%s?page=%s&perPage=%s%s" % (self.url, 0, 
                                                               perPage, params), 
                                 "xml")
         self.pages.append(["first", firstUrl, 0])
         if page > 0:
            previousUrl = escapeString("%s?page=%s&perPage=%s%s" % (\
                                            self.url, page-1, perPage, params), 
                                       "xml")
            self.pages.append(["previous", previousUrl, page-1])
         currentUrl = escapeString("%s?page=%s&perPage=%s%s" % (self.url, page, 
                                                          perPage, params), 
                                   "xml")
         self.pages.append(["current", currentUrl, page])
         if page < count/perPage:
            nextUrl = escapeString("%s?page=%s&perPage=%s%s" % (self.url, page+1, 
                                                             perPage, params),
                             "xml")
            self.pages.append(["next", nextUrl, page+1])
         
         lastUrl = escapeString("%s?page=%s&perPage=%s%s" % (self.url, count/perPage, 
                                                          perPage, params), 
                                "xml")
         self.pages.append(["last", lastUrl, count/perPage])
      except Exception, e:
         pass
      
      try:
         modUrl = self.url.replace('atom/', '').replace('xml/', '').replace('json/', '')
         self.interfaces = [(i, "%s%s" % (modUrl, i)) for i in self.parameters["interfaces"]] 
      except:
         self.interfaces = []
         
      try:
         self.parameters = self.obj.queryParameters
      except:
         pass
   
# .............................................................................
class FormatterResponse(object):
   """
   @summary: Class for formatter responses.  At a minimum it is just a string
                representation of the response, but can contain other metadata
                as well
   """
   # ..................................
   def __init__(self, resp, contentType="", filename="", otherHeaders={}):
      """
      @summary: Constructor
      @param resp: The formatter response
      @param contentType: (optional) The content type of the response
      @param filename: (optional) The filename of the response (useful for HTTP)
      """
      self.resp = resp
      self.contentType = contentType
      self.fileName = filename
      self.headers = otherHeaders
      
   # ..................................
   def __str__(self):
      """
      @summary: Called when converting the response to a string
      """
      try:
         return str(self.resp)
      except:
         retUni = self.resp.encode(ENCODING)
         return retUni
         #return unicode(str(self.resp), 'utf-8').encode('utf-8')