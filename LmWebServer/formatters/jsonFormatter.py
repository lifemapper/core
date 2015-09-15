"""
@summary: Module containing JSON Formatter class and helping functions
@author: CJ Grady
@version: 1.0
@status: beta
@note: Part of the Factory pattern
@see: Formatter
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
from types import ListType

from LmServer.base.serviceobject import ServiceObject
from LmServer.base.utilities import escapeString, ObjectAttributeIterator
from LmServer.common.jsonTree import JsonArray, JsonObject, tostring

from LmWebServer.formatters.formatter import Formatter, FormatterResponse


# .............................................................................
class JsonFormatter(Formatter):
   """
   @summary: Formatter class for JSON output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      # Fill metadata properties
      self._getMetadataProperties()

      try:
         name = self.obj.serviceType[:-1]
      except:
         name = "items"

      jsonObj = JsonObject()
      title = self.title if self.title is not None else ""
      name = self.name if self.name is not None else name

      jsonObj.addValue("title", title)
      _addJsonValues(jsonObj, ObjectAttributeIterator(name, self.obj))
      ret = tostring(jsonObj)
      
      
      ct = "application/json"
      if isinstance(self.obj, ServiceObject):
         fn = "%s%s.json" % (name, self.obj.getId())
      else:
         fn = "lmJsonFeed.json"
      
      return FormatterResponse(ret, contentType=ct, filename=fn)

# .............................................................................
def _addJsonValues(jsonObj, obj):
   matchNames = []
   if isinstance(obj.obj, ListType):
      ary = jsonObj.addArray(obj.name)
      matchNames.append(obj.name)
      if obj.name.endswith('s'):
         matchNames.append(obj.name[:-1])
   for k in obj.attributes.keys():
      jsonObj.addValue(k, escapeString(str(obj.attributes[k]), "json"))
   for name, value in obj:
      if value is not None:
         if name in matchNames:
            if isinstance(value, ObjectAttributeIterator):
               o = ary.addObject(name)
               _addJsonValues(o, value)
            else:
               ary.addValue(escapeString(str(value), "json"))
         else:
            if name == "feature":
               _addJsonValues(jsonObj, value)
            else:
               if isinstance(value, ObjectAttributeIterator):
                  o = jsonObj.addObject(name)
                  _addJsonValues(o, value)
               else:
                  jsonObj.addValue(name, escapeString(str(value), "json"))

