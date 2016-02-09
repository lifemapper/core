"""
@summary:  Python Json module that allows addition of elements similar to the
             way that ElementTree allows addition of xml elements.
@author: CJ Grady
@version: 1.0
@status: stable
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
@todo: Should this module move to LmCommon?  Should it be removed completely 
          in favor of the built-in json module?
"""

from types import StringType, UnicodeType

from LmCommon.common.lmconstants import ENCODING
from LmServer.base.lmobj import LMObject
from LmServer.base.utilities import escapeString

# .............................................................................
class JsonObject(LMObject):
   """
   @summary: Class for JSON objects
   """
   # ..................................
   def __init__(self):
      """
      @summary: Constructor for JSON objects
      """
      LMObject.__init__(self)
      self.values = []
   
   # ..................................
   def addValue(self, name, value):
      """
      @summary: Adds a name value pair to an object
      @param name: The attribute name
      @param value: The attribute value
      """
      if not isinstance(value, UnicodeType):
         value = unicode(value, ENCODING, errors='ignore')
      self.values.append((name, value.encode(ENCODING)))
      
   # ..................................
   def addArray(self, name):
      """
      @summary: Adds a JSON array to the object
      @param name: The name of the array to add
      @return: The newly created JsonArray
      @rtype: JsonArray
      """
      newArray = JsonArray()
      self.values.append((name, newArray))
      return newArray
   
   # ..................................
   def addObject(self, name):
      """
      @summary: Adds a new JSON object to the object
      @param name: The name of the new object
      @return: The newly created object
      @rtype: JsonObject
      """
      newObj = JsonObject()
      self.values.append((name, newObj))
      return newObj
   
# .............................................................................
class JsonArray(LMObject):
   """
   @summary: Object representing a JSON array
   """
   # ..................................
   def __init__(self):
      """
      @summary: Constructor for JSON arrays
      """
      LMObject.__init__(self)
      self.values = []
      
   # ..................................
   def addValue(self, value):
      """
      @summary: Adds a value to the array
      """
      if not isinstance(value, UnicodeType):
         value = unicode(value, ENCODING, errors='ignore')
      self.values.append(value.encode(ENCODING))
      
   # ..................................
   def addArray(self, name):
      """
      @summary: Adds an array to the JSON array
      @param name: The name of the new array
      @return: The newly created array
      @rtype: JsonArray
      """
      newArray = JsonArray()
      self.values.append(newArray)
      return newArray
   
   # ..................................
   def addObject(self, name):
      """
      @summary: Adds a new object to the JSON array
      @param name: The name of the new object
      @return: The newly create JSON object
      @rtype: JsonObject
      """
      newObj = JsonObject()
      self.values.append(newObj)
      return newObj
   
# .............................................................................
def tostring(obj):
   """
   @summary: Converts the JSON object into a string
   @return: The string representation of the JSON object
   @rtype: String
   """
   return _displayValue(obj, "   ", -1)

# .............................................................................
def _displayValue(obj, delim, depth):
   """
   @summary: Returns the display representation of a value
   @param obj: The value to represent
   @param delim: The delimiter to use (for spacing)
   @param depth: The depth of this object
   @note: Padding is determined by prepending display with depth copies of 
             delim.  So if the delimiter is "abc" and the depth is 2, the 
             resulting padding is "abcabc"
   @return: The string representation of the value
   @rtype: String
   """
   if isinstance(obj, JsonObject):
      return _displayObject(obj, delim, depth+1)
   elif isinstance(obj, JsonArray):
      return _displayArray(obj, delim, depth+1)
   elif isinstance(obj, StringType):
      return ''.join(("\"", escapeString(obj, "json"), "\""))
   elif isinstance(obj, UnicodeType):
      return u''.join((u"\"", escapeString(obj, "json"), u"\""))
   else:
      return str(obj)
      

# .............................................................................
def _displayObject(obj, delim, depth):
   """
   @summary: Returns the display representation of an object
   @param obj: The object to represent
   @param delim: The delimiter to use (for spacing)
   @param depth: The depth of this object
   @note: Padding is determined by prepending display with depth copies of 
             delim.  So if the delimiter is "abc" and the depth is 2, the 
             resulting padding is "abcabc"
   @return: The string representation of the object
   @rtype: String
   """
   ret = []
   ret.append("")
   ret.append(''.join((delim*depth, "{")))
   
   ret2 = []
   for val in obj.values:
      ret2.append('%s"%s": %s' % (delim*(depth+1), val[0], _displayValue(val[1], delim, depth)))
      #ret2.append(''.join((delim*(depth+1), "\"", val[0], "\": ", _displayValue(val[1], delim, depth))))
   ret.append(',\n'.join(ret2))
   
   ret.append(''.join((delim*depth, "}")))
   return '\n'.join(ret)

# .............................................................................
def _displayArray(obj, delim, depth):
   """
   @summary: Returns the display representation of an array
   @param obj: The array to represent
   @param delim: The delimiter to use (for spacing)
   @param depth: The depth of this object
   @note: Padding is determined by prepending display with depth copies of 
             delim.  So if the delimiter is "abc" and the depth is 2, the 
             resulting padding is "abcabc"
   @return: The string representation of the array
   @rtype: String
   """
   ret = []
   ret.append("")
   ret.append(''.join((delim*depth, "[")))
   
   ret2 = []
   for val in obj.values:
      ret2.append(''.join((delim*(depth+1), _displayValue(val, delim, depth+1))))
   ret.append(',\n'.join(ret2))
   
   ret.append(''.join((delim*depth, "]")))
   return '\n'.join(ret)

