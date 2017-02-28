#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
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
import decimal
import mx.DateTime
import numpy
from types import (BuiltinFunctionType, BuiltinMethodType, CodeType, 
         FloatType, FunctionType, IntType, LambdaType, MethodType, NoneType, 
         StringType, TypeType, UnicodeType)

from LmCommon.common.lmconstants import (LM_USER, ISO_8601_TIME_FORMAT_FULL, 
         ISO_8601_TIME_FORMAT_TRUNCATED, LM_NAMESPACE, YMD_HH_MM_SS, ENCODING)
from LmServer.base.lmobj import LMObject
from LmServer.common.lmconstants import STRING_ESCAPE_FORMATS

# ..............................................................................
class ObjectAttributeIterator(LMObject):
   """
   @summary: Iterates over an object's public attributes
   """
   # .........................................
   def __init__(self, name, obj, returnSubIterators=True, maxDepth=10,
                filter=lambda x: not x.startswith('_'),
                timeFormat=lambda x: formatTimeHuman(dt=x)):
      """
      @summary: Constructor
      @param name: The name of the object iterator
      @param obj: The object to iterate over
      @param returnSubIterators: (optional) Boolean value indicating if 
                                    iterator objects should be returned for 
                                    sub objects
      @param maxDepth: (optional) The maximum depth to return iterators
      @param filter: (optional) Function to filter out object attributes
      @param timeFormat: (optional) Function to format time
      """
      LMObject.__init__(self)
      
      self.attributes = {} # These are similar to xml attributes vs tags
      self.obj = obj
      self.idx = 0
      self.filter = filter
      self.formatTime = timeFormat
      self.name = name
      self.maxDepth = maxDepth
      self.returnSubIterators = returnSubIterators and self.maxDepth > 0
      self.items = []
      
      try: # Named Tuples
         items = self.obj._asdict()
         for key in items.keys():
            self.addItem(key, self.obj.__getattribute__(key))
      except Exception, e1:
         try: # Pick up attributes from attribute lists and objects
            for k in self.obj.getAttributes().keys():
               att = self.obj.__getattr__(k)
               if isinstance(att, (IntType, FloatType)):
                  self.attributes[k] = att
               elif isinstance(att, StringType):
                  tmp = unicode(att, ENCODING).encode(ENCODING)
                  self.attributes[k] = tmp
               else:
                  self.addItem(k, att)
         except Exception, e3:
            pass
         
         try: # Handle dictionaries
            for key in self.obj.keys():
               if not isinstance(self.obj[key], numpy.ndarray):
                  self.addItem(key, self.obj[key])
               else:
                  self.addItem(key, self.obj[key].tolist())
         except Exception, e2:
            try: # Handle iterables (picks up attribute list items)
               name = self.name[:-1] if self.name.endswith('s') else self.name
               for item in self.obj:
                  self.addItem(name, item)
            except Exception, e4:
               pass
         
         try: # Other objects (picks up attribute object properties)
            itms = dir(self.obj)
            filteredItems = [itm for itm in itms if filter(itm)]
            for key in filteredItems:
               try:
                  att = self.obj.__getattribute__(key)
                  if not isinstance(att, (TypeType, MethodType, CodeType, 
                                          FunctionType, LambdaType, NoneType, 
                                          BuiltinFunctionType, BuiltinMethodType)):
                     if key == "features":
                        self.addItem("feature", att)
                     elif key == "numpyDataType":
                        pass
                     elif isinstance(att, numpy.ndarray):
                        self.addItem(key, att.tolist())
                     else:
                        self.addItem(key, att)
               except:
                  pass
         except Exception, e5:
            pass

   # .........................................
   def addItem(self, key, item):
      """
      @summary: Adds an item to the list
      @param key: The key for the item (name)
      @param item: The item to add
      """
      try:
         if key == "bbox":
            pass
         elif isinstance(item, (IntType, FloatType, StringType, UnicodeType)):
            try:
               if key.lower().find("time") > 0 or key.lower().find("date") > 0:
                  item = self.formatTime(item)
               else:
                  raise
            except:
               try:
                  item = unicode(item, ENCODING).encode(ENCODING)
               except:
                  item = unicode(str(item), ENCODING).encode(ENCODING)
                  try:
                     item = str(decimal.Decimal(item))
                  except:
                     pass
         elif self.returnSubIterators:
            item = ObjectAttributeIterator(key, item, returnSubIterators=True,
                                      maxDepth=self.maxDepth-1, filter=self.filter, 
                                      timeFormat=self.formatTime)
         else:
            try:
               item = unicode(str(item), ENCODING).encode(ENCODING)
               item = unicode(str(item.id), ENCODING).encode(ENCODING)
               item = unicode(str(item.metadataUrl), ENCODING).encode(ENCODING)
            except:
               pass
         self.items.append((key, item))
      except:
         pass
   
   # .........................................
   def next(self):
      """
      @summary: Returns the next item
      """
      if self.idx < len(self.items):
         name, value = self.items[self.idx]
         self.idx = self.idx + 1
         return name, value
      else:
         raise StopIteration
   
   # .........................................
   def __iter__(self):
      return self

# .............................................
def formatTimeAtom(dt=None):
   """
   @summary: Gets the time in Atom format
   @param dt: (optional) Date time in mjd format
   @return: The string representation of time in the required format for Atom
   @rtype: String
   """
   if dt is None:
      dTime = mx.DateTime.gmt()
   else:
      dTime = mx.DateTime.DateTimeFromMJD(dt)
   return dTime.strftime('%Y-%m-%dT%H:%M:%SZ')

# .............................................
def formatTimeYear(dt=None):
   """
   @summary: Gets the time in Atom format
   @param dt: (optional) Date time in mjd format
   @return: The string representation of time in the required format for Atom
   @rtype: String
   """
   if dt is None:
      dTime = mx.DateTime.gmt()
   else:
      dTime = mx.DateTime.DateTimeFromMJD(dt)
   return dTime.strftime('%Y')

# ...........................................
def formatTimeHuman(dt=None):
   """
   @summary: Gets the time in human readable format
   @param dt: (optional) Date time in mjd format
   @return: The string representation of time in the required format for humans
   @rtype: String
   """
   if dt is None:
      dTime = mx.DateTime.gmt()
   elif dt == 0:
      return ""
   else:
      dTime = mx.DateTime.DateTimeFromMJD(dt)
   return dTime.strftime('%Y-%m-%d %H:%M:%S')

# ...........................................
def formatTimeUrl(dt=None):
   """
   @summary: Gets the time for a Lifemapper url
   """
   if dt is None or dt == 0:
      return ""
   else:
      dTime = mx.DateTime.DateTimeFromMJD(dt)
      return dTime.strftime('%Y-%m-%d')

# ...........................................
def getMjdTimeFromISO8601(dt):
   try:
      return mx.DateTime.strptime(dt, 
                                     ISO_8601_TIME_FORMAT_FULL).mjd
   except:
      try:
         return mx.DateTime.strptime(dt, YMD_HH_MM_SS).mjd
      except:
         try:
            return mx.DateTime.strptime(dt, 
                                ISO_8601_TIME_FORMAT_TRUNCATED).mjd
         except:
            return mx.DateTime.gmt().mjd
   
# .............................................................................
def getPackageId(item, separator='.'):
   """
   @summary: Gets the package id of an object, useful for EML or UUID
   """
   parts = ['kubi', 'lifemapper', item.moduleType, item.serviceType, str(item.getId())]
   return separator.join(parts)

# ..............................................................................
def getUrlParameter(param, parameterGroup):
   """
   Returns the value of the url parameter or list of values if there are 
   more than one.
   """
   try:
      if parameterGroup.has_key(param.lower()):
         temp = parameterGroup[param.lower()]
         if temp is None:
            ret = None
         else:
            try:
               ret = temp.value
            except Exception:
               ret = temp
      else:
         ret = None
   except Exception, e3:
      ret = None
   if ret == "":
      ret = None
   return ret

# .......................................
def getXmlValueFromTree(tree, name, namespace=LM_NAMESPACE, 
                        default=None, func=lambda x: x.text.strip()):
   """
   @summary: Gets the value of an xml variable.  If it is not present, 
             return the default value.
   @param tree: An element tree instance that should contain the value
   @param name: The name (no namespace) of the value to return
   @param namespace: (optional) The namespace of the variable
   @param default: (optional) The default value to return if not found
   """
   ret = default
   try:
      if not isinstance(name, list):
         nameMod = [name]
      else:
         nameMod = name
         
      if namespace is None:
         pth = '/'.join([val for val in nameMod])
      else:
         pth = '/'.join(["{%s}%s" % (namespace, val) for val in nameMod])
      
      temp = tree.find(pth)
      
      if temp is None:
         temp = tree.get(name, default=default)
         ret = temp
      else:
         ret = func(temp)
   except Exception, e:
      pass
   return ret

# .......................................
def getXmlListFromTree(tree, name, namespace=LM_NAMESPACE, 
                        default=None, func=lambda x: x.text.strip()):
   """
   @summary: Gets the value of an xml variable.  If it is not present, 
             return the default value.
   @param tree: An element tree instance that should contain the value
   @param name: The name (no namespace) of the value to return
   @param namespace: (optional) The namespace of the variable
   @param default: (optional) The default value to return if not found
   """
   ret = default
   try:
      if not isinstance(name, list):
         nameMod = [name]
      else:
         nameMod = name
         
      if namespace is None:
         pth = '/'.join([val for val in nameMod])
      else:
         pth = '/'.join(["{%s}%s" % (namespace, val) for val in nameMod])
      
      temp = tree.findall(pth)
      
      if temp is None:
         temp = tree.get(name, default=default)
         ret = temp
      else:
         ret = [func(t) for t in temp]
   except Exception, e:
      pass
   return ret

# .............................................................................
def getFileContents(filename):
   f = open(filename)
   contents = ''.join(f.readlines())
   f.close()
   return contents

# .............................................................................
def escapeString(value, format):
   """
   @summary: Escapes string special characters
   """
   try:
      if isinstance(value, StringType):
         value = unicode(value, ENCODING)
      elif isinstance(value, UnicodeType):
         value = value
      else:
         value = str(value)
      replaceStrings = []
      if format in STRING_ESCAPE_FORMATS:
         replaceStrings = STRING_ESCAPE_FORMATS[format]
   
      for replaceStr, withStr in replaceStrings:
         value = value.replace(replaceStr, withStr)
   except Exception, e: # Can't escape for some reason
      value = ""
   return value

# .............................
def isCorrectUser():
   """
   @summary: Determine if current user is the non-root Lifemapper user 
   """
   import subprocess
   cmd = "/usr/bin/whoami"
   info, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE).communicate()
   usr = info.split()[0]
   if usr == LM_USER:
      return True
   return False
