#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
TODO: CONSIDER REMOVING, Does not appear to be used now.
"""
import decimal
import mx.DateTime
import numpy
from types import (BuiltinFunctionType, BuiltinMethodType, CodeType, 
         FloatType, FunctionType, IntType, LambdaType, MethodType, NoneType, 
         StringType, TypeType, UnicodeType)

from LmBackend.common.lmobj import LMObject

from LmCommon.common.lmconstants import (LM_USER, ISO_8601_TIME_FORMAT_FULL, 
         ISO_8601_TIME_FORMAT_TRUNCATED, LM_NAMESPACE, YMD_HH_MM_SS, ENCODING)

from LmServer.common.lmconstants import STRING_ESCAPE_FORMATS
from LmServer.common.localconstants import PUBLIC_USER

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
         for key in list(items.keys()):
            self.addItem(key, self.obj.__getattribute__(key))
      except Exception as e1:
         try: # Pick up attributes from attribute lists and objects
            for k in list(self.obj.get_attributes().keys()):
               att = self.obj.__getattr__(k)
               if isinstance(att, (IntType, FloatType)):
                  self.attributes[k] = att
               elif isinstance(att, StringType):
                  tmp = str(att, ENCODING).encode(ENCODING)
                  self.attributes[k] = tmp
               else:
                  self.addItem(k, att)
         except Exception as e3:
            pass
         
         try: # Handle dictionaries
            for key in list(self.obj.keys()):
               if not isinstance(self.obj[key], numpy.ndarray):
                  self.addItem(key, self.obj[key])
               else:
                  self.addItem(key, self.obj[key].tolist())
         except Exception as e2:
            try: # Handle iterables (picks up attribute list items)
               name = self.name[:-1] if self.name.endswith('s') else self.name
               for item in self.obj:
                  self.addItem(name, item)
            except Exception as e4:
               pass
         
         try: # Other objects (picks up attribute object properties)
            itms = dir(self.obj)
            filteredItems = [itm for itm in itms if list(filter(itm))]
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
         except Exception as e5:
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
                  item = str(item, ENCODING).encode(ENCODING)
               except:
                  item = str(str(item), ENCODING).encode(ENCODING)
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
               item = str(str(item), ENCODING).encode(ENCODING)
               item = str(str(item.id), ENCODING).encode(ENCODING)
               item = str(str(item.metadataUrl), ENCODING).encode(ENCODING)
            except:
               pass
         self.items.append((key, item))
      except:
         pass
   
   # .........................................
   def __next__(self):
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
def getColor(colorString, allowRamp=False):
   pass

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
   parts = [PUBLIC_USER, 'lifemapper', item.serviceType, str(item.getId())]
   return separator.join(parts)

# ..............................................................................
def getUrlParameter(param, parameterGroup):
   """
   Returns the value of the url parameter or list of values if there are 
   more than one.
   """
   try:
      if param.lower() in parameterGroup:
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
   except Exception as e3:
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
   except Exception as e:
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
   except Exception as e:
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
         value = str(value, ENCODING)
      elif isinstance(value, UnicodeType):
         value = value
      else:
         value = str(value)
      replaceStrings = []
      if format in STRING_ESCAPE_FORMATS:
         replaceStrings = STRING_ESCAPE_FORMATS[format]
   
      for replaceStr, withStr in replaceStrings:
         value = value.replace(replaceStr, withStr)
   except Exception as e: # Can't escape for some reason
      value = ""
   return value

# .............................
def isRootUser():
   """
   @summary: Determine if current user is the non-root Lifemapper user 
   """
   import subprocess
   cmd = "/usr/bin/whoami"
   info, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE).communicate()
   usr = info.split()[0]
   if usr == 'root':
      return True
   return False

# .............................
def isLMUser():
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
