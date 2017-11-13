"""
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import inspect
import json
import mx.DateTime
from osgeo.osr import CoordinateTransformation, SpatialReference
import sys
import traceback
from types import TupleType, ListType

from LmCommon.common.lmconstants import LMFormat

# ............................................................................
# ............................................................................
class LMObject(object):
   """
   Base class for all objects in the Lifemapper project
   """   
# ...............................................
   def getLineno(self):
      return inspect.currentframe().f_back.f_lineno
    
# ...............................................
   def getModuleName(self):
#      return '{0}.{1}'.format(__name__, self.__class__.__name__)
      return '{}.{}'.format(__name__, self.__class__.__name__)

# ...............................................
   def getLocation(self, lineno=None):
#      return '{0}.{1} line {2}'.format(__name__, 
#                                       self.__class__.__name__,
#                                       self.getLineno())
      loc = '{}.{}'.format(__name__, self.__class__.__name__)
      if lineno:
         loc += ' Line {}'.format(lineno)         
      return loc
   
# ...............................................
   @classmethod
   def readyFilename(cls, fullfilename, overwrite=False):
      """
      @summary: On existing file, 
                   if overwrite true: delete
                               false: return false
                Non-existing file:
                   create parent directories if needed
                   return true if parent directory exists
      """
      if fullfilename is None:
         raise LMError('Full filename is None')
      
      import os
      
      if os.path.exists(fullfilename):
         if overwrite:
            success, msg = cls.deleteFile(fullfilename)
            if not success:
               raise LMError('Unable to delete {}'.format(fullfilename))
            else:
               return True
         else:
            return False
      else:
         pth, basename = os.path.split(fullfilename)
         
         # If the file path is in cwd we don't need to create directories
         if len(pth) == 0:
            return True
         
         try:
            os.makedirs(pth, 0775)
         except:
            pass
            
         if os.path.isdir(pth):
            return True
         else:
            raise LMError('Failed to create directories {}'.format(pth))
   
# ...............................................
   @classmethod
   def deleteFile(self, fname, deleteDir=False):
      """
      @summary: Delete the file if it exists, delete enclosing directory if 
                it is now empty, print only warning if fails.  If filename is a 
                shapefile (ends in '.shp'), delete all other files that comprise
                the shapefile.
      """
      success = True
      msg = ''
      if fname is None:
         msg = 'Cannot delete file \'None\''
      else:
         import os
         pth, basename = os.path.split(fname)
         if fname is not None and os.path.exists(fname):
            base, ext = os.path.splitext(fname)
            if  ext == LMFormat.SHAPE.ext:
               import glob
               similarFnames = glob.glob(base + '.*')
               try:
                  for simfname in similarFnames:
                     simbase, simext = os.path.splitext(simfname)
                     if simext in LMFormat.SHAPE.getExtensions():
                        os.remove(simfname)
               except Exception, e:
                  success = False
                  msg = 'Failed to remove {}, {}'.format(simfname, str(e))
            else:
               try:
                  os.remove(fname)
               except Exception, e:
                  success = False
                  msg = 'Failed to remove {}, {}'.format(fname, str(e))
            if deleteDir and len(os.listdir(pth)) == 0:
               try:
                  os.removedirs(pth)
               except Exception, e:
                  success = False
                  msg = 'Failed to remove {}, {}'.format(pth, str(e))
      return success, msg
   
# ...............................................
   def _addMetadata(self, newMetadataDict, existingMetadataDict={}):
      for key, val in newMetadataDict.iteritems():
         try:
            existingVal = existingMetadataDict[key]
         except:
            existingMetadataDict[key] = val
         else:
            # if metadata exists and is ...
            if type(existingVal) is list: 
               # a list, add to it
               if type(val) is list:
                  newVal = list(set(existingVal.extend(val)))
                  existingMetadataDict[key] = newVal
                  
               else:
                  newVal = list(set(existingVal.append(val)))
                  existingMetadataDict[key] = newVal
            else:
               # not a set, replace it
               existingMetadataDict[key] = val
      return existingMetadataDict
         
# ...............................................
   def _dumpMetadata(self, metadataDict):
      metadataStr = None
      if metadataDict:
         metadataStr = json.dumps(metadataDict)
      return metadataStr

# ...............................................
   def _loadMetadata(self, newMetadata):
      """
      @note: Adds to dictionary or modifies values for existing keys
      """
      objMetadata = {}
      if newMetadata is not None:
         if type(newMetadata) is dict: 
            objMetadata = newMetadata
         else:
            try:
               objMetadata = json.loads(newMetadata)
            except Exception, e:
               print('Failed to load JSON object from type {} object {}'
                     .format(type(newMetadata), newMetadata))
      return objMetadata

# ============================================================================
class LMError(Exception, LMObject):
   """
   Base class for exceptions in the lifemapper project
   """
   
   def __init__(self, currargs=None, prevargs=None, lineno=None,  
                doTrace=False, logger=None):
      """
      @todo: Exception will change in Python 3.0: update this.  
             args will no longer exist, message can be any object
      @summary Constructor for the LMError class
      @param currargs: Current arguments (sequence or single string)
      @param prevargs: (optional) sequence of previous arguments for exception
                        being wrapped by LMError
      """
      super(LMError, self).__init__()
      self.lineno = lineno
      
      allargs = []
      if doTrace:
         sysinfo = sys.exc_info()
         tb = sysinfo[2]
         if tb is not None:
            tbargs = traceback.format_tb(tb)
         else:
            tbargs = [str(sysinfo)]
         
         for r in tbargs:
            allargs.append(r)
            
      if isinstance(currargs, TupleType) or isinstance(currargs, ListType):
         allargs.extend(currargs)
      elif currargs is not None:
         allargs.append(currargs)
         
      if isinstance(prevargs, TupleType) or isinstance(prevargs, ListType):
         allargs.extend(prevargs)
      elif prevargs is not None:
         allargs.append(prevargs)
      self.args = tuple(allargs)
      
# ............................................................................
   def __str__(self):
      """
      @summary get the string representation of an LMError
      @return String representation of an LMError
      """
      # Added because the error number was coming through as an integer
      l = [self.getLocation(), self.getTraceback()]
      for x in self.args:
         try:
            sarg = str(x)
         except UnicodeDecodeError, e:
            sarg = 'some unicode arg'
         except Exception, e:
            sarg = 'some other non-string arg ({})'.format(e)
         l.append(sarg)
         
      return repr('\n'.join(l))

# ............................................................................
   def getTraceback(self):
      msg = '\n'
      excType, excValue, thisTraceback = sys.exc_info()                                                 
      while thisTraceback :                                                                  
         framecode = thisTraceback.tb_frame.f_code                                                 
         filename = str(framecode.co_filename)                          
         line_no = str(traceback.tb_lineno(thisTraceback))
         msg += 'Traceback : Line: {}; File: {}\n'.format(line_no, filename)                                                    
         thisTraceback = thisTraceback.tb_next    
      return msg
