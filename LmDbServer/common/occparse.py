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

import ast
import csv
import os
from osgeo.ogr import OFTInteger, OFTReal, OFTString
import sys
from types import DictionaryType, DictType

from LmServer.base.lmobj import LMObject, LMError
# from LmServer.common.localconstants import APP_PATH, TROUBLESHOOTERS
# from LmServer.common.lmconstants import LOG_PATH
# from LmServer.common.log import ScriptLogger
# .............................................................................
class OccDataParser(LMObject):
   """
   @summary: Object with metadata and open file.  OccDataParser maintains 
             file position and most recently read data chunk
   """
   def __init__(self, logger, dataFname, metadataFname):
      """
      @summary Reader for arbitrary user CSV data with metadata description
      @param logger: Logger to use for the main thread
      """
      self.log = logger
      self.metadataFname = metadataFname
      self.dataFname = dataFname
      self.fieldNames = None 
      self.fieldCount = 0
      # Requires ID, GroupBy, Longitude, Latitude
      self.requiredCount = 4
      self._fieldTypes = None
      self._idIdx = None
      self._sortIdx = None
      self._nameIdx = None
      
      try:
         self._file = open(self.dataFname, 'r')
      except Exception, e:
         raise LMError('Failed to open %s' % self.dataFname)
      self._csvreader = csv.reader(self._file, delimiter='\t')
      self.header = self._csvreader.next()

      # Read metadata file and close
      self._getMetadata(self.header)
      
      self.currLine = None
      self.currRecnum = 0
      self.chunk = []
      self.key = None
      # populates key, currLine and currRecnum
      self.sortFail = 0
      self.getNextSortVal()

   # .............................................................................
   @property
   def sortIdx(self):
      return self._sortIdx
   
   # .............................................................................
   def _readMetadata(self):
      try:
         f = open(self.metadataFname, 'r')
      except Exception, e:
         raise LMError('Failed to open %s' % self.metadataFname)
      
      try:
         metaStr = f.read()
         fldmeta = ast.literal_eval(metaStr)
      except Exception, e:
         raise LMError('Failed to evaluate contents of metadata file %s' 
                       % self.metadataFname)
      finally:
         f.close()
         
      if type(fldmeta) not in (DictionaryType, DictType):
         raise LMError('Contents of metadata file %s is not a Python dictionary' 
                       % self.metadataFname)
      return fldmeta
         
   # .............................................................................
   def _getMetadata(self, origfldnames):
      self.fieldNames = []
      self.fieldTypes = []
      fldmeta = self._readMetadata()
      
      for i in range(len(origfldnames)):
         
         oname = origfldnames[i]
         shortname = fldmeta[oname][0]
         ogrtype = self.getOgrFieldType(fldmeta[oname][1])
         self.fieldNames.append(shortname)
         self.fieldTypes.append(ogrtype)
         
         if len(fldmeta[oname]) == 3:
            role = fldmeta[oname][2].lower()
            if role == 'id':
               self._idIdx = i
            elif role == 'groupby':
               self._sortIdx = i
            elif role == 'dataname':
               self._nameIdx = i
      self.fieldCount = len(self.fieldNames)
      
      if self._idIdx == None:
         raise LMError('Missing \'id\' unique identifier field')
      if self._sortIdx == None:
         raise LMError('Missing \'groupby\' sorting field')
      if self._nameIdx == None:
         raise LMError('Missing \'dataname\' dataset name field')


   # .............................................................................
   @staticmethod
   def getOgrFieldType(typeString):
      typestr = typeString.lower()
      if typestr == 'integer':
         return OFTInteger
      elif typestr == 'string':
         return OFTString
      elif typestr == 'real':
         return OFTReal
      else:
         raise Exception('Unsupported field type %s (use integer, string, or real)' 
                         % typeString)
   
   # ...............................................
   def _getLine(self):
      """
      Fills in self.currLine, self.currRecnum
      """
      success = False
      line = None
      while not success:
         try:
            self.currLine = self._csvreader.next()
            success = True
            self.currRecnum += 1
         except OverflowError, e:
            self.currRecnum += 1
            self.log.debug( 'Overflow on %d (%s)' % (self.currRecnum, str(e)))
         except Exception, e:
            self.log.debug('Exception reading line %d, probably EOF (%s)' 
                      % (self.currRecnum, str(e)))
            self.close()
            self.currRecnum = self.currLine = None
            success = True
   
   # ...............................................
   def skipToRecord(self, targetnum):
      complete = False
      while self.currLine is not None and self.currRecnum < targetnum:
         self._getLine()

   # ...............................................
   def getNextSortVal(self):
      """
      Fills in self.key and self.currLine
      """
      complete = False
      self.key = None
      self._getLine()
      try:
         while self._csvreader is not None and not complete:
            if len(self.currLine) >= self.requiredCount:
               val = self.currLine[self._sortIdx]
               try:
                  sortval = int(val)
               except Exception, e:
                  self.log.debug('Failed to retrieve sort field on line %d (%s)' 
                            % (self.currRecnum, str(val)))
                  self.sortFail += 1
               else:
                  self.key = sortval
                  complete = True
                     
            if not complete:
               self._getLine()
               if self.currLine is None:
                  complete = True
                  self.key = None
                  
      except Exception, e:
         self.log.error('Failed in getNextSortVal, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.key = None

   # ...............................................
   def getNextKey(self):
      """
      Fills in self.key and self.currLine
      """
      complete = False
      self.key = None
      self._getLine()
      try:
         while self._csvreader is not None and not complete:
            if len(self.currLine) >= 16:
               try:
                  txkey = int(self.currLine[self._sortIdx])
               except Exception, e:
                  self.log.debug('Failed on line %d (%s)' 
                            % (self.currRecnum, str(self.currLine)))
               else:
                  self.key = txkey
                  complete = True
                     
            if not complete:
               self._getLine()
               if self.currLine is None:
                  complete = True
                  self.key = None
                  
      except Exception, e:
         self.log.error('Failed in getNextKey, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.key = None
         
         
   # ...............................................
   def getThisChunk(self):
      """
      Returns chunk for self.key, updates with next key and currline 
      """
      complete = False
      currCount = 0
      firstLineno = self.currRecnum
      currkey = self.key
      chunk = []

      try:
         while self._csvreader is not None and not complete:
            chunk.append(self.currLine)
            self.getNextSortVal()
            if self.key == currkey:
               currCount += 1
               chunk.append(self.currLine)
            else:
               complete = True
                     
            if self.currLine is None:
               complete = True
               
            return chunk
                  
      except Exception, e:
         self.log.error('Failed in getNextChunkForCurrKey, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.key = None

   # ...............................................
   def getSizeChunk(self, maxsize):
      """
      Returns chunk for self.key, updates with next key and currline 
      """
      complete = False
      currCount = 0
      firstLineno = self.currRecnum
      chunk = []

      try:
         while self._csvreader is not None and not complete:
            chunk.append(self.currLine)
            if self.currLine is None or sys.getsizeof(chunk) >= maxsize:
               complete = True
            else:
               self._getLine()
               
         return chunk
                  
      except Exception, e:
         self.log.error('Failed in getNextChunkForCurrKey, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.key = None

   # ...............................................
   def eof(self):
      return self.currLine is None
   
   # ...............................................
   def close(self):
      try:
         self._file.close()
      except:
         pass
      self._csvreader = None

