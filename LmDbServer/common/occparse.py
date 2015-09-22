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
from types import DictionaryType, DictType, ListType, TupleType

from LmServer.base.lmobj import LMObject, LMError
# .............................................................................
class OccDataParser(LMObject):
   """
   @summary: Object with metadata and open file.  OccDataParser maintains 
             file position and most recently read data chunk
   """
   def __init__(self, logger, dataFname, metadataFname):
      """
      @summary Reader for arbitrary user CSV data file with header record and 
               metadata file
      @param logger: Logger to use for the main thread
      """
      self.log = logger
      self.metadataFname = metadataFname
      self.dataFname = dataFname
      self.fieldNames = [] 
      self.fieldCount = 0
      self.fieldTypes = []
      self.filters = {}
      self._idIdx = None
      self._xIdx = None
      self._yIdx = None
      self._sortIdx = None
      self._nameIdx = None
      self.currLine = None
      # currRecnum is 0 based, starting after header
      self.currRecnum = 0
      self.keyFirstRec = self.currRecnum
      # Requires ID, GroupBy, Longitude, Latitude
      self.requiredCount = 4
      self.currIsGoodEnough = True
      self.currIsBadId  = False 
      self.currIsBadGeo = False
      self.currIsBadGroup = False
      self.currIsBadName = False
      self.currIsBadFilters = False
      
      self.chunk = []
      self.key = None
      
      try:
         self._file = open(self.dataFname, 'r')
      except Exception, e:
         raise LMError('Failed to open %s' % self.dataFname)
      csv.field_size_limit(sys.maxsize)
      self._csvreader = csv.reader(self._file, delimiter='\t')
      
      self.header = self._csvreader.next()
      self.currLine = None
      self.currRecnum = 0

      # Read metadata file and close
      self._getMetadata(self.header)
            
      # populates key, currLine and currRecnum
      self.sortFail = 0
      self.pullNextValidRec()

   # .............................................................................
   @property
   def sortIdx(self):
      return self._sortIdx

   @property
   def idValue(self):
      idVal = None
      if self.currLine is not None:
         idVal = self.currLine[self._idIdx]
      return idVal
   
   @property
   def xValue(self):
      xVal = None
      if self.currLine is not None:
         xVal = self.currLine[self._xIdx]
      return xVal
   
   @property
   def yValue(self):
      yVal = None
      if self.currLine is not None:
         yVal = self.currLine[self._yIdx]
      return yVal   
      
   @property
   def sortValue(self):
      sortVal = None
      if self.currLine is not None:
         sortVal = self.currLine[self._sortIdx]
      return sortVal
   
   @property
   def nameValue(self):
      nameVal = None
      if self.currLine is not None:
         nameVal = self.currLine[self._nameIdx]
      return nameVal
   
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
      fldmeta = self._readMetadata()
      
      for i in range(len(origfldnames)):         
         oname = origfldnames[i]
         shortname = fldmeta[oname][0]
         ogrtype = self.getOgrFieldType(fldmeta[oname][1])
         self.fieldNames.append(shortname)
         self.fieldTypes.append(ogrtype)
         
         if len(fldmeta[oname]) == 3:
            if type(fldmeta[oname][2]) in (ListType, TupleType):
               acceptedVals = fldmeta[oname][2]
               if ogrtype == OFTString:
                  acceptedVals = [val.lower() for val in fldmeta[oname][2]]
               self.filters[i] = acceptedVals 
            else:
               role = fldmeta[oname][2].lower()
               if role == 'id':
                  self._idIdx = i
               elif role == 'longitude':
                  self._xIdx = i
               elif role == 'latitude':
                  self._yIdx = i
               elif role == 'groupby':
                  self._sortIdx = i
               elif role == 'dataname':
                  self._nameIdx = i
      self.fieldCount = len(self.fieldNames)
      
      if self._idIdx == None:
         raise LMError('Missing \'id\' unique identifier field')
      if self._xIdx == None:
         raise LMError('Missing \'longitude\' georeference field')
      if self._yIdx == None:
         raise LMError('Missing \'latitude\' georeference field')
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
   def _testLine(self):
      self.currIsGoodEnough = True
      self.currIsBadId = self.currIsBadGeo = self.currIsBadGroup = False
      self.currIsBadName = self.currIsBadFilters = False
      
      for filterIdx, acceptedVals in self.filters.iteritems():
         val = self.currLine[filterIdx]
         try:
            val = val.lower()
         except:
            pass
         if val not in acceptedVals:
            self.currIsBadFilters = True
            break

      try:
         int(self.currLine[self._idIdx])
      except Exception, e:
         if self.currLine[self._idIdx] == '':
            self.currIsBadId = True
         
      try:
         float(self.currLine[self._xIdx])
         float(self.currLine[self._yIdx])
      except Exception, e:
         self.currIsBadGeo = True
      else:
         if self.currLine[self._xIdx] == 0 and self.currLine[self._yIdx] == 0:
            self.currIsBadGeo = True
               
      try:
         sortkey = int(self.currLine[self._sortIdx])
      except Exception, e:
         self.currIsBadGroup = True
         
      if self.currLine[self._nameIdx] == '':
         self.currIsBadName = True
            
      if (self.currIsBadId or self.currIsBadGeo 
          or self.currIsBadGroup or self.currIsBadFilters):
         self.currIsGoodEnough = False 
         
   # ...............................................
   def _getLine(self):
      """
      Fills in:
         self.currLine, self.currRecnum
         self.currIsGoodEnough, self.currIsBadId, self.currIsBadGeo,
         self.currIsBadGroup, self.currIsBadName
      """
      success = False
      while not success:
         try:
            self.currLine = self._csvreader.next()
            self._testLine()
            success = True
            self.currRecnum += 1
         except OverflowError, e:
            self.currRecnum += 1
            self.log.debug( 'Overflow on %d (%s)' % (self.currRecnum, str(e)))
         except StopIteration:
            self.log.debug('EOF on rec %d' % (self.currRecnum))
            self.close()
            self.currRecnum = self.currLine = None
            success = True
         except Exception, e:
            raise LMError('Bad record {}'.format(e))

   # ...............................................
   def skipToRecord(self, targetnum):
      complete = False
      while self.currLine is not None and self.currRecnum < targetnum:
         self._getLine()

   # ...............................................
   def pullNextValidRec(self):
      """
      Fills in self.key and self.currLine
      """
      complete = False
      self.key = None
      self._getLine()
      try:
         while self._csvreader is not None and not complete:
            if self.currIsGoodEnough:
               self.key = int(self.currLine[self._sortIdx])
               complete = True
                     
            if not complete:
               self._getLine()
               if self.currLine is None:
                  complete = True
                  self.key = None
                  
      except Exception, e:
         self.log.error('Failed in pullNextValidRec, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.key = None

   # ...............................................
   def checkInput(self, maxSize=None):
      """
      Finds number of records with correctly populated required fields 
      """
      total = totalGood = totalMostlyGood = 0
      badId = badGeo = badGroup = badName = 0
      groups = set()
      complete = False
      self.key = None
      if self._csvreader.line_num > 2:
         self.log.error('File is on line {}; checkInput must be run immediately following initialization' 
                        % self._csvreader.line_num)
      
      self._getLine()
      try:
         while self._csvreader is not None and not complete:
            total += 1
                  
            if self.currIsGoodEnough:
               totalGood += 1
            elif self.currIsGoodEnough:
               totalMostlyGood += 1
            if self.currIsBadId:
               badId += 1
            if self.currIsBadGeo:
               badGeo += 1
            if self.currIsBadGroup:
               badGroup += 1
               groups.add(self.currLine[self._sortIdx])
               
            if not complete:
               self._getLine()
               if self.currLine is None:
                  complete = True
                  self.key = None
         
      except Exception, e:
         self.log.error('Failed in getNextKey, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.key = None
      finally:
         report = """
         Totals for {}
         -------------------------------------------------------------
         Total records read: {}
         Total groupings: {}
         Total good records: {}
         Total mostly good records (missing only Dataname): {}
         Breakdown
         ----------
         Records with missing or invalid ID value: {}
         Records with missing or invalid Longitude/Latitude values: {}
         Records with missing or invalid GroupBy value: {}
         Records with missing or invalid Dataname value: {}
         """.format(self.dataFname, total, len(groups), totalGood, 
                    totalMostlyGood, badId, badGeo, badGroup, badName)
         self.log.info(report)

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
   def pullCurrentChunk(self):
      """
      Returns chunk for self.key, updates with next key and currline 
      """
      complete = False
      currCount = 0
      currkey = self.key
      chunk = []

      try:
         while self._csvreader is not None and not complete:
            chunk.append(self.currLine)
            self.pullNextValidRec()
            if self.key == currkey:
               currCount += 1
               chunk.append(self.currLine)
            else:
               complete = True
               self.keyFirstRec = self.currRecnum
               
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


if __name__ == '__main__':
   from LmServer.common.log import ScriptLogger
   metafname = '/share/lmserver/data/species/gbif_borneo_simple.meta'
   datafname = '/share/lmserver/data/species/sorted_gbif_borneo_simple.csv'
#    datafname = '/tank/data/input/species/gbif_borneo_simple.csv'
   
   log = ScriptLogger('occparse_checkInput')
   op = OccDataParser(log, datafname, metafname)
   op.checkInput(maxSize=None)
   op.close()
