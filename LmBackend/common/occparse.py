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
import sys
import StringIO
from types import DictionaryType, DictType, ListType, TupleType

from LmCommon.common.lmconstants import (ENCODING,OFTInteger, OFTReal, OFTString)

# .............................................................................
class OccDataParser(object):
   """
   @summary: Object with metadata and open file.  OccDataParser maintains 
             file position and most recently read data chunk
   """
   def __init__(self, logger, data, metadata, delimiter='\t'):
      """
      @summary Reader for arbitrary user CSV data file with header record and 
               metadata file
      @param logger: Logger to use for the main thread
      @param data: raw data or filename for CSV data
      @param metadata: dictionary or filename containing dictionary of metadata
      """
      self.metadataFname = None
      self.dataFname = None
      self.delimiter = delimiter
      
      self.log = logger
      self.fieldNames = [] 
      self.fieldCount = 0
      self.fieldTypes = []
      
      # Record values to check
      self.filters = {}
      self._idIdx = None
      self._xIdx = None
      self._yIdx = None
      self._sortIdx = None
      self._nameIdx = None
      
      self.currLine = None 
      # record number of the chunk of current key     
      self.keyFirstRec = 0      
      self.currIsGoodEnough = True
      
      # Overall stats
      self.recTotal = 0
      self.recTotalGood = 0
      self.groupTotal = 0
      self.groupVals = set()
      self.badIds = 0
      self.badGeos = 0
      self.badGroups = 0
      self.badNames = 0
      self.badFilters = 0
      self.badFilterVals = set()

      self.chunk = []
      self.key = None
      self.currLine = None
      
      csv.field_size_limit(sys.maxsize)
      try:
         self._file = open(data, 'r')
         self._csvreader = csv.reader(self._file, delimiter=self.delimiter)
         self.dataFname = data
      except Exception, e:
         try:
            csvData = StringIO.StringIO()
            csvData.write(data.encode(ENCODING))
            csvData.seek(0)
            self._csvreader = csv.reader(csvData, delimiter=self.delimiter)
         except Exception, e:
            raise Exception('Failed to read or open {}'.format(data))
      
      # Assume header in first row of data
      self.header = self._csvreader.next()

      try:
         # Read metadata file and close
         self._getMetadata(metadata, self.header)
      except Exception, e:
         raise Exception(currargs='Failed to read header or metadata, ({})'
                         .format(str(e.args))) 
         
      # populates key, currLine and currRecnum
      self.pullNextValidRec()

   # .............................................................................
   @property
   def currRecnum(self):
      if self._csvreader:
         return self._csvreader.line_num
      elif self.eof():
         return -9999
      else:
         return None
   
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
   
   @property
   def idFieldName(self):
      return self.fieldNames[self._idIdx]
   
   @property
   def xFieldName(self):
      return self.fieldNames[self._xIdx]
   
   @property
   def yFieldName(self):
      return self.fieldNames[self._yIdx]
      
   # .............................................................................
   def _readMetadata(self, metadata):
      fldmeta = None
      try:
         f = open(metadata, 'r')
      except Exception, e:
         fldmeta = metadata            
      else:
         self.metadataFname = metadata
         try:
            metaStr = f.read()
            fldmeta = ast.literal_eval(metaStr)
         except Exception, e:
            raise Exception('Failed to evaluate contents of metadata file {}'
                            .format(self.metadataFname))
         finally:
            f.close()
            
      if type(fldmeta) not in (DictionaryType, DictType):
         raise Exception('Failed to read or open {}'.format(metadata))
      return fldmeta
         
   # .............................................................................
   def _getMetadata(self, metadata, origfldnames):
      fldmeta = self._readMetadata(metadata)
      
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
         raise Exception('Missing \'id\' unique identifier field')
      if self._xIdx == None:
         raise Exception('Missing \'longitude\' georeference field')
      if self._yIdx == None:
         raise Exception('Missing \'latitude\' georeference field')
      if self._sortIdx == None:
         raise Exception('Missing \'groupby\' sorting field')
      if self._nameIdx == None:
         raise Exception('Missing \'dataname\' dataset name field')


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
   def _testLine(self, line):
      goodEnough = True
      self.recTotal += 1
      
      # Field filters
      for filterIdx, acceptedVals in self.filters.iteritems():
         val = line[filterIdx]
         try:
            val = val.lower()
         except:
            pass
         if val not in acceptedVals:
            self.badFilterVals.add(val)
            self.badFilters += 1
            goodEnough = False

      # Sort/Group value
      try:
         gval = int(line[self._sortIdx])
      except Exception, e:
         self.badGroups += 1
         goodEnough = False
      else:
         self.groupVals.add(gval)
         
      # Unique ID value
      try:
         int(line[self._idIdx])
      except Exception, e:
         if line[self._idIdx] == '':
            self.badIds += 1
            goodEnough = False
         
      # Lat/long values
      try:
         float(line[self._xIdx])
         float(line[self._yIdx])
      except Exception, e:
         self.badGeos += 1
         goodEnough = False
      else:
         if line[self._xIdx] == 0 and line[self._yIdx] == 0:
            self.badGeos += 1
            goodEnough = False
               
      # Dataset name value
      if line[self._nameIdx] == '':
         self.badNames += 1
         goodEnough = False
         
      if goodEnough:
         self.recTotalGood += 1
         
      return goodEnough
            
   # ...............................................
   def _getLine(self):
      """
      """
      success = goodEnough = False
      line = None
      while not success and self._csvreader is not None:
         try:
            line = self._csvreader.next()
            goodEnough = self._testLine(line)
            success = True
         except OverflowError, e:
            self.log.debug( 'Overflow on %d (%s)' % (self.currRecnum, str(e)))
         except StopIteration:
            self.log.debug('EOF on rec %d' % (self.currRecnum))
            self.close()
            self.currLine = None
            success = True
         except Exception, e:
            self.log.warning('Bad record {}'.format(e))
         
      return line, goodEnough

   # ...............................................
   def skipToRecord(self, targetnum):
      """
      @note: Reads up to, not including, targetnum line.  
      """
      complete = False
      while self.currLine is not None and self.currRecnum < targetnum-1:
         line, goodEnough = self._getLine()

   # ...............................................
   def readAllRecs(self):
      """
      @note: Does not check for goodEnough line
      """
      complete = False
      while self.currLine is not None:
         line, goodEnough = self._getLine()

   # ...............................................
   def pullNextValidRec(self):
      """
      Fills in self.key and self.currLine
      """
      complete = False
      self.key = None
      line, goodEnough = self._getLine()
      try:
         while self._csvreader is not None and not complete:
            if line and goodEnough:
               self.currLine = line
               self.key = int(line[self._sortIdx])
               complete = True
                     
            if not complete:
               line, goodEnough = self._getLine()
               if line is None:
                  complete = True
                  self.currLine = None
                  self.key = None
                  
      except Exception, e:
         self.log.error('Failed in pullNextValidRec, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.key = None

   # ...............................................
   def printStats(self):
      if not self.eof():
         self.log.error('File is on line {}; printStats must be run after reading complete file' 
                        % self._csvreader.line_num)      
      else:
         report = """
         Totals for {}
         -------------------------------------------------------------
         Total records read: {}
         Total good records: {}
         Total groupings: {}
         Breakdown
         ----------
         Records with missing or invalid ID value: {}
         Records with missing or invalid Longitude/Latitude values: {}
         Records with missing or invalid GroupBy value: {}
         Records with missing or invalid Dataname value: {}
         Records with unacceptable value for filter fields: {}
            Indexes: {} 
            Accepted vals: {}
            Bad filter values: {}
         """.format(self.dataFname, self.recTotal, self.recTotalGood, 
                    len(self.groupVals), self.badIds, self.badGeos, 
                    self.badGroups, self.badNames, self.badFilters,  
                    str(self.filters.keys()), str(self.filters.values()), 
                    str(self.badFilterVals))
         self.log.info(report)

   # ...............................................
   def pullCurrentChunk(self):
      """
      Returns chunk for self.key, updates with next key and currline 
      """
      complete = False
      currCount = 0
      currkey = self.key
      chunk = []

      # first line of chunk is currLine
      goodEnough = self._testLine(self.currLine)
      if goodEnough:
         chunk.append(self.currLine)
      else:
         print ('Tried to append bad rec')
         
      try:
         while not self.closed and not complete:
            # get next line
            self.pullNextValidRec()
            
            # Add to or complete chunk
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
               self.pullNextValidRec()
               
         return chunk
                  
      except Exception, e:
         self.log.error('Failed in getNextChunkForCurrKey, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.key = None

   # ...............................................
   def eof(self):
      return self.currLine is None
   
# ...............................................
   @property
   def closed(self):
      return self._file.closed
      
   # ...............................................
   def close(self):
      try:
         self._file.close()
      except:
         pass
      self._csvreader = None


if __name__ == '__main__':
   from LmCompute.common.log import TestLogger
   metafname = '/tank/data/input/species/gbif_borneo_simple.meta'
   datafname = '/tank/data/input/species/sorted_gbif_borneo_simple.csv'
#    datafname = '/tank/data/input/species/gbif_borneo_simple.csv'
   
   log = TestLogger('occparse_checkInput')
   op = OccDataParser(log, datafname, metafname)
   op.readAllRecs()
   op.printStats()
   op.close()
