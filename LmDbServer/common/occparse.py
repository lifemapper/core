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
      
      # Record values to check
      self.filters = {}
      self._idIdx = None
      self._xIdx = None
      self._yIdx = None
      self._sortIdx = None
      self._nameIdx = None
      
      self.currLine = None      
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
      
      try:
         self._file = open(self.dataFname, 'r')
      except Exception, e:
         raise LMError('Failed to open %s' % self.dataFname)
      csv.field_size_limit(sys.maxsize)
      self._csvreader = csv.reader(self._file, delimiter='\t')
      
      self.header = self._csvreader.next()
      self.currLine = None

      # Read metadata file and close
      self._getMetadata(self.header)
            
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
      while not success:
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
            raise LMError('Bad record {}'.format(e))
         
      return line, goodEnough

   # ...............................................
   def skipToRecord(self, targetnum):
      """
      @note: Does not check for goodEnough line
      """
      complete = False
      while self.currLine is not None and self.currRecnum < targetnum:
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

#    # ...............................................
#    def getNextKey(self):
#       """
#       Fills in self.key and self.currLine
#       """
#       complete = False
#       self.key = None
#       self.pullNextValidRec()
#       try:
#          while self._csvreader is not None and not complete:
#             if line and goodEnough:
#                self.currLine = line
#                self.key = int(line[self._sortIdx])
#                complete = True
# #             try:
# #                txkey = int(line[self._sortIdx])
# #             except Exception, e:
# #                self.log.debug('Failed on line %d (%s)' 
# #                          % (self.currRecnum, str(self.currLine)))
# #             else:
# #                self.key = txkey
# #                complete = True
#                      
#             if not complete:
#                line, goodEnough = self._getLine()
#                if line is None:
#                   complete = True
#                   self.key = None
#                   
#       except Exception, e:
#          self.log.error('Failed in getNextKey, currRecnum=%s, e=%s' 
#                    % (str(self.currRecnum), str(e)))
#          line = self.key = None
         
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
         while self._csvreader is not None and not complete:
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
   def close(self):
      try:
         self._file.close()
      except:
         pass
      self._csvreader = None


if __name__ == '__main__':
   from LmServer.common.log import ScriptLogger
   metafname = '/tank/data/input/species/gbif_borneo_simple.meta'
   datafname = '/tank/data/input/species/sorted_gbif_borneo_simple.csv'
   datafname = '/tank/data/input/species/gbif_borneo_simple.csv'
   
   log = ScriptLogger('occparse_checkInput')
   op = OccDataParser(log, datafname, metafname)
   op.readAllRecs()
   op.printStats()
   op.close()
