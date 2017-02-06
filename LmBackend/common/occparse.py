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
   FIELD_ROLE_IDENTIFIER = 'UniqueID'
   FIELD_ROLE_LONGITUDE = 'Longitude'
   FIELD_ROLE_LATITUDE = 'Latitude'
   FIELD_ROLE_GROUPBY = 'GroupBy'
   FIELD_ROLE_TAXANAME = 'Taxa'
   REQUIRED_FIELD_ROLES = [FIELD_ROLE_LONGITUDE, FIELD_ROLE_LATITUDE, 
                           FIELD_ROLE_GROUPBY, FIELD_ROLE_TAXANAME]

   def __init__(self, logger, datafile, metadatafile, delimiter='\t'):
      """
      @summary Reader for arbitrary user CSV data file with header record and 
               metadata file
      @param logger: Logger to use for the main thread
      @param data: raw data or filename for CSV data
      @param metadata: dictionary or filename containing dictionary of metadata
      """
      self.metadataFname = metadatafile
      self.dataFname = datafile
      self.delimiter = delimiter
      self._file = None
      
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
      self.groupVal = None
      self.groupFirstRec = 0      
      self.currLine = None
      
      self._csvreader, self._file = self.getReader(datafile, delimiter)
      if self._file is None:
         self.dataFname = None
      
      # Read CSV header
      tmpHeader = self._csvreader.next()
      self.header = [fldname.strip() for fldname in tmpHeader]
      # Read metadata file/stream
      fieldmeta, metadataFname = self.readMetadata(metadatafile)
      if metadataFname is None:
         self.metadataFname = metadataFname
      
      self._populateMetadata(fieldmeta, self.header)
         
      # Start by pulling line 1; populates groupVal, currLine and currRecnum
      self.pullNextValidRec()
      # record number of the chunk of current key
      self.groupFirstRec = self.currRecnum     
      self.currIsGoodEnough = True

   # .............................................................................
   @staticmethod
   def getReader(datafile, delimiter):
      f = None  
      csv.field_size_limit(sys.maxsize)
      try:
         f = open(datafile, 'r')
         csvreader = csv.reader(f, delimiter=delimiter)
      except Exception, e:
         try:
            csvData = StringIO.StringIO()
            csvData.write(datafile.encode(ENCODING))
            csvData.seek(0)
            csvreader = csv.reader(csvData, delimiter=delimiter)
         except Exception, e:
            raise Exception('Failed to read or open {}'.format(datafile))
      return csvreader, f

   # .............................................................................
   def _populateMetadata(self, fieldmeta, header):
      try:
         (fieldNames, fieldTypes, filters, idIdx, xIdx, yIdx, sortIdx, 
          nameIdx) = self.getMetadata(fieldmeta, self.header)
      except Exception, e:
         self.log.warning(str(e))
         try:
            (fieldNames, fieldTypes, filters, idIdx, xIdx, yIdx, sortIdx, 
             nameIdx) = self.getMetadataDeprecated(fieldmeta, self.header)
         except Exception, e:
            raise Exception('Failed to read header or metadata, ({})'
                            .format(str(e))) 
      self.fieldNames = fieldNames
      self.fieldCount = len(fieldNames)
      self.fieldTypes = fieldTypes
      self.filters = filters
      self._idIdx = idIdx
      self._xIdx = xIdx
      self._yIdx = yIdx
      self._sortIdx = sortIdx
      self._nameIdx = nameIdx

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
      if self.currLine is not None and self._idIdx is not None:
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
      if self._idIdx is None:
         return None
      else:
         return self.fieldNames[self._idIdx]
   
   @property
   def xFieldName(self):
      return self.fieldNames[self._xIdx]
   
   @property
   def yFieldName(self):
      return self.fieldNames[self._yIdx]
      
   # .............................................................................
   @staticmethod
   def readMetadata(metadata):
      fieldmeta = metadataFname = None
      try:
         f = open(metadata, 'r')
      except Exception, e:
         fieldmeta = metadata            
      else:
         metadataFname = metadata
         try:
            metaStr = f.read()
            fieldmeta = ast.literal_eval(metaStr)
         except Exception, e:
            raise Exception('Failed to evaluate contents of metadata file {}'
                            .format(metadataFname))
         finally:
            f.close()
            
      if type(fieldmeta) not in (DictionaryType, DictType):
         raise Exception('Failed to read or open {}'.format(metadata))
      return fieldmeta, metadataFname
         
   # .............................................................................
   @staticmethod
   def getMetadataDeprecated(fldmeta, header):
      """
      @copydoc LmBackend.common.occparse.OccDataParser::getMetadata()
      """
      fieldNames = []
      fieldTypes = []
      filters = {}
      idIdx = xIdx = yIdx = sortIdx = nameIdx = None

      for i in range(len(header)):         
         oname = header[i]
         shortname = fldmeta[oname][0]
         ogrtype = OccDataParser.getOgrFieldType(fldmeta[oname][1])
         fieldNames.append(shortname)
         fieldTypes.append(ogrtype)
         
         if len(fldmeta[oname]) == 3:
            if type(fldmeta[oname][2]) in (ListType, TupleType):
               acceptedVals = fldmeta[oname][2]
               if ogrtype == OFTString:
                  acceptedVals = [val.lower() for val in fldmeta[oname][2]]
               OccDataParser.filters[i] = acceptedVals 
            else:
               role = fldmeta[oname][2].lower()
               if role == 'id':
                  idIdx = i
               elif role == 'longitude':
                  xIdx = i
               elif role == 'latitude':
                  yIdx = i
               elif role == 'groupby':
                  sortIdx = i
               elif role == 'dataname':
                  nameIdx = i
      
      if (xIdx == None or yIdx == None or sortIdx == None or nameIdx == None):
         raise Exception('Missing one of required field roles ({}) in header'
                         .format(','.join(OccDataParser.REQUIRED_FIELD_ROLES)))
      return (fieldNames, fieldTypes, filters, 
              idIdx, xIdx, yIdx, sortIdx, nameIdx)

   # .............................................................................
   @staticmethod
   def getMetadata(fldmeta, header):
      """
      @summary: Identify data columns from metadata dictionary and data header
      @param fldmeta: Dictionary of field names, types, and filter values.
                      Keywords identify which fields are the x, y, id, grouping 
                      field, taxa name.
      @param header: First row of data file containing field names for values 
                     in subsequent rows. Field names match those in fldmeta 
                     dictionary
      @return: list of: fieldName list (order of data columns)
                        fieldType list (order of data columns)
                        dictionary of filters for accepted values for one or 
                            more fields 
                        integer indexes for id, x, y, groupBy, and name fields
      """
      fieldNames = []
      fieldTypes = []
      filters = {}
      idIdx = xIdx = yIdx = sortIdx = nameIdx = None
      try:
         fldId = fldmeta[OccDataParser.FIELD_ROLE_IDENTIFIER]
      except:
         fldId = None
         
      try:
         fldLon = fldmeta[OccDataParser.FIELD_ROLE_LONGITUDE]
         fldLat = fldmeta[OccDataParser.FIELD_ROLE_LATITUDE]
         fldGrp = fldmeta[OccDataParser.FIELD_ROLE_GROUPBY]
         fldTaxa = fldmeta[OccDataParser.FIELD_ROLE_TAXANAME]
      except:
         raise Exception('Missing one of required field roles ({}) in metadata'
                         .format(','.join(OccDataParser.REQUIRED_FIELD_ROLES)))
      
      for i in range(len(header)):         
         oname = header[i]
         shortname = fldmeta[oname][0]
         ogrtype = OccDataParser.getOgrFieldType(fldmeta[oname][1])
         fieldNames.append(shortname)
         fieldTypes.append(ogrtype)
         # Check for optional filter AcceptedValues.  Records without an
         # AcceptedValue value will be ignored
         if len(fldmeta[oname]) == 3:
            if type(fldmeta[oname][2]) in (ListType, TupleType):
               acceptedVals = fldmeta[oname][2]
               if ogrtype == OFTString:
                  acceptedVals = [val.lower() for val in fldmeta[oname][2]]
               filters[i] = acceptedVals 
         # Find column index of important fields
         # Id, lat, long will always be separate fields
         if oname == fldId:
            idIdx = i
         elif oname == fldLon:
            xIdx = i
         elif oname == fldLat:
            yIdx = i
         # May group by Taxa
         elif oname == fldTaxa:
            nameIdx = i
         if oname == fldGrp:
            sortIdx = i         
      
      if (xIdx == None or yIdx == None or sortIdx == None or nameIdx == None):
         raise Exception('Missing one of required field roles ({}) in header'
                         .format(','.join(OccDataParser.REQUIRED_FIELD_ROLES)))
      return (fieldNames, fieldTypes, filters, 
              idIdx, xIdx, yIdx, sortIdx, nameIdx)
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
         raise Exception('Unsupported field type {} (requires int, string, real)'
                         .format(typeString))
   
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

      # Sort/Group value; may be a string or integer
      try:
         gval = self._getGroupByValue(line)
      except Exception, e:
         self.badGroups += 1
         goodEnough = False
      else:
         self.groupVals.add(gval)
         
      # If present, unique ID value
      if self._idIdx is not None:
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
   def _getGroupByValue(self, line):
      try:
         value = int(line[self._sortIdx])
      except:
         value = str(line[self._sortIdx])
      return value

   # ...............................................
   def pullNextValidRec(self):
      """
      Fills in self.groupVal and self.currLine
      """
      complete = False
      self.groupVal = None
      line, goodEnough = self._getLine()
      try:
         while self._csvreader is not None and not complete:
            if line and goodEnough:
               self.currLine = line
               self.groupVal = self._getGroupByValue(line)
               complete = True
                     
            if not complete:
               line, goodEnough = self._getLine()
               if line is None:
                  complete = True
                  self.currLine = None
                  self.groupVal = None
                  
      except Exception, e:
         self.log.error('Failed in pullNextValidRec, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.groupVal = None

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
      @summary: Returns chunk for self.groupVal, updates with groupFirstRec  
                for next chunk and currline 
      """
      complete = False
      currCount = 0
      currgroup = self.groupVal
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
            if self.groupVal == currgroup:
               currCount += 1
               chunk.append(self.currLine)
            else:
               complete = True
               self.groupFirstRec = self.currRecnum
               
            if self.currLine is None:
               complete = True
               
         return chunk
                  
      except Exception, e:
         self.log.error('Failed in getNextChunkForCurrKey, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.groupVal = None

   # ...............................................
   def readAllChunks(self):
      """
      @note: Does not check for goodEnough line
      """
      chunkCount = 0
      while self.currLine is not None:
         chunk = self.pullCurrentChunk()
         self.log.info('Pulled chunk with {} records'.format(len(chunk)))
         chunkCount += 1
      self.log.info('Pulled {} total chunks'.format(chunkCount))
      return chunkCount

   # ...............................................
   def getSizeChunk(self, maxsize):
      """
      @summary: Returns chunk for self.groupVal, updates with groupFirstRec  
                for next chunk and currline 
      """
      complete = False
      chunk = []
      try:
         while self._csvreader is not None and not complete:
            chunk.append(self.currLine)
            if self.currLine is None or sys.getsizeof(chunk) >= maxsize:
               complete = True
            else:
               self.pullNextValidRec()
      except Exception, e:
         self.log.error('Failed in getNextChunkForCurrKey, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.groupVal = None      
      return chunk

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
   from LmCommon.common.lmconstants import OutputFormat
   
   try:
      from LmServer.common.localconstants import APP_PATH
   except:
      try:
         from LmCompute.common.localconstants import LM_PATH as APP_PATH
      except:
         raise Exception('Testing must be done on a Lifemapper instance')
   relpath = 'LmTest/data/sdm'
      
#    dataname = 'gbif_borneo_simple'
   dataname = 'user_heuchera_all'

   pthAndBasename = os.path.join(APP_PATH, relpath, dataname)
   log = TestLogger('occparse_checkInput')
   op = OccDataParser(log, pthAndBasename + OutputFormat.CSV, 
                      pthAndBasename + OutputFormat.METADATA)
   op.readAllRecs()
   op.printStats()
   op.close()

"""
import ast
import csv
import os
import sys
import StringIO
from types import DictionaryType, DictType, ListType, TupleType

from LmBackend.common.occparse import OccDataParser
from LmCommon.common.lmconstants import (OutputFormat, ENCODING,
                                         OFTInteger, OFTReal, OFTString)
from LmCompute.common.log import TestLogger
from LmServer.common.localconstants import APP_PATH

relpath = 'LmTest/data/sdm'
dataname = 'user_heuchera_all'

pthAndBasename = os.path.join(APP_PATH, relpath, dataname)
log = TestLogger('occparse_checkInput')
data = pthAndBasename + OutputFormat.CSV
metadata = pthAndBasename + OutputFormat.METADATA
        
op = OccDataParser(log, data, metadata, delimiter=',')
op.readAllRecs()
op.printStats()
op.close()

"""