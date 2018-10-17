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

import csv
import json
import os
import sys
import StringIO
from types import DictionaryType, DictType

from LmCommon.common.lmconstants import (ENCODING, OFTInteger, OFTReal, 
                                         OFTString, LMFormat)

# .............................................................................
class OccDataParser(object):
   """
   @summary: Object with metadata and open file.  OccDataParser maintains 
             file position and most recently read data chunk
   """
   FIELD_NAME_KEY = 'name'
   FIELD_TYPE_KEY = 'type'
   FIELD_ROLE_KEY = 'role'
   FIELD_VALS_KEY = 'acceptedvals'

   FIELD_ROLE_IDENTIFIER = 'uniqueid'
   FIELD_ROLE_LONGITUDE = 'longitude'
   FIELD_ROLE_LATITUDE = 'latitude'
   FIELD_ROLE_GEOPOINT = 'geopoint'
   FIELD_ROLE_GROUPBY = 'groupby'
   FIELD_ROLE_TAXANAME = 'taxaname'
   FIELD_ROLES = [FIELD_ROLE_LONGITUDE, FIELD_ROLE_LATITUDE, FIELD_ROLE_GEOPOINT,
                  FIELD_ROLE_GROUPBY, FIELD_ROLE_TAXANAME, FIELD_ROLE_IDENTIFIER]

   def __init__(self, logger, data, metadata, delimiter=',', pullChunks=False):
      """
      @summary Reader for arbitrary user CSV data file with
               - header record and metadata with column **names** first, OR
               - no header and and metadata with column **positions** first
      @param logger: Logger to use for the main thread
      @param data: raw data or filename for CSV data
      @param metadata: dictionary or filename containing metadata
      @param pullChunks: use the object to pull chunks of data based on the 
               groupBy column.  This results in 'pre-fetching' a line of
               data at the start of a chunk to establish the group, and 
               identifying the end of a chunk when the current line does not 
               match the existing chunk.
      """
      self._rawMetadata = metadata
      self._fieldmeta = None
      self._metadataFname = None
         
      self.dataFname = data
      self.delimiter = delimiter
      self._doPreFetch = pullChunks
      self._file = None
      
      self.log = logger
      self.fieldCount = 0
      
      # Record values to check
      self.filters = {}
      self._idIdx = None
      self._xIdx = None
      self._yIdx = None
      self._geoIdx = None
      self._groupByIdx = None
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
      
      self._csvreader, self._file = self.getReader(data, delimiter)
      if self._file is None:
         self.dataFname = None
      
      self.header = None
      self.filters = None
      self._idIdx = None
      self._xIdx = None
      self._yIdx = None
      self._geoIdx = None
      self._groupByIdx = None
      self._nameIdx = None
      self.fieldCount = None
      self.groupFirstRec = None
      self.currIsGoodEnough = None

   # .............................
   def initializeMe(self):
      """
      @summary: Initializes CSV Reader and interprets metadata
      """
#       fieldmeta, self._metadataFname, doMatchHeader = self.readMetadata(self._rawMetadata)
      fieldmeta, doMatchHeader = self.readMetadata2(self._rawMetadata)
      if doMatchHeader:
         # Read CSV header
         tmpList = self._csvreader.next()
         print ('Header = {}'.format(tmpList))
         self.header = [fldname.strip() for fldname in tmpList]

      (self.columnMeta,
       self.filters,
       self._idIdx,
       self._xIdx,
       self._yIdx,
       self._geoIdx,
       self._groupByIdx, 
       self._nameIdx) = self.getCheckIndexedMetadata(fieldmeta, self.header)
      self.fieldCount = len(self.columnMeta)
      
      # Start by pulling line 1; populates groupVal, currLine and currRecnum
      if self._doPreFetch:
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
            f = StringIO.StringIO()
            f.write(datafile.encode(ENCODING))
            f.seek(0)
            csvreader = csv.reader(f, delimiter=delimiter)
         except IOError, e:
            raise 
         except Exception, e:
            raise Exception('Failed to read or open {}, ({})'
                            .format(datafile, str(e)))
      return csvreader, f

   # .............................................................................
   @property
   def currRecnum(self):
      if self._csvreader:
         return self._csvreader.line_num
      elif self.closed:
         return -9999
      else:
         return None
   
   @property
   def groupByIdx(self):
      return self._groupByIdx

   @property
   def idValue(self):
      idVal = None
      if self.currLine is not None and self._idIdx is not None:
         idVal = self.currLine[self._idIdx]
      return idVal
   
#    @property
#    def xValue(self):
#       xVal = None
#       if self.currLine is not None:
#          xVal = self.currLine[self._xIdx]
#       return xVal
#    
#    @property
#    def yValue(self):
#       yVal = None
#       if self.currLine is not None:
#          yVal = self.currLine[self._yIdx]
#       return yVal   
#       
#    @property
#    def xyStrings(self):
#       x = y = None
#       if self.currLine is not None:
#          x, y = self.getXY(self.currLine, self._xIdx, self._yIdx, self._ptIdx)
#       return x, y   
      
   @property
   def groupByValue(self):
      value = None
      if self.currLine is not None:
         tmp = self.currLine[self._groupByIdx]
         try:
            value = int(tmp)
         except:
            value = str(tmp)
      return value
   
   @property
   def nameValue(self):
      nameVal = None
      if self.currLine is not None:
         nameVal = self.currLine[self._nameIdx]
      return nameVal
   
   @property
   def nameIdx(self):
      return self._nameIdx
   
   @property
   def idFieldName(self):
      if self._idIdx is None:
         return None
      else:
         return self.columnMeta[self._idIdx][self.FIELD_NAME_KEY]
   
   @property
   def idIdx(self):
      return self._idIdx
   
   @property
   def xFieldName(self):
      try:
         return self.columnMeta[self._xIdx][self.FIELD_NAME_KEY]
      except:
         return None

   @property
   def xIdx(self):
      return self._xIdx
   
   
   @property
   def yFieldName(self):
      try:
         return self.columnMeta[self._yIdx][self.FIELD_NAME_KEY]
      except:
         return None
      
   @property
   def yIdx(self):
      return self._yIdx
   
   @property
   def ptFieldName(self):
      try:
         return self.columnMeta[self._geoIdx][self.FIELD_NAME_KEY]
      except:
         return None

   @property
   def ptIdx(self):
      return self._geoIdx
   

   # .............................................................................
   @staticmethod
   def readMetadata2(metadata):
      """
      @summary: Reads a stream/string of metadata describing a CSV file of  
                species occurrence point data, and converts roles to lowercase
      @return: a dictionary with 
         Key = column name or column index
         Value = dictionary of keys 'name', 'type', 'role', and 'acceptedVals'
                               values 
      @note: A full description of the input data is at 
             LmDbServer/boom/occurrence.meta.example
      """
      # Read file contents 
      try:
         with open(metadata) as f:
            meta = json.load(f)
      except IOError, e:
         print( 'Failed to open {} err: {}'.format(metadata, str(e)))
         raise
      except Exception, e:
         # or string
         try:
            meta = json.loads(metadata)
         # parse as oldstyle
         except:
            with open(metadata) as f:
               metalines = f.readlines()
            meta = OccDataParser.readOldMetadata(metalines)
         
      # If keys are column indices, change to ints
      doMatchHeader = False
      columnMeta = {}
      for k, v in meta.iteritems():
         try:
            columnMeta[int(k)] = v
         except:
            doMatchHeader = True
            break
      if not(doMatchHeader):
         meta = columnMeta
         
      return meta, doMatchHeader
      

   # .............................................................................
   @staticmethod
   def readMetadata(metadata):
      """
      @summary: Reads a stream/string of metadata describing a CSV file of  
                species occurrence point data, and converts roles to lowercase
      @return: a dictionary with 
         Key = column name or column index
         Value = dictionary of keys 'name', 'type', 'role', and 'acceptedVals'
                               values 
      @note: A full description of the input data is at 
             LmDbServer/boom/occurrence.meta.example
      """
      doMatchHeader = False
      try:
         f = open(metadata, 'r')
      except IOError, e:
         print( 'Failed to open {} err: {}'.format(metadata, str(e)))
         raise
      except Exception, e:
         fieldmeta = metadata  
      else:
         fieldmeta = {} 
         try:
            for line in f:
               if not line.startswith('#'):
                  tmp = line.split(',')
                  parts = [p.strip() for p in tmp]
                  # First value is original fieldname or column index
                  key = parts[0]
                  try:
                     key = int(parts[0])
                  except:
                     if len(key) == 0:
                        key = None
                  if key is not None:
                     if len(tmp) < 3:
                        print('Skipping field {} without name or type'.format(key))
                        fieldmeta[key] = None
                     else:
                        # Required second value is fieldname, must 
                        # be 10 chars or less to write to a shapefile
                        # Required third value is string/real/integer or None to ignore
                        name = parts[1]
                        ftype = parts[2]
                        # Convert to OGR field type
                        ogrtype = OccDataParser.getOgrFieldType(ftype)
                        if ogrtype is None:
                           # Skip field without OGR type
                           fieldmeta[key] = None
                        else:
                           fieldmeta[key] = {OccDataParser.FIELD_NAME_KEY: name, 
                                             OccDataParser.FIELD_TYPE_KEY: ogrtype}
                           # Optional remaining values are role and/or allowable values
                           if len(parts) >= 4:
                              # Convert to lowercase 
                              rest = []
                              for val in parts[3:]:
                                 try:
                                    rest.append(val.lower())
                                 except:
                                    rest.append(val)
                              # If there are 4+ values, fourth may be role of this field: 
                              #   longitude, latitude, geopoint, groupby, taxaname, uniqueid
                              # Convert to lowercase
                              if rest[0] in OccDataParser.FIELD_ROLES:
                                 fieldmeta[key][OccDataParser.FIELD_ROLE_KEY] = rest[0]
                                 rest = rest[1:]
                              # Remaining values are acceptable values for this field
                              if len(rest) >= 1:
                                 fieldmeta[key][OccDataParser.FIELD_VALS_KEY] = rest
         except Exception, e:
            raise Exception('Failed to evaluate contents of metadata {}, ({})'
                            .format(metadata, e))
         finally:
            f.close()
            
      if type(fieldmeta) not in (DictionaryType, DictType):
         raise Exception('Failed to read or open {}'.format(metadata))
      
      for key in fieldmeta.keys():
         try:
            int(key)
         except:
            doMatchHeader = True
            break
            
      return fieldmeta, doMatchHeader
         
   # .............................................................................
   @staticmethod
   def readOldMetadata(metalines):
      """
      @summary: Reads a stream/string of metadata describing a CSV file of  
                species occurrence point data, and converts roles to lowercase
      @return: a dictionary with 
         Key = column name or column index
         Value = dictionary of keys 'name', 'type', 'role', and 'acceptedVals'
                               values 
      @note: A full description of the input data is at 
             LmDbServer/boom/occurrence.meta.example
      """
      fieldmeta = {} 
      try:
         for line in metalines:
            if not line.startswith('#'):
               tmp = line.split(',')
               parts = [p.strip() for p in tmp]
               # First value is original fieldname or column index
               key = parts[0]
               try:
                  key = int(parts[0])
               except:
                  if len(key) == 0:
                     key = None
               if key is not None:
                  if len(tmp) < 3:
                     print('Skipping field {} without name or type'.format(key))
                     fieldmeta[key] = None
                  else:
                     # Required second value is fieldname, must 
                     # be 10 chars or less to write to a shapefile
                     # Required third value is string/real/integer or None to ignore
                     name = parts[1]
                     ftype = parts[2]
                     # Convert to OGR field type
                     ogrtype = OccDataParser.getOgrFieldType(ftype)
                     if ogrtype is None:
                        # Skip field without OGR type
                        fieldmeta[key] = None
                     else:
                        fieldmeta[key] = {OccDataParser.FIELD_NAME_KEY: name, 
                                          OccDataParser.FIELD_TYPE_KEY: ogrtype}
                        # Optional remaining values are role and/or allowable values
                        if len(parts) >= 4:
                           # Convert to lowercase 
                           rest = []
                           for val in parts[3:]:
                              try:
                                 rest.append(val.lower())
                              except:
                                 rest.append(val)
                           # If there are 4+ values, fourth may be role of this field: 
                           #   longitude, latitude, geopoint, groupby, taxaname, uniqueid
                           # Convert to lowercase
                           if rest[0] in OccDataParser.FIELD_ROLES:
                              fieldmeta[key][OccDataParser.FIELD_ROLE_KEY] = rest[0]
                              rest = rest[1:]
                           # Remaining values are acceptable values for this field
                           if len(rest) >= 1:
                              fieldmeta[key][OccDataParser.FIELD_VALS_KEY] = rest
      except Exception, e:
         raise Exception('Failed to parse metadata, ({})'.format(e))
                     
      return fieldmeta

   # .............................................................................
   @staticmethod
   def getCheckIndexedMetadata(fldmeta, header):
      """
      @summary: Identify data columns from metadata dictionary and optional 
                data header. If the header is present, convert keys 
                in the fldmeta dictionary from column names to column indices.
      @param fldmeta: Dictionary of field names, types, roles, and accepted values.
                      If the first level 'Value' is None, this field will be ignored
                         Key = name in header or column index
                         Value = None or
                                 Dictionary of 
                                   key = ['name', 
                                          'type', 
                                          optional 'role', and 
                                          optional 'acceptedVals']
                                   values for those items
                      Keywords identify roles for x, y, id, grouping, taxa name.
      @param header: First row of data file containing field names for values 
                     in subsequent rows. Field names match those in fldmeta 
                     dictionary
      @return: list of: fieldName list (order of data columns)
                        fieldType list (order of data columns)
                        dictionary of filters for accepted values for zero or 
                            more fields, keys are the new field indexes
                        column indexes for id, x, y, geopoint, groupBy, and name fields
      """
      filters = {}
      idIdx = xIdx = yIdx = ptIdx = groupByIdx = nameIdx = None
      # If necessary, build new metadata dict with column indexes as keys
      if header is None:
         # keys are column indexs
         fieldIndexMeta = fldmeta
      else:
         # keys are fieldnames
         fieldIndexMeta = {}
         for i in range(len(header)):
            try:
               fieldIndexMeta[i] = fldmeta[header[i]]
            except:
               fieldIndexMeta[i] = None
      
      for idx, vals in fieldIndexMeta.iteritems():
         # add placeholders in the fieldnames and fieldTypes lists for 
         # columns we will not process 
         ogrtype = role = acceptedVals = None
         if vals is not None:
            # Get required vals for columns to save  
            name = fieldIndexMeta[idx][OccDataParser.FIELD_NAME_KEY]
            ogrtype = fieldIndexMeta[idx][OccDataParser.FIELD_TYPE_KEY]
            # Check for optional filter AcceptedValues.  
            try:
               acceptedVals = fieldIndexMeta[idx]['acceptedVals']
            except:
               pass
            else:
               # Convert acceptedVals to lowercase
               if ogrtype == OFTString:
                  fieldIndexMeta[idx][OccDataParser.FIELD_VALS_KEY] = [val.lower() for val in acceptedVals]
            # Find column index of important fields
            try:
               role = fieldIndexMeta[idx][OccDataParser.FIELD_ROLE_KEY].lower()
            except:
               pass
            else:
               # If role exists, convert to lowercase
               fieldIndexMeta[idx]['role'] = role
               if role == OccDataParser.FIELD_ROLE_IDENTIFIER:
                  idIdx = idx
                  print ('Found id index {}').format(idx)
               elif role == OccDataParser.FIELD_ROLE_LONGITUDE:
                  xIdx = idx
                  print ('Found X index {}').format(idx)
               elif role == OccDataParser.FIELD_ROLE_LATITUDE:
                  yIdx = idx
                  print ('Found Y index {}').format(idx)
               elif role == OccDataParser.FIELD_ROLE_GEOPOINT:
                  ptIdx = idx
                  print ('Found point index {}').format(idx)
               elif role == OccDataParser.FIELD_ROLE_TAXANAME:
                  nameIdx = idx
                  print ('Found name index {}').format(idx)
               elif role == OccDataParser.FIELD_ROLE_GROUPBY:
                  groupByIdx = idx
                  print ('Found group index {}').format(idx)
         filters[idx] = acceptedVals

      # Check existence of required roles
      if nameIdx is None:
         nameIdx = groupByIdx
      if (xIdx is None or yIdx is None) and ptIdx is None:
         print ('Found x {}, y {}, point {}').format(xIdx, yIdx, ptIdx)
         raise Exception('Missing `LATITUDE`-`LONGITUDE` pair or `GEOPOINT` roles in metadata')
      if groupByIdx is None:
         raise Exception('Missing `GROUPBY` required role in metadata')
      return (fieldIndexMeta, filters, idIdx, xIdx, yIdx, ptIdx, groupByIdx, nameIdx)
      
   # .............................................................................
   @staticmethod
   def getOgrFieldType(typeString):
      typestr = typeString.lower()
      if typestr == 'none':
         return None
      elif typestr in ('int', 'integer'):
         return OFTInteger
      elif typestr == 'string':
         return OFTString
      elif typestr == 'real':
         return OFTReal
      else:
         raise Exception('Unsupported field type {} (requires None, int, string, real)'
                         .format(typeString))
   
   # ...............................................
   @staticmethod
   def getXY(line, xIdx, yIdx, geoIdx):
      """
      @note: returns Longitude/X Latitude/Y from x, y fields or geopoint
      """
      x = y = None
      try:
         x = line[xIdx]
         y = line[yIdx]
      except:
         pt = line[geoIdx]
         npt = pt.strip('{').strip('}')
         newcoords = npt.split(',')
         for coord in newcoords:
            try:
               latidx = coord.index('lat')
            except:
               # Longitude
               try:
                  lonidx = coord.index('lon')
               except:
                  pass
               else:
                  if lonidx >= 0:
                     tmp = coord[lonidx+3:].strip()
                     x = tmp.replace('"', '').replace(':', '').replace(',', '').strip()
            # Latitude
            else:
               if latidx >= 0:
                  tmp = coord[latidx+3:].strip()
                  y = tmp.replace('"', '').replace(':', '').replace(',', '').strip()
      return x, y

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
         if acceptedVals is not None and val not in acceptedVals:
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
      x, y = self.getXY(line, self._xIdx, self._yIdx, self._geoIdx)
      try:
         float(x)
         float(y)
      except Exception, e:
         self.badGeos += 1
         goodEnough = False
      else:
         if x == 0 and y == 0:
            self.badGeos += 1
            goodEnough = False
               
#       # Dataset name value
#       if line[self._nameIdx] == '':
#          self.badNames += 1
         
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
            self.log.debug('EOF after rec %d' % (self.currRecnum))
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
   # TODO: get rid of this, use property groupByValue
   def _getGroupByValue(self, line):
      try:
         value = int(line[self._groupByIdx])
      except:
         value = str(line[self._groupByIdx])
      return value

   # ...............................................
   def pullNextValidRec(self):
      """
      Fills in self.groupVal and self.currLine
      @TODO: get rid of self.groupVal, use property groupByValue
      """
      complete = False
      self.groupVal = None
      line, goodEnough = self._getLine()
      if self.closed:
         self.currLine = self.groupVal = None
      try:
         while self._csvreader is not None and not self.closed and not complete:
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
                  self.log.info('Unable to pullNextValidRec; completed')
                  
      except Exception, e:
         self.log.error('Failed in pullNextValidRec, currRecnum=%s, e=%s' 
                   % (str(self.currRecnum), str(e)))
         self.currLine = self.groupVal = None

   # ...............................................
   def printStats(self):
      if not self.closed:
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
      chunkGroup = self.groupByValue
      chunkName = self.nameValue
      chunk = []
      
      if self.currLine is not None:
         # first line of chunk is currLine
         goodEnough = self._testLine(self.currLine)
         if goodEnough:
            chunk.append(self.currLine)
            
         try:
            while not self.closed and not complete:
               # get next line
               self.pullNextValidRec()
               
               # Add to or complete chunk
               if self.groupByValue == chunkGroup:
                  currCount += 1
                  chunk.append(self.currLine)
               else:
                  complete = True
                  self.groupFirstRec = self.currRecnum
                  
               if self.currLine is None:
                  complete = True
                                    
         except Exception, e:
            self.log.error('Failed in getNextChunkForCurrKey, currRecnum=%s, e=%s' 
                      % (str(self.currRecnum), str(e)))
            self.currLine = self.groupVal = None
      return chunk, chunkGroup, chunkName

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

#    # ...............................................
#    def eof(self):
#       return self.currLine is None
   
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
   op = OccDataParser(log, pthAndBasename + LMFormat.CSV.ext, 
                      pthAndBasename + LMFormat.METADATA.ext, pullChunks=True)
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
from LmCommon.common.lmconstants import (LMFormat, ENCODING,
                                         OFTInteger, OFTReal, OFTString)
from LmCompute.common.log import TestLogger
from LmServer.common.localconstants import APP_PATH

dataname = 'LmTest/data/sdm/heuchera_all'

pthAndBasename = os.path.join(APP_PATH, datafname)
log = TestLogger('occparse_checkInput')

dataFname = pthAndBasename + LMFormat.CSV.ext
metadataFname = pthAndBasename + LMFormat.METADATA.ext
delimiter = ','

# Read metadata file/stream
fieldmeta, metadataFname, doMatchHeader = OccDataParser.readMetadata(metadata)   
csvreader, _file = OccDataParser.getReader(data, delimiter)

# Read CSV header
tmpList = csvreader.next()
header = [fldname.strip() for fldname in tmpList]

(fieldIndexMeta, filters, idIdx, xIdx, yIdx, ptIdx, groupByIdx, 
nameIdx) = OccDataParser.getCheckIndexedMetadata(fieldmeta, header)

# open parser
occparser = OccDataParser(log, data, metadata, delimiter=delimiter)

# Read CSV header
csvreader, f = OccDataParser.getReader(data, delimiter)
tmpHeader = csvreader.next()
header = [fldname.strip() for fldname in tmpHeader]
# Read metadata file/stream
fieldmeta, metadataFname, doMatchHeader = OccDataParser.readMetadata(metadata)


(fieldIndexMeta, filters, 
 idIdx, xIdx, yIdx, groupByIdx, nameIdx) = OccDataParser.getCheckIndexedMetadata(fieldmeta, 
                                                                  header)       
op = OccDataParser(log, data, metadata, delimiter=',')
op.readAllRecs()
op.printStats()
op.close()

"""