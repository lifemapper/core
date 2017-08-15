#!/opt/python/bin/python2.7
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
import argparse
import csv
import os
import sys
import time

ONE_MIN = 1.0/1440.0
try:
   from osgeo.ogr import OFTInteger, OFTReal, OFTString, OFTBinary
except:
   OFTInteger = 0 
   OFTReal = 2 
   OFTString = 4
   OFTBinary = 8

# from LmCommon.common.lmconstants import (GBIF, GBIF_QUERY) 
class DWCNames:
   OCCURRENCE_ID = {'FULL': 'occurrenceID', 'SHORT': 'occurid'}
   INSTITUTION_CODE = {'FULL': 'institutionCode', 'SHORT': 'inst_code'}
   INSTITUTION_ID = {'FULL': 'institutionID', 'SHORT': 'inst_id'}
   COLLECTION_CODE = {'FULL': 'collectionCode', 'SHORT': 'coll_code'}
   COLLECTION_ID = {'FULL': 'collectionID', 'SHORT': 'coll_id'}
   CONTINENT = {'FULL': 'continent', 'SHORT': 'continent'}
   CATALOG_NUMBER = {'FULL': 'catalogNumber', 'SHORT': 'catnum'}
   BASIS_OF_RECORD = {'FULL': 'basisOfRecord', 'SHORT': 'basisofrec'}
   DECIMAL_LATITUDE = {'FULL': 'decimalLatitude', 'SHORT': 'dec_lat'}
   DECIMAL_LONGITUDE = {'FULL': 'decimalLongitude', 'SHORT': 'dec_long'}
   SCIENTIFIC_NAME = {'FULL': 'scientificName', 'SHORT': 'sciname'}
   DAY = {'FULL': 'day', 'SHORT': 'day'}
   MONTH = {'FULL': 'month', 'SHORT': 'month'}
   YEAR = {'FULL': 'year', 'SHORT': 'year'}
   RECORDED_BY = {'FULL': 'recordedBy', 'SHORT': 'rec_by'}
   COUNTRY_CODE = {'FULL': 'countryCode', 'SHORT': 'ctrycode'}
   STATE_PROVINCE = {'FULL': 'stateprovince', 'SHORT': 'stprov'}

class GBIF:
   TAXON_KEY = 'specieskey'
   TAXON_NAME = DWCNames.SCIENTIFIC_NAME['SHORT']
   PROVIDER = 'puborgkey'
   GBIFID = 'gbifid'
   WAIT_TIME = 3 * ONE_MIN
   LIMIT = 300
   REST_URL = 'http://api.gbif.org/v1'
   
   SPECIES_SERVICE = 'species'
   OCCURRENCE_SERVICE = 'occurrence'
   DATASET_SERVICE = 'dataset'
   ORGANIZATION_SERVICE = 'organization'
   
   TAXONKEY_FIELD = 'specieskey'
   TAXONNAME_FIELD = DWCNames.SCIENTIFIC_NAME['SHORT']
   PROVIDER_FIELD = 'puborgkey'
   ID_FIELD = 'gbifid'

   REQUEST_SIMPLE_QUERY_KEY = 'q'
   REQUEST_NAME_QUERY_KEY = 'name'
   REQUEST_TAXON_KEY = 'TAXON_KEY'
   REQUEST_RANK_KEY = 'rank'
   REQUEST_DATASET_KEY = 'dataset_key'                
   
   DATASET_BACKBONE_VALUE = 'GBIF Backbone Taxonomy'
   
   SEARCH_COMMAND = 'search'
   COUNT_COMMAND = 'count'
   MATCH_COMMAND = 'match'
   DOWNLOAD_COMMAND = 'download'
   DOWNLOAD_REQUEST_COMMAND = 'request'
   RESPONSE_IDENTIFIER_KEY = 'key'
   RESPONSE_RESULT_KEY = 'results'
   RESPONSE_END_KEY = 'endOfRecords'
   RESPONSE_COUNT_KEY = 'count'
   RESPONSE_GENUS_ID_KEY = 'genusKey'
   RESPONSE_GENUS_KEY = 'genus'
   RESPONSE_SPECIES_ID_KEY = 'speciesKey'
   RESPONSE_SPECIES_KEY = 'species'
   RESPONSE_MATCH_KEY = 'matchType'
   RESPONSE_NOMATCH_VALUE = 'NONE'
   
   # For writing files from GBIF DarwinCore download, 
   # DWC translations in lmCompute/code/sdm/gbif/constants
   # We are adding the 2 fields: LM_WKT_FIELD and LINK_FIELD
   LINK_FIELD = 'gbifurl'
   # Ends in / to allow appending unique id
   LINK_PREFIX = 'http://www.gbif.org/occurrence/'

class GBIF_QUERY:
   TAXON_FIELDS = {0: ('taxonkey', OFTString), 
                   1: ('kingdom', OFTString),
                   2: ('phylum', OFTString),
                   3: ('class', OFTString), 
                   4: ('order', OFTString),
                   5: ('family', OFTString),
                   6: ('genus', OFTString),
                   7: (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                   8: ('genuskey', OFTInteger),
                   9: (GBIF.TAXONKEY_FIELD, OFTInteger),
                   10:('count', OFTInteger)}
   EXPORT_FIELDS = {0: (GBIF.ID_FIELD, OFTInteger), 
                    1: (DWCNames.OCCURRENCE_ID['SHORT'], OFTInteger), 
                    2: ('taxonkey', OFTInteger),
                    3: ('datasetkey', OFTString),
                    4: (GBIF.PROVIDER_FIELD, OFTString),
                    5: (DWCNames.BASIS_OF_RECORD['SHORT'], OFTString),
                    6: ('kingdomkey', OFTInteger),
                    7: ('phylumkey', OFTInteger),
                    8: ('classkey', OFTInteger),
                    9: ('orderkey', OFTInteger),
                    10: ('familykey', OFTInteger), 
                    11: ('genuskey', OFTInteger),
                    12: (GBIF.TAXONKEY_FIELD, OFTInteger),
                    13: (DWCNames.SCIENTIFIC_NAME['SHORT'], OFTString),
                    14: (DWCNames.DECIMAL_LATITUDE['SHORT'], OFTReal),
                    15: (DWCNames.DECIMAL_LONGITUDE['SHORT'], OFTReal),
                    16: (DWCNames.DAY['SHORT'], OFTInteger),
                    17: (DWCNames.MONTH['SHORT'], OFTInteger),
                    18: (DWCNames.YEAR['SHORT'], OFTInteger),
                    19: (DWCNames.RECORDED_BY['SHORT'], OFTString),
                    20: (DWCNames.INSTITUTION_CODE['SHORT'], OFTString),
                    21: (DWCNames.COLLECTION_CODE['SHORT'], OFTString),
                    22: (DWCNames.CATALOG_NUMBER['SHORT'], OFTString)}
   PARAMS = {GBIF.SPECIES_SERVICE: {'status': 'ACCEPTED',
                                    GBIF.REQUEST_RANK_KEY: None,
                                    GBIF.REQUEST_DATASET_KEY: None,
                                    GBIF.REQUEST_NAME_QUERY_KEY: None},
             GBIF.OCCURRENCE_SERVICE: {"GEOREFERENCED": True,
                                       "SPATIAL_ISSUES": False,
#                                     "BASIS_OF_RECORD": ["PRESERVED_SPECIMEN"],
                                       GBIF.REQUEST_TAXON_KEY: None},
             GBIF.DOWNLOAD_COMMAND: {"creator": "aimee",
                                     "notification_address": 
                                       ["lifemapper@mailinator.com"]}}

# .............................................................................
class FileData(object):
   """
   @summary: Object with open file and maintaining file position and most 
             recently read data chunk
   """
   # ...............................................
   def __init__(self, filename, keyCol):
      self._file = open(filename, 'r')
      self._keyCol = keyCol
      self.currLine = None
      self.currRecnum = 0
      self.chunk = []
      self.key = None
      self._csvreader = csv.reader(self._file, delimiter='\t')
      # populates key, currLine and currRecnum
      self.getNextKey()
      
   # ...............................................
   def _getLine(self):
      """
      Fills in self.currLine, self.currRecnum
      """
      success = False
      while not success:
         try:
            self.currLine = self._csvreader.next()
            success = True
            self.currRecnum += 1
         except OverflowError, e:
            self.currRecnum += 1
            logfile.write( 'Overflow on {} ({})\n'.format(self.currRecnum, str(e)))
         except StopIteration, e:
            logfile.write('EOF reading line {}\n'.format(self.currRecnum))
            self.close()
            self.currRecnum = self.currLine = None
            success = True
         except Exception, e:
            logfile.write('Exception reading line {}, maybe EOF? ({})\n'.format(
                                                      self.currRecnum, str(e)))
            raise(e)
#             self.close()
#             self.currRecnum = self.currLine = None
#             success = True
   
   # ...............................................
   def skipToRecord(self, targetnum):
      while self.currLine is not None and self.currRecnum < targetnum:
         self._getLine()

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
                  txkey = int(self.currLine[self._keyCol])
               except Exception, e:
                  logfile.write('ERROR: Failed on line {} ({})\n'
                                .format(self.currRecnum, self.currLine))
               else:
                  self.key = txkey
                  complete = True
                     
            if not complete:
               self._getLine()
               if self.currLine is None:
                  complete = True
                  self.key = None
                  
      except Exception, e:
         logfile.write('ERROR: Failed in getNextKey, currRecnum={}, e={}\n'
                       .format(self.currRecnum, str(e)))
         self.currLine = self.key = None
         
   # ...............................................
   def getThisChunk(self):
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
            self.getNextKey()
            if self.key == currkey:
               currCount += 1
               chunk.append(self.currLine)
            else:
               complete = True
                     
            if self.currLine is None:
               complete = True
               
            return chunk
                  
      except Exception, e:
         logfile.write('ERROR: Failed in getNextChunkForCurrKey, currRecnum={}, e={}\n'
                       .format(self.currRecnum, e))
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
      
# .............................................................................
# .............................................................................
    
# ...............................................
def _getSortedName(datapath, outprefix, run=None):
   if run is not None:
      outprefix = '%s_%d' % (outprefix, run)
#    datestr = '%d-%d-%d' % (today.year, today.month, today.day)
#    return os.path.join(datapath, '%s_%s.txt' % (outprefix, datestr))
   return os.path.join(datapath, '%s.txt' % (outprefix))

# ...............................................
def _getSmallestKeyAndPosition(splitFiles, lastKey=None):
   """
   Get smallest data key and index of file containing that record
   """
   idxOfSmallest = 0
   smallest = splitFiles[idxOfSmallest].key
   for idx in range(len(splitFiles)):
      if splitFiles[idx].key is not None:
         if smallest is None or splitFiles[idx].key < smallest:
            smallest = splitFiles[idx].key
            idxOfSmallest = idx
   # Log each search for smallest chunk
   if lastKey is None:
      logfile.write('New key = {} (file {})\n'.format(smallest, idxOfSmallest))
   else:
      if smallest > lastKey:
         logfile.write('New key = {} (file {})\n'.format(smallest, idxOfSmallest))
      elif smallest < lastKey:
         logfile.write('Problem = {} (file {})\n'.format(smallest, idxOfSmallest))
   if smallest is None:
      return None, None
   else:
      return smallest, idxOfSmallest
   
# ...............................................
def splitIntoSortedFiles(datapath, dumpFilename, sortedSubsetPrefix, keyCol, logfile):
   fulldumpfilename = os.path.join(datapath, dumpFilename)
   if not os.path.exists(fulldumpfilename):
      raise Exception('ERROR: {} file does not exist'.format(fulldumpfilename))
    
   # Indexes begin with 1
   sortedRuns = 1
   spCount = 0
 
   currfname = _getSortedName(datapath, sortedSubsetPrefix, run=sortedRuns)
   currSortedFile = open(currfname, 'wb') 
   csvwriter = csv.writer(currSortedFile, delimiter='\t')
   dumpData = FileData(fulldumpfilename, keyCol)
   try:
      key, count = _popChunkAndWrite(csvwriter, dumpData)
      while dumpData.currLine is not None:
         # Start new file when encountering a key out of order
         if dumpData.key < key:
            logfile.write('  Wrote {} species to {}, next line = {}\n'.format(
                           spCount, currSortedFile.name, dumpData.currRecnum))
            sortedRuns += 1
            spCount = 0
            currSortedFile, csvwriter = _switchFiles(currSortedFile, csvwriter, 
                        datapath, sortedSubsetPrefix, logfile, run=sortedRuns)
            key, count = _popChunkAndWrite(csvwriter, dumpData)
             
         elif dumpData.key >= key:
            spCount += 1
            key, count = _popChunkAndWrite(csvwriter, dumpData)
             
   except Exception, e:
      logfile.write('ERROR: {}\n'.format(e))
   finally:
      csvwriter = None
      logfile.write('  Wrote final {} species to {}, next line = {}\n'
                    .format(spCount, currfname, dumpData.currRecnum))
      dumpData.close()
      try:
         currSortedFile.close()
      except:
         pass
       
   logfile.write('{} sorted runs; final line {}\n'
                 .format(sortedRuns, dumpData.currRecnum))
   return sortedRuns
    

# ...............................................

def mergeSortedFiles(datapath, inputPrefix, mergePrefix, keyCol, logfile,
                     maxFileSize=None):
   """
   @summary: Merge multiple files of csv records sorted on keyCol, with the 
             same keyCol values in one or more files, into a single file, 
             sorted on keyCol, placing records containing the same keyCol value
             together.
   @param datapath: Path for input and output files
   @param inputPrefix: Filename prefix for input sorted files
   @param mergePrefix: Filename prefix for sorted output file(s)
   @param keyCol: index of the column the data is sorted on
   @param maxFileSize: (optional) maximum number of bytes for output files; 
                       this results in multiple, numbered, sorted output files,  
                       with no keys in more than one file
   """
   # Indexes begin with 1
   inIdx = 1
   outIdx = None
   if maxFileSize is not None:
      outIdx = 1
   # Open output sorted file
   mergeFname = _getSortedName(datapath, mergePrefix, run=outIdx)
   mergeFile = open(mergeFname, 'wb')
   csvwriter = csv.writer(mergeFile, delimiter='\t')
   complete = False

   # Open all input split files
   splitFiles = []
   splitFname = _getSortedName(datapath, inputPrefix, run=inIdx)
   while os.path.exists(splitFname):
      fd = FileData(splitFname, keyCol)
      splitFiles.append(fd)
      inIdx += 1
      splitFname = _getSortedName(datapath, inputPrefix, run=inIdx)
            
   try:
      if len(splitFiles) < 2:
         raise Exception('ERROR: Only {} files to merge (expecting \'{}\')'.format(
                           len(splitFiles), splitFname))
      # find file with record containing smallest key
      smallKey, pos = _getSmallestKeyAndPosition(splitFiles, lastKey=None)
      smallKeyCount = 0
      while pos is not None and not complete:
         # Output records in this file with smallKey 
         key, count = _popChunkAndWrite(csvwriter, splitFiles[pos])
         smallKeyCount += count
         logfile.write('   thisCount = {} (file ({}), totalCount = {}\n'
                       .format(count, pos, smallKeyCount))         
         # If size limit is reached, switch to new file
         if (maxFileSize is not None and 
             os.fstat(mergeFile.fileno()).st_size >= maxFileSize):
            outIdx += 1
            mergeFile, csvwriter = _switchFiles(mergeFile, csvwriter, datapath, 
                                                mergePrefix, logfile, run=outIdx)
            
         # Find smallest again
         smallKey, pos = _getSmallestKeyAndPosition(splitFiles, lastKey=key)
#          _logProgress(pos, smallKey, lastKey, lastCount, smallKeyCount, logfile)
#          if smallKey != thisKey:
#             smallKeyCount = 0
         
   except Exception, e:
      raise
   finally:
      mergeFile.close()
      csvwriter = None
      for fd in splitFiles:
         fd.close()
            
# ...............................................
def _switchFiles(openFile, csvwriter, datapath, prefix, logfile, run=None):
   openFile.close()
   csvwriter = None
   # Open next output sorted file
   newFname = _getSortedName(datapath, prefix, run=run)
   newFile = open(newFname, 'wb')
   logfile.write('Closed {}\n'.format(openFile.name))
   logfile.write('\n')
   logfile.write('Opened {}\n'.format(newFile.name))
   csvwriter = csv.writer(newFile, delimiter='\t')
   return newFile, csvwriter
      
# ...............................................
def _popChunkAndWrite(csvwriter, filedata):
   # first get chunk
   thiskey = filedata.key
   thischunk = filedata.getThisChunk()
   thiscount = len(thischunk)
   
   for rec in thischunk:
      csvwriter.writerow(rec)

   if filedata.eof():
      filedata.close()
   else:
      filedata.getNextKey()
   return thiskey, thiscount
            
  
# ...............................................
def _logProgress(idx, smallKey, thisKey, thisCount, currentCount, logfile):
   if idx is None:
      logfile.write('Completed, final key {}\n'.format(thisKey))
   else:
      logfile.write('         count = {} (file {})\n'.format(thisCount, idx))
      if smallKey > thisKey:
         logfile.write('         Total = {}\n'.format(currentCount))
         logfile.write('New key     = {} (file {})\n'.format(smallKey, idx))
      elif smallKey < thisKey:
         logfile.write('Problem = {} (file {})\n'.format(smallKey, idx))

      
# ...............................................
def checkMergedFile(datapath, filePrefix, keyCol, logfile):
   uniqueCount = failCount = 0
   filename = _getSortedName(datapath, filePrefix)
   bigSortedData = FileData(filename, keyCol)
   prevKey = bigSortedData.key
   tmp = bigSortedData.getThisChunk()
   
   try:
      while bigSortedData.currLine is not None:
         if bigSortedData.key > prevKey: 
            uniqueCount += 1
         elif bigSortedData.key < prevKey:
            logfile.write('ERROR: Failure to sort prevKey {}, bigSortedData.key {}\n'
                  .format(prevKey, bigSortedData.key))
            failCount += 1
         else:
            logfile.write('ERROR: Failure to chunk key {}\n'.format(prevKey))
         tmp = bigSortedData.getThisChunk()
#          logfile.write('ok {}  fail {}'.format(uniqueCount, failCount))    

   except Exception, e:
      logfile.write('ERROR: {}\n'.format(e))
   finally:
      bigSortedData.close()
      
   logfile.write('uniqueCount {}\n'.format(uniqueCount))
   logfile.write('failCount {}\n'.format(failCount))
   logfile.write('currRecnum {}\n'.format(bigSortedData.currRecnum))

# ..............................................................................
# MAIN
# ..............................................................................
# datestr = subprocess.check_output(['date', '+%F']).strip().replace('-', '_')
# datapath = '/tank/data/input/gbif/{}/'.format(datestr)
# dumpFname = 'aimee_export.txt'
# ...............................................
if __name__ == '__main__':
   secs = time.time()
   timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
   logfname = '/tmp/sortGbifExport.{}.log'.format(timestamp)

   splitPrefix = 'gbif_split'
   mergedPrefix = 'gbif_merged'
   oneGb = 1000000000
   
   csv.field_size_limit(sys.maxsize)
   keyCol = None
   for idx, vals in GBIF_QUERY.EXPORT_FIELDS.iteritems():
      if vals[0] == GBIF.TAXONKEY_FIELD:
         keyCol = idx
         break
   
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Process a GBIF CSV database dump file containing ' +
                         'multiple sections of sorted data ' +
                         'into a single sorted file, or multiple sorted files ' +
                         'of approximately max_output_size. File(s) will be ' + 
                         'sorted on column {} (field {}). '
                         .format(keyCol, GBIF.TAXONKEY_FIELD) +
                         'Sorted chunks of data will stay together.'))
   parser.add_argument('filename', metavar='filename', type=str,
                       help='The absolute path of the datafile')
   parser.add_argument('--split', action='store_true',
            help=('Split large CSV file into separate sorted files'))
   parser.add_argument('--merge', action='store_true',
            help=('Merge separate sorted files into one large or multiple ' +
                  'sorted file(s) (data aggregated by {}).'.format(GBIF.TAXONKEY_FIELD)))
   parser.add_argument('--check', action='store_true',
            help=('Test large file for proper sorting.'))
   parser.add_argument('--max_output_size', default=None,
            help=('Maximum size (approximate) in bytes for merged output files.'))
   args = parser.parse_args()   
   fullfilename = args.filename
   doSplit = args.split
   doMerge = args.merge
   doCheck = args.check
   maxFileSize = args.max_output_size

   if not(doSplit or doMerge or doCheck):
      parser.print_help()
      parser.usage
      exit(-1)

   print('doSplit: {}, doMerge: {}, doCheck: {}'.format(doSplit, doMerge, doCheck))

   if not os.path.exists(fullfilename):
      print('Missing input file {}'.format(fullfilename))
      exit(0)

   datapath, dumpFname = os.path.split(fullfilename)      
   try:
      logfile = open(logfname, 'w')
      logfile.write('Process {} to be split, merged on column {}, {}; logfile {}\n'
                    .format(dumpFname, keyCol, GBIF.TAXONKEY_FIELD, logfname))
      if doSplit:         
         # Split big, semi-sorted file, to multiple smaller sorted files
         logfile.write('Splitting ...\n')
         sortedRuns = splitIntoSortedFiles(datapath, dumpFname, splitPrefix, 
                                           keyCol, logfile)
         
      if doMerge:
         # Merge all data for production system into one or more files
         logfile.write('Merging ...\n')
         mergeSortedFiles(datapath, splitPrefix, mergedPrefix, keyCol, logfile,
                          maxFileSize=maxFileSize)
         
      if doCheck and maxFileSize is None:
         # Check final output (only for a single sorted output file)
         logfile.write('Checking ...\n')
         checkMergedFile(datapath, mergedPrefix, keyCol, logfile)
   finally:
      logfile.close()
