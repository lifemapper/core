"""
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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

from LmCommon.common.lmconstants import (GBIF_EXPORT_FIELDS, GBIF_TAXONKEY_FIELD) 
from LmServer.common.log import ScriptLogger

# .............................................................................
class FileData(object):
   """
   @summary: Object with open file and maintaining file position and most 
             recently read data chunk
   """
   # ...............................................
   def __init__(self, filename, keyCol, log):
      self.log = log
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
      line = None
      while not success:
         try:
            self.currLine = self._csvreader.next()
            success = True
            self.currRecnum += 1
         except OverflowError, e:
            self.currRecnum += 1
            self.log.debug( 'Overflow on {} ({})'.format(self.currRecnum, str(e)))
         except Exception, e:
            self.log.debug('Exception reading line {}, probably EOF ({})'.format(
                                                      self.currRecnum, str(e)))
            self.close()
            self.currRecnum = self.currLine = None
            success = True
   
   # ...............................................
   def skipToRecord(self, targetnum):
      complete = False
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
                  self.log.debug('Failed on line {} ({})'.format(self.currRecnum, 
                                                            self.currLine))
               else:
                  self.key = txkey
                  complete = True
                     
            if not complete:
               self._getLine()
               if self.currLine is None:
                  complete = True
                  self.key = None
                  
      except Exception, e:
         self.log.error('Failed in getNextKey, currRecnum={}, e={}'.format(
                                                self.currRecnum, str(e)))
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
         self.log.error('Failed in getNextChunkForCurrKey, currRecnum={}, e={}'.format(
                   self.currRecnum, str(e)))
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
def _getSmallestKeyAndPosition(splitFiles):
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
   if smallest is None:
      return None, None
   else:
      return smallest, idxOfSmallest
   
# ...............................................
def splitIntoSortedFiles(log, datapath, dumpFilename, sortedSubsetPrefix, keyCol):
   fulldumpfilename = os.path.join(datapath, dumpFilename)
   if not os.path.exists(fulldumpfilename):
      raise Exception('{} file does not exist'.format(fulldumpfilename))
    
   # Indexes begin with 1
   sortedRuns = 1
   spCount = 0
 
   currfname = _getSortedName(datapath, sortedSubsetPrefix, run=sortedRuns)
   currSortedFile = open(currfname, 'wb') 
   csvwriter = csv.writer(currSortedFile, delimiter='\t')
   dumpData = FileData(fulldumpfilename, keyCol)
   try:
      prevkey, prevcount = _popChunkAndWrite(csvwriter, dumpData)
      while dumpData.currLine is not None:
         # Start new file when encountering a key out of order
         if dumpData.key < prevkey:
            log.debug('Wrote {} species to {}, next line = {}'.format(spCount, 
                                    currSortedFile.name, dumpData.currRecnum))
            sortedRuns += 1
            spCount = 0
            currSortedFile, csvwriter = _switchFiles(currSortedFile, csvwriter, 
                                                     datapath, sortedSubsetPrefix, 
                                                     run=sortedRuns)
            prevkey, prevcount = _popChunkAndWrite(csvwriter, dumpData)
             
         elif dumpData.key >= prevkey:
            spCount += 1
            prevkey, prevcount = _popChunkAndWrite(csvwriter, dumpData)
             
   except Exception, e:
      log.error(str(e))
   finally:
      csvwriter = None
      log.debug('Wrote final species to {}, next line = {}'.format(currfname, 
                                                         dumpData.currRecnum))
      dumpData.close()
      try:
         currSortedFile.close()
      except:
         pass
       
   log.debug('{} sorted runs; final line {}'.format(sortedRuns, 
                                                    dumpData.currRecnum))
   return sortedRuns
    

# ...............................................
   # mergeSortedFiles(log, datapath, prefix, bigSortedFilePrefix, keyCol)
#    mergeSortedFiles(log, datapath, prefix, subsetFilePrefix, keyCol, subset=20000)

def mergeSortedFiles(log, datapath, inputPrefix, mergePrefix, keyCol, 
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
      fd = FileData(splitFname, keyCol, log)
      splitFiles.append(fd)
      inIdx += 1
      splitFname = _getSortedName(datapath, inputPrefix, run=inIdx)
            
   try:
      if len(splitFiles) < 2:
         raise Exception('Only {} files to merge (expecting \'{}\')'.format(
                           len(splitFiles), splitFname))
      # find file with record containing smallest key
      smallKey, pos = _getSmallestKeyAndPosition(splitFiles)
      smallKeyCount = 0
      while pos is not None and not complete:
         # Output records in this file with smallKey 
         lastKey, lastCount = _popChunkAndWrite(csvwriter, splitFiles[pos])
         smallKeyCount += lastCount
#          lastKey = smallKey
         
         # If size limit is reached, switch to new file
         if (maxFileSize is not None and 
             os.fstat(mergeFile.fileno()).st_size >= maxFileSize):
            outIdx += 1
            mergeFile, csvwriter = _switchFiles(mergeFile, csvwriter, datapath, 
                                                mergePrefix, run=outIdx)
            
         # Find smallest again
         smallKey, pos = _getSmallestKeyAndPosition(splitFiles)
         _logProgress(log, pos, smallKey, lastKey, lastCount, smallKeyCount)
         if smallKey != lastKey:
            smallKeyCount = 0
         
   except Exception, e:
      raise
   finally:
      mergeFile.close()
      csvwriter = None
      for fd in splitFiles:
         fd.close()
            
# ...............................................
def _switchFiles(openFile, csvwriter, datapath, prefix, run=None):
   openFile.close()
   csvwriter = None
   # Open next output sorted file
   newFname = _getSortedName(datapath, prefix, run=run)
   newFile = open(newFname, 'wb')
   csvwriter = csv.writer(newFile, delimiter='\t')
   return newFile, csvwriter
      
# ...............................................
def _popChunkAndWriteOld(csvwriter, filedata):
   # write current chunk for current key to outfile
   for rec in filedata.chunk:
      csvwriter.writerow(rec)
   prevkey = filedata.key
   # update FileData with next key and chunk for that file
   if not(filedata.eof()):
      filedata.getNextChunk()
   else:
      filedata.close()
   return prevkey
          
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
def _logProgress(log, idx, smallKey, lastKey, lastCount, currentCount):
   if idx is None:
      log.debug('Completed, final key {}'.format(lastKey))
   else:
      log.debug('         count = {} (file {})'.format(lastCount, idx))
      if smallKey > lastKey:
         log.debug('         Total = {}'.format(currentCount))
         log.debug('New key     = {} (file {})'.format(smallKey, idx))
      elif smallKey < lastKey:
         log.debug('Problem = {} (file {})' % (smallKey, idx))

      
# ...............................................
def checkMergedFile(log, datapath, filePrefix, keyCol):
   uniqueCount = failCount = 0
   filename = _getSortedName(datapath, filePrefix)
   bigSortedData = FileData(filename, keyCol, log)
   prevKey = bigSortedData.key
   tmp = bigSortedData.getThisChunk()
   
   try:
      while bigSortedData.currLine is not None:
         if bigSortedData.key > prevKey: 
            uniqueCount += 1
         elif bigSortedData.key < prevKey:
            log.debug('Failure to sort prevKey %d, bigSortedData.key %d' 
                      % (prevKey, bigSortedData.key))
            failCount += 1
         else:
            log.debug('Failure to chunk key %d' % (prevKey))
         tmp = bigSortedData.getThisChunk()
#          log.debug('ok {}  fail {}'.format(uniqueCount, failCount))    

   except Exception, e:
      log.error(str(e))
   finally:
      bigSortedData.close()
      
   log.debug('{} uniqueCount; {} failCount; currRecnum {}'.format( 
             uniqueCount, failCount, bigSortedData.currRecnum))

# ...............................................
def usage():
   output = """
   Usage:
      sortGbifExport [split | merge | check] <datapath>
   """
   print output
   
# ..............................................................................
# MAIN
# ..............................................................................
# datestr = subprocess.check_output(['date', '+%F']).strip().replace('-', '_')
# datapath = '/tank/data/input/gbif/{}/'.format(datestr)
# dumpFname = 'aimee_export.txt'
splitPrefix = 'gbif_split'
mergedPrefix = 'gbif_merged'
oneGb = 1000000000
maxFileSize = None

# ...............................................
if __name__ == '__main__':
   
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Process a GBIF CSV database dump file containing ' +
                         'multiple sections of sorted data (one per node)' +
                         'into a single file sorted on the TaxonomyKey field'))
   parser.add_argument('filename', metavar='filename', type=str,
                       help='The absolute path of the top directory containing datafile')   
   args = parser.parse_args()
   
   fullfilename = args.filename
   if not os.path.exists(fullfilename):
      print 'Missing input file {}'.format(fullfilename)
      return -1
   
   datapath, dumpFname = os.path.split(fullfilename)
   basename, ext = os.path.splitext(dumpFname) 
   
   keyCol = None
   for idx, vals in GBIF_EXPORT_FIELDS.iteritems():
      if vals[0] == GBIF_TAXONKEY_FIELD:
         keyCol = idx
         break
      
   csv.field_size_limit(sys.maxsize)
   
   log = ScriptLogger('{}'.format(os.path.basename(__file__)))
   # Split big, semi-sorted file, to multiple smaller sorted files
   sortedRuns = splitIntoSortedFiles(log, datapath, dumpFname, 
                                     splitPrefix, keyCol)
   # Merge all data for production system into one or more files
   mergeSortedFiles(log, datapath, splitPrefix, mergedPrefix, keyCol, 
                    maxFileSize=maxFileSize)
   # Check final output (only for a single sorted output file)
   if maxFileSize is None:
      checkMergedFile(log, datapath, mergedPrefix, keyCol)
