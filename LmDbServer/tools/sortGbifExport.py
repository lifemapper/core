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

from LmCommon.common.lmconstants import (GBIF, GBIF_QUERY) 

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
      line = None
      while not success:
         try:
            self.currLine = self._csvreader.next()
            success = True
            self.currRecnum += 1
         except OverflowError, e:
            self.currRecnum += 1
            logfile.write( 'Overflow on {} ({})\n'.format(self.currRecnum, str(e)))
         except Exception, e:
            logfile.write('Exception reading line {}, probably EOF ({})\n'.format(
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
      prevkey, prevcount = _popChunkAndWrite(csvwriter, dumpData)
      while dumpData.currLine is not None:
         # Start new file when encountering a key out of order
         if dumpData.key < prevkey:
            logfile.write('Wrote {} species to {}, next line = {}\n'.format(
                           spCount, currSortedFile.name, dumpData.currRecnum))
            sortedRuns += 1
            spCount = 0
            currSortedFile, csvwriter = _switchFiles(currSortedFile, csvwriter, 
                        datapath, sortedSubsetPrefix, logfile, run=sortedRuns)
            prevkey, prevcount = _popChunkAndWrite(csvwriter, dumpData)
             
         elif dumpData.key >= prevkey:
            spCount += 1
            prevkey, prevcount = _popChunkAndWrite(csvwriter, dumpData)
             
   except Exception, e:
      logfile.write('ERROR: {}\n'.format(e))
   finally:
      csvwriter = None
      logfile.write('Wrote final species to {}, next line = {}\n'
                    .format(currfname, dumpData.currRecnum))
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
                                                mergePrefix, logfile, run=outIdx)
            
         # Find smallest again
         smallKey, pos = _getSmallestKeyAndPosition(splitFiles)
         _logProgress(pos, smallKey, lastKey, lastCount, smallKeyCount, logfile)
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
def _switchFiles(openFile, csvwriter, datapath, prefix, logfile, run=None):
   openFile.close()
   csvwriter = None
   # Open next output sorted file
   newFname = _getSortedName(datapath, prefix, run=run)
   newFile = open(newFname, 'wb')
   logfile.write('Closed {}, opened {}'.format(openFile.name, newFile.name))
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
def _logProgress(idx, smallKey, lastKey, lastCount, currentCount, logfile):
   if idx is None:
      logfile.write('Completed, final key {}\n'.format(lastKey))
   else:
      logfile.write('         count = {} (file {})\n'.format(lastCount, idx))
      if smallKey > lastKey:
         logfile.write('         Total = {}\n'.format(currentCount))
         logfile.write('New key     = {} (file {})\n'.format(smallKey, idx))
      elif smallKey < lastKey:
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
      
   logfile.write('{} uniqueCount; {} failCount; currRecnum {}\n'.format( 
             uniqueCount, failCount, bigSortedData.currRecnum))

# ..............................................................................
# MAIN
# ..............................................................................
# datestr = subprocess.check_output(['date', '+%F']).strip().replace('-', '_')
# datapath = '/tank/data/input/gbif/{}/'.format(datestr)
# dumpFname = 'aimee_export.txt'
# ...............................................
if __name__ == '__main__':
   splitPrefix = 'gbif_split'
   mergedPrefix = 'gbif_merged'
   oneGb = 1000000000
   maxFileSize = None
   
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
      print('Missing input file {}'.format(fullfilename))
      exit(0)

   datapath, dumpFname = os.path.split(fullfilename)   
   csv.field_size_limit(sys.maxsize)
   keyCol = None
   for idx, vals in GBIF_QUERY.EXPORT_FIELDS.iteritems():
      if vals[0] == GBIF.TAXONKEY_FIELD:
         keyCol = idx
         break
   
   thisname = os.path.basename(__file__)
   sname, ext = os.path.split(thisname)
   logfname = '/tmp/{}.log'.format(sname)
   try:
      logfile = open(logfname, 'w')
      logfile.write('{} to be sorted on column {}, {}; logfile {}\n'
                    .format(dumpFname, keyCol, GBIF.TAXONKEY_FIELD, logfname))
      
      # Split big, semi-sorted file, to multiple smaller sorted files
      logfile.write('Splitting ...\n')
      sortedRuns = splitIntoSortedFiles(datapath, dumpFname, splitPrefix, 
                                        keyCol, logfile)
      # Merge all data for production system into one or more files
      logfile.write('Merging ...\n')
      mergeSortedFiles(datapath, splitPrefix, mergedPrefix, keyCol, logfile,
                       maxFileSize=maxFileSize)
      # Check final output (only for a single sorted output file)
      if maxFileSize is None:
         logfile.write('Checking ...\n')
         checkMergedFile(datapath, mergedPrefix, keyCol, logfile)
   finally:
      logfile.close()
