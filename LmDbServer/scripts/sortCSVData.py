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
import os
import sys

from LmDbServer.common.localconstants import (USER_OCCURRENCE_META, 
                                              USER_OCCURRENCE_CSV)
from LmDbServer.common.occparse import OccDataParser
from LmServer.base.lmobj import LMObject, LMError
from LmServer.common.log import ScriptLogger

# ...............................................
def _getOPFilename(datapath, prefix, base, run=None):
   basename = '%s_%s' % (prefix, base)
   if run is not None:
      basename = '%s_%d' % (basename, run)
   return os.path.join(datapath, '%s.csv' % (basename))

# ...............................................
def _getSmallestKeyAndPosition(sortedFiles):
   """
   Get smallest data key and index of file containing that record
   """
   idxOfSmallest = 0
   smallest = sortedFiles[idxOfSmallest].key
   for idx in range(len(sortedFiles)):
      if sortedFiles[idx].key is not None:
         if smallest is None or sortedFiles[idx].key < smallest:
            smallest = sortedFiles[idx].key
            idxOfSmallest = idx
   if smallest is None:
      return None, None
   else:
      return smallest, idxOfSmallest
   
# ...............................................
def checkMergedFile(log, mergefname, metafname):
   uniqueCount = failCount = 0
   bigSortedData = OccDataParser(log, mergefname, metafname)
   prevKey = bigSortedData.key
   tmp = bigSortedData.getThisChunk()
   
   try:
      while bigSortedData.currLine is not None and not(os.path.exists(killfile)) :
         if bigSortedData.key > prevKey: 
            uniqueCount += 1
         elif bigSortedData.key < prevKey:
            log.debug('Failure to sort prevKey %d, bigSortedData.key %d' 
                      % (prevKey, bigSortedData.key))
            failCount += 1
         else:
            log.debug('Failure to chunk key %d' % (prevKey))            

   except Exception, e:
      log.error(str(e))
   finally:
      bigSortedData.close()
      
   log.debug('%d uniqueCount; %d failCount; currRecnum %d' 
             % (uniqueCount, failCount, bigSortedData.currRecnum))

# .............................................................................
def sortRecs(array, idx):
   less = []
   equals = []
   greater = []

   if len(array) > 1:
      pivot = array[0][idx]
      for rec in array:
         if rec[idx] < pivot:
            less.append(rec)
         if rec[idx] == pivot:
            equals.append(rec)
         if rec[idx] > pivot:
            greater.append(rec)
      # Don't forget to return something!
      return sortRecs(less, idx) + equals + sortRecs(greater, idx)  
   # Note that you want equals ^^^^^ not pivot
   else:  
      # At the end of the recursion - when only one element, return the array.
      return array

# .............................................................................
def splitIntoFiles(occparser, datapath, prefix, basename, maxFileSize):
   idx = 0
   while not occparser.eof():
      chunk = occparser.getSizeChunk(maxFileSize)
      fname = _getOPFilename(datapath, prefix, basename, run=idx)
      newFile = open(fname, 'wb')
      csvwriter = csv.writer(newFile, delimiter='\t')
      csvwriter.writerow(occparser.header)
      for rec in chunk:
         csvwriter.writerow(rec)
      csvwriter = None
      idx += 1
   return idx
   
# .............................................................................
def sortFiles(sortvalIdx, datapath, inprefix, outprefix, basename):
   idx = 0
   
   infname = _getOPFilename(datapath, inprefix, basename, run=idx)
   while os.path.exists(infname):
      outfname = _getOPFilename(datapath, outprefix, basename, run=idx)
      # Read rows
      unsRows = []
      with open(infname, 'r') as csvfile:
         occreader = csv.reader(csvfile, delimiter='\t')
         header = occreader.next()
         while True:
            try: 
               unsRows.append(occreader.next())
            except StopIteration:
               break
            except Exception, e:
               print('Error file %s, line %d: %s' % 
                     (infname, occreader.line_num, e))
               break
      # Sort records into new array
      srtRows = sortRecs(unsRows, sortvalIdx)
      # Write sorted records to file
      with open(outfname, 'wb') as csvfile:
         occwriter = csv.writer(csvfile, delimiter='\t')
         occwriter.writerow(header)
         for rec in srtRows:
            occwriter.writerow(rec)
      # Try again
      idx += 1
      infname = _getOPFilename(datapath, inprefix, basename, run=idx)
   return idx

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
def _popChunkAndWrite(csvwriter, filedata):
   # first get chunk
   thiskey = filedata.key
   chunk = filedata.getThisChunk()
   for rec in chunk:
      csvwriter.writerow(rec)

   if filedata.eof():
      filedata.close()
   else:
      filedata.getNextKey()
   return thiskey

# ...............................................
def mergeSortedFiles(log, mergefname, datapath, inputPrefix, basename, 
                     metafname, maxFileSize=None):
   """
   @summary: Merge multiple files of csv records sorted on keyCol, with the 
             same keyCol values in one or more files, into a single file, 
             sorted on keyCol, placing records containing the same keyCol value
             together.
   @param mergefname: Output filename
   @param datapath: Path for input files
   @param inputPrefix: Filename prefix for input sorted files
   @param basename: original base filename
   @param metafname: Metadata filename for these data
   @param maxFileSize: (optional) maximum number of bytes for output files; 
                       this results in multiple, numbered, sorted output files,  
                       with no keys in more than one file
   """
   # Indexes begin with 0
   inIdx = 0
   outIdx = None
   if maxFileSize is not None:
      outIdx = 0
   # Open output sorted file
   mergeFile = open(mergefname, 'wb')
   csvwriter = csv.writer(mergeFile, delimiter='\t')
   complete = False

   # Open all input split files
   sortedFiles = []
   srtFname = _getOPFilename(datapath, inputPrefix, basename, run=inIdx)
   while os.path.exists(srtFname):
      fd = OccDataParser(log, srtFname, metafname)
      sortedFiles.append(fd)
      inIdx += 1
      srtFname = _getOPFilename(datapath, inputPrefix, basename, run=inIdx)
      
   try:
      # Write header from the first file
      csvwriter.writerow(sortedFiles[0].header)
      # find file with record containing smallest key
      smallKey, pos = _getSmallestKeyAndPosition(sortedFiles)
      while pos is not None and not complete:
         # Output records in this file with smallKey 
         _popChunkAndWrite(csvwriter, sortedFiles[pos])
         lastKey = smallKey
         
         # If size limit is reached, switch to new file
         if (maxFileSize and os.fstat(mergeFile.fileno()).st_size >= maxFileSize):
            outIdx += 1
            mergeFile, csvwriter = _switchFiles(mergeFile, csvwriter, datapath, 
                                                mergePrefix, run=outIdx)
         # Find smallest again
         smallKey, pos = _getSmallestKeyAndPosition(sortedFiles)
         
   except Exception, e:
      raise
   finally:
      mergeFile.close()
      csvwriter = None
      for fd in sortedFiles:
         print 'File %s failed on %d records' % (fd.dataFname, fd.sortFail)
         fd.close()
   
# ...............................................
def usage():
   output = """
   Usage:
      sortCSVData [split | merge | check]
   """
   print output

# .............................................................................
if __name__ == "__main__":   
   WORKPATH = '/tank/data/input/species/'
   USER_OCCURRENCE_META = 'gbif_borneo_simple.meta'
   USER_OCCURRENCE_CSV = 'gbif_borneo_simple.csv'
   unsortedPrefix = 'chunk'
   sortedPrefix = 'smallsort'
   mergedPrefix = 'sorted'
   basename = os.path.splitext(USER_OCCURRENCE_CSV)[0]
    
   if len(sys.argv) == 2:
      log = ScriptLogger('occparse_%s' % sys.argv[1])
      csv.field_size_limit(sys.maxsize)
   else:
      usage()
    
   datafname = os.path.join(WORKPATH, USER_OCCURRENCE_CSV)
   metafname = os.path.join(WORKPATH, USER_OCCURRENCE_META)
   mergefname = os.path.join(WORKPATH, '%s_%s.csv' % (mergedPrefix, basename))

   if sys.argv[1] == 'sort':   
      # Split into smaller unsorted files
      occparser = OccDataParser(log, datafname, metafname)
      sortvalIdx = occparser.sortIdx
      splitIntoFiles(occparser, WORKPATH, unsortedPrefix, basename, 200000)
      occparser.close()
      print 'sortvalIdx = ', sortvalIdx
           
      # Sort smaller files
      sortFiles(sortvalIdx, WORKPATH, unsortedPrefix, sortedPrefix, basename)

      # Merge all data for production system into multiple subset files
      mergeSortedFiles(log, mergefname, WORKPATH, sortedPrefix, basename, 
                       metafname, maxFileSize=None)

   elif sys.argv[1] == 'check':
      # Check final output (only for single file now)
      checkMergedFile(log, mergefname, metafname)
   else:
      usage()

