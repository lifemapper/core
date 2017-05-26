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
import csv
import os
import sys

from LmCommon.common.lmconstants import LMFormat
from LmBackend.common.occparse import OccDataParser
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
   Get smallest groupVal (grouping data key) and index of file containing 
   that record
   """
   idxOfSmallest = 0
   smallest = sortedFiles[idxOfSmallest].groupVal
   for idx in range(len(sortedFiles)):
      if sortedFiles[idx].groupVal is not None:
         if smallest is None or sortedFiles[idx].groupVal < smallest:
            smallest = sortedFiles[idx].groupVal
            idxOfSmallest = idx
   if smallest is None:
      return None, None
   else:
      return smallest, idxOfSmallest
   
# ...............................................
def checkMergedFile(log, mergefname, metafname):
   chunkCount = recCount = failSortCount = failChunkCount = 0
   bigSortedData = OccDataParser(log, mergefname, metafname)
   prevKey = bigSortedData.groupVal
   
   chunk = bigSortedData.pullCurrentChunk()
   try:
      while not bigSortedData.eof() and len(chunk) > 0:
         if bigSortedData.groupVal > prevKey: 
            chunkCount += 1
            recCount += len(chunk)
         elif bigSortedData.groupVal < prevKey:
            log.debug('Failure to sort prevKey {},  currKey {}'.format( 
                      prevKey, bigSortedData.groupVal))
            failSortCount += 1
         else:
            log.debug('Current chunk key = prev key %d' % (prevKey))
            failChunkCount += 1
            
         prevKey = bigSortedData.groupVal
         chunk = bigSortedData.pullCurrentChunk()

   except Exception, e:
      log.error(str(e))
   finally:
      bigSortedData.close()
   
   msg = """ Test {} file: 
                 recCount {}
                 chunkCount {} 
                 failSortCount {} 
                 failChunkCount{}  
                 currRecnum {}""".format(mergefname, recCount, chunkCount, 
                                         failSortCount, failChunkCount, 
                                         bigSortedData.currRecnum)
   log.debug(msg)

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
def _switchFiles(openFile, csvwriter, datapath, prefix, basename, run=None):
   openFile.close()
   # Open next output sorted file
   newFname = _getOPFilename(datapath, prefix, basename, run=run)
   newFile = open(newFname, 'wb')
   csvwriter = csv.writer(newFile, delimiter='\t')
   return newFile, csvwriter

# ...............................................
def _popChunkAndWrite(csvwriter, occPrsr):
   # first get chunk
   thiskey = occPrsr.groupVal
   chunk = occPrsr.pullCurrentChunk()
   for rec in chunk:
      csvwriter.writerow(rec)

   if occPrsr.eof():
      occPrsr.close()
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

   # Open all input split files
   sortedFiles = []
   srtFname = _getOPFilename(datapath, inputPrefix, basename, run=inIdx)
   while os.path.exists(srtFname):
      op = OccDataParser(log, srtFname, metafname)
      sortedFiles.append(op)
      inIdx += 1
      srtFname = _getOPFilename(datapath, inputPrefix, basename, run=inIdx)
      
   try:
      # Write header from the first file
      csvwriter.writerow(sortedFiles[0].header)
      # find file with record containing smallest key
      smallKey, pos = _getSmallestKeyAndPosition(sortedFiles)
#       # Debug 2 badly sorted keys
#       if smallKey in (5666, 9117):
#          pass
      while pos is not None:
         # Output records in this file with smallKey 
         _popChunkAndWrite(csvwriter, sortedFiles[pos])
         
#          # If size limit is reached, switch to new file
#          if (maxFileSize and os.fstat(mergeFile.fileno()).st_size >= maxFileSize):
#             outIdx += 1
#             mergeFile, csvwriter = _switchFiles(mergeFile, csvwriter, datapath, 
#                                                 mergePrefix, basename, run=outIdx)
         # Find smallest again
         smallKey, pos = _getSmallestKeyAndPosition(sortedFiles)
         
   except Exception, e:
      raise
   finally:
      mergeFile.close()
      csvwriter = None
      for op in sortedFiles:
         print 'Closing file %s' % (op.dataFname)
         op.close()
   
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
   OCCURRENCE_BASENAME = 'seasia_gbif.meta'
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
    
   datafname = os.path.join(WORKPATH, OCCURRENCE_BASENAME + LMFormat.CSV.ext)
   metafname = os.path.join(WORKPATH, OCCURRENCE_BASENAME + LMFormat.METADATA.ext)
   mergefname = os.path.join(WORKPATH, '%s_%s.csv' % (mergedPrefix, basename))

   if sys.argv[1] == 'sort':   
      # Split into smaller unsorted files
      occparser = OccDataParser(log, datafname, metafname)
      sortvalIdx = occparser.sortIdx
       
      splitIntoFiles(occparser, WORKPATH, unsortedPrefix, basename, 500000)
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

