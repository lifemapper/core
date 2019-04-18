#!/opt/python/bin/python2.7
"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
import os
import sys

from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.readyfile import get_unicodecsv_reader, get_unicodecsv_writer

from LmServer.common.log import ScriptLogger

OUT_DELIMITER = '\t'

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
    bigSortedData = OccDataParser(log, mergefname, metafname, 
                                  delimiter=OUT_DELIMITER, pullChunks=True)
    bigSortedData.initializeMe()
    prevKey = bigSortedData.groupVal
    
    chunk, chunkGroup, chunkName = bigSortedData.pullCurrentChunk()
    try:
        while not bigSortedData.closed and len(chunk) > 0:
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
           
        prevKey = chunkGroup
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
    while not occparser.closed:
        chunk = occparser.getSizeChunk(maxFileSize)
        fname = _getOPFilename(datapath, prefix, basename, run=idx)
        csvwriter, f = get_unicodecsv_writer(fname, OUT_DELIMITER)
        # Skip header, rely on metadata with column indices
        #       csvwriter.writerow(occparser.header)
        for rec in chunk:
            csvwriter.writerow(rec)
        f.close()
        csvwriter = None
        idx += 1
    return idx
   
# .............................................................................
def sortFiles(groupByIdx, datapath, inprefix, outprefix, basename):
    idx = 0
    
    infname = _getOPFilename(datapath, inprefix, basename, run=idx)
    while os.path.exists(infname):
        outfname = _getOPFilename(datapath, outprefix, basename, run=idx)
        # Read rows
        unsRows = []
#         with open(infname, 'r') as csvfile:
        occreader, infile = get_unicodecsv_reader(infname, OUT_DELIMITER)
        # Skip header, rely on metadata with column indices
        #          header = occreader.next()
        while True:
            try: 
                unsRows.append(occreader.next())
            except StopIteration:
                break
            except Exception, e:
                print('Error file %s, line %d: %s' % 
                     (infname, occreader.line_num, e))
                break
        infile.close()
        # Sort records into new array
        srtRows = sortRecs(unsRows, groupByIdx)
        # Write sorted records to file
        occwriter, outfile = get_unicodecsv_writer(outfname, OUT_DELIMITER)
        # TODO: Write header, but rely on metadata with column indices
#             occwriter.writerow(header)
        for rec in srtRows:
            occwriter.writerow(rec)
        outfile.close()
        # Try again
        idx += 1
        infname = _getOPFilename(datapath, inprefix, basename, run=idx)
    return idx

# ...............................................
def _switchFiles(openFile, datapath, prefix, basename, run=None):
    openFile.close()
    # Open next output sorted file
    newFname = _getOPFilename(datapath, prefix, basename, run=run)
    newFile = open(newFname, 'wb')
    csvwriter, outfile = get_unicodecsv_writer(newFile, OUT_DELIMITER)
    return newFile, csvwriter, outfile

# ...............................................
def _popChunkAndWrite(csvwriter, occPrsr):
    # first get chunk
    thiskey = occPrsr.groupVal
    chunk, chunkGroup, chunkName = occPrsr.pullCurrentChunk()
    for rec in chunk:
        csvwriter.writerow(rec)
    return thiskey

# ...............................................
def mergeSortedFiles(log, mergefname, datapath, inputPrefix, basename, 
                     metafname, inIdx=0, maxFileSize=None):
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
    # Open output sorted file
    csvwriter, outfile = get_unicodecsv_writer(mergefname, OUT_DELIMITER)
    
    # Open all input split files
    sortedFiles = []
    srtFname = _getOPFilename(datapath, inputPrefix, basename, run=inIdx)
    currIdx = inIdx
    enoughAlready = False
    while not enoughAlready and os.path.exists(srtFname):
        try:
            op = OccDataParser(log, srtFname, metafname, delimiter=OUT_DELIMITER, 
                               pullChunks=True)
            op.initializeMe()
        except IOError, e:
            log.warning('Enough already, IOError! Final file {}. Using only indices {} - {}, err {}'
                        .format(srtFname, inIdx, currIdx, str(e)))
            enoughAlready = True
        except Exception, e:
            log.warning('Enough already, Unknown error! Final file {}. Using only indices {} - {}, err {}'
                        .format(srtFname, inIdx, currIdx, str(e)))
            enoughAlready = True
        else:
            sortedFiles.append(op)
            currIdx += 1
            srtFname = _getOPFilename(datapath, inputPrefix, basename, run=currIdx)
       
    try:
        # Skip header (no longer written to files)
        # find file with record containing smallest key
        smallKey, pos = _getSmallestKeyAndPosition(sortedFiles)
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
        outfile.close()
        csvwriter = None
        for op in sortedFiles:
            print 'Closing file %s' % (op.dataFname)
            op.close()
   
# ...............................................
def usage():
    output = """
    Usage:
       sortCSVData inputDelimiter inputCSVFile [split | sort | merge | check]
    """
    print output

# .............................................................................
if __name__ == "__main__": 
    import argparse  
    if len(sys.argv) in (3,4):
        import csv
        csv.field_size_limit(sys.maxsize)
        if len(sys.argv) == 3:
            cmd = 'all'
        else:
            cmd = sys.argv[3]
    else:
        usage()

    parser = argparse.ArgumentParser(
             description=('Populate a Lifemapper archive with metadata ' +
                          'for single- or multi-species computations ' + 
                          'specific to the configured input data or the ' +
                          'data package named.'))
    parser.add_argument('dump_filename', 
             help=('Filename for unsorted database dump.'))
    parser.add_argument('--delimiter', default='\t',
             help=('Field delimiter for these data.'))
    parser.add_argument('--command', default='all',
             help=("""Process to be excecuted: 
             split: split large unsorted input into smaller files
             sort: sort smaller files individually
             merge: merge smaller sorted files into large sorted file
             check: test large sorted file for errors
             all: perform all (split, sort, merge) functions"""))
    parser.add_argument('--start_idx', default=0,
             help=(""""For merging, start at this file number (limit on number
             of open files may require doing this in 2 or more steps)."""))
    
    args = parser.parse_args()
    inDelimiter = args.delimiter
    datafname = args.dump_filename
    cmd = args.command
    start_idx = args.start_idx
    
    # Only 2 options
    if inDelimiter == ',':
        print 'delimiter is comma'
    else:
        inDelimiter = '\t'
    if cmd not in ('split', 'sort', 'merge', 'check', 'all'):   
        usage()
    
    unsortedPrefix = 'chunk'
    sortedPrefix = 'smsort'
    mergedPrefix = 'sorted'
    
    basepath, ext = os.path.splitext(datafname)
    pth, basename = os.path.split(basepath)
    logname = 'sortCSVData_{}_{}'.format(basename, cmd)
    metafname = basepath + LMFormat.JSON.ext
    mergefname = os.path.join(pth, '{}_{}{}'.format(mergedPrefix, basename, 
                                                    LMFormat.CSV.ext))
    if not os.path.exists(datafname) or not os.path.exists(metafname):
        print('Files {} and {} must exist'.format(datafname, metafname))
        exit()
       
    log = ScriptLogger(logname)
    if cmd in ('split', 'sort', 'all'):   
        # Split into smaller unsorted files
        occparser = OccDataParser(log, datafname, metafname, delimiter=inDelimiter,
                                  pullChunks=True)
        occparser.initializeMe()
        groupByIdx = occparser.groupByIdx
        print 'groupByIdx = ', groupByIdx
         
        if cmd in ('split', 'all'):   
            # Split into smaller unsorted files
            print('Split huge data file into smaller files')
            splitIntoFiles(occparser, pth, unsortedPrefix, basename, 500000)   
        if cmd in ('sort', 'all'):              
            # Sort smaller files
            print('Sort smaller files')
            sortFiles(groupByIdx, pth, unsortedPrefix, sortedPrefix, basename)
        occparser.close()
    
    if cmd in ('merge', 'all'):   
        # Merge all data for production system into multiple subset files
        print('Merge smaller sorted files into huge sorted file')
        mergeSortedFiles(log, mergefname, pth, sortedPrefix, basename, 
                         metafname, inIdx=start_idx, maxFileSize=None)
    
    if cmd == 'check':
        # Check final output (only for single file now)
        checkMergedFile(log, mergefname, metafname)

"""
import csv
import os
import sys

from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.readyfile import get_unicodecsv_reader, get_unicodecsv_writer

from LmServer.common.log import ScriptLogger

OUT_DELIMITER = '\t'

from LmDbServer.tools.sortCSVData import *
from LmDbServer.tools.sortCSVData import _getOPFilename, _popChunkAndWrite

cmd = 'split'

OUT_DELIMITER = '\t'
unsortedPrefix = 'chunk'
sortedPrefix = 'smsort'
mergedPrefix = 'sorted'

datafname = '/tank/zdata/occ/gbif/aimee_export2019.csv'
inDelimiter = '\t'
basepath, ext = os.path.splitext(datafname)
pth, basename = os.path.split(basepath)
logname = 'sortCSVData_{}_{}'.format(basename, cmd)
metafname = basepath + '.json'
mergefname = os.path.join(pth, '{}_{}{}'.format(mergedPrefix, basename, 
                                                LMFormat.CSV.ext))
   
log = ScriptLogger(logname)

occparser = OccDataParser(log, datafname, metafname, delimiter=inDelimiter,
                          pullChunks=True)
occparser.initializeMe()
groupByIdx = occparser.groupByIdx
print 'groupByIdx = ', groupByIdx

(datapath, prefix, maxFileSize) = (pth, unsortedPrefix, 5000000)
 
splitIntoFiles(occparser, pth, unsortedPrefix, basename, 5000000)   

sortFiles(groupByIdx, pth, unsortedPrefix, sortedPrefix, basename)

occparser.close()


mergeSortedFiles(log, mergefname, pth, sortedPrefix, basename, metafname)

checkMergedFile(log, mergefname, metafname)

"""