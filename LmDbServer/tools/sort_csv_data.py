#!/bin/python
"""Sort a csv file
"""
import argparse
import csv
import os
import sys

from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.occ_parse import OccDataParser
from LmServer.common.log import ScriptLogger

OUT_DELIMITER = '\t'


# .............................................................................
def _get_op_filename(data_path, prefix, base, run=None):
    base_name = '{}_{}'.format(prefix, base)
    if run is not None:
        base_name = '{}_{}'.format(base_name, run)
    return os.path.join(data_path, '{}.csv'.format(base_name))


# .............................................................................
def _get_smallest_key_and_position(sorted_files):
    """Get the smallest group data key and index of file containing it
    """
    idx_of_smallest = 0
    smallest = sorted_files[idx_of_smallest].group_val
    for idx, sorted_file in enumerate(sorted_files):
        if sorted_file.group_val is not None:
            if smallest is None or sorted_file.group_val < smallest:
                smallest = sorted_file.group_val
                idx_of_smallest = idx
    if smallest is None:
        return None, None

    return smallest, idx_of_smallest


# .............................................................................
def check_merged_file(log, merge_fname, meta_fname):
    chunkCount = recCount = failSortCount = failChunkCount = 0
    bigSortedData = OccDataParser(log, merge_fname, meta_fname,
                                  delimiter=OUT_DELIMITER, pull_chunks=True)
    bigSortedData.initialize_me()
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

    except Exception as e:
        log.error(str(e))
    finally:
        bigSortedData.close()

    msg = """ Test {} file: 
                  recCount {}
                  chunkCount {} 
                  failSortCount {} 
                  failChunkCount{}  
                  currRecnum {}""".format(merge_fname, recCount, chunkCount,
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
def split_into_files(log, occ_parser, data_path, prefix, basename, max_file_size):
    idx = 0
    while not occ_parser.closed:
        chunk = occ_parser.getSizeChunk(max_file_size)
        fname = _get_op_filename(data_path, prefix, basename, run=idx)
        csvwriter, f = get_unicodecsv_writer(fname, OUT_DELIMITER, doAppend=False)
        # Skip header, rely on metadata with column indices
        #       csvwriter.writerow(occ_parser.header)
        for rec in chunk:
            csvwriter.writerow(rec)
        f.close()
        log.debug('Wrote {} records to {}'.format(len(chunk), fname))
        csvwriter = None
        idx += 1
    return idx


# .............................................................................
def sort_files(log, group_by_idx, data_path, inprefix, outprefix, basename):
    idx = 0

    infname = _get_op_filename(data_path, inprefix, basename, run=idx)
    while os.path.exists(infname):
        outfname = _get_op_filename(data_path, outprefix, basename, run=idx)
        log.debug('Write from {} to {}'.format(infname, outfname))
        # Read rows
        unsRows = []
        occreader, infile = get_unicodecsv_reader(infname, OUT_DELIMITER)
        occwriter, outfile = get_unicodecsv_writer(outfname, OUT_DELIMITER,
                                                   doAppend=False)

        # Skip header, rely on metadata with column indices
        while True:
            try:
                unsRows.append(next(occreader))
            except StopIteration:
                break
            except Exception as e:
                print(('Error file %s, line %d: %s' %
                     (infname, occreader.line_num, e)))
                break
        infile.close()

        # Sort records into new array, then write to file, no header
        srtRows = sortRecs(unsRows, group_by_idx)
        for rec in srtRows:
            occwriter.writerow(rec)
        outfile.close()

        # Move to next file
        idx += 1
        infname = _get_op_filename(data_path, inprefix, basename, run=idx)
    return idx


# .............................................................................
def _switchFiles(openFile, data_path, prefix, basename, run=None):
    openFile.close()
    # Open next output sorted file
    fname = _get_op_filename(data_path, prefix, basename, run=run)
    csvwriter, outfile = get_unicodecsv_writer(fname, OUT_DELIMITER,
                                               doAppend=False)
    return outfile, csvwriter


# .............................................................................
def _pop_chunk_and_write(csvwriter, occPrsr):
    # first get chunk
    thiskey = occPrsr.groupVal
    chunk, chunkGroup, chunkName = occPrsr.pullCurrentChunk()
    for rec in chunk:
        csvwriter.writerow(rec)
    return thiskey


# .............................................................................
def merge_sorted_files(log, merge_fname, data_path, input_prefix, basename,
                     meta_fname, in_idx=0, max_file_size=None):
    """
    @summary: Merge multiple files of csv records sorted on keyCol, with the 
              same keyCol values in one or more files, into a single file, 
              sorted on keyCol, placing records containing the same keyCol value
              together.
    @param merge_fname: Output filename
    @param data_path: Path for input files
    @param input_prefix: Filename prefix for input sorted files
    @param basename: original base filename
    @param meta_fname: Metadata filename for these data
    @param max_file_size: (optional) maximum number of bytes for output files; 
                        this results in multiple, numbered, sorted output files,  
                        with no keys in more than one file
    """
    # Open all input split files
    sorted_files = []
    srt_fname = _get_op_filename(data_path, input_prefix, basename, run=in_idx)
    curr_idx = in_idx
    enough_already = False
    while not enough_already and os.path.exists(srt_fname):
        try:
            op = OccDataParser(log, srt_fname, meta_fname, delimiter=OUT_DELIMITER,
                               pull_chunks=True)
            op.initialize_me()
        except IOError as e:
            log.warning('Enough already, IOError! Final file {}. Using only indices {} - {}, err {}'
                        .format(srt_fname, in_idx, curr_idx, str(e)))
            enough_already = True
        except Exception as e:
            log.warning(
                'Enough already! Final file {}. {}'.format(
                    srt_fname, 'Using only indices {} - {}, err {}'.format(
                        in_idx, curr_idx, e)))
            enough_already = True
        else:
            sorted_files.append(op)
            curr_idx += 1
            srt_fname = _get_op_filename(
                data_path, input_prefix, basename, run=curr_idx)

    try:
        # Open output sorted file
        with open(merge_fname, 'w') as out_file:
            csv_writer = csv.writer(out_file, delimiter=OUT_DELIMITER)
            # Skip header (no longer written to files)
            # find file with record containing smallest key
            _, pos = _get_smallest_key_and_position(sorted_files)
            while pos is not None:
                # Output records in this file with small_key
                _pop_chunk_and_write(csv_writer, sorted_files[pos])

                # Find smallest again
                _, pos = _get_smallest_key_and_position(sorted_files)

    except Exception as e:
        raise
    finally:
        for op in sorted_files:
            log.debug('Closing file {}'.format(op.data_fname))
            op.close()


# .............................................................................
def usage():
    output = """
    Usage:
       sortCSVData inputDelimiter inputCSVFile [split | sort | merge | check]
    """
    print(output)


# .............................................................................
def main():
    """Main method of the script
    """
    if len(sys.argv) in (3, 4):
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
    parser.add_argument(
        'dump_filename', help=('Filename for unsorted database dump.'))
    parser.add_argument(
        '--delimiter', default='\t', help=('Field delimiter for these data.'))
    # TODO: Use options or choices parameter to limit inputs
    parser.add_argument(
        '--command', default='all',
        help=('Process to be executed:\n'
              '    split: split large unsorted input into smaller files\n'
              '    sort: sort smaller files individually\n'
              '    merge: Merge smaller sorted files into large sorted file\n'
              '    check: Test large sorted file for errors\n'
              '    all: performa ll (split, sort, merge) functions'))
    parser.add_argument(
        '--start_idx', default=0,
        help=('For merging, start at this file number (limit on number of '
              'open files, may require doing this in 2 or more steps'))

    args = parser.parse_args()
    in_delimiter = args.delimiter
    data_fname = args.dump_filename
    cmd = args.command
    start_idx = args.start_idx

    # Only 2 options
    if in_delimiter == ',':
        print('delimiter is comma')
    else:
        in_delimiter = '\t'
    if cmd not in ('split', 'sort', 'merge', 'check', 'all'):
        usage()

    unsorted_prefix = 'chunk'
    sorted_prefix = 'smsort'
    merged_prefix = 'sorted'

    basepath, _ = os.path.splitext(data_fname)
    pth, basename = os.path.split(basepath)
    log_name = 'sortCSVData_{}_{}'.format(basename, cmd)
    meta_fname = basepath + LMFormat.JSON.ext
    merge_fname = os.path.join(pth, '{}_{}{}'.format(
        merged_prefix, basename, LMFormat.CSV.ext))
    if not os.path.exists(data_fname) or not os.path.exists(meta_fname):
        print(('Files {} and {} must exist'.format(data_fname, meta_fname)))
        sys.exit()

    log = ScriptLogger(log_name)
    if cmd in ('split', 'sort', 'all'):
        # Split into smaller unsorted files
        occ_parser = OccDataParser(
            log, data_fname, meta_fname, delimiter=in_delimiter,
            pull_chunks=True)
        occ_parser.initialize_me()
        group_by_idx = occ_parser.group_by_idx
        print('group_by_idx = ', group_by_idx)

        if cmd in ('split', 'all'):
            # Split into smaller unsorted files
            print('Split huge data file into smaller files')
            split_into_files(
                log, occ_parser, pth, unsorted_prefix, basename, 500000)
            occ_parser.close()

        if cmd in ('sort', 'all'):
            # Sort smaller files
            print('Sort smaller files')
            sort_files(
                log, group_by_idx, pth, unsorted_prefix, sorted_prefix,
                basename)

    if cmd in ('merge', 'all'):
        # Merge all data for production system into multiple subset files
        print('Merge smaller sorted files into huge sorted file')
        merge_sorted_files(
            log, merge_fname, pth, sorted_prefix, basename, meta_fname,
            in_idx=start_idx, max_file_size=None)

    if cmd == 'check':
        # Check final output (only for single file now)
        check_merged_file(log, merge_fname, meta_fname)


# .............................................................................
if __name__ == '__main__':
    main()

"""
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
occparser.initialize_me()
groupByIdx = occparser.groupByIdx
print 'groupByIdx = ', groupByIdx

(datapath, prefix, maxFileSize) = (pth, unsortedPrefix, 5000000)
 
######################### SPLIT
# splitIntoFiles(log, occparser, pth, unsortedPrefix, basename, 5000000)   
# occparser.close()


######################### SORT 
groupByIdx = 2
# sortFiles(log, groupByIdx, pth, unsortedPrefix, sortedPrefix, basename)
(datapath, inprefix, outprefix)=(pth, unsortedPrefix, sortedPrefix)
idx = 0
infname = _getOPFilename(datapath, inprefix, basename, run=idx)

######################### 
######################### loop
######################### 

outfname = _getOPFilename(datapath, outprefix, basename, run=idx)
log.debug('Write from {} to {}'.format(infname, outfname))

unsRows = []
occreader, infile = get_unicodecsv_reader(infname, OUT_DELIMITER)
occwriter, outfile = get_unicodecsv_writer(outfname, OUT_DELIMITER, 
                                           doAppend=False)

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

srtRows = sortRecs(unsRows, groupByIdx)

for rec in srtRows:
    occwriter.writerow(rec)

outfile.close()
idx += 1
infname = _getOPFilename(datapath, inprefix, basename, run=idx)


######################### 
######################### end loop
######################### 

######################### MERGE
mergeSortedFiles(log, mergefname, pth, sortedPrefix, basename, metafname)

checkMergedFile(log, mergefname, metafname)

"""
