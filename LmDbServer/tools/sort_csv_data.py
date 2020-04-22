#!/bin/python
"""Sort a csv file
"""
import argparse
import csv
import os
import sys

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import LMFormat, ENCODING
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
    """Check the status of a merged file."""
    chunk_count = rec_count = fail_sort_count = fail_chunk_count = 0
    big_sorted_data = OccDataParser(
        log, merge_fname, meta_fname, delimiter=OUT_DELIMITER,
        pull_chunks=True)
    big_sorted_data.initialize_me()
    prev_key = big_sorted_data.group_val

    chunk, chunk_group, _ = big_sorted_data.pull_current_chunk()
    try:
        while not big_sorted_data.closed and len(chunk) > 0:
            if big_sorted_data.group_val > prev_key:
                chunk_count += 1
                rec_count += len(chunk)
            elif big_sorted_data.group_val < prev_key:
                log.debug('Failure to sort prev_key {},  currKey {}'.format(
                    prev_key, big_sorted_data.group_val))
                fail_sort_count += 1
            else:
                log.debug('Current chunk key = prev key %d' % (prev_key))
                fail_chunk_count += 1

        prev_key = chunk_group
        chunk = big_sorted_data.pull_current_chunk()

    except Exception as e:
        log.error(str(e))
    finally:
        big_sorted_data.close()

    msg = """\
        Test {} file:
            rec_count {}
            chunk_count {}
            fail_sort_count {}
            fail_chunk_count{}
            curr_rec_num {}""".format(
                merge_fname, rec_count, chunk_count, fail_sort_count,
                fail_chunk_count, big_sorted_data.curr_rec_num)
    log.debug(msg)


# .............................................................................
def sort_recs(array, idx):
    """Sort records."""
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
        return sort_recs(less, idx) + equals + sort_recs(greater, idx)

    # At the end of the recursion - when only one element, return the array.
    return array


# .............................................................................
def split_into_files(log, occ_parser, data_path, prefix, basename,
                     max_file_size):
    """Split occurrence data into files."""
    idx = 0
    while not occ_parser.closed:
        chunk = occ_parser.get_size_chunk(max_file_size)
        fname = _get_op_filename(data_path, prefix, basename, run=idx)
        with open(fname, 'w', encoding=ENCODING) as out_file:
            csv_writer = csv.writer(out_file, delimiter=OUT_DELIMITER)
            # Skip header, rely on metadata with column indices
            #       csvwriter.writerow(occ_parser.header)
            for rec in chunk:
                csv_writer.writerow(rec)
        log.debug('Wrote {} records to {}'.format(len(chunk), fname))
        idx += 1
    return idx


# .............................................................................
def sort_files(log, group_by_idx, data_path, in_prefix, out_prefix, basename):
    """Sort files."""
    idx = 0

    in_fname = _get_op_filename(data_path, in_prefix, basename, run=idx)
    while os.path.exists(in_fname):
        out_fname = _get_op_filename(data_path, out_prefix, basename, run=idx)
        log.debug('Write from {} to {}'.format(in_fname, out_fname))
        # Read rows
        uns_rows = []
        with open(in_fname, 'r', encoding=ENCODING) as in_file, open(out_fname, 'w', encoding=ENCODING) as out_file:
            with csv.reader(in_file, delimiter=OUT_DELIMITER) as occ_reader:
                with csv.writer(
                        out_file, delimiter=OUT_DELIMITER) as occ_writer:
                    while True:
                        try:
                            uns_rows.append(next(occ_reader))
                        except StopIteration:
                            break
                        except Exception as err:
                            print('Error file {}, line {}: {}'.format(
                                in_fname, occ_reader.line_num, err))
                            break

                    # Sort records into new array, then write to file, no
                    #    header
                    srt_rows = sort_recs(uns_rows, group_by_idx)
                    for rec in srt_rows:
                        occ_writer.writerow(rec)

        # Move to next file
        idx += 1
        in_fname = _get_op_filename(data_path, in_prefix, basename, run=idx)
    return idx


# .............................................................................
def _pop_chunk_and_write(csv_writer, occ_parser):
    # first get chunk
    this_key = occ_parser.group_val
    chunk, _, _ = occ_parser.pull_current_chunk()
    for rec in chunk:
        csv_writer.writerow(rec)
    return this_key


# .............................................................................
def merge_sorted_files(log, merge_fname, data_path, input_prefix, basename,
                       meta_fname, in_idx=0):
    """Merge multile files of csv records.

    Merge multiple files of csv records sorted on keyCol, with the same key col
    values in one or more files, into a single file, sorted on key col, placing
    records containing the same key col value together.

    Args:
        merge_fname: Output filename
        data_path: Path for input files
        input_prefix: Filename prefix for input sorted files
        basename: original base filename
        meta_fname: Metadata filename for these data
    """
    # Open all input split files
    sorted_files = []
    srt_fname = _get_op_filename(data_path, input_prefix, basename, run=in_idx)
    curr_idx = in_idx
    enough_already = False
    while not enough_already and os.path.exists(srt_fname):
        try:
            occ_parser = OccDataParser(
                log, srt_fname, meta_fname, delimiter=OUT_DELIMITER,
                pull_chunks=True)
            occ_parser.initialize_me()
        except IOError as err:
            log.warning(
                ('Enough already, IOError! Final file {}. Using only '
                 'indices {} - {}, err {}').format(
                     srt_fname, in_idx, curr_idx, str(err)))
            enough_already = True
        except Exception as err:
            log.warning(
                'Enough already! Final file {}. {}'.format(
                    srt_fname, 'Using only indices {} - {}, err {}'.format(
                        in_idx, curr_idx, err)))
            enough_already = True
        else:
            sorted_files.append(occ_parser)
            curr_idx += 1
            srt_fname = _get_op_filename(
                data_path, input_prefix, basename, run=curr_idx)

    try:
        # Open output sorted file
        with open(merge_fname, 'w', encoding=ENCODING) as out_file:
            csv_writer = csv.writer(out_file, delimiter=OUT_DELIMITER)
            # Skip header (no longer written to files)
            # find file with record containing smallest key
            _, pos = _get_smallest_key_and_position(sorted_files)
            while pos is not None:
                # Output records in this file with small_key
                _pop_chunk_and_write(csv_writer, sorted_files[pos])

                # Find smallest again
                _, pos = _get_smallest_key_and_position(sorted_files)

    except Exception as err:
        raise LMError(err)
    finally:
        for open_file in sorted_files:
            log.debug('Closing file {}'.format(open_file.data_fname))
            open_file.close()


# .............................................................................
def main():
    """Main method of the script
    """
    parser = argparse.ArgumentParser(
        description=(
            'Populate a Lifemapper archive with metadata for single- or '
            'multi-species computations specific to the configured input data '
            'or the data package named.'))
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
              '    all: perform all (split, sort, merge) functions'),
        choices=('split', 'sort', 'merge', 'check', 'all'))
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
            in_idx=start_idx)

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
(datapath, in_prefix, out_prefix)=(pth, unsortedPrefix, sortedPrefix)
idx = 0
in_fname = _getOPFilename(datapath, in_prefix, basename, run=idx)

#########################
######################### loop
#########################

out_fname = _getOPFilename(datapath, out_prefix, basename, run=idx)
log.debug('Write from {} to {}'.format(in_fname, out_fname))

uns_rows = []
occreader, infile = get_unicodecsv_reader(in_fname, OUT_DELIMITER)
occwriter, outfile = get_unicodecsv_writer(out_fname, OUT_DELIMITER,
                                           doAppend=False)

while True:
    try:
        uns_rows.append(occreader.next())
    except StopIteration:
        break
    except Exception, e:
        print('Error file %s, line %d: %s' %
             (in_fname, occreader.line_num, e))
        break

infile.close()

srtRows = sort_recs(uns_rows, groupByIdx)

for rec in srtRows:
    occwriter.writerow(rec)

outfile.close()
idx += 1
in_fname = _getOPFilename(datapath, in_prefix, basename, run=idx)


#########################
######################### end loop
#########################

######################### MERGE
mergeSortedFiles(log, mergefname, pth, sortedPrefix, basename, metafname)

checkMergedFile(log, mergefname, metafname)

"""
