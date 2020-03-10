"""Merge data
"""
import argparse
import logging
import os
import time

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (ONE_HOUR)
from LmCommon.common.occ_parse import OccDataParser
from LmServer.common.log import ScriptLogger

TROUBLESHOOT_UPDATE_INTERVAL = ONE_HOUR


# .............................................................................
def main():
    """Main method for script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'base_point_data',
        help='Path and basename of base CSV data and JSON metadata to merge')
    parser.add_argument(
        '--base_point_delimiter', default='\t',
        help='Delimter for base CSV point data')

    parser.add_argument(
        '--merge_point_data', default=None,
        help='Path and basename of additional files to merge')
    parser.add_argument(
        '--merge_point_delimiter', default='\t',
        help='Delimiter for additional CSV point data')

    args = parser.parse_args()
    base_csv_fname = args.base_point_data + '.csv'
    base_meta_fname = args.base_point_data + '.json'
    base_delimiter = args.base_point_delimiter
    if base_delimiter not in ('\t', ','):
        base_delimiter = '\t'
    if not os.path.exists(base_csv_fname) and os.path.exists(base_meta_fname):
        raise LMError(
            'Base point data {} does not exist'.format(args.base_point_data))

    merge_csv_fname = None
    # merge_meta_fname = None
    if args.merge_point_data is not None:
        merge_csv_fname = args.merge_point_data + '.csv'
        # merge_meta_fname = args.merge_point_data + '.json'
        merge_delimiter = args.merge_point_delimiter
        if merge_delimiter not in ('\t', ','):
            merge_delimiter = '\t'

    secs = time.time()
    timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
    scriptname = os.path.splitext(os.path.basename(__file__))[0]
    logname = '{}.{}'.format(scriptname, timestamp)
    logger = ScriptLogger(logname, level=logging.INFO)

    try:
        occ_parser = OccDataParser(
            logger, base_csv_fname, base_meta_fname, delimiter=base_delimiter,
            pull_chunks=True)
    except Exception as e:
        raise LMError('Failed to construct OccDataParser, {}'.format(e))

    if all([
            merge_csv_fname, os.path.exists(base_csv_fname),
            os.path.exists(base_meta_fname)]):
        raise LMError(
            'Base point data {} does not exist'.format(args.base_point_data))

    try:
        occ_parser = OccDataParser(
            logger, base_csv_fname, base_meta_fname, delimiter=base_delimiter,
            pull_chunks=True)
    except Exception as e:
        raise LMError('Failed to construct OccDataParser, {}'.format(e))

    _field_names = occ_parser.header
    occ_parser.initialize_me()


# .............................................................................
if __name__ == '__main__':
    main()
