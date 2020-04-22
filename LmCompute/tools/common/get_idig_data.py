# !/bin/bash
"""Get data from iDigBio
"""
import argparse
import os

from LmBackend.common.lmobj import LMError
from LmCommon.common.api_query import IdigbioAPI
from LmCommon.common.lmconstants import ENCODING
from LmCommon.common.ready_file import ready_filename


# ...............................................
def _get_user_input(filename):
    items = []
    if os.path.exists(filename):
        try:
            for line in open(filename, 'r', encoding=ENCODING):
                items.append(line.strip())
        except Exception:
            raise LMError('Failed to read file {}'.format(filename))
    else:
        raise LMError('File {} does not exist'.format(filename))
    return items


# ...............................................
def get_partner_species_data(taxon_id_file, point_output_file,
                             meta_output_file, missing_id_file=None):
    """Get species data from partner services
    """
    taxon_ids = _get_user_input(taxon_id_file)
    idig_api = IdigbioAPI()
    # Writes points, metadata, unmatched ids to respective files
    idig_api.assemble_idigbio_data(
        taxon_ids, point_output_file, meta_output_file,
        missing_id_file=missing_id_file)


# .............................................................................
def main():
    """Main method for script
    """
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description='This script attempts to build a shapegrid')

    parser.add_argument(
        'taxon_id_file', type=str,
        help='File location for list of GBIF Taxon IDs')
    parser.add_argument(
        'point_output_file', type=str,
        help='File location for output point data file')
    parser.add_argument(
        'meta_output_file', type=str,
        help='File location for output metadata file')
    parser.add_argument(
        'success_file', type=str,
        help='File location to write success indication')
    parser.add_argument(
        '--missing_id_file', default=None, type=str,
        help='File location for output unmatched taxonids file')

    args = parser.parse_args()

    # This writes data with tab delimiter
    get_partner_species_data(
        args.taxon_id_file, args.point_output_file, args.meta_output_file,
        missing_id_file=args.missing_id_file)
    ready_filename(args.success_file)
    with open(args.success_file, 'w', encoding=ENCODING) as out_f:
        out_f.write('1')


# .............................................................................
if __name__ == '__main__':
    main()
