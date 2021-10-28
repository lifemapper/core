"""This script processes an exported CSV from the db and indexes the taxonomy

Note:
    Change the value of taxon_source_id for the taxonomy to index.
    This script processes the output of:
       set client_encoding = 'UTF8'
       \copy (select * from lm_v3.lm_taxon_solr where taxon_source_id = 1) to '/state/partition1/taxonomy.export.csv' csv header
"""
import argparse
import csv
import os
import time

from LmServer.common.lmconstants import NUM_DOCS_PER_POST
import LmServer.common.solr as lm_solr
from LmBackend.common.lmobj import LMError

# .............................................................................
def main():
    """Main method of script"""
    parser = argparse.ArgumentParser(
        description='Reindex taxonomy from a CSV file')
    # parser.add_argument('-n', '--number_per_post', type=int,
    #                     default=NUM_DOCS_PER_POST,
    #                     help='Index this many taxa per Solr POST')
    parser.add_argument('csv_filename', type=str,
                        help='The CSV file with taxon information')
    args = parser.parse_args()

    if not os.path.exists(args.csv_filename):
        raise LMError('CSV path: {} does not exist'.format(args.csv_filename))
    
    lm_solr.add_taxa_to_taxonomy_from_csv(args.csv_filename)

    # with open(args.csv_filename, 'r', encoding=ENCODING) as in_csv:
    #     index_taxonomy_csv_flo(in_csv, num_per_post=args.number_per_post)


# .............................................................................
if __name__ == '__main__':
    main()
