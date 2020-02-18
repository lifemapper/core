#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script processes an exported CSV from the db and indexes the taxonomy

Note:
    It processes the output of:
       set client_encoding = 'UTF8'
       \copy taxon to '/state/partition1/taxonomy.export.csv' csv header
"""
import argparse
import csv
import os
import time

from LmServer.common.lmconstants import NUM_DOCS_PER_POST
import LmServer.common.solr as lm_solr
from LmBackend.common.lmobj import LMError


# .............................................................................
def index_taxonomy_csv_flo(taxonomy_flo, num_per_post=NUM_DOCS_PER_POST):
    """Index the taxonomy in the file-like object
    """
    taxonomy_dicts = []
    reader = csv.DictReader(taxonomy_flo)
    for rec in reader:
        taxonomy_dicts.append(rec)
        if len(taxonomy_dicts) >= num_per_post:
            lm_solr.add_taxa_to_taxonomy_index_dicts(taxonomy_dicts)
            taxonomy_dicts = []
            print(('{} - Posted {} taxonomy documents to solr index'.format(
                time.strftime('%Y/%m/%d %H:%M:%S %Z'), num_per_post)))
    if len(taxonomy_dicts) > 0:
        lm_solr.add_taxa_to_taxonomy_index_dicts(taxonomy_dicts)
        print(('{} - Posted {} taxonomy documents to solr index'.format(
            time.strftime('%Y/%m/%d %H:%M:%S %Z'), len(taxonomy_dicts))))


# .............................................................................
def main():
    """Main method of script
    """
    parser = argparse.ArgumentParser(
        description='Reindex taxonomy from a CSV file')
    parser.add_argument('-n', '--number_per_post', type=int,
                        default=NUM_DOCS_PER_POST,
                        help='Index this many taxa per Solr POST')
    parser.add_argument('csv_filename', type=str,
                        help='The CSV file with taxon information')
    args = parser.parse_args()

    if not os.path.exists(args.csv_filename):
        raise LMError('CSV path: {} does not exist'.format(args.csv_filename))

    with open(args.csv_filename) as in_csv:
        index_taxonomy_csv_flo(in_csv, num_per_post=args.number_per_post)


# .............................................................................
if __name__ == '__main__':
    main()
