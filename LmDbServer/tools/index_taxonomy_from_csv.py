"""This script processes an exported CSV from the db and indexes the taxonomy
@author: CJ Grady
@status: beta
@version: 4.0.0

@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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

from LmServer.common.lmconstants import NUM_DOCS_PER_POST
import LmServer.common.solr as lm_solr

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

# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Reindex taxonomy from a CSV file')
    parser.add_argument('-n', '--number_per_post', type=int, 
                        default=NUM_DOCS_PER_POST, 
                        help='Index this many taxa per Solr POST')
    parser.add_argument('csv_filename', type=str, 
                        help='The CSV file with taxon information')
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_filename):
        raise Exception('CSV path: {} does not exist'.format(args.csv_filename))
    else:
        with open(args.csv_filename) as in_csv:
            index_taxonomy_csv_flo(in_csv, num_per_post=args.number_per_post)
