#!/bin/bash
"""
@summary: This script pulls iDigBio data and writes points to a CSV file and 
             metadata to a JSON file
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
import argparse
import os

from LmCommon.common.apiquery import IdigbioAPI

# ...............................................
def _getUserInput(filename):
    items = []
    if os.path.exists(filename):
        try:
            for line in open(filename):
                items.append(line.strip())
        except:
            raise Exception('Failed to read file {}'.format(filename))
    else:
        raise Exception('File {} does not exist'.format(filename))
    return items

# ...............................................
def getPartnerSpeciesData(taxon_id_file, 
                          point_output_file, meta_output_file,
                          missing_id_file=None):
    taxon_ids = _getUserInput(taxon_id_file)
    idigAPI = IdigbioAPI()
    # Writes points, metadata, unmatched ids to respective files
    summary = idigAPI.assembleIdigbioData(taxon_ids, point_output_file, 
                                          meta_output_file, 
                                          missing_id_file=missing_id_file)
                
            

# .............................................................................
if __name__ == "__main__":
    
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="This script attempts to build a shapegrid") 
    
    parser.add_argument('taxon_id_file', type=str, 
                        help="File location for list of GBIF Taxon IDs")
    parser.add_argument('point_output_file', type=str, 
                        help="File location for output point data file")
    parser.add_argument('meta_output_file', type=str, 
                        help="File location for output metadata file")
    parser.add_argument('--missing_id_file', default=None, type=str,
                        help="File location for output unmatched taxonids file")
    
    args = parser.parse_args()
    

    getPartnerSpeciesData(args.taxon_id_file, args.point_output_file, 
                          args.meta_output_file, 
                          missing_id_file=args.missing_id_file)
    
"""
import os

from LmCommon.common.apiquery import IdigbioAPI

def _getUserInput(filename):
    items = []
    if os.path.exists(filename):
        try:
            for line in open(filename):
                items.append(line.strip())
        except:
            raise Exception('Failed to read file {}'.format(filename))
    else:
        raise Exception('File {} does not exist'.format(filename))
    return items


taxon_id_file =  '/state/partition1/lmscratch/temp/user_taxon_ids_37488.txt'

point_output_file = 'mf_2101/user_taxon_ids_37488.csv'           
meta_output_file = 'mf_2101/user_taxon_ids_37488.json'
missing_id_file= None

taxon_ids = _getUserInput(taxon_id_file)
self = IdigbioAPI()

for fname in (point_output_file, meta_output_file):
    if os.path.exists(fname):
        print('Deleting existing file {} ...'.format(fname))
        os.remove(fname)


summary = {self.GBIF_MISSING_KEY: []}
writer, f = self._getCSVWriter(point_output_file, doAppend=False)
fldnames = None

gid = taxon_ids[0]
ptCount, fldnames = self._getIdigbioRecords(gid, fldnames, 
                                            writer, meta_output_file)
#################
import idigbio
gbifTaxonId = gid

api = idigbio.json()
limit = 100
offset = 0
currcount = 0
total = 0
recordQuery = {'taxonid':str(gbifTaxonId), 
               'geopoint': {'type': 'exists'}}
output = api.search_records(rq=recordQuery,
                            limit=limit, offset=offset)
total = output['itemCount']

# First gbifTaxonId where this data retrieval is successful, 
# get and write header and metadata
if total > 0 and fields is None:
    fields = self._getIdigbioFields(output['items'][0])
    # Write header in datafile
    writer.writerow(fields)
    # Write metadata file with column indices 
    meta = self._writeIdigbioMetadata(fields, meta_output_file)
    
# Write these records
recs = output['items']
currcount += len(recs)
print("  Retrieved {} records, {} records starting at {}"
      .format(len(recs), limit, offset))
for rec in recs:
    recdata = rec['indexTerms']
    vals = []
    for fldname in fields:
        # Pull long, lat from geopoint
        if fldname == 'dec_long':
            try:
                vals.append(recdata['geopoint']['lon'])
            except:
                vals.append('')
        elif fldname == 'dec_lat':
            try:
                vals.append(recdata['geopoint']['lat'])
            except:
                vals.append('')
        # or just append verbatim
        else:
            try:
                vals.append(recdata[fldname])
            except:
                vals.append('')
    
    writer.writerow(vals)

#################                                            
    if ptCount > 0:
        summary[gid] = ptCount
    else:
        summary[self.GBIF_MISSING_KEY].append(gid)

summary = idigAPI.assembleIdigbioData(taxon_ids, point_output_file, meta_output_file)



"""
