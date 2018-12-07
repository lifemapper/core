#!/bin/bash
"""
@summary: This script pulls iDigBio data and writes points to a CSV file and 
             metadata to a JSON file
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

from LmCommon.common.apiquery import IdigbioAPI

# ...............................................
def getPartnerSpeciesData(self, taxon_id_file, 
                          point_output_file, meta_output_file,
                          missing_id_file=None):
    idigAPI = IdigbioAPI()
    # Writes point and metadata to point_output_file, and meta_output_file
    summary = idigAPI.assembleIdigbioData(taxon_id_file, point_output_file, 
                                          meta_output_file, missing_id_file)
    # Return to user at some checkpoint, match with user names 
    # If user originally sent taxon names for resolution, return names
    # with the unmatched taxonids
    idig_unmatched_gbif_ids = summary['unmatched_gbif_ids']
    if missing_id_file is not None and len(idig_unmatched_gbif_ids) > 0:
        try: 
            f = open(missing_id_file, 'w')
            for gid in idig_unmatched_gbif_ids:
                f.write(gid + '\n')
        except Exception, e:
            raise
        finally:
            f.close()
                
            

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
    