#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script intersects a shapegrid and a vector layer to create a PAV
"""
import argparse
import os

from LmCommon.common.readyfile import readyFilename
from LmCommon.encoding.layer_encoder import LayerEncoder

# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=('This script performs a vector intersect with a shapegrid'
                     ' to produce a PAV'))
    
    parser.add_argument(
        'shapegrid_filename', type=str, 
        help='This is the shapegrid to intersect the layer with')
    parser.add_argument(
        'vector_filename', type=str, 
        help='The file location of the vector file to use for intersection')
    parser.add_argument(
        'pav_filename', type=str,
        help='The file location to write the output PAV Matrix object')
    
    parser.add_argument(
        'min_presence', type=float, 
        help='The minimum value to consider present')
    parser.add_argument(
        'max_presence', type=float, 
        help='The maximum value to consider present')
    parser.add_argument(
        'min_coverage', type=float, 
        help=('The proportion of a cell that must be present to determine '
              'the cell is present (0.0 - 1.0]'))
    parser.add_argument(
        'event_field', type=str, 
        help='Use this attribute to determine presence')
    
    parser.add_argument(
        '--squid', type=str, dest='squid', 
        help=('A species identifier to be attached to the PAV Matrix column as'
              ' metadata'))
    
    parser.add_argument(
        '--layer_status_file', type=str, help='Status file for input layer')
    parser.add_argument(
        '-s', '--status_file', type=str, help='Output status file')

    args = parser.parse_args()

    squid = os.path.basename(os.path.splitext(args.vector_filename)[0])
    if args.squid is not None:
        squid = args.squid
        
    # Scale percent presence if necessary
    min_coverage = args.min_coverage
    if min_coverage > 1.0:
        min_coverage = min_coverage / 100.0
    
    encoder = LayerEncoder(args.shapegrid_filename)
    encoder.encode_presence_absence(
            args.vector_filename, squid, args.min_presence, args.max_presence, 
            min_coverage, attribute_name=args.event_field)
    pav = encoder.get_encoded_matrix()
        
    if pav is not None:
        readyFilename(args.pav_filename, overwrite=True)
        with open(args.pav_filename, 'w') as pav_out_f:
            pav.save(pav_out_f)
