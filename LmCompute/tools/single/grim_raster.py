#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script intersects a shapegrid and a raster layer to create a GRIM column
"""
import argparse
import os

from LmCommon.common.readyfile import readyFilename
from LmCommon.encoding.layer_encoder import LayerEncoder

# .............................................................................
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=('This script performs a raster intersect with a shapegrid'
                     ' to produce a GRIM column'))
    
    parser.add_argument(
        'shapegrid_filename', type=str,
        help="This is the shapegrid to intersect the layer with")
    parser.add_argument(
        'raster_filename', type=str,
        help=('This is the file location of the raster file to use for '
              'intersection'))
    parser.add_argument(
        'grim_column_filename', type=str, 
        help='Location to write the GRIM column Matrix object')

    parser.add_argument('-m', '--method', dest='method', 
                        choices=['largest_class', 'mean'], default='mean', 
                        help='Use this method for encoding the GRIM layer')
    parser.add_argument(
        '-i', '--ident', type=str, dest='ident', 
        help='An identifer to be used as metadata for this column')

    args = parser.parse_args()
    
    ident = os.path.splitext(os.path.basename(args.raster_filename))[0]
    if args.ident is not None:
        if args.ident.lower() is not 'none':
            ident = args.ident

    encoder = LayerEncoder(args.shapegrid_filename)
    if args.method == 'largest_class':
        encoder.encode_largest_class(args.raster_filename, args.ident)
    else:
        encoder.encode_mean_value(args.raster_filename, args.ident)
    
    grim_col = encoder.get_encoded_matrix()
    
    readyFilename(args.grim_column_filename, overwrite=True)
    with open(args.grim_column_filename, 'w') as grim_col_out_f:
        grim_col.save(grim_col_out_f)
