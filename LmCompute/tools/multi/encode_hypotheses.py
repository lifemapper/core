#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script encodes biogeographic hypothesis shapefiles into a Matrix

Note:
    If you want to encode multiple layers with different event fields, call
        this script for each set of layers and then use the
        concatenate_matrices script to stitch the results together.

Todo:
    Consider providing an option other than shapegrid, ex. (site id, x, y)
    Provide a method for naming layers and providing event field in one
"""
import argparse
import os

from LmCommon.encoding.layer_encoder import LayerEncoder

# .............................................................................
if __name__ == "__main__":
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="This script encodes a biogeographic hypothesis shapegrid")

    parser.add_argument('-e', '--event_field', dest='event_field', type=str,
                        help="Use this field in the layer to determine events")
    parser.add_argument(
        '-m', '--min_coverage', dest='min_coverage', type=float, default=0.25,
        help=('The minimum proportion of a shapegrid cell that must be covered'
              ' by a hypothesis value (0.0 - 1.0]'))
    parser.add_argument(
        'shapegrid_filename', type=str,
        help="The file location of the shapegrid to use for encoding")
    parser.add_argument("out_filename", type=str, 
                        help="The file location to write the resulting matrix")
    parser.add_argument(
        "layer", type=str, nargs='+',
        help=('A file location of a shapegrid with one or more BioGeo '
              'hypotheses'))

    args = parser.parse_args()

    encoder = LayerEncoder(args.shapegrid_filename)
    for layer in args.layer:
        # Use the base file name as the column name
        column_name = os.path.splitext(os.path.basename(layer))[0]
        encoder.encode_biogeographic_hypothesis(
            layer, column_name, args.min_coverage,
            event_field=args.event_field)

    bg_mtx = encoder.get_encoded_matrix()

    with open(args.out_filename, 'w') as out_f:
        bg_mtx.save(out_f)
