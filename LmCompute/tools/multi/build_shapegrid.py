#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script attempts to build a shapegrid
"""
import argparse

from LmCommon.shapes.build_shapegrid import build_shapegrid


# .............................................................................
def main():
    """Script main function
    """
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description='This script attempts to build a shapegrid')

    parser.add_argument(
        'shapegrid_fn', type=str, help='File location for new shapegrid')
    parser.add_argument(
        'min_x', type=float, help='The minimum X value for the shapegrid')
    parser.add_argument(
        'min_y', type=float, help='The minimum Y value for the shapegrid')
    parser.add_argument(
        'max_x', type=float, help='The maximum X value for the shapegrid')
    parser.add_argument(
        'max_y', type=float, help='The maximum Y value for the shapegrid')
    parser.add_argument(
        'cell_size', type=float,
        help='The size of each cell in the appropriate units')
    parser.add_argument(
        'epsg', type=int, help='The EPSG code to use for this shapegrid')
    parser.add_argument(
        'cell_sides', type=int, choices=[4,6],
        help='The number of cides for each cell')
    parser.add_argument(
        '--cutout_wkt_fn', dest='cutout_fn', type=str,
        help='File location of a cutout WKT')

    args = parser.parse_args()

    build_shapegrid(
        args.shapegrid_fn, args.min_x, args.min_y, args.max_x, args.max_y,
        args.cell_size, args.epsg, args.cell_sides, cutout_wkt=args.cutout_fn)


# .............................................................................
if __name__ == '__main__':
    main()
