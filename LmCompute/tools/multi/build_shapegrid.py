#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script attempts to build a shapegrid
"""
import argparse

from LmCommon.shapes.buildShapegrid import buildShapegrid

# .............................................................................
if __name__ == "__main__":
    
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="This script attempts to build a shapegrid") 
    
    parser.add_argument('shapegridFn', type=str, 
                              help="File location for new shapegrid")
    parser.add_argument('minX', type=float, 
                              help="The minimum X value for the shapegrid")
    parser.add_argument('minY', type=float, 
                              help="The minimum Y value for the shapegrid")
    parser.add_argument('maxX', type=float, 
                              help="The maximum X value for the shapegrid")
    parser.add_argument('maxY', type=float, 
                              help="The maximum Y value for the shapegrid")
    parser.add_argument('cellSize', type=float, 
                              help="The size of each cell in the appropriate units")
    parser.add_argument('epsg', type=int, 
                              help="The EPSG code to use for this shapegrid")
    parser.add_argument('cellSides', type=int, choices=[4,6],
                              help="The number of cides for each cell")
    parser.add_argument('--cutoutWktFn', dest='cutoutFn', type=str,
                              help="File location of a cutout WKT")
    
    args = parser.parse_args()
    
    cutout = None
    if args.cutoutFn is not None:
        cutout = args.cutoutFn

    buildShapegrid(
        args.shapegridFn, args.minX, args.minY, args.maxX, args.maxY,
        args.cellSize, args.epsg, args.cellSides, cutoutWKT=cutout)
    