#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script randomizes a PAM using the splotch method

Todo:
     Determine if this should be removed and splotch randomize added to multi
          species run
"""
import argparse

from LmCommon.common.matrix import Matrix
from LmCompute.plugins.multi.randomize.splotch import splotchRandomize

# .............................................................................
if __name__ == "__main__":
    
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="This script randomizes a PAM using the splotch method") 

    parser.add_argument('pamFn', type=str, help="File location for PAM data")
    parser.add_argument(
        'shapegridFn', type=str, help="File location for shapegrid shapefile")
    parser.add_argument(
        'numSides', type=int, choices=[4,6],
        help="The number of sides for each cell in the shapegrid")
    parser.add_argument(
        'outRandomFn', type=str, help="File location to write randomized PAM")
    
    args = parser.parse_args()
    
    pam = Matrix.load(args.pamFn)
    
    randPam = splotchRandomize(pam, args.shapegridFn, args.numSides)
    
    with open(args.outRandomFn, 'w') as outPamF:
        randPam.save(outPamF)
