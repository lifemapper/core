#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script randomizes a PAM using the swap method

Todo:
     Consider adding an option to multi-species run script and deleting this
"""
import argparse

from LmCommon.common.matrix import Matrix
from LmCompute.plugins.multi.randomize.swap import swapRandomize

# .............................................................................
if __name__ == "__main__":
    
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="This script randomizes a PAM using the swap method") 

    parser.add_argument('pamFn', type=str, help="File location for PAM data")
    parser.add_argument('numSwaps', type=int, 
                              help="The number of successful swaps to perform")
    parser.add_argument('outRandomFn', type=str, 
                              help="File location to write randomized PAM")
    #parser.add_argument('--maxTries', type=int, 
    #        help="If provided, this is the maximum number of attempts to find a swap before giving up (Default: 1 million)")
    
    args = parser.parse_args()
    
    pam = Matrix.load(args.pamFn)
    
    randPam = swapRandomize(pam, args.numSwaps)
    
    with open(args.outRandomFn, 'w') as outPamF:
        randPam.save(outPamF)
