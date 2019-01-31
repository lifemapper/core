#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""Randomize a PAM using the Grady algorithm

Todo:
    * Consider if we still need this since we do this in multi species run
"""
import argparse

from LmCommon.common.matrix import Matrix
from LmCompute.plugins.multi.randomize.grady import gradyRandomize

# .............................................................................
if __name__ == "__main__":
    
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description=('This script randomizes a PAM using the parallel Grady'
                     ' method while maintaining marginal totals')) 

    parser.add_argument('pamFn', type=str, help="File location for PAM data")
    parser.add_argument(
        'outRandomFn', type=str, help="File location to write randomized PAM")
    
    args = parser.parse_args()
    
    pam = Matrix.load(args.pamFn)
    
    randPam = gradyRandomize(pam)
    
    with open(args.outRandomFn, 'w') as outPamF:
        randPam.save(outPamF)
    