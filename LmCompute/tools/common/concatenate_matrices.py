#!/bin/bash
"""This script concatenates two (or more) matrices along a specified axis
"""
import argparse

from LmBackend.common.lmobj import LMObject
from LmCommon.common.matrix import Matrix

# .............................................................................
if __name__ == "__main__":
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="This script concatenates two (or more) matrices along an axis")
    
    parser.add_argument(
        "outFn", type=str,
        help="The file location to write the resulting matrix")
    parser.add_argument(
        "axis", type=int,
        help="The (Matrix) axis to concatenate these matrices on")
    parser.add_argument(
        "mtxFn", type=str, nargs='*',
        help="The file location of the first matrix")
    parser.add_argument(
        "--mashedPotato", type=str, dest="mashedPotato",
        help="A mashed potato file of file names to concatenate")
    
    args = parser.parse_args()
    
    mtxs = []
    if args.mashedPotato is not None:
        with open(args.mashedPotato, 'r') as mashIn:
            for line in mashIn:
                squid, pav = line.split(':')
                mtxs.append(Matrix.load(pav.strip()))
    if args.mtxFn:
        for mtxFn in args.mtxFn:
            mtxs.append(Matrix.load(mtxFn))
    
    joinedMtx = Matrix.concatenate(mtxs, axis=args.axis)

    # Make sure directory exists
    LMObject().readyFilename(args.outFn)
    
    with open(args.outFn, 'w') as outF:
        joinedMtx.save(outF)

