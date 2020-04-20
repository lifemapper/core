# !/bin/bash
"""This script concatenates two (or more) matrices along a specified axis
"""
import argparse

from LmBackend.common.lmobj import LMObject
from lmpy import Matrix


# .............................................................................
def main():
    """Main method of the script
    """
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description='Concatenate two (or more) matrices along an axis')

    parser.add_argument(
        'out_fn', type=str,
        help='The file location to write the resulting matrix')
    parser.add_argument(
        'axis', type=int,
        help='The (Matrix) axis to concatenate these matrices on')
    parser.add_argument(
        'mtx_fn', type=str, nargs='*',
        help="The file location of the first matrix")

    args = parser.parse_args()

    mtxs = []
    if args.mtx_fn:
        for mtx_fn in args.mtx_fn:
            mtxs.append(Matrix.load(mtx_fn))

    joined_mtx = Matrix.concatenate(mtxs, axis=args.axis)

    # Make sure directory exists
    LMObject().ready_filename(args.out_fn)

    joined_mtx.write(args.out_fn)


# .............................................................................
if __name__ == "__main__":
    main()
