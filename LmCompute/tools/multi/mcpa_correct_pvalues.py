#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script calculates corrected P-values for F-pseudo values.
"""
import argparse

from LmCommon.statistics.permutation_testing import correct_p_values
from LmCompute.plugins.multi.mcpa.mcpa import get_p_values
from lmpy import Matrix


# .............................................................................
def main():
    """Main method for script
    """
    parser = argparse.ArgumentParser(
        description="This script calculates and corrects P-Values")

    parser.add_argument(
        'observed_filename', type=str,
        help='File location of observed values to test')
    parser.add_argument(
        'p_values_filename', type=str,
        help='File location to store the P-Values')
    parser.add_argument(
        'bh_values_filename', type=str,
        help='File location to store the Benjamini-Hochberg output matrix')
    parser.add_argument(
        'f_value_filename', nargs='+', type=str,
        help='A file of F-values or a stack of F-Values')

    args = parser.parse_args()

    # Load the matrices
    test_values = []
    num_values = 0

    for f_val in args.f_value_filename:
        test_mtx = Matrix.load_flo(f_val)

        # Add the values to the test values list
        test_values.append(test_mtx)

        # Add to the number of values
        if test_mtx.ndim == 3:  # Stack of values
            num_values += test_mtx.shape[2]
        else:
            num_values += 1

    obs_vals = Matrix.load_flo(args.observed_filename)
    p_values = get_p_values(obs_vals, test_values,
                            num_permutations=num_values)

    p_values.write(args.p_values_filename)

    bh_values = correct_p_values(p_values)

    bh_values.write(args.bh_values_filename)


# .............................................................................
if __name__ == "__main__":
    main()
