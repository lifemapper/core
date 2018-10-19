#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script calculates corrected P-values for F-pseudo values.
"""
import argparse

from LmCommon.common.matrix import Matrix
from LmCommon.statistics.pValueCorrection import correctPValues
from LmCompute.plugins.multi.mcpa.mcpa import get_p_values

# .............................................................................
if __name__ == "__main__":
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
        test_mtx = Matrix.load(f_val)
      
        # Add the values to the test values list
        test_values.append(test_mtx)
      
        # Add to the number of values
        if test_mtx.data.ndim == 3: # Stack of values
            num_values += test_mtx.data.shape[2]
        else:
            num_values += 1
   
    obs_vals = Matrix.load(args.observed_filename)
    p_values = get_p_values(obs_vals, test_values,
                            num_permutations=num_values)
   
    bh_values = correctPValues(p_values)
   
    with open(args.p_values_filename, 'w') as p_val_f:
        p_values.save(p_val_f)
      
    with open(args.bh_values_filename, 'w') as bh_val_f:
        bh_values.save(bh_val_f)
