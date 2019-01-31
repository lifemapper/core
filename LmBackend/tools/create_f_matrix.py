"""This script creates a Frequency matrix

This script creates a F or frequency matrix indicating the frequency in which
values from randomized computations are greater than the values of observed
data.  The frequency for each cell in the matrix is equal to the proportion of
times the value, or absolute value, was found to be greater out of the total
number of permuted runs
"""
import argparse
import numpy as np

from LmCommon.common.matrix import Matrix
from LmCommon.common.readyfile import readyFilename

# .............................................................................
def compare_absolute_values(obs, rand):
    """Compares the absolute value of the observed data and the random data

    Args:
        obs (:obj: `Numpy array`): A numpy array of observed values
        rand (:obj: `Numpy array`): A numpy array of random values
    """
    return np.abs(rand) > np.abs(obs)

# .............................................................................
def compare_signed_values(obs, rand):
    """Compares the signed value of the observed data and the random data

    Args:
        obs (:obj: `Numpy array`): A numpy array of observed values
        rand (:obj: `Numpy array`): A numpy array of random values
    """
    return rand > obs

# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create frequency matrix for a set of permutation tests')
    parser.add_argument('observed_matrix', type=str,
                        help='File location of observed matrix')
    parser.add_argument(
        'f_matrix_filename', type=str, help='File location to write F-matrix')
    parser.add_argument(
        'random_matrix', type=str, nargs='+',
        help='File location of random matrix')
    parser.add_argument(
        '-a', action='store_true', help='If set, compare absolute values')
    args = parser.parse_args()

    if args.a:
        cmp_func = compare_absolute_values
    else:
        cmp_func = compare_signed_values
    
    obs = Matrix.load(args.observed_matrix)
    f_mtx = Matrix(np.zeros(obs.data.shape), headers=obs.headers)
    num_permutations = 0
    
    # Compare all matrices
    for fn in args.random_matrix:
        rand = Matrix.load(fn)
        if rand.data.ndim > obs.data.ndim:
            f_mtx.data += np.sum(cmp_func(obs.data, rand.data), axis=0)
            num_permutations += rand.data.shape[rand.data.ndim - 1]
        else:
            num_permutations += 1
            f_mtx.data += cmp_func(obs.data, rand.data)
    
    # Scale values
    f_mtx.data = f_mtx.data / num_permutations
    
    readyFilename(args.f_matrix_filename)
    with open(args.f_matrix_filename, 'w') as out_f:
        f_mtx.save(out_f)
