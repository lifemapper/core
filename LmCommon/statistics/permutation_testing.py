"""Module containing functions for permutation testing
"""
from copy import deepcopy

import numpy as np

from lmpy import Matrix


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
def correct_p_values(p_values_matrix, false_discovery_rate=0.05):
    """Perform P-value correction

    Args:
        p_values_matrix (:obj: `Matrix`): A Matrix of p-values to correct
        false_discovery_rate (:obj: `float`): An acceptable false discovery
            rate (alpha) value to declare a cell significant

    Todo:
        * Enable other correction types
        * Consider how metadata may be added
        * Consider producing a matrix of the maximum FDR value that would mark
            each cell as significant
    """
    # Reshape data into one-dimensional array
    p_flat = p_values_matrix.flatten()

    num_vals = p_flat.size

    # 1. Order p-values
    # 2. Assign rank
    # 3. Create critical values
    # 4. Find the largest p-value such that P(i) < critical value
    # 5. All P(j) such that j <= i are significant
    rank = 1
    comp_p = 0.0
    for p_val in sorted(p_flat.tolist()):
        crit_val = false_discovery_rate * (float(rank) / num_vals)

        # Check if the p value is less than the critical value
        if p_val < crit_val:
            # If this p is smaller, all p values smaller than this one are
            #    "significant", even those that were greater than their
            #    respective critical value
            comp_p = p_val
        rank += 1

    headers = deepcopy(p_values_matrix.headers)
    headers[str(p_values_matrix.ndim - 1)] = ['BH Corrected']
    sig_values = (p_values_matrix <= comp_p).astype(int)
    return Matrix(sig_values, headers=headers)


# .............................................................................
def get_p_values(observed_matrix, test_matrices,
                 compare_func=compare_absolute_values):
    """Gets p-values by comparing the observed and random data

    Args:
        observed_matrix (:obj: `Matrix`): A Matrix object with observed values
        test_matrices (:obj: `list`): A list of Matrix objects with values
            obtained through permutations
        compare_func (:obj: `function`): A function that, when given two
            values, returns True if the second meets the condition

    Todo:
        * Take optional clip values
        * Take optional number of permutations
    """
    p_val_headers = deepcopy(observed_matrix.headers)
    ndim = observed_matrix.ndim
    p_val_headers[str(ndim - 1)] = ['P-Values']

    # Create the P-values matrix.  The shape should be the same as the observed
    #    data with one extra dimension if the last dimension has size > 1
    if observed_matrix.shape[-1] == 1:
        p_vals_shape = observed_matrix.shape
    else:
        p_vals_shape = list(observed_matrix.shape) + [1]
    p_values = Matrix(np.zeros(p_vals_shape), headers=observed_matrix.headers)

    num_permutations = 0
    for rand in test_matrices:
        # If the random matrices are a stack with more dimensions or more
        #    layers, compare each layer to observed
        if rand.ndim > ndim or \
                rand.shape[-1] > observed_matrix.shape[-1]:
            # Determine shape of test matrix
            if rand.ndim > ndim:
                test_shape = list(rand.shape)[:-1]
            else:
                test_shape = observed_matrix.shape
            # Loop through each
            for i in range(rand.shape[-1]):
                p_values += compare_func(
                    observed_matrix,
                    # Slice off one test layer
                    rand[..., i].reshape(test_shape))
                num_permutations += 1
        else:
            p_values += compare_func(observed_matrix, rand)
            num_permutations += 1

    # Divide by number of permutations and clip just in case
    p_values = np.clip(
        np.nan_to_num(p_values / num_permutations), 0.0, 1.0)
    return p_values
