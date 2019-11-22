#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script calculates MCPA for a set of matrices.

Todo:
    * Determine if this is obsolete now that we have a script for performing
        PAM stats and MCPA
"""
import argparse
import numpy as np

from LmCommon.common.matrix import Matrix
from LmCompute.plugins.multi.mcpa.mcpa import mcpa, mcpa_parallel

# .............................................................................
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='This script calculates MCPA for observed data')
    
    # Optional outputs
    parser.add_argument(
        '-co', '--corr_output', type=str, 
        help=('File location to store semi-partial correlation outputs (only'
              ' for observed)'))
    parser.add_argument(
        '-fo', '--freq_output', type=str, 
        help=('File location to store (stack of) semi-partial correlation'
              ' outputs'))
    parser.add_argument(
        '-p', '--parallel', action='store_true',
        help='Use the parallel version of MCPA')
    
    # Randomizations
    parser.add_argument(
        '-n', '--num_permutations', type=int, default=1,
        help='Run this many permutations (only if random)')
    parser.add_argument(
        '--randomize', action='store_true', help='Randomize the data')
    
    # Inputs
    parser.add_argument(
        'incidence_matrix_filename', type=str, 
        help='Incidence matrix for the analysis (PAM) file name')
    parser.add_argument(
        'phylo_encoding_filename', type=str,
        help='Encoded phylogenetic matrix file location')
    parser.add_argument(
        'env_predictors_filename', type=str,
        help='Environment predictor matrix file location')
    parser.add_argument(
        'biogeo_predictors_filename', type=str,
        help='Biogeographic predictor matrix file location')
    args = parser.parse_args()

    # Load the matrices
    incidence_matrix = Matrix.load(args.incidence_matrix_filename)
    phylo_matrix = Matrix.load(args.phylo_encoding_filename)
    env_pred_matrix = Matrix.load(args.env_predictors_filename)
    bg_pred_matrix = Matrix.load(args.biogeo_predictors_filename)

    if args.parallel:
        mcpa_method = mcpa_parallel
    else:
        mcpa_method = mcpa

    if not args.randomize:
        obs_matrix, f_matrix = mcpa_method(
            incidence_matrix, phylo_matrix, env_pred_matrix, bg_pred_matrix)
        if args.corr_output is not None:
            with open(args.corr_output, 'w') as corr_f:
                obs_matrix.save(corr_f)
    else:
        f_stack = []

        num_permutations = 1
        if args.num_permutations > 1:
            num_permutations = args.num_permutations

        # Note: randomize by permuting rows of predictor matrices and columns
        #    of phylo matrix.
        # TODO(CJ): Consider how we could randomize the PAM instead

        # Create an array of indices.  This will be used to reorder the
        #    predictor matrices.
        index_order = np.arange(env_pred_matrix.data.shape[0])
        phylo_order = np.arange(phylo_matrix.data.shape[1])
        for i in range(num_permutations):
            # Note: Keep the predictor matrices in sync by using the same row
            #    order for both.
            np.random.shuffle(index_order)
            env_pred_matrix.data = env_pred_matrix.data[index_order]
            bg_pred_matrix.data = bg_pred_matrix.data[index_order]
            
            # Shuffle phylo matrix columns
            np.random.shuffle(phylo_order)
            phylo_matrix.data = phylo_matrix.data[:, phylo_order]

            _, f_matrix = mcpa_method(incidence_matrix, phylo_matrix,
                                        env_pred_matrix, bg_pred_matrix)
            
            f_stack.append(f_matrix)

        f_matrix = Matrix.concatenate(f_stack, axis=2)

    # Write F-matrix / stack
    if args.freq_output is not None:
        with open(args.freq_output, 'w') as f_vals_f:
            f_matrix.save(f_vals_f)
