#!/bin/bash
"""
@summary: This script calculates MCPA values for observed data
@author: CJ Grady
@version: 4.0.0
@status: beta
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
import argparse

from LmCommon.common.matrix import Matrix
from LmCompute.plugins.multi.mcpa.mcpa import mcpa_run

# .............................................................................
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='This script calculates MCPA for observed data')
    
    # Optional outputs
    parser.add_argument('-co', '--corr_output', type=str, 
        help='File location to store semi-partial correlation outputs (only for observed')
    parser.add_argument('-fo', '--freq_output', type=str, 
        help='File location to store (stack of) semi-partial correlation outputs')
    
    # Randomizations
    parser.add_argument('-n', '--num_permutations', type=int, default=1,
                        help='Run this many permutations (only if random)')
    parser.add_argument('--randomize', action='store_true', 
                        help='Randomize the data')
    
    # Inputs
    parser.add_argument(
        'incidence_matrix_filename', 
        help='Incidence matrix for the analysis (PAM) file name')
    parser.add_argument('phylo_encoding_filename',
                        help='Encoded phylogenetic matrix file location')
    parser.add_argument('env_predictors_filename',
                        help='Environment predictor matrix file location')
    parser.add_argument('biogeo_predictors_filename',
                        help='Biogeographic predictor matrix file location')
    args = parser.parse_args()

    # Load the matrices
    incidence_matrix = Matrix.load(args.incidence_matrix_filename)
    phylo_matrix = Matrix.load(args.phylo_encoding_filename)
    env_pred_matrix = Matrix.load(args.env_predictors_filename)
    bg_pred_matrix = Matrix.load(args.biogeo_predictors_filename)

    if not args.randomize:
        obs_matrix, f_matrix = mcpa_run(incidence_matrix, phylo_matrix, 
                                        env_pred_matrix, bg_pred_matrix)
        if args.corr_output is not None:
            with open(args.corr_output, 'w') as corr_f:
                obs_matrix.save(corr_f)
    else:
        f_stack = []

        num_permutations = 1
        if args.num_permutations > 1:
            num_permutations = args.num_permutations
        for i in range(num_permutations):
            _, f_matrix = mcpa_run(incidence_matrix, phylo_matrix, 
                                   env_pred_matrix, bg_pred_matrix,
                                   randomize=True)
            f_stack.append(f_matrix)

        f_matrix = Matrix.concatenate(f_stack, axis=2)

    # Write F-matrix / stack
    if args.freq_output is not None:
        with open(args.freq_output, 'w') as f_vals_f:
            f_matrix.save(f_vals_f)
