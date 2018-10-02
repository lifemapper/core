"""
@summary: Module containing functions used to perform a MetaCommunity 
            Phylogenetics Analysis
@author: CJ Grady (originally from MATLAB code in the referenced literature 
            supplemental materials
@version: 1.0
@status: alpha
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
@see: Leibold, m.A., E.P. Economo and P.R. Peres-Neto. 2010. Metacommunity
         phylogenetics: separating the roles of environmental filters and 
         historical biogeography. Ecology letters 13: 1290-1299.
@todo: Original method, randomize in method
@todo: New method, randomize first
@author: cjgrady
"""
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import numpy as np
try:
    from cmath import sqrt
except:
    from math import sqrt

from LmCommon.common.matrix import Matrix

#NUM_THREADS = 100
NUM_THREADS = 4

# .............................................................................
def _predicted_calc(pred_std, r_div_q_transverse, site_weights, p_sigma_std):
    """
    @summary: This function calculates the 'predicted' matrix but uses less 
                memory by not storing large temporary matrices.  It will run 
                slower because of that
    @todo: Document
    """
    num_rows = pred_std.shape[0]
    predicted = np.empty((num_rows,), dtype=float)
    
    tmp = pred_std.dot(r_div_q_transverse)
    pred_std_transverse = pred_std.T
    
    for i in xrange(num_rows):
        # Get row from (A)
        a_row = tmp[i].dot(pred_std_transverse) * site_weights
        predicted[i] = a_row.dot(p_sigma_std)
    
    return predicted
    
# .............................................................................
def _standardize_matrix(mtx, weights):
    """
    @summary: Standardizes either a phylogenetic or environment matrix
    @param mtx: The matrix to be standardized
    @param weights: A one dimensional array of sums to use for standardization
    @note: Formula for standardization:
              Mstd = M - 1c.1r.W.M(1/trace(W)) ./ 1c(1r.W(M*M) 
                        - ((1r.W.M)*(1r.W.M))(1/trace(W))(1/trace(W)-1))^0.5
    @note: M - Matrix to be standardized
           W - A k by k diagonal matrix of weights, where each non-zero value 
                is the column or row sum (depending on the M) for a incidence 
                matrix
           1r - A row of k ones
           1c - A column of k ones
           trace - Returns the sum of the input matrix
    @note: "./" indicates Hadamard division
    @note: "*" indicates Hadamard multiplication
    @see: Literature supplemental materials
    @note: Code adopted from supplemental material MATLAB code
    @note: This function assumes that mtx and weights are Numpy arrays
    """
    # Create a row of ones, we'll transpose for a column
    ones = np.ones((1, weights.shape[0]), dtype=float)
    # This maps to trace(W)
    total_sum = np.sum(weights)
    
    # s1 = 1r.W.M
    s1 = (ones * weights).dot(mtx)
    # s2 = 1r.W.(M*M)
    s2 = (ones * weights).dot(mtx*mtx)
    
    mean_weighted = s1 / total_sum
    
    std_dev_weighted = ((s2 - (s1**2.0 / total_sum)) / (total_sum))**0.5
    
    # Fixes any invalid values created previously
    tmp = np.nan_to_num(ones.T.dot(std_dev_weighted)**-1.0)
    
    std_mtx = tmp * (mtx - ones.T.dot(mean_weighted))
    
    return std_mtx

# .............................................................................
def cj_func(phylo_column, purged_incidence, raw_env_predictors, raw_bg_predictors, 
                       num_env_predictors, num_bg_predictors, randomize=False):
    """
    I duplicated the MCPA work function because I was having a hard time with
    the ProcessPoolExecutor and just in time defined functions.
    
    TODO: Address this by combining or getting rid of the duplicate version
    """
    # Initialize numpy objects
    # Semi partial correlation column (observed only)
    env_semi_partial_col = np.zeros((num_env_predictors),
                                       dtype=float)
    bg_semi_partial_col = np.zeros((num_bg_predictors),
                                      dtype=float)
    
    # F semi partial matrices
    env_f_semi_partial_col = np.zeros((num_env_predictors),
                                         dtype=float)
    bg_f_semi_partial_col = np.zeros((num_bg_predictors),
                                        dtype=float)
    
    
    # TODO: Consider a logger
    # Get species present in clade
    species_present_at_node = np.where(phylo_column != 0)[0]
    
    if randomize:
        # Shuffle species around
        species_present_at_node = np.random.permutation(
            species_present_at_node)
    
    # Incidence full is a subset of the PAM where clade species are present  
    #incidence_full = pam.data[:,species_present_at_node]
    incidence = purged_incidence[:, species_present_at_node]
    
    sites_present = np.where(incidence.sum(axis=1) > 0)
    incidence = incidence[sites_present]
    
    env_predictors = raw_env_predictors[sites_present]
    bg_predictors = raw_bg_predictors[sites_present]
    
    # Get the number of sites and predictors from the shape of the 
    #     predictors matrices
    num_sites = env_predictors.shape[0]
    # Check that the number of sites matches in both predictor matrices
    assert env_predictors.shape[0] == bg_predictors.shape[0]
    
    # Get site weights
    site_weights = np.sum(incidence, axis=1)
    
    # Get species weights
    species_weights = np.sum(incidence, axis=0)
    
    # Standardize predictor matrices
    env_pred_std = _standardize_matrix(env_predictors, site_weights)
    bg_pred_std = _standardize_matrix(bg_predictors, site_weights)
    
    # Standardize P-matrix
    p_std = _standardize_matrix(
        phylo_column[species_present_at_node], 
        species_weights)
    # Get standardized P-Sigma
    p_sigma_std = np.dot(incidence, p_std)
    
    # Regression
    env_q, env_r = np.linalg.qr(
        (env_pred_std.T * site_weights).dot(env_pred_std))
    bg_q, bg_r = np.linalg.qr(
        (bg_pred_std.T * site_weights).dot(bg_pred_std))
    
    # r / q.T - Least squares
    env_r_div_q_transverse = np.linalg.lstsq(env_r, env_q.T)[0]
    bg_r_div_q_transverse = np.linalg.lstsq(bg_r, bg_q.T)[0]
    
    # H is Beta(all)
    # Note: I use predicted_calc to minimize the memory footprint so it 
    #           appears to deviate from the original code.  In reality, the 
    #           'H' variable is wrapped in the computation and the 
    #           resulting 'predicted' matrix is the same
    env_predicted = _predicted_calc(env_pred_std, env_r_div_q_transverse,
                                    site_weights, p_sigma_std)
    bg_predicted = _predicted_calc(bg_pred_std, bg_r_div_q_transverse,
                                   site_weights, p_sigma_std)
    
    env_total_p_sigma_residual = np.sum(
        (p_sigma_std - env_predicted).T.dot(p_sigma_std - env_predicted))
    bg_total_p_sigma_residual = np.sum(
        (p_sigma_std - bg_predicted).T.dot(p_sigma_std - bg_predicted))
    
    # Calculate R-squared values
    env_r_sq = 1.0 * np.sum(
        env_predicted.T.dot(env_predicted)) / np.sum(
            p_sigma_std.T.dot(p_sigma_std))
    bg_r_sq = 1.0 * np.sum(
        bg_predicted.T.dot(bg_predicted)) / np.sum(
            p_sigma_std.T.dot(p_sigma_std))
    
    # Calculate adjusted R-squared
    # (Classic method) Should be interpreted with some caution as the
    #     degrees of freedom for weighted models are different from
    #     non-weighted models adjustments based on effective degrees of 
    #     freedom should be considered
    try:
        env_adj_r_sq = 1.0 - (
            (num_sites - 1.0) / (num_sites - num_env_predictors - 1.0)) * (
                1.0 - env_r_sq)    
    except Exception as e:
        env_adj_r_sq = 0.0
        
    try:
        bg_adj_r_sq = 1.0 - ((num_sites - 1.0) / (
            num_sites - num_bg_predictors - 1.0)) * (1.0 - bg_r_sq)
    except Exception as e:
        bg_adj_r_sq = 0.0
    
    # F-global values
    env_f_global = np.sum(
        env_predicted.T.dot(env_predicted)) / env_total_p_sigma_residual
    bg_f_global = np.sum(
        bg_predicted.T.dot(bg_predicted)) / bg_total_p_sigma_residual
                                                            
    # Environmental predictors
    for i in xrange(num_env_predictors):
        print(' - Environment predictor {} of {}'.format(
            i+1, num_env_predictors))
        # Get the ith predictor, needs to be a column
        ith_predictor = env_predictors[:,i].reshape(
            env_predictors.shape[0], 1)
        # Get predictors without ith
        wo_ith_predictor = np.delete(env_predictors, i, axis=1)
        
        # Slope for ith predictor
        ith_q, ith_r = np.linalg.qr(
            (ith_predictor.T * site_weights).dot(ith_predictor))
        ith_r_div_q_transverse = np.linalg.lstsq(ith_r, ith_q.T)[0]
        ith_slope = (
            ith_r_div_q_transverse.dot(ith_predictor.T) * site_weights
            ).dot(p_sigma_std)
        
        # Regression for remaining predictors
        wo_ith_q, wo_ith_r = np.linalg.qr(
            (wo_ith_predictor.T * site_weights).dot(wo_ith_predictor))
        wo_ith_r_div_q_transverse = np.linalg.lstsq(wo_ith_r, 
                                                    wo_ith_q.T)[0]
        
        predicted = _predicted_calc(wo_ith_predictor, 
                                    wo_ith_r_div_q_transverse, 
                                    site_weights, p_sigma_std)
        # Get remaining r squared
        remaining_r_sq = np.sum(
            predicted.T.dot(predicted)) / np.sum(p_sigma_std.T.dot(
                p_sigma_std))
        
        # Calculate the semi-partial correlation
        try:
            env_semi_partial_col[i] = (
                ith_slope * sqrt(env_r_sq - remaining_r_sq)) / abs(
                    ith_slope)
        except ValueError as e: # Square root of a negative
            env_semi_partial_col[i] = 0.0
            
        # Calculate F semi-partial
        env_f_semi_partial_col[i] = (
            env_r_sq - remaining_r_sq) / env_total_p_sigma_residual
    
    all_predictors = np.concatenate((bg_predictors, env_predictors), 
                                    axis=1)
    
    # Biogeo predictors
    for i in xrange(num_bg_predictors):
        print(' - Biogeographic hypothesis {} of {}'.format(
            i+1, num_bg_predictors))
        # Get the ith predictor, needs to be a column
        ith_predictor = all_predictors[:,i].reshape(
            all_predictors.shape[0], 1)
        # Get predictors without ith
        wo_ith_predictor = np.delete(all_predictors, i, axis=1)
        
        # Slope for ith predictor
        ith_q, ith_r = np.linalg.qr((ith_predictor.T * site_weights).dot(
            ith_predictor))
        ith_r_div_q_transverse = np.linalg.lstsq(ith_r, ith_q.T)[0]
        ith_slope = (
            ith_r_div_q_transverse.dot(ith_predictor.T) * site_weights
            ).dot(p_sigma_std)
        
        # Regression for remaining predictors
        wo_ith_q, wo_ith_r = np.linalg.qr(
            (wo_ith_predictor.T * site_weights).dot(wo_ith_predictor))
        wo_ith_r_div_q_transverse = np.linalg.lstsq(
            wo_ith_r, wo_ith_q.T)[0]
        
        predicted = _predicted_calc(wo_ith_predictor,
                                    wo_ith_r_div_q_transverse,
                                    site_weights, p_sigma_std)
        # Get remaining r squared
        remaining_r_sq = np.sum(
            predicted.T.dot(predicted)) / np.sum(p_sigma_std.T.dot(
                p_sigma_std))
        
        # Calculate the semi-partial correlation
        try:
            bg_semi_partial_col[i] = (
                ith_slope * sqrt(bg_r_sq - remaining_r_sq)) / abs(
                    ith_slope)
        except ValueError as e: # Square root of a negative
            bg_semi_partial_col[i] = 0.0
            
        # Calculate F semi-partial
        bg_f_semi_partial_col[i] = (
            bg_r_sq - remaining_r_sq) / bg_total_p_sigma_residual
            
    return (env_adj_r_sq, bg_adj_r_sq, env_f_global, bg_f_global, 
            env_semi_partial_col, bg_semi_partial_col, 
            env_f_semi_partial_col, bg_f_semi_partial_col)


# .............................................................................
def get_node_mcpa_func(purged_incidence, raw_env_predictors, raw_bg_predictors, 
                       num_env_predictors, num_bg_predictors, randomize=False):
    """Get the MCPA function for nodes
    """
    
    def node_mcpa_func(phylo_column):
        """
        phylo_column is a column in the phylo matrix for the node to be processed
                it should be a numpy data structure
        """
        # Initialize numpy objects
        # Semi partial correlation column (observed only)
        env_semi_partial_col = np.zeros((num_env_predictors),
                                           dtype=float)
        bg_semi_partial_col = np.zeros((num_bg_predictors),
                                          dtype=float)
        
        # F semi partial matrices
        env_f_semi_partial_col = np.zeros((num_env_predictors),
                                             dtype=float)
        bg_f_semi_partial_col = np.zeros((num_bg_predictors),
                                            dtype=float)
        
        
        # TODO: Consider a logger
        # Get species present in clade
        species_present_at_node = np.where(phylo_column != 0)[0]
        
        if randomize:
            # Shuffle species around
            species_present_at_node = np.random.permutation(
                species_present_at_node)
        
        # Incidence full is a subset of the PAM where clade species are present  
        #incidence_full = pam.data[:,species_present_at_node]
        incidence = purged_incidence[:, species_present_at_node]
        
        sites_present = np.where(incidence.sum(axis=1) > 0)
        incidence = incidence[sites_present]
        
        env_predictors = raw_env_predictors[sites_present]
        bg_predictors = raw_bg_predictors[sites_present]
        
        # Get the number of sites and predictors from the shape of the 
        #     predictors matrices
        num_sites = env_predictors.shape[0]
        # Check that the number of sites matches in both predictor matrices
        assert env_predictors.shape[0] == bg_predictors.shape[0]
        
        # Get site weights
        site_weights = np.sum(incidence, axis=1)
        
        # Get species weights
        species_weights = np.sum(incidence, axis=0)
        
        # Standardize predictor matrices
        env_pred_std = _standardize_matrix(env_predictors, site_weights)
        bg_pred_std = _standardize_matrix(bg_predictors, site_weights)
        
        # Standardize P-matrix
        p_std = _standardize_matrix(
            phylo_column[species_present_at_node], 
            species_weights)
        # Get standardized P-Sigma
        p_sigma_std = np.dot(incidence, p_std)
        
        # Regression
        env_q, env_r = np.linalg.qr(
            (env_pred_std.T * site_weights).dot(env_pred_std))
        bg_q, bg_r = np.linalg.qr(
            (bg_pred_std.T * site_weights).dot(bg_pred_std))
        
        # r / q.T - Least squares
        env_r_div_q_transverse = np.linalg.lstsq(env_r, env_q.T)[0]
        bg_r_div_q_transverse = np.linalg.lstsq(bg_r, bg_q.T)[0]
        
        # H is Beta(all)
        # Note: I use predicted_calc to minimize the memory footprint so it 
        #           appears to deviate from the original code.  In reality, the 
        #           'H' variable is wrapped in the computation and the 
        #           resulting 'predicted' matrix is the same
        env_predicted = _predicted_calc(env_pred_std, env_r_div_q_transverse,
                                        site_weights, p_sigma_std)
        bg_predicted = _predicted_calc(bg_pred_std, bg_r_div_q_transverse,
                                       site_weights, p_sigma_std)
        
        env_total_p_sigma_residual = np.sum(
            (p_sigma_std - env_predicted).T.dot(p_sigma_std - env_predicted))
        bg_total_p_sigma_residual = np.sum(
            (p_sigma_std - bg_predicted).T.dot(p_sigma_std - bg_predicted))
        
        # Calculate R-squared values
        env_r_sq = 1.0 * np.sum(
            env_predicted.T.dot(env_predicted)) / np.sum(
                p_sigma_std.T.dot(p_sigma_std))
        bg_r_sq = 1.0 * np.sum(
            bg_predicted.T.dot(bg_predicted)) / np.sum(
                p_sigma_std.T.dot(p_sigma_std))
        
        # Calculate adjusted R-squared
        # (Classic method) Should be interpreted with some caution as the
        #     degrees of freedom for weighted models are different from
        #     non-weighted models adjustments based on effective degrees of 
        #     freedom should be considered
        try:
            env_adj_r_sq = 1.0 - (
                (num_sites - 1.0) / (num_sites - num_env_predictors - 1.0)) * (
                    1.0 - env_r_sq)    
        except Exception as e:
            env_adj_r_sq = 0.0
            
        try:
            bg_adj_r_sq = 1.0 - ((num_sites - 1.0) / (
                num_sites - num_bg_predictors - 1.0)) * (1.0 - bg_r_sq)
        except Exception as e:
            bg_adj_r_sq = 0.0
        
        # F-global values
        env_f_global = np.sum(
            env_predicted.T.dot(env_predicted)) / env_total_p_sigma_residual
        bg_f_global = np.sum(
            bg_predicted.T.dot(bg_predicted)) / bg_total_p_sigma_residual
                                                                
        # Environmental predictors
        for i in xrange(num_env_predictors):
            print(' - Environment predictor {} of {}'.format(
                i+1, num_env_predictors))
            # Get the ith predictor, needs to be a column
            ith_predictor = env_predictors[:,i].reshape(
                env_predictors.shape[0], 1)
            # Get predictors without ith
            wo_ith_predictor = np.delete(env_predictors, i, axis=1)
            
            # Slope for ith predictor
            ith_q, ith_r = np.linalg.qr(
                (ith_predictor.T * site_weights).dot(ith_predictor))
            ith_r_div_q_transverse = np.linalg.lstsq(ith_r, ith_q.T)[0]
            ith_slope = (
                ith_r_div_q_transverse.dot(ith_predictor.T) * site_weights
                ).dot(p_sigma_std)
            
            # Regression for remaining predictors
            wo_ith_q, wo_ith_r = np.linalg.qr(
                (wo_ith_predictor.T * site_weights).dot(wo_ith_predictor))
            wo_ith_r_div_q_transverse = np.linalg.lstsq(wo_ith_r, 
                                                        wo_ith_q.T)[0]
            
            predicted = _predicted_calc(wo_ith_predictor, 
                                        wo_ith_r_div_q_transverse, 
                                        site_weights, p_sigma_std)
            # Get remaining r squared
            remaining_r_sq = np.sum(
                predicted.T.dot(predicted)) / np.sum(p_sigma_std.T.dot(
                    p_sigma_std))
            
            # Calculate the semi-partial correlation
            try:
                env_semi_partial_col[i] = (
                    ith_slope * sqrt(env_r_sq - remaining_r_sq)) / abs(
                        ith_slope)
            except ValueError as e: # Square root of a negative
                env_semi_partial_col[i] = 0.0
                
            # Calculate F semi-partial
            env_f_semi_partial_col[i] = (
                env_r_sq - remaining_r_sq) / env_total_p_sigma_residual
        
        all_predictors = np.concatenate((bg_predictors, env_predictors), 
                                        axis=1)
        
        # Biogeo predictors
        for i in xrange(num_bg_predictors):
            print(' - Biogeographic hypothesis {} of {}'.format(
                i+1, num_bg_predictors))
            # Get the ith predictor, needs to be a column
            ith_predictor = all_predictors[:,i].reshape(
                all_predictors.shape[0], 1)
            # Get predictors without ith
            wo_ith_predictor = np.delete(all_predictors, i, axis=1)
            
            # Slope for ith predictor
            ith_q, ith_r = np.linalg.qr((ith_predictor.T * site_weights).dot(
                ith_predictor))
            ith_r_div_q_transverse = np.linalg.lstsq(ith_r, ith_q.T)[0]
            ith_slope = (
                ith_r_div_q_transverse.dot(ith_predictor.T) * site_weights
                ).dot(p_sigma_std)
            
            # Regression for remaining predictors
            wo_ith_q, wo_ith_r = np.linalg.qr(
                (wo_ith_predictor.T * site_weights).dot(wo_ith_predictor))
            wo_ith_r_div_q_transverse = np.linalg.lstsq(
                wo_ith_r, wo_ith_q.T)[0]
            
            predicted = _predicted_calc(wo_ith_predictor,
                                        wo_ith_r_div_q_transverse,
                                        site_weights, p_sigma_std)
            # Get remaining r squared
            remaining_r_sq = np.sum(
                predicted.T.dot(predicted)) / np.sum(p_sigma_std.T.dot(
                    p_sigma_std))
            
            # Calculate the semi-partial correlation
            try:
                bg_semi_partial_col[i] = (
                    ith_slope * sqrt(bg_r_sq - remaining_r_sq)) / abs(
                        ith_slope)
            except ValueError as e: # Square root of a negative
                bg_semi_partial_col[i] = 0.0
                
            # Calculate F semi-partial
            bg_f_semi_partial_col[i] = (
                bg_r_sq - remaining_r_sq) / bg_total_p_sigma_residual
                
        return (env_adj_r_sq, bg_adj_r_sq, env_f_global, bg_f_global, 
                env_semi_partial_col, bg_semi_partial_col, 
                env_f_semi_partial_col, bg_f_semi_partial_col)
    return node_mcpa_func

# .............................................................................
def get_p_values(observed_value, test_values, num_permutations=None):
    """
    @summary: Gets an (1 or 2 dimension) array of P values where the P value
                for an array location is determined by finding the number of
                test values at corresponding locations are greater than or
                equal to that value and then dividing that number by the number
                of permutations
    @param observed_value: An array of observed values to use as a reference
    @param test_values: A list of arrays generated from randomizations that
                            will be compared to the observed
    @param num_permutations: (optional) The total number of randomizations
                                performed.  Divide the P-values by this if
                                provided
    @note: This method assumes that the inputs are Matrix objects and not
            plain Numpy arrays
    """
    # Create the P-Values matrix
    p_vals = np.zeros(observed_value.data.shape, dtype=float)
    # For each matrix in test values
    for test_mtx in test_values:
        # Add 1 where every value in the test matrix is greater than or equal
        #    to the value in the observed value.  Numpy comparisons will create
        #    a matrix of boolean values for each cell, which when added to the
        #    p_vals matrix will be treated as 1 for True and 0 for False

        # If this is a stack
        if test_mtx.data.ndim == 3:
            for i in range(test_mtx.data.shape[2]):
                p_vals += np.abs(np.round(test_mtx.data[:,:,i], 5)
                                 ) >= np.abs(np.round(observed_value.data, 5))
        else:
            p_vals += np.abs(np.round(test_mtx.data, 5)
                             ) >= np.abs(np.round(observed_value.data, 5))
    # Scale and return the pVals matrix
    if num_permutations:
        return Matrix(p_vals / num_permutations, 
                      headers=observed_value.headers)
    else:
        return Matrix(p_vals, headers=observed_value.headers)

# .............................................................................
def mcpa_run(pam, phylo_matrix, env_matrix, biogeo_matrix, randomize=False):
    """
    @summary: Perform a sing MCPA run with the input matrices producing a node
                by predictor matrix of semi partial correlations with adjusted
                R-squared value columns between
    @param pam: The PAM to use for this run (also referred to as an incidence
                    matrix).  This is the entire PAM and not a subset.
    @param phylo_matrix: A species by clade (node) matrix encoded from a 
                            phylogenetic tree
    @param env_matrix: A matrix of site by environmental predictors.  The cell 
                            values are the encoding of the environment data at 
                            each location
    @param biogeo_matrix: A matrix of site by biogeographic hypothesis 
                            predictors where the value at each cell is the 
                            Helmert contrast encoding of the particular 
                            biogeographic hypothesis at that site
    @note: All inputs should be LmCommon.common.matrix.Matrix objects
    
    @todo: Do we need to randomize the phylo matrix too?
    @todo: Metadata to outputs
    @todo: Move randomization logic out of this method, or separate into 
                 observed and random
    """
    # Initialize
    num_env_predictors = env_matrix.data.shape[1]
    num_bg_predictors = biogeo_matrix.data.shape[1]
    num_nodes = phylo_matrix.data.shape[1]
    
    # Adjusted R-squared columns (observed only)
    env_adj_r_sq = np.zeros((num_nodes, 1), dtype=float)
    bg_adj_r_sq = np.zeros((num_nodes, 1), dtype=float)
    
    # F-global matries
    env_f_global = np.zeros((num_nodes, 1), dtype=float)
    bg_f_global = np.zeros((num_nodes, 1), dtype=float)
    
    # Semi partial correlation matrices (observed only)
    env_semi_partial_matrix = np.zeros((num_nodes, num_env_predictors),
                                       dtype=float)
    bg_semi_partial_matrix = np.zeros((num_nodes, num_bg_predictors),
                                      dtype=float)
    
    # F semi partial matrices
    env_f_semi_partial_matrix = np.zeros((num_nodes, num_env_predictors),
                                         dtype=float)
    bg_f_semi_partial_matrix = np.zeros((num_nodes, num_bg_predictors),
                                        dtype=float)
    
    # Set up the initial incidence matrix and randomize if requested
    init_incidence = pam.data
    if randomize:
        init_incidence = np.random.permutation(init_incidence)
    
    site_present = np.any(init_incidence, axis=1)
    empty_sites = np.where(site_present == False)[0]
    purged_incidence = np.delete(init_incidence, empty_sites, axis=0)
    
    # Remove sites from the predictor matrices
    env_predictors = np.delete(env_matrix.data, empty_sites, axis=0)
    bg_predictors = np.delete(biogeo_matrix.data, empty_sites, axis=0)

    func = get_node_mcpa_func(purged_incidence, env_predictors, bg_predictors, 
                              num_env_predictors, num_bg_predictors, randomize)

    ret = []

    #with ThreadPoolExecutor(NUM_THREADS) as executor:
    with ProcessPoolExecutor(NUM_THREADS) as executor:
        for col in phylo_matrix.data.T:
            ret.append(
                executor.submit(cj_func, col, purged_incidence, 
                                env_predictors, bg_predictors, 
                              num_env_predictors, num_bg_predictors, randomize))
        #ret = executor.map(func, phylo_matrix.data.T)

        i = 0
        for (col_env_adj_r_sq, col_bg_adj_r_sq, col_env_f_global, col_bg_f_global, 
             col_env_semi_partial, col_bg_semi_partial, col_env_f_semi_partial, 
             col_bg_f_semi_partial) in [t.result() for t in ret]:
            
            env_adj_r_sq[i, 0] = col_env_adj_r_sq
            bg_adj_r_sq[i, 0] = col_bg_adj_r_sq
            env_f_global[i, 0] = col_env_f_global
            bg_f_global[i, 0] = col_bg_f_global
            env_semi_partial_matrix[i] = col_env_semi_partial
            bg_semi_partial_matrix[i] = col_bg_semi_partial
            env_f_semi_partial_matrix[i] = col_env_f_semi_partial
            bg_f_semi_partial_matrix[i] = col_bg_f_semi_partial
            i += 1
        


    
    # Observed values matrix
    obs_data = np.nan_to_num(np.concatenate([env_semi_partial_matrix, 
                                             env_adj_r_sq, 
                                             bg_semi_partial_matrix, 
                                             bg_adj_r_sq], axis=1))
    obs_col_headers = env_matrix.getColumnHeaders()
    obs_col_headers.append('Env - Adjusted R-squared')
    obs_col_headers.extend(biogeo_matrix.getColumnHeaders())
    obs_col_headers.append('BG - Adjusted R-squared')
    
    obs_matrix = Matrix(obs_data, 
                              headers={'0': phylo_matrix.getColumnHeaders(), 
                                       '1': obs_col_headers})

    # F values matrix
    f_data = np.nan_to_num(np.concatenate([env_f_semi_partial_matrix,
                                           env_f_global,
                                           bg_f_semi_partial_matrix,
                                           bg_f_global], axis=1))
    f_col_headers = env_matrix.getColumnHeaders()
    f_col_headers.append('Env - F-Global')
    f_col_headers.extend(biogeo_matrix.getColumnHeaders())
    f_col_headers.append('BG - F-Global')
    
    f_matrix = Matrix(f_data, headers={'0': phylo_matrix.getColumnHeaders(),
                                       '1': f_col_headers})
    return obs_matrix, f_matrix
