"""Module for performing a MetaCommunity Phylogenetics Analysis

See:
    Leibold, M.A., E.P. Economo and P.R. Peres-Neto. 2010. Metacommunity
         phylogenetics: separating the roles of environmental filters and 
         historical biogeography. Ecology letters 13: 1290-1299.
"""
from concurrent.futures import ThreadPoolExecutor as ExecutorClass
from functools import partial
import numpy as np
import threading

from LmCommon.common.matrix import Matrix

# Note: This lock is used when calculating beta to keep memory usage down
lock = threading.Lock()

# Note: This will depend on the executor class used and is subject to tuning.
#    Threads should have a higher level of concurrency than processes.
CONCURRENCY_FACTOR = 5

# .............................................................................
def _calculate_beta(pred_std, weights, phylo_std, use_lock=True):
    """Calculates the regression model (beta) for the provided inputs

    Args:
        pred_std (numpy array): A standardized predictor matrix 
            (n [sites] by i [predictors]).
        weights (numpy array): A matrix of site weights (n by n).
        phylo_std (numpy array): A standardized phylo matrix (n by k [nodes]).
        use_lock (boolean): If true, use a lock when performing the computation
            to save on overall memory usage.  This is useful for large, or
            full, predictor matrices but can be skipped for single columns to
            improve performance.

    Note:
        * The computation is::
            beta = (M_T.W.M)^-1.M_T.W.P
        * M is the predictor matrix
        * M_T is the transverse of the predictor matrix
        * W is the weights column
        * P is the phylo matrix
        * "^-1" is the inverse of the matrix
        * Locking is available to prevent many threads / subprocesses from
            performing memory intensive computations concurrently and
            overwhelming the system.

    Todo:
        * Update documentation so that note shows symbols in equation

    Returns:
        * An (i by k) numpy ndarray, where i is the number of predictors in
            pred_std and k is the number of nodes in phylo_std.
    """
    if use_lock:
        lock.acquire()
    tmp = pred_std.T.dot(np.diag(weights))
    tmp2 = tmp.dot(pred_std)
    tmp3 = np.linalg.inv(tmp2)
    tmp4 = tmp3.dot(tmp)
    beta = tmp4.dot(phylo_std)
    if len(beta.shape) == 1:
        beta = beta.reshape((beta.shape[0], 1))
    if use_lock:
        lock.release()
    return beta

# .............................................................................
def _calculate_r_squared(y_hat, phylo_std):
    """Calculates the R-squared value for the inputs

    Args:
        y_hat (numpy array): A predicted value for a regression
            (n [sites] by k [nodes]).
        phylo_std (numpy array): A standardized phylo matrix
            (n [sites] by k [nodes]).

    Note:
        * R^2 = trace(y_hat . y_hat) / trace(P . P_T)
        * y_hat_T is the transverse of y_hat
        * P_T is the transverse of phylo_std
    """
    #t1 = np.trace(y_hat.dot(y_hat.T))
    t1 = _trace_mtx_by_transverse(y_hat)
    #t2 = np.trace(phylo_std.dot(phylo_std.T))
    t2 = _trace_mtx_by_transverse(phylo_std)
    r2 = t1 / t2
    #r2 = np.trace(y_hat.T.dot(y_hat)) / np.trace(phylo_std.T.dot(phylo_std.T))
    return r2

# .............................................................................
def _calculate_y_hat(pred_std, beta):
    """Calculates the predicted value for a regression (Y hat)

    pred_std (numpy array): A standardized predictor matrix 
        (n [sites] by i [predictors]).
    beta (numpy array): The regression model associated with this prediction
        (i by k [nodes]).

    Returns:
        * An (n [sites] by k [nodes]) numpy ndarray
    """
    return pred_std.dot(beta)

# .............................................................................
def _mcpa_for_node(incidence_mtx, env_mtx, bg_mtx, phylo_col, use_locks=False):
    """Runs MCPA computations for a single tree node.

    Args:
        incidence_mtx (numpy array): An incidence matrix (PAM).
        env_mtx (numpy array): An environmental matrix (GRIM).
        bg_mtx (numpy array): A matrix of encoded Biogeographic hypotheses.
        phylo_col (numpy array): A column from the phylo matrix for a
            single node.
        use_locks (boolean): Indicator if locks are needed for larger
            computations.  This is probably only true for parallel runs.
    """
    species_present_at_node = np.where(phylo_col != 0)[0]
    phylo_col = phylo_col[species_present_at_node, :]
    
    # Purge incidence matrix to only those species present for this node
    incidence_temp = incidence_mtx[:, species_present_at_node]
    
    # Get the sites present for this node and purge the other matrices
    sites_present = np.where(incidence_temp.sum(axis=1) > 0)
    incidence = incidence_temp[sites_present]
    env_predictors = env_mtx[sites_present]
    bg_predictors = bg_mtx[sites_present]

    # Get the number of sites and check that all matrices have been purged
    num_sites = sites_present[0].size
    num_env_predictors = env_predictors.shape[1]
    num_bg_predictors = bg_predictors.shape[1]
    
    obs_values = np.zeros((1, num_env_predictors + num_bg_predictors + 2))
    f_values = np.zeros((1, num_env_predictors + num_bg_predictors + 2))

    try:
        if len(sites_present) > 0:
            # Standardize matrices
            site_weights = np.sum(incidence, axis=1)
            species_weights = np.sum(incidence, axis=0)
        
            e_std = _standardize_matrix(env_predictors, site_weights)
            b_std = _standardize_matrix(bg_predictors, site_weights)
            all_std = np.concatenate([b_std, e_std], axis=1)
            p_std = _standardize_matrix(phylo_col, species_weights)
            p_sigma_std = np.dot(incidence, p_std)
            # Get Beta, Y(hat), Rho, R-squared, F-pseudo
            beta_env_all = _calculate_beta(e_std, site_weights, p_sigma_std,
                                           use_lock=use_locks)
            y_hat_env_all = _calculate_y_hat(e_std, beta_env_all)
            beta_bg_all = _calculate_beta(all_std, site_weights, p_sigma_std,
                                          use_lock=use_locks)
            y_hat_bg_all = _calculate_y_hat(all_std, beta_bg_all)
            env_r2 = _calculate_r_squared(y_hat_env_all, p_sigma_std)
            bg_r2 = _calculate_r_squared(y_hat_bg_all, p_sigma_std)
            env_adj_r2 = 1.0 - ((num_sites - 1.0) / (
                num_sites - num_env_predictors - 1.0)) * (1.0 - env_r2)
            bg_adj_r2 = 1.0 - ((num_sites - 1.0) / (
                num_sites - num_bg_predictors - 1.0)) * (1.0 - bg_r2)
        
            #env_f_pseudo_numerator = np.trace(y_hat_env_all.T.dot(y_hat_env_all))
            env_f_pseudo_numerator = _trace_mtx_by_transverse(y_hat_env_all.T)
            #bg_f_pseudo_numerator = np.trace(y_hat_bg_all.T.dot(y_hat_bg_all))
            bg_f_pseudo_numerator = _trace_mtx_by_transverse(y_hat_bg_all.T)
        
            # Pre-calculate the denominator for the F-pseudo stats
            env_denom_temp = p_sigma_std.T - y_hat_env_all.T
            #env_f_pseudo_denominator = np.trace(env_denom_temp.T.dot(env_denom_temp))
            env_f_pseudo_denominator = _trace_mtx_by_transverse(env_denom_temp.T)
            bg_denom_temp = p_sigma_std.T - y_hat_bg_all.T
            #bg_f_pseudo_denominator = np.trace(bg_denom_temp.T.dot(bg_denom_temp))
            bg_f_pseudo_denominator = _trace_mtx_by_transverse(bg_denom_temp.T)
        
            x = 0
            # Environment
            # For each environmental predictor, compute the semi-partial correlation
            #    and the F-pseudo value
            for i in range(num_env_predictors):
                wo_predictor = np.delete(e_std, i, axis=1)
                predictor = e_std[:,[i]]
                
                # Semi-partial correlation
                beta_wo_pred = _calculate_beta(
                    wo_predictor, site_weights, p_sigma_std, use_lock=use_locks)
                y_hat_wo_pred = _calculate_y_hat(wo_predictor, beta_wo_pred)
                beta_j_i = _calculate_beta(
                    predictor, site_weights, p_sigma_std, use_lock=False)
                r2_j_i = _calculate_r_squared(y_hat_wo_pred, p_sigma_std)
                sp = beta_j_i * np.sqrt(env_r2 - r2_j_i) / np.abs(beta_j_i)
                f_pseudo_env_i = (env_r2 - r2_j_i) / env_f_pseudo_denominator
                obs_values[0, x] = sp
                f_values[0, x] = f_pseudo_env_i
                x += 1
            # Add Environment adjusted R squared
            obs_values[0, x] = env_adj_r2
            f_values[0, x] = env_f_pseudo_numerator / env_f_pseudo_denominator
            x += 1
        
            # Biogeography
            for i in range(num_bg_predictors):
                wo_predictor = np.delete(all_std, i, axis=1)
                predictor = all_std[:,[i]]
                
                # Semi-partial correlation
                beta_wo_pred = _calculate_beta(
                    wo_predictor, site_weights, p_sigma_std, use_lock=use_locks)
                y_hat_wo_pred = _calculate_y_hat(wo_predictor, beta_wo_pred)
                beta_j_i = _calculate_beta(
                    predictor, site_weights, p_sigma_std, use_lock=False)
                r2_j_i = _calculate_r_squared(y_hat_wo_pred, p_sigma_std)
                sp = beta_j_i * np.sqrt(bg_r2 - r2_j_i) / np.abs(beta_j_i)
                f_pseudo_bg_i = (bg_r2 - r2_j_i) / bg_f_pseudo_denominator
                obs_values[0, x] = sp
                f_values[0, x] = f_pseudo_bg_i
                x += 1
            # Add Biogeography adjusted R squared
            obs_values[0, x] = bg_adj_r2
            f_values[0, x] = bg_f_pseudo_numerator / bg_f_pseudo_denominator
    except np.linalg.linalg.LinAlgError:
        # Singular matrix that does not have inverse
        pass
    
    return (obs_values, f_values)

# .............................................................................
def _standardize_matrix(mtx, weights):
    """Standardizes a phylogenetic or predictor matrix

    Args:
        mtx (numpy array): The matrix to standardize
        weights (numpy array): A one-dimensional array of sums to use for
            standardization.

    Note:
        * Formula for standardization ::
            Mstd = M - 1c.1r.W.M(1/trace(W)) ./ 1c(1r.W(M*M)
                       - ((1r.W.M)*(1r.W.M)(1/trace(W))(1/trace(W)-1))^0.5
        * M - Matrix to be standardized
        * W - A k by k diagonal matrix of weights, where each non-zero value is
            the column or row sum (depending on the M) for an incidence matrix.
        * 1r - A row of k ones
        * 1c - A column of k ones
        * trace - Returns the sum of the input matrix
        * "./" indicates Hadamard division
        * "*" indicates Hadamard multiplication
        * Code adopted from supplemental material MATLAB code

    See:
        Literature supplemental materials
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
def _trace_mtx_by_transverse(mtx):
    """Prevent a memory bomb by performing trace(mtx . mtx.T) in a smarter way.

    This method takes advantage of the fact that we are really only interested
        in the diagonal matrix created by performing a dot product of a matrix
        and its transverse.  Because of that, we can perform the dot product of
        each row by its transverse and then sum the results.

    Args:
        mtx (numpy array): A matrix to use to calculate the trace(M . M_T).

    Note:
        * There was an error in the formula that called for a dot product of
            each matrix transverse with itself.  This was a typo as that would
            only work with square matrices.
    """
    return np.sum([row.dot(row.T) for row in mtx])

# .............................................................................
def get_p_values(observed_value, test_values, num_permutations=None):
    """Gets an array of P-Values

    Gets an (1 or 2 dimension) array of P values where the P value for an array
        location is determined by finding the number of test values at 
        corresponding locations are greater than or equal to that value and
        then dividing that number by the number of permutations

    Args:
        observed_value (Matrix): An array of observed values to use as a
            reference.
        test_values (Matrix): A list of arrays generated from randomizations
            that will be compared to the observed
        num_permutations: (optional) The total number of randomizations 
            performed.  Divide the P-values by this if provided.
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
                p_vals += np.abs(np.round(test_mtx.data[:,:,[i]], 5)
                                 ) >= np.abs(np.round(observed_value.data, 5))
        else:
            p_vals += np.abs(np.round(test_mtx.data, 5)
                             ) >= np.abs(np.round(observed_value.data, 5))
    # Reshape and adding depth header
    if len(p_vals.shape) == 2:
        p_vals = np.expand_dims(p_vals, axis=2)
    headers = observed_value.headers
    headers['2'] = ['P-values']
    # Scale and return the pVals matrix
    if num_permutations:
        return Matrix(p_vals / num_permutations, 
                      headers=headers)
    else:
        return Matrix(p_vals, headers=headers)

# .............................................................................
def mcpa(incidence_matrix, phylo_mtx, env_mtx, bg_mtx):
    """Runs MCPA for a set of matrices.

    Args:
        incidence_matrix (Matrix): A binary Lifemapper Matrix object
            representing the incidence of each species for each site by coding
            them as ones.  This is the same thing as a Lifemapper Presence
            Absence Matrix, or PAM (n [sites] by k+1 [species]).
        phylo_mtx (Matrix): A matrix encoding of a phylogenetic tree where each
            cell represents the relative contribution of each tip to each
            inner tree node (k+1 [species] by k [nodes]).
        env_mtx (Matrix): A matrix encoding of the environment for each site
            (n [sites] by ei [environmental predictors]).
        bg_mtx (Matirx): A matrix of Helmert contrasts (-1, 0, 1) for
            Biogeographic hypotheses (n [sites] by bi [biogeographic 
            predictors]).

    Returns:
        * A Matrix object representing the observed values from the
            calculation.
        * A Matrix object representing the F-pseudo values from the
            calculation.
    """
    site_present = np.any(incidence_matrix.data, axis=1)
    empty_sites = np.where(site_present == False)[0]
    
    # Initial purge of empty sites
    init_incidence = np.delete(incidence_matrix.data, empty_sites, axis=0)
    env_predictors = np.delete(env_mtx.data, empty_sites, axis=0)
    bg_predictors = np.delete(bg_mtx.data, empty_sites, axis=0)
    
    num_nodes = phylo_mtx.data.shape[1]
    num_predictors = env_predictors.shape[1] + bg_predictors.shape[1]
    
    obs_results = np.empty((num_nodes, num_predictors + 2))
    f_results = np.empty((num_nodes, num_predictors + 2))
    for i in range(num_nodes):
        print('Node {} of {}'.format(i+1, num_nodes))
        obs, f_vals = _mcpa_for_node(
            init_incidence, env_predictors, bg_predictors,
            phylo_mtx.data[:, [i]])
        obs_results[i] = obs
        f_results[i] = f_vals

    # Correct any nans and add depth
    obs_results = np.expand_dims(np.nan_to_num(obs_results), axis=2)
    f_results = np.expand_dims(np.nan_to_num(f_results), axis=2)

    column_headers = env_mtx.getColumnHeaders()
    column_headers.append('Env - Adjusted R-squared')
    column_headers.extend(bg_mtx.getColumnHeaders())
    column_headers.append('BG - Adjusted R-squared')
    obs_headers = {
        '0' : phylo_mtx.getColumnHeaders(),
        '1' : column_headers,
        '2' : ['Observed']
    }
    f_headers = {
        '0' : phylo_mtx.getColumnHeaders(),
        '1' : column_headers,
        '2' : ['F-values']
    }
    obs_mtx = Matrix(obs_results, headers=obs_headers)
    f_mtx = Matrix(f_results, headers=f_headers)
    return (obs_mtx, f_mtx)

# .............................................................................
def mcpa_parallel(incidence_matrix, phylo_mtx, env_mtx, bg_mtx):
    """Run MCPA for a set of matrices using parallelism.

    Performs MCPA across each of the tree nodes in parallel.

    Args:
        incidence_matrix (Matrix): A binary Lifemapper Matrix object
            representing the incidence of each species for each site by coding
            them as ones.  This is the same thing as a Lifemapper Presence
            Absence Matrix, or PAM (n [sites] by k+1 [species]).
        phylo_mtx (Matrix): A matrix encoding of a phylogenetic tree where each
            cell represents the relative contribution of each tip to each
            inner tree node (k+1 [species] by k [nodes]).
        env_mtx (Matrix): A matrix encoding of the environment for each site
            (n [sites] by ei [environmental predictors]).
        bg_mtx (Matirx): A matrix of Helmert contrasts (-1, 0, 1) for
            Biogeographic hypotheses (n [sites] by bi [biogeographic 
            predictors]).

    Returns:
        * A Matrix object representing the observed values from the
            calculation.
        * A Matrix object representing the F-pseudo values from the
            calculation.
    """
    site_present = np.any(incidence_matrix.data, axis=1)
    empty_sites = np.where(site_present == False)[0]
    
    # Initial purge of empty sites
    init_incidence = np.delete(incidence_matrix.data, empty_sites, axis=0)
    env_predictors = np.delete(env_mtx.data, empty_sites, axis=0)
    bg_predictors = np.delete(bg_mtx.data, empty_sites, axis=0)
    
    num_nodes = phylo_mtx.data.shape[1]
    num_predictors = env_predictors.shape[1] + bg_predictors.shape[1]
    
    obs_results = np.empty((num_nodes, num_predictors + 2))
    f_results = np.empty((num_nodes, num_predictors + 2))
    
    func = partial(
        _mcpa_for_node, init_incidence, env_predictors, bg_predictors,
        use_locks=True)
    
    # Use an Executor to parallelize the computations over each tree node
    # Note: The executor class is determined at the module level, so see top of
    #    module for more information about executor class and concurrency
    with ExecutorClass(CONCURRENCY_FACTOR) as executor:
        r = executor.map(
            func, [phylo_mtx.data[:, [i]] for i in range(num_nodes)])
        i = 0
        for obs, f in r:
            obs_results[i] = obs
            f_results[i] = f

    # Correct any nans
    obs_results = np.nan_to_num(obs_results)
    f_results = np.nan_to_num(f_results)

    column_headers = env_mtx.getColumnHeaders()
    column_headers.append('Env - Adjusted R-squared')
    column_headers.extend(bg_mtx.getColumnHeaders())
    column_headers.append('BG - Adjusted R-squared')
    obs_headers = {
        '0' : phylo_mtx.getColumnHeaders(),
        '1' : column_headers,
        '2' : ['Observed']
    }
    f_headers = {
        '0' : phylo_mtx.getColumnHeaders(),
        '1' : column_headers,
        '2' : ['F-values']
    }
    obs_mtx = Matrix(obs_results, headers=obs_headers)
    f_mtx = Matrix(f_results, headers=f_headers)
    return (obs_mtx, f_mtx)
