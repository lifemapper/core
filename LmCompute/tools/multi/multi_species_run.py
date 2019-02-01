#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script performs a multi-species analysis for a PAM

Todo:
    * Enable alternate randomization methods
    * Enable Phylo randomization
"""
import argparse

import dendropy

from LmCommon.common.matrix import Matrix

from LmCompute.plugins.multi.calculate.calculate import PamStats
from LmCompute.plugins.multi.mcpa.mcpa import mcpa, mcpa_parallel
from LmCompute.plugins.multi.randomize.grady import gradyRandomize

# .............................................................................
def do_runs(pam, num_permutations, do_pam_stats=False, do_mcpa=False,
            tree=None, biogeo=None, grim=None, tree_mtx=None, parallel=False):
    """Run multi-species analyses

    Args:
        pam (:obj: `Matrix`): The PAM or incidence matrix to use for analysis
        num_permutations (:obj: `int`): The number of permutations to perform,
            setting to zero performs an observed run
        do_pam_stats (:obj: `bool`): Should PAM stats be calculated
        do_mcpa (:obj: `bool`): Should MCPA be calculated
        tree (:obj: `Dendropy.Tree`): A tree instance to use for PAM stats
        biogeo (:obj: `Matrix`): A matrix of biogeographic hypotheses for MCPA
        grim (:obj: `Matrix`): A matrix of environment values for MCPA
        tree_mtx (:obj: `Matrix`): An encoded phylogenetic tree for MCPA
        parallel (:obj: `bool`): If true, use the parallel version of MCPA
    """
    pam_stats = []
    mcpa_outs = []

    if parallel:
        mcpa_method = mcpa_parallel
    else:
        mcpa_method = mcpa

    if num_permutations >= 1:
        for _ in range(num_permutations):
            i_pam = gradyRandomize(pam)
            if do_pam_stats:
                pam_stats.append(PamStats(i_pam, tree=tree))
            if do_mcpa:
                mcpa_outs.append(mcpa_method(i_pam, tree_mtx, grim, biogeo))
    else:
        if do_pam_stats:
            pam_stats.append(PamStats(pam, tree=tree))
        if do_mcpa:
            mcpa_outs.append(mcpa_method(pam, tree_mtx, grim, biogeo))
    return pam_stats, mcpa_outs

# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Perform a multi-species run')
    parser.add_argument(
        'pam_filename', type=str, help='The file location of the PAM to use')
    parser.add_argument(
        'num_permutations', type=int,
        help=('How many permuted runs to perform,'
              ' if zero, perform observed run'))
    parser.add_argument(
        'do_pam_stats', type=int, help='1 to perform PAM stats, 0 to skip')
    parser.add_argument(
        'do_mcpa', type=int,
        help=('1 to perform MCPA, 0 to skip. '
              ' Must provide GRIM, BIOGEO, and TREE matrix to perform'))
    parser.add_argument(
        '-p', '--parallel', action='store_true', help='Use parallelism')
    parser.add_argument(
        '-g', '--grim', type=str,
        help='The file location of the GRIM to use for MCPA')
    parser.add_argument(
        '-b', '--biogeo', type=str,
        help=('The file location of the biogeographic hypotheses'
              ' matrix to use for MCPA'))
    parser.add_argument(
        '-tm', '--tree_matrix', type=str,
        help='The file location of the encoded tree to use for MCPA')
    parser.add_argument(
        '-t', '--tree_filename', type=str,
        help='If provided, use this tree when calculating PAM tree stats')

    # Output files
    parser.add_argument(
        '--diversity_stats_filename',
        type=str, help=('File location where diversity statistics'
                        ' should be stored'))
    parser.add_argument(
        '--site_stats_filename', type=str,
        help='File location where site statistics should be stored')
    parser.add_argument(
        '--species_stats_filename', type=str,
        help='File location where species statistics should be stored')
    parser.add_argument(
        '--site_covariance_filename', type=str,
        help='File location to store site covariance matrix')
    parser.add_argument(
        '--species_covariance_filename', type=str,
        help='File location to store species covariance matrix')
    parser.add_argument(
        '--mcpa_output_matrix_filename', type=str,
        help='File location to store MCPA output matrix')
    parser.add_argument(
        '--mcpa_f_matrix_filename', type=str,
        help='File location to store MCPA F-matrix')
    
    args = parser.parse_args()

    pam = Matrix.load(args.pam_filename)
    tree = None
    biogeo = None
    grim = None
    tree_mtx = None

    if args.tree_filename is not None:
        tree = dendropy.Tree.get(path=args.tree_filename, schema='nexus')
    if args.do_mcpa:
        try:
            biogeo = Matrix.load(args.biogeo)
            grim = Matrix.load(args.grim)
            tree_mtx = Matrix.load(args.tree_matrix)
        except Exception as e:
            print(str(e))
            print(('Cannot perform MCPA without PAM, Grim, Biogeo,'
                   ' and Tree matrix'))
            raise e

    pam_stats, mcpa_outs = do_runs(
        pam, args.num_permutations, do_pam_stats=args.do_pam_stats,
        do_mcpa=args.do_mcpa, tree=tree, biogeo=biogeo, grim=grim,
        tree_mtx=tree_mtx, parallel=args.parallel)

    # Write outputs
    # TODO: Determine what happens if we try to write out a metric that is not
    #    computed
    # PAM stats - diversity
    if args.diversity_stats_filename is not None:
        with open(args.diversity_stats_filename, 'w') as out_f:
            diversity_mtx = Matrix.concatenate(
                [i.getDiversityStatistics() for i in pam_stats], axis=2)
            diversity_mtx.save(out_f)

    # PAM stats - site stats
    if args.site_stats_filename is not None:
        with open(args.site_stats_filename, 'w') as out_f:
            site_stats_mtx = Matrix.concatenate(
                [i.getSiteStatistics() for i in pam_stats], axis=2)
            site_stats_mtx.save(out_f)

    # PAM stats - species stats
    if args.species_stats_filename is not None:
        with open(args.species_stats_filename, 'w') as out_f:
            species_stats_mtx = Matrix.concatenate(
                [i.getSpeciesStatistics() for i in pam_stats], axis=2)
            species_stats_mtx.save(out_f)

    # PAM stats - covariance matrices
    if args.site_covariance_filename is not None\
        or args.species_covariance_filename is not None:

        site_covs = []
        sp_covs = []
        for i in pam_stats:
            site_c, species_c = i.getCovariancematrices()
            site_covs.append(site_c)
            sp_covs.append(species_c)

        if args.site_covariance_filename is not None:
            with open(args.site_covariance_filename, 'w') as out_f:
                site_cov_mtx = Matrix.concatenate(site_covs, axis=2)
                site_cov_mtx.save(out_f)

        if args.species_covariance_filename is not None:
            with open(args.species_covariance_filename, 'w') as out_f:
                sp_cov_mtx = Matrix.concatenate(sp_covs, axis=2)
                sp_cov_mtx.save(out_f)

    # MCPA - observed values
    if args.mcpa_output_matrix_filename is not None:
        with open(args.mcpa_output_matrix_filename, 'w') as out_f:
            mcpa_out_mtx = Matrix.concatenate(
                [i for i, _ in mcpa_outs], axis=2)
            mcpa_out_mtx.save(out_f)

    # MCPA - F values
    if args.mcpa_f_matrix_filename is not None:
        with open(args.mcpa_f_matrix_filename, 'w') as out_f:
            mcpa_f_mtx = Matrix.concatenate([j for _, j in mcpa_outs], axis=2)
            mcpa_f_mtx.save(out_f)    
