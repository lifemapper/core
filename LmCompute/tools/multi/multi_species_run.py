#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script performs a multi-species analysis for a PAM

Todo:
    * Enable alternate randomization methods
    * Enable Phylo randomization
    * Remove do_(pam_stats | mcpa) script parameters and just use filenames to
        determine if they should be done
"""
import argparse

from LmCompute.plugins.multi.calculate.calculate import PamStats
from LmCompute.plugins.multi.mcpa.mcpa import mcpa, mcpa_parallel
from lmpy import Matrix, TreeWrapper
from lmpy.randomize.grady import grady_randomize
from LmBackend.common.lmobj import LMError


# .............................................................................
def do_runs(pam, num_permutations, do_mcpa=False, tree=None, biogeo=None,
            grim=None, tree_mtx=None, parallel=False, do_diversity_stats=False,
            do_site_cov_stats=False, do_site_stats=True,
            do_species_cov_stats=False, do_species_stats=True):
    """Run multi-species analyses

    Args:
        pam (:obj: `Matrix`): The PAM or incidence matrix to use for analysis
        num_permutations (:obj: `int`): The number of permutations to perform,
            setting to zero performs an observed run
        do_mcpa (:obj: `bool`): Should MCPA be calculated
        tree (:obj: `Dendropy.Tree`): A tree instance to use for PAM stats
        biogeo (:obj: `Matrix`): A matrix of biogeographic hypotheses for MCPA
        grim (:obj: `Matrix`): A matrix of environment values for MCPA
        tree_mtx (:obj: `Matrix`): An encoded phylogenetic tree for MCPA
        parallel (:obj: `bool`): If true, use the parallel version of MCPA
        do_diversity_stats (:obj: `bool`) : Should diversity stats be
            calculated
        do_site_cov_stats (:obj: `bool`) : Should site covariance stats be
            calculated
        do_site_stats (:obj: `bool`) : Should site stats be calculated
        do_species_cov_stats (:obj: `bool`) : Should species covariance stats
            be calculated
        do_species_stats (:obj: `bool`) : Should species stats be calculated
    """
    diversity_stats = []
    site_cov_stats = []
    site_stats = []
    species_cov_stats = []
    species_stats = []
    mcpa_outs = []
    mcpa_fs = []

    # If any of the pam stats are True, this will evaluate to true
    do_pam_stats = bool(
        sum([
            do_diversity_stats, do_site_cov_stats, do_site_stats,
            do_species_cov_stats, do_species_stats]))

    if parallel:
        mcpa_method = mcpa_parallel
    else:
        mcpa_method = mcpa

    if num_permutations >= 1:
        for i in range(num_permutations):
            print(('Iteration {}'.format(i)))
            i_pam = grady_randomize(pam)
            if do_pam_stats:
                multi_stats = PamStats(i_pam, tree=tree)
                # Append to diversity stats if we want them
                if do_diversity_stats:
                    diversity_stats.append(
                        multi_stats.get_diversity_statistics())
                # If we want either covariance matrix, calculate them both
                if do_site_cov_stats or do_species_cov_stats:
                    site_c, species_c = multi_stats.get_covariance_matrices()
                    if do_site_cov_stats:
                        site_cov_stats.append(site_c)
                    if do_species_cov_stats:
                        species_cov_stats.append(species_c)
                # Site stats
                if do_site_stats:
                    site_stats.append(multi_stats.get_site_statistics())
                # Species stats
                if do_species_stats:
                    species_stats.append(multi_stats.get_species_statistics())

                multi_stats = None

            if do_mcpa:
                mcpa_out, f_mtx = mcpa_method(i_pam, tree_mtx, grim, biogeo)
                mcpa_outs.append(mcpa_out)
                mcpa_fs.append(f_mtx)
            i_pam = None
    else:
        if do_pam_stats:
            multi_stats = PamStats(pam, tree=tree)
            # Append to diversity stats if we want them
            if do_diversity_stats:
                diversity_stats.append(multi_stats.get_diversity_statistics())
            # If we want either covariance matrix, calculate them both
            if do_site_cov_stats or do_species_cov_stats:
                site_c, species_c = multi_stats.get_covariance_matrices()
                if do_site_cov_stats:
                    site_cov_stats.append(site_c)
                if do_species_cov_stats:
                    species_cov_stats.append(species_c)
            # Site stats
            if do_site_stats:
                site_stats.append(multi_stats.get_site_statistics())
            # Species stats
            if do_species_stats:
                species_stats.append(multi_stats.get_species_statistics())
        if do_mcpa:
            mcpa_out, f_mtx = mcpa_method(pam, tree_mtx, grim, biogeo)
            mcpa_outs.append(mcpa_out)
            mcpa_fs.append(f_mtx)

    return (
        diversity_stats, site_cov_stats, site_stats, species_cov_stats,
        species_stats, mcpa_outs, mcpa_fs)


# .............................................................................
def main():
    """Main method for script
    """
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

    pam = Matrix.load_flo(args.pam_filename)
    tree = None
    biogeo = None
    grim = None
    tree_mtx = None

    if args.tree_filename is not None:
        tree = TreeWrapper.from_filename(args.tree_filename)
    if args.do_mcpa:
        try:
            biogeo = Matrix.load_flo(args.biogeo)
            grim = Matrix.load_flo(args.grim)
            tree_mtx = Matrix.load_flo(args.tree_matrix)
        except Exception as err:
            print((str(err)))
            msg = ('Cannot perform MCPA without PAM, Grim, Biogeo, '
                   'and Tree matrix')
            print(msg)
            raise LMError(msg, err)

    (diversity_stats, site_cov_stats, site_stats, species_cov_stats,
        species_stats, mcpa_outs, mcpa_fs) = do_runs(
            pam, args.num_permutations, do_mcpa=args.do_mcpa, tree=tree,
            biogeo=biogeo, grim=grim, tree_mtx=tree_mtx,
            parallel=args.parallel,
            do_diversity_stats=args.diversity_stats_filename is not None,
            do_site_cov_stats=args.site_covariance_filename is not None,
            do_site_stats=args.site_stats_filename is not None,
            do_species_cov_stats=args.species_covariance_filename is not None,
            do_species_stats=args.species_stats_filename is not None)

    # Write outputs if they are not empty lists
    # PAM stats - diversity
    if diversity_stats:
        diversity_mtx = Matrix.concatenate(diversity_stats, axis=2)
        diversity_mtx.write(args.diversity_stats_filename)

    # PAM stats - site stats
    if site_stats:
        site_stats_mtx = Matrix.concatenate(site_stats, axis=2)
        site_stats_mtx.write(args.site_stats_filename)

    # PAM stats - species stats
    if species_stats:
        species_stats_mtx = Matrix.concatenate(species_stats, axis=2)
        species_stats_mtx.write(args.species_stats_filename)

    # PAM stats - site covariance
    if site_cov_stats:
        site_cov_mtx = Matrix.concatenate(site_cov_stats, axis=2)
        site_cov_mtx.write(args.site_covariance_filename)

    # PAM stats - species covariance
    if species_cov_stats:
        sp_cov_mtx = Matrix.concatenate(species_cov_stats, axis=2)
        sp_cov_mtx.write(args.species_covariance_filename)

    # MCPA - observed values
    if args.mcpa_output_matrix_filename is not None:
        mcpa_out_mtx = Matrix.concatenate(mcpa_outs, axis=2)
        mcpa_out_mtx.write(args.mcpa_output_matrix_filename)

    # MCPA - F values
    if args.mcpa_f_matrix_filename is not None:
        mcpa_f_mtx = Matrix.concatenate(mcpa_fs, axis=2)
        mcpa_f_mtx.write(args.mcpa_f_matrix_filename)


# .............................................................................
if __name__ == '__main__':
    main()
