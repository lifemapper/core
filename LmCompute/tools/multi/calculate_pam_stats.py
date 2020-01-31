#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script calculates statistics for the PAM

Todo:
    * Determine if we want to continue to support this script now that we have
        a script that performs PAM stats and MCPA
"""
import argparse

from lmpy import Matrix, TreeWrapper

from LmCompute.plugins.multi.calculate.calculate import PamStats


# .............................................................................
def main():
    """Script main method
    """
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description='This script calculates statistics for the PAM')

    parser.add_argument('pam_fn', type=str, help='File location of PAM')
    parser.add_argument(
        'sites_fn', type=str,
        help='File location to store sites statistics Matrix')
    parser.add_argument(
        'species_fn', type=str,
        help='File location to store species statistics Matrix')
    parser.add_argument(
        'diversity_fn', type=str,
        help='File location to store diversity statistics Matrix')

    parser.add_argument(
        '-t', '--tree_file', dest='tree_fn', type=str,
        help='File location of tree if tree stats should be computed')

    parser.add_argument(
        '--schluter', dest='schluter', action='store_true',
        help=('If this argument exists, compute Schluter statistics and'
              ' append them to diversity stats'))

    parser.add_argument(
        '--species_cov_fn', dest='sp_cov_fn', type=str,
        help='If provided, write species covariance matrix here')
    parser.add_argument(
        '--site_cov_fn', dest='site_cov_fn', type=str,
        help='If provided, write site covariance matrix here')

    args = parser.parse_args()

    # Load PAM
    pam = Matrix.load_flo(args.pam_fn)

    # Load tree if exists
    if args.tree_fn is not None:
        tree = TreeWrapper.from_filename(args.tree_fn)
    else:
        tree = None

    calculator = PamStats(pam, tree=tree)

    # Write site statistics
    site_stats = calculator.get_site_statistics()
    with open(args.sites_fn, 'w') as sites_out:
        site_stats.save(sites_out)

    # Write species statistics
    species_stats = calculator.get_species_statistics()
    with open(args.species_fn, 'w') as species_out:
        species_stats.save(species_out)

    # Write diversity statistics
    diversity_stats = calculator.get_diversity_statistics()
    # Check if Schluter should be calculated
    if args.schluter:
        schluter_stats = calculator.get_schluter_covariances()
        diversity_stats.append(schluter_stats, axis=1)
    with open(args.diversity_fn, 'w') as diversity_out:
        diversity_stats.save(diversity_out)

    # Check if covariance matrices should be computed
    if args.sp_cov_fn is not None or args.site_cov_fn is not None:
        sigma_sites, sigma_species = calculator.get_covariance_matrices()

        # Try writing covariance matrices.  Will throw exception and pass if
        #    None
        try:
            with open(args.sp_cov_fn, 'w') as sp_cov_out:
                sigma_species.save(sp_cov_out)
        except AttributeError:
            pass

        try:
            with open(args.site_cov_fn, 'w') as site_cov_out:
                sigma_sites.save(site_cov_out)
        except AttributeError:
            pass


# .............................................................................
if __name__ == '__main__':
    main()
