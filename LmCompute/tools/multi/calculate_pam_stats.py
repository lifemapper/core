#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script calculates statistics for the PAM

Todo:
    * Determine if we want to continue to support this script now that we have
        a script that performs PAM stats and MCPA
"""
import argparse

from LmCommon.common.lmconstants import DEFAULT_TREE_SCHEMA
from LmCommon.common.matrix import Matrix
from LmCommon.trees.lmTree import LmTree
from LmCompute.plugins.multi.calculate.calculate import PamStats

# .............................................................................
if __name__ == "__main__":
    
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="This script calculates statistics for the PAM") 
    
    parser.add_argument('pamFn', type=str, help="File location of PAM")
    parser.add_argument(
        'sitesFn', type=str,
        help="File location to store sites statistics Matrix")
    parser.add_argument(
        'speciesFn', type=str,
        help="File location to store species statistics Matrix")
    parser.add_argument(
        'diversityFn', type=str,
        help="File location to store diversity statistics Matrix")
    
    parser.add_argument(
        '-t', '--tree_file', dest='treeFn', type=str,
        help="File location of tree if tree stats should be computed")

    parser.add_argument(
        '--schluter', dest='schluter', action='store_true',
        help=('If this argument exists, compute Schluter statistics and'
              ' append them to diversity stats'))
    
    parser.add_argument(
        '--speciesCovFn', dest='spCovFn', type=str,
        help="If provided, write species covariance matrix here")
    parser.add_argument(
        '--siteCovFn', dest='siteCovFn', type=str,
        help="If provided, write site covariance matrix here")
    
    args = parser.parse_args()

    # Load PAM
    pam = Matrix.load(args.pamFn)
    
    # Load tree if exists
    if args.treeFn is not None:
        tree = LmTree.initFromFile(args.treeFn, DEFAULT_TREE_SCHEMA)
    else:
        tree = None
    
    calculator = PamStats(pam, tree=tree)
    
    # Write site statistics
    siteStats = calculator.getSiteStatistics()
    with open(args.sitesFn, 'w') as sitesOut:
        siteStats.save(sitesOut)
        
    # Write species statistics
    speciesStats = calculator.getSpeciesStatistics()
    with open(args.speciesFn, 'w') as speciesOut:
        speciesStats.save(speciesOut)
        
    # Write diversity statistics
    diversityStats = calculator.getDiversityStatistics()
    # Check if Schluter should be calculated
    if args.schluter:
        schluterStats = calculator.getSchluterCovariances()
        diversityStats.append(schluterStats, axis=1)
    with open(args.diversityFn, 'w') as diversityOut:
        diversityStats.save(diversityOut)
        
    # Check if covariance matrices should be computed
    if args.spCovFn is not None or args.siteCovFn is not None:
        sigmaSites, sigmaSpecies = calculator.getCovarianceMatrices()
        
        # Try writing covariance matrices.  Will throw exception and pass if
        #    None
        try:
            with open(args.spCovFn, 'w') as spCovOut:
                sigmaSpecies.save(spCovOut)
        except:
            pass

        try:
            with open(args.siteCovFn, 'w') as siteCovOut:
                sigmaSites.save(siteCovOut)
        except:
            pass
        