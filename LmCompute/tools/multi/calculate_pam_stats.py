#!/bin/bash
"""
@summary: This script calculates statistics for the PAM
@author: CJ Grady
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
   parser.add_argument('sitesFn', type=str, 
                       help="File location to store sites statistics Matrix")
   parser.add_argument('speciesFn', type=str, 
                       help="File location to store species statistics Matrix")
   parser.add_argument('diversityFn', type=str,
                     help="File location to store diversity statistics Matrix")
   
   parser.add_argument('-t', '--tree_file', dest='treeFn', type=str, 
                 help="File location of tree if tree stats should be computed")

   parser.add_argument('--schluter', dest='schluter', action='store_true',
             help="If this argument exists, compute Schluter statistics and append them to diversity stats")
   
   parser.add_argument('--speciesCovFn', dest='spCovFn', type=str,
                       help="If provided, write species covariance matrix here")
   parser.add_argument('--siteCovFn', dest='siteCovFn', type=str,
                       help="If provided, write site covariance matrix here")
   
   args = parser.parse_args()

   # Load PAM
   pam = Matrix.load(args.pamFn)
   
   # Load tree if exists
   if args.treeFn is not None:
      tree = LmTree(args.treeFn, DEFAULT_TREE_SCHEMA)
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
      
      # Try writing covariance matrices.  Will throw exception and pass if None
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
      