#!/bin/bash
"""
@summary: This script calculates MCPA values for a number of randomizations
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

from LmCommon.common.matrix import Matrix
from LmCompute.plugins.multi.mcpa.mcpa import mcpaRun

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(
                   description="This script calculates MCPA for random data")
   # Inputs
   parser.add_argument("incidenceMtxFn", 
                      help="Incidence matrix for the analysis (PAM) file name")
   parser.add_argument("phyloEncodingFn", 
                              help="Encoded phylogenetic matrix file location")
   parser.add_argument("envFn", 
                             help="Environment predictor matrix file location")
   parser.add_argument('-b', dest='bio', 
                           help="Biogeographic predictor matrix file location")
   parser.add_argument('-n', dest='numRandomizations', type=int,
                    help="The number of randomizations to perform in this set")
   # Outputs
   parser.add_argument("fGlobalRandomFn", 
          help="The file path to store the (stacked) random F-Global matrices")
   parser.add_argument("fPartialRandomFn", 
      help="The file path for the (stacked) f matrix for partial correlations")
   
   args = parser.parse_args()
   
   # Load the matrices
   incidenceMtx = Matrix.load(args.incidenceMtxFn)
   envMtx = Matrix.load(args.envFn)
   phyloMtx = Matrix.load(args.phyloEncodingFn)
   
   if args.bio:
      # I believe that Jeff may have made a mistake in his interpretation of 
      #    how to handle BioGeo and they should not be concatenated here.
      #    Will comment out in case we do need to do that
      # If a biogeographic matrix is supplied, concatenate environment matrix
      #bgMtx = Matrix.load(args.bio)
      #predictorMtx = Matrix.concatenate([bgMtx, envMtx], axis=1)
      
      predictorMtx = Matrix.load(args.bio)
   else:
      predictorMtx = envMtx
   
   numNodes = phyloMtx.data.shape[1]
   numPredictors = predictorMtx.data.shape[1]
   
   fGlobals = []
   fSemiPartials = []
   
   if args.numRandomizations:
      numRandomizations = args.numRandomizations
   else:
      numRandomizations = 1
   
   for i in xrange(numRandomizations):
      # TODO: Update when this returns Matrix objects
      _, fGlobalRand, _, fSemiPartialRand = mcpaRun(incidenceMtx, 
                                                    predictorMtx, phyloMtx,
                                                    randomize=True)
      # Add values to stacks
      fGlobals.append(fGlobalRand)
      fSemiPartials.append(fSemiPartialRand)

   fGlobalsStack = Matrix.concatenate(fGlobals, axis=2)
   fSemiPartialStack = Matrix.concatenate(fSemiPartials, axis=2)

   # Write outputs
   with open(args.fGlobalRandomFn, 'w') as fGlobalF:
      fGlobalsStack.save(fGlobalF)
   with open(args.fPartialRandomFn, 'w') as fPartF:
      fSemiPartialStack.save(fPartF)
      
