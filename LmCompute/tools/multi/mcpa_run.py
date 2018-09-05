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
from LmCompute.plugins.multi.mcpa.mcpa import mcpaRun

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(
                   description="This script calculates MCPA for observed data")
   # Inputs
   parser.add_argument("incidenceMtxFn", 
                      help="Incidence matrix for the analysis (PAM) file name")
   parser.add_argument("phyloEncodingFn", 
                              help="Encoded phylogenetic matrix file location")
   parser.add_argument("envFn", 
                             help="Environment predictor matrix file location")
   parser.add_argument('-b', dest='bio', 
                           help="Biogeographic predictor matrix file location")
   # Outputs
   parser.add_argument("adjRsqFn", 
                   help="The file path to store the adjusted R squared vector")
   parser.add_argument("partCorMtxFn", 
                  help="Where the partial correlation matrix should be stored")
   parser.add_argument("fGlobalMtxFn", 
                                  help="The file path for the f global matrix")
   parser.add_argument("fPartialMtxFn", 
                help="The file path for the f matrix for partial correlations")
   
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
      hp = 'BG - '
   else:
      predictorMtx = envMtx
      hp = 'ENV - '
   
   adjRsq, fGlobal, semiPartialMtx, fSemiPartial = mcpaRun(incidenceMtx, 
                                                           predictorMtx, 
                                                           phyloMtx, 
                                                           headerPrefix=hp)

   # Write outputs
   with open(args.adjRsqFn, 'w') as adjRsqF:
      adjRsq.save(adjRsqF)

   with open(args.partCorMtxFn, 'w') as outPartCorF:
      semiPartialMtx.save(outPartCorF)

   with open(args.fGlobalMtxFn, 'w') as fGlobalF:
      fGlobal.save(fGlobalF)

   with open(args.fPartialMtxFn, 'w') as fPartialF:
      fSemiPartial.save(fPartialF)
      