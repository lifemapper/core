#!/bin/bash
"""
@summary: This script assembles the outputs of MCPA into a single matrix
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

from LmBackend.common.lmobj import LMObject
from LmCommon.common.matrix import Matrix

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(
               description="This script assembles the outputs of MCPA into a single matrix")
   # Inputs
   # Environment
   parser.add_argument('envPartCorMtxFn', 
               help='The partial correlation matrix for environment')
   parser.add_argument('envAdjRsqFn', 
               help='The adjusted R-squared matrix for environment')
   parser.add_argument('envFGlobalMtxFn', 
               help='The file path for the environment F-global matrix')
   parser.add_argument('envFpartialMtxFn', 
               help='The file path for the environment F-matrix for partial correlations')
   parser.add_argument('envBHfGlobalMtxFn',
               help='The file path for the environment Benjamini-Hochberg F-global matrix')
   parser.add_argument('envBHfPartialMtxFn',
               help='The file path for the environment Benjamini-Hochberg F-partial matrix')

   # Bio Geo
   parser.add_argument('bgPartCorMtxFn', 
               help='The partial correlation matrix for biogeo')
   parser.add_argument('bgAdjRsqFn', 
               help='The adjusted R-squared matrix for biogeo')
   parser.add_argument('bgFGlobalMtxFn', 
               help='The file path for the biogeo F-global matrix')
   parser.add_argument('bgFpartialMtxFn', 
               help='The file path for the biogeo F-matrix for partial correlations')
   parser.add_argument('bgBHfGlobalMtxFn',
               help='The file path for the biogeo Benjamini-Hochberg F-global matrix')
   parser.add_argument('bgBHfPartialMtxFn',
               help='The file path for the biogeo Benjamini-Hochberg F-partial matrix')
   
   # Output
   parser.add_argument('outputMtxFn', 
               help='The file location to store the aggregated outputs')
   
   args = parser.parse_args()
   
   # Load the matrices
   envPartCorMtx = Matrix.load(args.envPartCorMtxFn)
   envAdjRsqMtx = Matrix.load(args.envAdjRsqFn)
   envFglobalMtx = Matrix.load(args.envFGlobalMtxFn)
   envFpartialMtx = Matrix.load(args.envFpartialMtxFn)
   envFglobalBHMtx = Matrix.load(args.envBHfGlobalMtxFn)
   envFpartialBHMtx = Matrix.load(args.envBHfPartialMtxFn)

   bgPartCorMtx = Matrix.load(args.bgPartCorMtxFn)
   bgAdjRsqMtx = Matrix.load(args.bgAdjRsqFn)
   bgFglobalMtx = Matrix.load(args.bgFGlobalMtxFn)
   bgFpartialMtx = Matrix.load(args.bgFpartialMtxFn)
   bgFglobalBHMtx = Matrix.load(args.bgBHfGlobalMtxFn)
   bgFpartialBHMtx = Matrix.load(args.bgBHfPartialMtxFn)
   
   # Concatenate matrices one axis at a time
   # Observed layer
   obsMtx = Matrix.concatenate(
      [envPartCorMtx, envAdjRsqMtx, bgPartCorMtx, bgAdjRsqMtx], axis=1)
   # Frequency layer
   freqMtx = Matrix.concatenate(
      [envFpartialMtx, envFglobalMtx, bgFpartialMtx, bgFglobalMtx], axis=1)

   bhMtx = Matrix.concatenate([envFpartialBHMtx, envFglobalBHMtx, 
                               bgFpartialBHMtx, bgFglobalBHMtx], axis=1)
   # Stack matrices
   outMtx = Matrix.concatenate([obsMtx, freqMtx, bhMtx], axis=2)
   
   # Add depth headers
   outMtx.setHeaders(['Observed', 'P-Values', 'BH Significant'], axis=2)
   
   # Write output
   LMObject().readyFilename(args.outputMtxFn)
   
   with open(args.outputMtxFn, 'w') as outF:
      outMtx.save(outF)
   
