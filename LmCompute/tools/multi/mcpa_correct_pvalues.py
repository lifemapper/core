#!/bin/bash
"""
@summary: This script calculates corrected P-values for F-Global or 
             F-Semi-partial
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
from LmCommon.statistics.pValueCorrection import correctPValues
from LmCompute.plugins.multi.mcpa.mcpa import getPValues

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(
                   description="This script calculates and corrects P-Values")

   parser.add_argument("observedFn", 
                               help="File location of observed values to test")
   parser.add_argument("pValuesFn", help="File location to store the P-Values")
   parser.add_argument("fValueFn", nargs='+', 
                              help="A file of F-values or a stack of F-Values")
   
   args = parser.parse_args()
   
   # Load the matrices
   testValues = []
   numValues = 0
   
   for fVal in args.fValueFn:
      testMtx = Matrix.load(fVal)
      
      # Add the values to the test values list
      testValues.append(testMtx)
      
      # Add to the number of values
      if testMtx.data.ndim == 3: # Stack of values
         numValues += testMtx.data.shape[2]
      else:
         numValues += 1
   
   obsVals = Matrix.load(args.observedFn)
   pValues = getPValues(obsVals, testValues, numPermutations=numValues)
   
   correctedPvals = correctPValues(pValues)
   
   with open(args.pValuesFn, 'w') as pValF:
      correctedPvals.save(pValF)

   
