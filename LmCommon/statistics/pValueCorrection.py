"""
@summary: This module contains code for P-Value correction
@author: Jeff Cavner (edited by CJ Grady)
@see: Leibold, m.A., E.P. Economo and P.R. Peres-Neto. 2010. Metacommunity
         phylogenetics: separating the roles of environmental filters and 
         historical biogeography. Ecology letters 13: 1290-1299.
@version: 1.0
@status: alpha
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
from copy import deepcopy
import numpy as np

from LmCommon.common.matrix import Matrix

# Constants
#..............................................................................
class CorrectionTypes:
   """
   @summary: Class constant holding available P-Value correction types
   """
   BENJAMINI_HOCHBERG = 1
   BONFERRONI = 2
   BONFERRONI_HOLM = 3
   
# .............................................................................
def correctPValues(pValues, correctionType=CorrectionTypes.BENJAMINI_HOCHBERG):                
   """
   @summary: Perform P-value correction using the specified method
   @param pValues: An array of P-values (can be 1 or 2 dimensions)
   @param correctionType: The type of p-value correction to perform
   @see: CorrectionTypes 
   @note: consistent with R - print correct_pvalues_for_multiple_testing([0.0, 
             0.01, 0.029, 0.03, 0.031, 0.05, 0.069, 0.07, 0.071, 0.09, 0.1])
   @todo: Consider how we might add metadata to this
   @todo: Re-enable other correction types 
   @todo: Consider producing a matrix of the maximum FDR value that would mark
             each cell as significant
   """
   # Get the original shape, we'll convert back to this
   #origShape = pValues.data.shape
   
   # Reshape into one-dimensional array
   pFlat = pValues.data.flatten()
   
   numVals = pFlat.shape[0]

   #correctedPvals = np.empty(numVals)
   
   #if correctionType == CorrectionTypes.BONFERRONI:                                                                   
   #   correctedPvals = numVals * pFlat
   #elif correctionType == CorrectionTypes.BONFERRONI_HOLM:                                                            
   #   values = [(pvalue, i) for i, pvalue in enumerate(pFlat)]                                      
   #   values.sort()
   #   for rank, vals in enumerate(values):                                                              
   #      pvalue, i = vals
   #      correctedPvals[i] = (numVals - rank) * pvalue                                                            
   
   #elif correctionType == CorrectionTypes.BENJAMINI_HOCHBERG:
   # TODO: This should be configurable
   fdr = 0.05
   # 1. Order p-values
   # 2. Assign rank
   # 3. Create critical values
   # 4. Find the largest p-value such that P(i) < critical value
   # All P(j) such that j <= i are significant
   rank = 1
   cmpP = 0.0
   for p in sorted(pFlat.tolist()):
      critVal = fdr * (float(rank) / numVals)
      
      # Check if the p value is less than the critical value
      if p < critVal:
         # If this p is smaller, all p values smaller than this one are 
         #    "significant", even those that were greater than their 
         #    respective critical value
         cmpP = p  
      
      rank += 1
   
   headers = deepcopy(pValues.headers)
   headers['2'] = ['BH Corrected']
   sigValues = (pValues.data <= cmpP).astype(int)
   return Matrix(sigValues, headers=headers)
      
   # Old code, probably remove
   #values = [(pvalue, i) for i, pvalue in enumerate(pFlat)]                                      
   #values.sort()
   #values.reverse()                                                                                  
   #newValues = []
   #for i, vals in enumerate(values):                                                                 
   #   rank = numVals - i
   #   pvalue, index = vals                                                                          
   #   newValues.append((1.0 * numVals / rank) * pvalue)                                                          
   #for i in xrange(0, numVals - 1):  
   #   if newValues[i] < newValues[i+1]:                                                           
   #      newValues[i+1] = newValues[i]                                                           
   #for i, vals in enumerate(values):
   #   pvalue, index = vals
   #   correctedPvals[index] = newValues[i]
   #else:
   #   # TODO: Throw a specific exception
   #   raise Exception, "Unknown correction type"
   
   # Convert back to original shape
   #correctedPvals = correctedPvals.reshape(origShape)
   #return Matrix(correctedPvals, pValues.headers)
   
