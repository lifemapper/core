"""
@summary: This module contains code for P-Value correction
@author: Jeff Cavner (edited by CJ Grady)
@see: Leibold, m.A., E.P. Economo and P.R. Peres-Neto. 2010. Metacommunity
         phylogenetics: separating the roles of environmental filters and 
         historical biogeography. Ecology letters 13: 1290-1299.
@version: 1.0
@status: alpha
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
   """
   # Get the original shape, we'll convert back to this
   origShape = pValues.shape
   
   # Reshape into one-dimensional array
   pFlat = pValues.flatten()
   
   numVals = pFlat.shape[0]

   correctedPvals = np.empty(numVals)
   
   if correctionType == CorrectionTypes.BONFERRONI:                                                                   
      correctedPvals = numVals * pFlat
   elif correctionType == CorrectionTypes.BONFERRONI_HOLM:                                                            
      values = [(pvalue, i) for i, pvalue in enumerate(pFlat)]                                      
      values.sort()
      for rank, vals in enumerate(values):                                                              
         pvalue, i = vals
         correctedPvals[i] = (numVals - rank) * pvalue                                                            
   elif correctionType == CorrectionTypes.BENJAMINI_HOCHBERG:                                                         
      values = [(pvalue, i) for i, pvalue in enumerate(pFlat)]                                      
      values.sort()
      values.reverse()                                                                                  
      newValues = []
      for i, vals in enumerate(values):                                                                 
         rank = numVals - i
         pvalue, index = vals                                                                          
         newValues.append((1.0 * numVals / rank) * pvalue)                                                          
      for i in xrange(0, numVals - 1):  
         if newValues[i] < newValues[i+1]:                                                           
            newValues[i+1] = newValues[i]                                                           
      for i, vals in enumerate(values):
         pvalue, index = vals
         correctedPvals[index] = newValues[i]
   else:
      # TODO: Throw a specific exception
      raise Exception, "Unknown correction type"
   
   # Convert back to original shape
   correctedPvals = correctedPvals.reshape(origShape)
   return Matrix(correctedPvals)
   
