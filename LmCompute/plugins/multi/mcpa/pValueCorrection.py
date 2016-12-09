"""
@summary: This module contains code for P-Value correction
@author: Jeff Cavner (edited by CJ Grady)
@see: Leibold, m.A., E.P. Economo and P.R. Peres-Neto. 2010. Metacommunity
         phylogenetics: separating the roles of environmental filters and 
         historical biogeography. Ecology letters 13: 1290-1299.
"""
import numpy as np
import os

# Constants
class CorrectionTypes:
   """
   @summary: Class constant holding available P-Value correction types
   """
   BENJAMINI_HOCHBERG = 1
   BONFERRONI = 2
   BONFERRONI_HOLM = 3
   

# .............................................................................
def pAdjustBH(p):
   """
   @summary: Benjamini-Hochberg P-Value correction for multiple hypothesis 
                testing.
   @todo: Document input
   @todo: Document function, what is it doing?
   """
   p = np.asfarray(p)
   byDescend = p.argsort()[::-1]
   byOrig = byDescend.argsort()
   steps = float(len(p)) / np.arange(len(p), 0, -1)
   q = np.minimum(1, np.minimum.accumulate(steps * p[byDescend]))
   return q[byOrig]

# .............................................................................
def correctPValuesForMultipleTesting(pValues, 
                            correctionType=CorrectionTypes.BENJAMINI_HOCHBERG):                
   """
   @todo: Document
   @summary: 
   @param pValues:
   @param correctionType: The type of p-value correction to perform
   @see: CorrectionTypes 
   @note: consistent with R - print correct_pvalues_for_multiple_testing([0.0, 
             0.01, 0.029, 0.03, 0.031, 0.05, 0.069, 0.07, 0.071, 0.09, 0.1]) 
   """
   # check for 2-dimensions
   doreshape = False
   if len(pValues.shape) == 2:
      # flatten
      rows, cols = pValues.shape
      pValues = flatten(pValues)
      doreshape = True
      
   n = float(pValues.shape[0])                                                                           
   newPvalues = np.empty(n)
   if correctionType == CorrectionTypes.BONFERRONI:                                                                   
      newPvalues = n * pValues
   elif correctionType == CorrectionTypes.BONFERRONI_HOLM:                                                            
      values = [ (pvalue, i) for i, pvalue in enumerate(pValues) ]                                      
      values.sort()
      for rank, vals in enumerate(values):                                                              
         pvalue, i = vals
         newPvalues[i] = (n-rank) * pvalue                                                            
   elif correctionType == CorrectionTypes.BENJAMINI_HOCHBERG:                                                         
      values = [ (pvalue, i) for i, pvalue in enumerate(pValues) ]                                      
      values.sort()
      values.reverse()                                                                                  
      newValues = []
      for i, vals in enumerate(values):                                                                 
         rank = n - i
         pvalue, index = vals                                                                          
         newValues.append((n/rank) * pvalue)                                                          
      for i in xrange(0, int(n)-1):  
         if newValues[i] < newValues[i+1]:                                                           
            newValues[i+1] = newValues[i]                                                           
      for i, vals in enumerate(values):
         pvalue, index = vals
         newPvalues[index] = newValues[i]
   if doreshape:     
      return reshape(newPvalues, rows, cols)
   else:   
      return newPvalues

# .............................................................................
def flatten(mtx):
   """
   @summary: Flatten matrix a
   @param mtx: The numpy matrix to flatten
   """
   rows, cols = mtx.shape
   return mtx.reshape((1, rows * cols))[0]

# .............................................................................
def reshape(mtx, rows, cols):
   """
   @summary: Reshape matrix mtx
   @param mtx: The numpy matrix to reshape
   @param rows: The number of rows for the new shape
   @param cols: The number of columns for the new shape
   """
   return mtx.reshape(rows, cols)
   
# .............................................................................
if __name__ == "__main__":
      
   
   p_values = np.array([.01, .004, .12, .34])
   print correctPValuesForMultipleTesting(p_values)
   
   print
   p_values = np.array([[.01, .004], [.12, .34]])
   print correctPValuesForMultipleTesting(p_values)  # good
   
   
   
   #print p_adjust_bh(p_values)
   
   base = "/home/jcavner/BiogeographyMtx_Inputs/Florida/outputs"
   pa_e = np.load(os.path.join(base,"Env_2_P_Values.npy"))
   
   c_p = correctPValuesForMultipleTesting(pa_e)
   np.save(os.path.join(base,'Env_2_P_Corrected.npy'),c_p)
   
   #>>> from rpy2.robjects.packages import importr
   #>>> from rpy2.robjects.vectors import FloatVector
   #>>> stats = importr('stats')
   #>>> p_values = [.01, .004, .12, .34, .12]
   #>>> print stats.p_adjust(FloatVector(pvalue_list), method = 'BH')
   
   
   #>>> a
   #array([[0, 1, 2],
   #       [3, 4, 5],
   #       [6, 7, 8]])
   #>>> f = a.reshape((1,9))[0]
   #>>> f
   #array([0, 1, 2, 3, 4, 5, 6, 7, 8])
   #
   #>>> f.reshape(3,3)
   #array([[0, 1, 2],
   #       [3, 4, 5],
   #       [6, 7, 8]])



