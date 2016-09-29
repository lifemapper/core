import numpy as np
import os


def p_adjust_bh(p):
   """Benjamini-Hochberg p-value correction for multiple hypothesis testing."""
   p = np.asfarray(p)
   by_descend = p.argsort()[::-1]
   by_orig = by_descend.argsort()
   steps = float(len(p)) / np.arange(len(p), 0, -1)
   q = np.minimum(1, np.minimum.accumulate(steps * p[by_descend]))
   return q[by_orig]



def correct_pvalues_for_multiple_testing(pvalues, correction_type = "Benjamini-Hochberg"):                
   """                                                                                                   
   consistent with R - print correct_pvalues_for_multiple_testing([0.0, 0.01, 0.029, 0.03, 0.031, 0.05, 0.069, 0.07, 0.071, 0.09, 0.1]) 
   """
   # check for 2-dimensions
   doreshape = False
   if len(pvalues.shape) == 2:
      # flatten
      r,c = pvalues.shape
      pvalues = flatten(pvalues)
      doreshape = True                                                                      
   #pvalues = np.array(pvalues) 
   n = float(pvalues.shape[0])                                                                           
   new_pvalues = np.empty(n)
   if correction_type == "Bonferroni":                                                                   
      new_pvalues = n * pvalues
   elif correction_type == "Bonferroni-Holm":                                                            
      values = [ (pvalue, i) for i, pvalue in enumerate(pvalues) ]                                      
      values.sort()
      for rank, vals in enumerate(values):                                                              
         pvalue, i = vals
         new_pvalues[i] = (n-rank) * pvalue                                                            
   elif correction_type == "Benjamini-Hochberg":                                                         
      values = [ (pvalue, i) for i, pvalue in enumerate(pvalues) ]                                      
      values.sort()
      values.reverse()                                                                                  
      new_values = []
      for i, vals in enumerate(values):                                                                 
         rank = n - i
         pvalue, index = vals                                                                          
         new_values.append((n/rank) * pvalue)                                                          
      for i in xrange(0, int(n)-1):  
         if new_values[i] < new_values[i+1]:                                                           
            new_values[i+1] = new_values[i]                                                           
      for i, vals in enumerate(values):
         pvalue, index = vals
         new_pvalues[index] = new_values[i]
   if doreshape:     
      return reshape(new_pvalues,r,c)
   else:   
      return new_pvalues



def flatten(a):
   
   r,c = a.shape
   return a.reshape((1,r*c))[0]

def reshape(f,r,c):
   return f.reshape(r,c)
   
if __name__ == "__main__":
      
   
   p_values = np.array([.01, .004, .12, .34])
   print correct_pvalues_for_multiple_testing(p_values)
   
   print
   p_values = np.array([[.01, .004], [.12, .34]])
   print correct_pvalues_for_multiple_testing(p_values)  # good
   
   
   
   #print p_adjust_bh(p_values)
   
   base = "/home/jcavner/BiogeographyMtx_Inputs/Florida/outputs"
   pa_e = np.load(os.path.join(base,"Env_2_P_Values.npy"))
   
   c_p = correct_pvalues_for_multiple_testing(pa_e)
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



