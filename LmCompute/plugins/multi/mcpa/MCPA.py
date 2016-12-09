"""
@summary: This module contains functions to perform MCPA
@author: Jeff Cavner (edited by CJ Grady)
@see: Leibold, m.A., E.P. Economo and P.R. Peres-Neto. 2010. Metacommunity
         phylogenetics: separating the roles of environmental filters and 
         historical biogeography. Ecology letters 13: 1290-1299.
"""
import concurrent.futures
import numpy as np
import os
import sys

#TODO: Fix import
from pValueCorrection import correctPValuesForMultipleTesting

# Constants
F_GLOBAL_KEY = 'fGlobal'
F_SEMI_PARTIAL_KEY = 'fSemiPartial'


# .............................................................................
def standardizeMatrix(siteWeights, mtx):
   """
   @summary: Standardizes matrix, mtx
   @param siteWeights: Vector of row / column totals for site weights
   @param mtx: Matrix to be standardized (M in math?)
   @todo: Switch to this
   @note: This function produces the same output as the old, but the end result
             seems to be different.  Something may be modifying something out 
             of scope
   """
   # Get the sum of the incidence matrix, which is a sum of the weights
   iSum = float(siteWeights.sum()) 
   
   sPred = np.dot(siteWeights, mtx)
   sPred2 = np.dot(siteWeights,(mtx**2))
   
   meanWeightedPred = sPred/iSum
   
   stdDevWeightedPred = ((sPred2-(sPred**2.0/iSum))/(iSum))**.5
   
   # The standardized matrix is just one duplicated row
   ones = np.ones((siteWeights.shape[0], 1))
   stdMtx = (1.0* (mtx - meanWeightedPred) * stdDevWeightedPred**-1.0) * ones
   
   return stdMtx

# .............................................................................
def standardizeMatrixOld(siteWeights, mtx, onesCol, pam):
   """
   @todo: Remove
   @deprecated: This method has been rewritten
   @note: Cannot remove until the difference is figured out between old and new.
             New method produces the same output but the final result is 
             different.  Could be something modified at incorrect scope
   @summary: Standardizes matrix, mtx
   @param siteWeights: Diagonal matrix for site weights (W in math?)
   @param mtx: Matrix to be standardized (M in math?)
   @param onesCol: Column vector of ones, sites (n) or species (k)
   @param pam: Incidence matrix (PAM) (I in math)
   """
   # Get the sum of the incidence matrix
   iSum = float(pam.sum()) 
   
   sPred = np.dot(np.dot(onesCol.T, siteWeights), mtx)
   sPred2 = np.dot(np.dot(onesCol.T, siteWeights), mtx**2)
   
   meanWeightedPred = sPred/iSum
   
   stdDevWeightedPred = ((sPred2 - (sPred**2.0 / iSum)) / (iSum))**.5
   
   stdMtx = ((np.dot(onesCol,stdDevWeightedPred))**-1.0) * (mtx-np.dot(onesCol, meanWeightedPred))
   
   return stdMtx
   
# ........................................
def semiPartialCorrelationLeiboldVectorize(pam, predictorMtx, nodeMtx, 
                                         randomize=False, fResultsObserved={}): 
   """
   @summary: A vectorizable function for computing the semi-partial correlation 
                of a matrix using Leibold's method
   @note: Follows Pedro's matlab code as far as loops that are necessary for
             treating nodes individually.  Exceptions to his code are 
             mathematical changes for efficiency, and corrections for 
             semi-partials in tree but not in PAM.  Also fault checks.
   @param pam: The PAM, or incidence matrix
   @param predictorMtx: Predictor matrix
   @param nodeMtx: Phylogenetic encoding (species (row) by node (column))
   @param randomize: Boolean indicating if this is a randomization
   @param fResultsObserved: Contains two F-Score matrices
   """
   numNodes = nodeMtx.shape[1] 
   numPredictors = predictorMtx.shape[1]  
   iDictPred = {}
   iDictNode = {'y':0}
   
   if not randomize:
      # put results here
      resultSemiPartialMtx = np.zeros((numNodes, numPredictors))
      resultFSemiPartialMtx = np.zeros((numNodes, numPredictors))
      resultRsqAdjMtx = np.zeros((numNodes, 1))
      resultFGlobalMtx = np.zeros((numNodes, 1))            
  
   else:
      
      mtxProbSemiPartial = np.zeros((numNodes, numPredictors))
      vectProbRsq = np.zeros((numNodes, 1))
      
   # ...........................
   def computeSemiPartials(predictorCol, predictors, swDiagonal, stdPSum, 
                           resultRsq, totalPSumResidual, nodeNumber):
      """
      @summary: Determines semi-partial correlations
      @note: Applied across column axis for predictor matrix
      @note: Predictor matrix can be either environment or 
                historical biogeography
      @param precitorCol: The column of the predictor matrix to use
      @param predictors: The entire predictor matrix
      @param swDiagonal: Vector of site totals
      @param stdPsum: Standardized P-sigma
      @param resultRsq: Result R-squared
      @param totalPSumResidual: Total P-sigma residual
      @param nodeNumber: The node number
      """
      try:
         predNumber = iDictPred['x']  # 'x' axis of results
         
         ithPredictor = np.array([predictorCol]).T #TODO: Fix
         withoutIthPredictor = np.delete(predictors, predNumber, axis=1)  
         
         # % slope for the ith predictor, Beta, regression coefficient
         q,r = np.linalg.qr(np.dot(
            np.einsum('ij,j->ij', ithPredictor.T, swDiagonal), ithPredictor))
         
         rDivQtrans = np.linalg.lstsq(r, q.T)[0]
         
         ithSlopPart1 = np.dot(rDivQtrans, ithPredictor.T)
         ithSlopePart2 = np.einsum('ij,j->ij', ithSlopPart1, swDiagonal)
         ithSlope = np.dot(ithSlopePart2, stdPSum)
         
         # % regression for the remaining predictors
         q,r = np.linalg.qr(np.dot(np.einsum('ij,j->ij', withoutIthPredictor.T,
                                             swDiagonal), withoutIthPredictor))
         rDivQtransR = np.linalg.lstsq(r, q.T)[0]
         withoutPredRQR = np.dot(withoutIthPredictor, rDivQtransR)
         hPart = np.dot(withoutPredRQR, withoutIthPredictor.T)
         h = np.einsum('ij,j->ij', hPart, swDiagonal)
         predicted = np.dot(h, stdPSum)
         remainingRsq = np.sum(predicted**2)/np.sum(stdPSum**2)
         
         if (resultRsq - remainingRsq) >= 0:
            resultSP = ithSlope * (
                        (resultRsq - remainingRsq)**.5) / np.absolute(ithSlope)
         else:
            resultSP = np.array([0.0])
            
         fSemiPartial = (resultRsq - remainingRsq) / totalPSumResidual
         if not randomize:
            resultFSemiPartialMtx[nodeNumber][predNumber] = fSemiPartial
         else:
            #if F_SEMI_PARTIAL_KEY in fResultsObserved.keys():
            if fResultsObserved.has_key(F_SEMI_PARTIAL_KEY):
               if fSemiPartial >= fResultsObserved[F_SEMI_PARTIAL_KEY][
                                                       nodeNumber][predNumber]:
                  mtxProbSemiPartial[nodeNumber][predNumber] = 1 
                  #mtxProbSemiPartial[nodeNumber][predNumber] +1
                  
         iDictPred['x'] += 1
      except Exception, e:
         print str(e)
         raise e
         resultSP = np.array([0.0])

      return resultSP
   
   # ...........................
   def nodes(nodeCol):
      """
      @summary: operation to be performed on each node column
      @param nodeCol: The node column to operate on
      """
      iDictPred['x'] = 0
      
      speciesPresentAtNode = np.where(nodeCol != 0)[0]
      
      if randomize:
         # move columns around
         speciesPresentAtNodeRand = np.random.permutation(speciesPresentAtNode)
         incidence = pam[:,speciesPresentAtNodeRand]
         
      else:
         incidence = pam[:,speciesPresentAtNode]  # might want to use a take here
      
      # added Jeff, find if any of the columns in sliced incidence are all zero
      bs = np.any(incidence, axis=0)
      emptyCol = np.where(bs == False)[0]

      # find rows in incidence that are all zero
      bs = np.any(incidence, axis=1)  # bolean selection row-wise logical OR
      emptySites = np.where(bs == False)[0]  # position of deletes
      incidence = np.delete(incidence, emptySites, 0)  # delete rows
      
      if incidence.shape[0] > 1:# and len(emptyCol) == 0: # might not need this last clause, get more good nodes for Tashi without it
         predictors = predictorMtx #TODO: Fix
         predictors = np.delete(predictors,emptySites,0) # delete rows
         numSites = incidence.shape[0]
         
         if randomize:
            incidence = np.random.permutation(incidence)
         
         if not randomize:
            # Column-wise variance
            if (numPredictors > (numSites - 2)) or (
                     len(np.where(np.var(predictors, axis=0) == 0)[0]) > 0):
               resultSemi = np.array([np.zeros(numPredictors)])
               resultSemiPartialMtx[iDictNode['y']] = resultSemi[0]
               return np.array([]) # TODO: Why?
         
         sumSites = np.sum(incidence, axis=1)  # sum of the rows, alpha
         sumSpecies = np.sum(incidence, axis=0)  # sum of the columns, omega
         numSpecies = incidence.shape[1]
         #TODO: Remove these weights?
         siteWeights = np.diag(sumSites)   # Wn, used?
         speciesWeights = np.diag(sumSpecies) # Wk , used?
         
         try:
            # standardize Predictor, in this case Env matrix
            #TODO: Switch to new method
            #StdPredictorsNew = standardizeMatrix(sumSites, predictors)#, Ones, incidence)
            #TODO: Remove these two lines
            predOnes = np.ones((numSites, 1))
            stdPredictors = standardizeMatrixOld(siteWeights, predictors, 
                                                 predOnes, incidence)
            
            ## p standardize 
            #TODO: Switch to new method
            nodeOnes = np.ones((numSpecies, 1))
            #stdNode = standardizeMatrix(sumSpecies, nodeCol[speciesPresentAtNode])#, Ones, incidence)
            stdNode = standardizeMatrixOld(speciesWeights, 
                                           nodeCol[speciesPresentAtNode], 
                                           nodeOnes, incidence)
              
         except Exception, e:
            print str(e)
            raise e
            resultSemi = np.array([np.zeros(numPredictors)])
         else:
            # Standardized P-sigma
            stdPSum = np.dot(incidence, stdNode)  
            
            # regression
            q,r = np.linalg.qr(np.dot(stdPredictors.T * sumSites, 
                                      stdPredictors))

            rDivQtrans = np.linalg.lstsq(r, q.T)[0]
            stdPredRQ = np.dot(stdPredictors, rDivQtrans)
            
            # h is BetaAll
            #h = np.dot(np.dot(stdPredRQ,stdPredictors.T),siteWeights)  # WON'T SCALE!!
            hFirst = np.dot(stdPredRQ, stdPredictors.T)
            h = np.einsum('ij,j->ij', hFirst, sumSites)
            
            predicted =  np.dot(h, stdPSum)
            totalPSumResidual = np.sum((stdPSum-predicted)**2)
            
            stdPSumSqrs = np.sum(stdPSum**2)
            
            if  stdPSumSqrs != 0:
               ##### R Squared ####
               resultRsq = np.sum(predicted**2)/stdPSumSqrs  
               ################################################3
               
               #% adjusted Rsq  (classic method) should be interpreted with some caution as the degrees of
               #% freedom for weighted models are different from non-weighted models
               #% adjustments based on effective degrees of freedom should be considered
               
               fGlobal = np.sum(predicted**2)/totalPSumResidual
               
               if not randomize:
                  if numSites-numPredictors-1 > 0:                  
                     rSqAdj = 1 - (((numSites-1) / (numSites-numPredictors-1)) * (1-resultRsq))   
                  else:
                     rSqAdj = -999
                     
                  resultRsqAdjMtx[iDictNode['y']] = rSqAdj
                  resultFGlobalMtx[iDictNode['y']] = fGlobal
               else:
                  if fResultsObserved.has_key(F_GLOBAL_KEY):
                     if fGlobal >= fResultsObserved[F_GLOBAL_KEY][iDictNode['y']]:
                        vectProbRsq[iDictNode['y']] = 1 # vectProbRsq[iDictNode['y']] + 1
                        
               # semi partial correlations 
               # sending whole Predictor mtx to computeSemiPartials func, and feeding it to apply_along_axis, feeds one col. at a time, 0 axis
               # 3 significance done: resultRsq, rSqAdj,fGlobal
               
               #Note: The predictors variable is sent twice, the first one is
               #         iterated over by numpy and the second is used for the
               #         calculation
               resultSemi = np.apply_along_axis(computeSemiPartials, 0, 
                                 predictors, predictors, sumSites, stdPSum, 
                                 resultRsq, totalPSumResidual, iDictNode['y'])
                  
            else:
               resultSemi = np.array([np.zeros(numPredictors)]) #TODO: Fix
      else:
         resultSemi = np.array([np.zeros(numPredictors)]) #TODO: Fix
      if not randomize:
         resultSemiPartialMtx[iDictNode['y']] = resultSemi[0]
      
      iDictNode['y'] += 1    
        
      return np.array([]) # Why is this being returned?
   
   np.apply_along_axis(nodes, 0, nodeMtx)
   
   
   if randomize:
      return mtxProbSemiPartial, vectProbRsq
   else: 
      return (resultSemiPartialMtx, resultRsqAdjMtx, 
              resultFSemiPartialMtx, resultFGlobalMtx) 
  
# .............................................................................
def sumProbabilities(toSum, divisor=None):
   """
   @note: if divisor exists divide sum by it, return mean. if not return sum
   """
   valuesSummed = reduce(np.add, toSum)
   if divisor is not None:
      return valuesSummed / float(divisor)
   else:
      return valuesSummed

# .............................................................................
def appendENVtoBG(b, e):
   """
   @summary: appends e to b to control for e in b
   """
   b = b.astype(np.float)
   b = np.concatenate((b,e), axis=1)
   return b

# .............................................................................
def correctPValue(pValues):
   """
   @summary: correction routine
   @todo: need to test correcting vector
   """
   corrected = correctPValuesForMultipleTesting(pValues)
   return corrected

# .............................................................................
def calculateMCPA(pam, p, pred, fGlobal=False, fSemiPartial=False, numPermute=0, 
                  numConcurrent=1, divisor=None): 
   """
   @todo: Documentation
   @summary: sends inputs to calculate
   """
   
   if fGlobal and fSemiPartial:
      fResultsObserved = {
                          F_GLOBAL_KEY: fGlobal, 
                          F_SEMI_PARTIAL_KEY:fSemiPartial
                         }
      tasks = []
      with concurrent.futures.ProcessPoolExecutor(
                                        max_workers=numConcurrent) as executor:
         for i in range(0, numPermute):
            tasks.append(executor.submit(
                  semiPartialCorrelationLeiboldVectorize, pam, pred, p, 
                  randomize=True, fResultsObserved=fResultsObserved))
      probSemiPartialsToSum = [t.result()[0] for t in tasks]
      probRsqToSum = [t.result()[1] for t in tasks]
      semiPartialResult = sumProbabilities(probSemiPartialsToSum, divisor)
      rSqResult = sumProbabilities(probRsqToSum, divisor)
      return semiPartialResult, rSqResult
   else:
      rSemiPartialMtx, rRsqAdjVct, rFSemiPartialMtx, rFGlobalMtx = \
                           semiPartialCorrelationLeiboldVectorize(pam, pred, p)
      return rSemiPartialMtx, rRsqAdjVct,rFSemiPartialMtx, rFGlobalMtx

# .............................................................................
if __name__ == "__main__":
   
   import cPickle
   import testWithData
   
   nodeMtx, pam = testWithData.makeInputsForTextTest()
   e = testWithData.getEnvTextMatrix()
   ########## Environmental ###########
   
   rSemiPartialMtx_E, rRsqAdjVct_E, rFSemiPartialMtx_E, rFGlobalMtx_E = calculateMCPA(pam, nodeMtx, e)
   fResultsObserved = {F_GLOBAL_KEY: rFGlobalMtx_E, F_SEMI_PARTIAL_KEY: rFSemiPartialMtx_E} # setting global
   cPickle.dump(fResultsObserved,open('/tmp/FScores.pkl','wb'))
   
   ## random calculations
   probSemiPartialToSumEnv = []
   probRsqToSumEnv = []
   numPermute = 10
   #pool = Pool()  # can parameterize to number of cores, otherwise no arg uses all available
   numConcurrent = 5
   tasks = []
   with concurrent.futures.ProcessPoolExecutor(
                                        max_workers=numConcurrent) as executor:
      for i in range(0,numPermute):
         #pool.apply_async(semiPartialCorrelationLeiboldVectorize_Randomize,callback = poolResults)
         tasks.append(executor.submit(semiPartialCorrelationLeiboldVectorize, pam, e, nodeMtx, 
                                      randomize=True, fResultsObserved=fResultsObserved))
   #tasks.all()  # look at https://github.com/cjgrady/irksome-broccoli/blob/master/src/singleTile/parallelDijkstra.py#L73    
   probSemiPartialToSumEnv = [t.result()[0] for t in tasks]
   probRsqToSumEnv = [t.result()[1] for t in tasks]
   #pool.close()
   #pool.join()
   
   randSPSum = reduce(np.add,probSemiPartialToSumEnv)
   probSemiPartialEnv = randSPSum/float(numPermute)
   print probSemiPartialEnv
   
   randRSum = reduce(np.add,probRsqToSumEnv)
   probRsqEnv = randRSum/float(numPermute)
   print probRsqEnv
