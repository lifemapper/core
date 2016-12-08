"""
@summary: This module contains functions to perform MCPA
@author: Jeff Cavner (edited by CJ Grady)

"""
import concurrent.futures
import numpy as np
import os
import sys


from P_Value_Correction import correct_pvalues_for_multiple_testing



############  Analysis ################


# .............................................................................
def standardizeMatrix(siteWeights, mtx):
   """
   @todo: Update documentation
   @summary: Standardizes matrix, mtx
   @param siteWeights: Diagonal matrix for site weights (W in math?)
   @param mtx: Matrix to be standardized (M in math?)
   @param onesCol: Column vector of ones, sites (n) or species (k)
   @param pam: Incidence matrix (PAM) (I in math)
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

def standardizeMatrixOld(siteWeights, mtx, onesCol, pam):
   """
   @todo: Remove
   @summary: Standardizes matrix, mtx
   @param siteWeights: Diagonal matrix for site weights (W in math?)
   @param mtx: Matrix to be standardized (M in math?)
   @param onesCol: Column vector of ones, sites (n) or species (k)
   @param pam: Incidence matrix (PAM) (I in math)
   """
   # Get the sum of the incidence matrix
   iSum = float(pam.sum()) 
   
   sPred = np.dot(np.dot(onesCol.T, siteWeights),mtx)
   sPred2 = np.dot(np.dot(onesCol.T, siteWeights),(mtx**2))
   
   meanWeightedPred = sPred/iSum
   
   stdDevWeightedPred = ((sPred2-(sPred**2.0/iSum))/(iSum))**.5
   
   stdMtx = ((np.dot(onesCol,stdDevWeightedPred))**-1.0) * (mtx-np.dot(onesCol,meanWeightedPred))
   
   return stdMtx
   

# ........................................
def semiPartCorrelation_Leibold_Vectorize(pam, predictorMtx, nodeMtx, randomize=False, 
                                          fResultsObserved={}): 
   """
   @todo: Fix documentation
   @todo: Variable names
   @todo: Function signature
   @summary: follows Pedro's matlab code as far as loops
   that are necessary for treating nodes individually. Exceptions to his code are mathematical changes
   for effeciency, and corrections for sps in tree but not in PAM, also fault checks.
   @param fResultsObserved: contains two F score matrices
   @param nodeMtx: Phylo Encoding (species (row) x node (column)
   @param pam: Incidence Mtx  (PAM) 
   """
   numNodes = nodeMtx.shape[1] 
   numPredictors = predictorMtx.shape[1]  
   iDictPred = {}
   iDictNode = {'y':0}
   
   if not randomize:
      # put results here
      resultSemiPartialMtx = np.zeros((numNodes,numPredictors))
      resultFSemiPartialMtx = np.zeros((numNodes,numPredictors))
      resultRsqAdjMtx = np.array([np.zeros(numNodes)]).T
      resultFGlobalMtx = np.array([np.zeros(numNodes)]).T                       
  
   else:
      
      mtxProbSemiPartial = np.zeros((numNodes,numPredictors))
      vectProbRsq = np.array([np.zeros(numNodes)]).T
      
   # ...........................
   def predictorsFn(predictorCol, predictors, swDiagonoal, stdPSum, resultRsq, totalPSumResidual, nodeNumber):
      """
      @todo: Fix documentation
      @summary: applied across column axis for predictor matrix.
      predictor matrix can be either env or hist biogeography
      @todo: Document each variable
      """
      try:
         predNumber = iDictPred['x']  # 'x' axis of results
         
         
         ithPredictor = np.array([predictorCol]).T
         withoutIthPredictor = np.delete(predictors,predNumber,axis=1)  
         
         # % slope for the ith predictor, Beta, regression coefficient
         q,r = np.linalg.qr(np.dot(np.einsum('ij,j->ij',ithPredictor.T,swDiagonoal),ithPredictor))
         
         rDivQtrans = np.linalg.lstsq(r,q.T)[0]
         
         ithSlopPart1 = np.dot(rDivQtrans,ithPredictor.T)
         ithSlopePart2 = np.einsum('ij,j->ij',ithSlopPart1,swDiagonoal)
         ithSlope = np.dot(ithSlopePart2,stdPSum)
         
         
         # % regression for the remaining predictors
         q,r = np.linalg.qr(np.dot(np.einsum('ij,j->ij',withoutIthPredictor.T,swDiagonoal),withoutIthPredictor))
         rDivQtransR = np.linalg.lstsq(r,q.T)[0]
         withoutPredRQR = np.dot(withoutIthPredictor,rDivQtransR)
         hPart = np.dot(withoutPredRQR,withoutIthPredictor.T)
         h = np.einsum('ij,j->ij',hPart,swDiagonoal)
         predicted = np.dot(h,stdPSum)
         remainingRsq = np.sum(predicted**2)/np.sum(stdPSum**2)
         
         if (resultRsq - remainingRsq) >= 0:
            resultSP = ithSlope * ((resultRsq - remainingRsq)**.5) / np.absolute(ithSlope)
         else:
            resultSP = np.array([0.0])
            
         fSemiPartial = (resultRsq - remainingRsq)/totalPSumResidual
         if not randomize:
            resultFSemiPartialMtx[nodeNumber][predNumber] = fSemiPartial
         else:
            if 'fSemiPartial' in fResultsObserved:
               if fSemiPartial >= fResultsObserved['fSemiPartial'][nodeNumber][predNumber]:
                  mtxProbSemiPartial[nodeNumber][predNumber] = 1 #mtxProbSemiPartial[nodeNumber][predNumber] +1
                  
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
      #############
      
     
      ###########
      # find rows in incidence that are all zero
      bs = np.any(incidence,axis=1)  # bolean selection row-wise logical OR
      emptySites = np.where(bs == False)[0]  # position of deletes
      incidence = np.delete(incidence,emptySites,0)  # delete rows
      
      if incidence.shape[0] > 1:# and len(emptyCol) == 0: # might not need this last clause, get more good nodes for Tashi without it
         
         #print "node number ",NodeNumber
         predictors = predictorMtx
         predictors = np.delete(predictors,emptySites,0) # delete rows
         numSites = incidence.shape[0]
         
         if randomize:
            # move rows around
            incidence = np.random.permutation(incidence)
         
         #######################
         
         if not randomize:
            if (numPredictors > (numSites -2)) or (len(np.where(np.var(predictors,axis=0) == 0)[0]) > 0):  # column-wise variance
            
               resultSemi = np.array([np.zeros(numPredictors)])
               resultSemiPartialMtx[iDictNode['y']] = resultSemi[0]
               return np.array([])
         
         sumSites = np.sum(incidence,axis = 1)  # sum of the rows, alpha
         sumSpecies = np.sum(incidence,axis = 0)  # sum of the columns, omega
         numSpecies = incidence.shape[1]
         siteWeights = np.diag(sumSites)   # Wn, used?
         speciesWeights = np.diag(sumSpecies) # Wk , used?
         
         try:
            # standardize Predictor, in this case Env matrix
            #StdPredictorsNew = standardizeMatrix(sumSites, predictors)#, Ones, incidence)
            
            predOnes = np.ones((numSites, 1))
            stdPredictors = standardizeMatrixOld(siteWeights, predictors, predOnes, incidence)
            
            ## p standardize 
            nodeOnes = np.ones((numSpecies, 1))
            
            #stdNode = standardizeMatrix(sumSpecies, nodeCol[speciesPresentAtNode])#, Ones, incidence)
            stdNode = standardizeMatrixOld(speciesWeights, nodeCol[speciesPresentAtNode], nodeOnes, incidence)
              
         except Exception, e:
            print str(e)
            raise e
            resultSemi = np.array([np.zeros(numPredictors)])
         else:
            
            # PsigStd
            stdPSum = np.dot(incidence,stdNode)  
            
            # regression #############3
            #q,r = np.linalg.qr(np.dot(np.dot(stdPredictors.T,siteWeights),stdPredictors))
            q,r = np.linalg.qr(np.dot(stdPredictors.T * sumSites, stdPredictors))

            rDivQtrans = np.linalg.lstsq(r,q.T)[0]
            
            stdPredRQ = np.dot(stdPredictors,rDivQtrans)
            
            
            # h is BetaAll
            #h = np.dot(np.dot(stdPredRQ,stdPredictors.T),siteWeights)  # WON'T SCALE!!
            hFirst = np.dot(stdPredRQ,stdPredictors.T)
            h = np.einsum('ij,j->ij',hFirst,sumSites)
            
            predicted =  np.dot(h,stdPSum)
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
                     rSqAdj = 1 - (((numSites-1)/(numSites-numPredictors-1))*(1-resultRsq))   
                  else:
                     rSqAdj = -999
                     
                  resultRsqAdjMtx[iDictNode['y']] = rSqAdj
                  resultFGlobalMtx[iDictNode['y']] = fGlobal
               else:
                  if 'fGlobal' in fResultsObserved:
                     if fGlobal >= fResultsObserved['fGlobal'][iDictNode['y']]:
                        vectProbRsq[iDictNode['y']] = 1 # vectProbRsq[iDictNode['y']] + 1
                        
               # semi partial correlations 
               # sending whole Predictor mtx to predictorsFn func, and feeding it to apply_along_axis, feeds one col. at a time, 0 axis
               # 3 significance done: resultRsq, rSqAdj,fGlobal
               
               resultSemi = np.apply_along_axis(predictorsFn, 0, predictors, *(predictors, sumSites, stdPSum, resultRsq, totalPSumResidual, iDictNode['y']))
               
                  
            else:
               resultSemi = np.array([np.zeros(numPredictors)]) #TODO: Fix
      else:
         resultSemi = np.array([np.zeros(numPredictors)]) #TODO: Fix
      if not randomize:
         resultSemiPartialMtx[iDictNode['y']] = resultSemi[0]
      
      iDictNode['y'] += 1    
        
      return np.array([])      
   
   np.apply_along_axis(nodes, 0, nodeMtx)
   
   
   if randomize:
      return mtxProbSemiPartial, vectProbRsq
   else: 
      return resultSemiPartialMtx,resultRsqAdjMtx,resultFSemiPartialMtx,resultFGlobalMtx 
# ........................................
############ End Analysis #################
 
  
def sumProbabilities(toSum, divisor=None):
   """
   @note: if divisor exists divide sum by it, return mean. if not return sum
   """
   valuesSummed = reduce(np.add,toSum)
   if divisor is not None:
      return valuesSummed/float(divisor)
   else:
      return valuesSummed

def appendENVtoBG(b, e):
   """
   @summary: appends e to b to control for e in b
   """
   b = b.astype(np.float)
   b = np.concatenate((b,e),axis=1)
   return b

def correctPValue(pValues):
   """
   @summary: correction routine
   @todo: need to test correcting vector
   """
   corrected = correct_pvalues_for_multiple_testing(pValues)
   return corrected

def calculateMCPA(pam, p, pred, fGlobal=False, fSemiPartial=False, numPermute=0, numConcurrent=1, divisor=None): 
   #calculateMCPA(pam, p, E, B, randomize=False, numPermute=0):
   """
   @summary: sends inputs to calculate
   """
   
   if fGlobal and fSemiPartial:
      fResultsObserved = {'fGlobal':fGlobal ,'fSemiPartial':fSemiPartial}
      tasks = []
      with concurrent.futures.ProcessPoolExecutor(
                                           max_workers=numConcurrent) as executor:
         for i in range(0,numPermute):
            tasks.append(executor.submit(semiPartCorrelation_Leibold_Vectorize, pam, pred, p, 
                                         randomize=True, fResultsObserved=fResultsObserved))
      probSemiPartialsToSum = [t.result()[0] for t in tasks]
      probRsqToSum = [t.result()[1] for t in tasks]
      semiPartialResult = sumProbabilities(probSemiPartialsToSum,divisor)
      rSqResult = sumProbabilities(probRsqToSum,divisor)
      return semiPartialResult, rSqResult
   else:
      rSemiPartialMtx, rRsqAdjVct,rFSemiPartialMtx, rFGlobalMtx = semiPartCorrelation_Leibold_Vectorize(pam,pred,p)
      return rSemiPartialMtx, rRsqAdjVct,rFSemiPartialMtx, rFGlobalMtx



if __name__ == "__main__":
   
   import cPickle
   import testWithData
   
   nodeMtx,pam = testWithData.makeInputsForTextTest()
   e = testWithData.getEnvTextMatrix()
   ########## Environmental ###########
   
   rSemiPartialMtx_E, rRsqAdjVct_E,rFSemiPartialMtx_E, rFGlobalMtx_E = calculateMCPA(pam, nodeMtx, e)
   fResultsObserved = {'fGlobal':rFGlobalMtx_E ,'fSemiPartial':rFSemiPartialMtx_E} # setting global
   cPickle.dump(fResultsObserved,open('/tmp/FScores.pkl','wb'))
   
   ## random calculations
   ProbSPtosum_E = []
   ProbRsqtosum_E = []
   numPermute = 10
   #pool = Pool()  # can parameterize to number of cores, otherwise no arg uses all available
   numConcurrent = 5
   tasks = []
   with concurrent.futures.ProcessPoolExecutor(
                                        max_workers=numConcurrent) as executor:
      for i in range(0,numPermute):
         #pool.apply_async(semiPartCorrelation_Leibold_Vectorize_Randomize,callback = poolResults)
         tasks.append(executor.submit(semiPartCorrelation_Leibold_Vectorize, pam, e, nodeMtx, 
                                      randomize=True, fResultsObserved=fResultsObserved))
   #tasks.all()  # look at https://github.com/cjgrady/irksome-broccoli/blob/master/src/singleTile/parallelDijkstra.py#L73    
   ProbSPtosum_E = [t.result()[0] for t in tasks]
   ProbRsqtosum_E = [t.result()[1] for t in tasks]
   #pool.close()
   #pool.join()
   
   randSPSum = reduce(np.add,ProbSPtosum_E)
   P_SP_E = randSPSum/float(numPermute)
   print P_SP_E
   
   randRSum = reduce(np.add,ProbRsqtosum_E)
   P_R_E = randRSum/float(numPermute)
   print P_R_E
