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
   #def predictorsFn(predictorCol, **kwargs):
   def predictorsFn(predictorCol, predictors, swDiagonoal, stdPSum, resultRsq, totalPSumRedidual, nodeNumber):
      #predictors, SumSites, stdPSum, resultRsq, totalPSumRedidual, iDictNode['y']
      #         d =  {'predictors' :predictors,'swDiagonoal': SumSites, 
      #               'stdPSum':stdPSum,'resultRsq':resultRsq,'totalPSumRedidual':totalPSumRedidual,
      #               'nodeNumber':iDictNode['y']}
      """
      @summary: applied across column axis for predictor matrix.
      predictor matrix can be either env or hist biogeography
      """
      try:
         # needs the node number
         #predictors = kwargs['predictors']
         #swDiagonoal = kwargs['swDiagonoal']
         #stdPSum  = kwargs['stdPSum']
         #resultRsq = kwargs['resultRsq']
         #totalPSumRedidual = kwargs['totalPSumRedidual']
         
         #nodeNumber = kwargs['nodeNumber']
         predNumber = iDictPred['x']  # 'x' axis of results
         
         
         ithPredictor = np.array([predictorCol]).T
         withoutIthPredictor = np.delete(predictors,predNumber,axis=1)  
         
         # % slope for the ith predictor, Beta, regression coefficient
         q,r = np.linalg.qr(np.dot(np.einsum('ij,j->ij',ithPredictor.T,swDiagonoal),ithPredictor))
         
         RdivQT = np.linalg.lstsq(r,q.T)[0]
         
         IthsSlope_part = np.dot(RdivQT,ithPredictor.T)
         IthsSlope_second_part = np.einsum('ij,j->ij',IthsSlope_part,swDiagonoal)
         IthSlope = np.dot(IthsSlope_second_part,stdPSum)
         
         
         # % regression for the remaining predictors
         q,r = np.linalg.qr(np.dot(np.einsum('ij,j->ij',withoutIthPredictor.T,swDiagonoal),withoutIthPredictor))
         RdivQT_r = np.linalg.lstsq(r,q.T)[0]
         WithoutPredRQ_r = np.dot(withoutIthPredictor,RdivQT_r)
         H_part = np.dot(WithoutPredRQ_r,withoutIthPredictor.T)
         H = np.einsum('ij,j->ij',H_part,swDiagonoal)
         Predicted = np.dot(H,stdPSum)
         RemainingRsq = np.sum(Predicted**2)/np.sum(stdPSum**2)
         
         if (resultRsq - RemainingRsq) >= 0:
            resultSP = IthSlope * ((resultRsq - RemainingRsq)**.5) / np.absolute(IthSlope)
         else:
            print 'a'
            resultSP = np.array([0.0])
            
         FSemiPartial = (resultRsq - RemainingRsq)/totalPSumRedidual
         if not randomize:
            resultFSemiPartialMtx[nodeNumber][predNumber] = FSemiPartial
         else:
            if 'FSemiPartial' in fResultsObserved:
               if FSemiPartial >= fResultsObserved['FSemiPartial'][nodeNumber][predNumber]:
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
      
      SpeciesPresentAtNode = np.where(nodeCol != 0)[0]
      
      if randomize:
         # move columns around
         SpeciesPresentAtNodeRand = np.random.permutation(SpeciesPresentAtNode)
         incidence = pam[:,SpeciesPresentAtNodeRand]
         
      else:
         incidence = pam[:,SpeciesPresentAtNode]  # might want to use a take here
      
      # added Jeff, find if any of the columns in sliced incidence are all zero
      bs = np.any(incidence, axis=0)
      emptyCol = np.where(bs == False)[0]
      #############
      
     
      ###########
      # find rows in incidence that are all zero
      bs = np.any(incidence,axis=1)  # bolean selection row-wise logical OR
      EmptySites = np.where(bs == False)[0]  # position of deletes
      incidence = np.delete(incidence,EmptySites,0)  # delete rows
      
      if incidence.shape[0] > 1:# and len(emptyCol) == 0: # might not need this last clause, get more good nodes for Tashi without it
         
         #print "node number ",NodeNumber
         predictors = predictorMtx
         predictors = np.delete(predictors,EmptySites,0) # delete rows
         NumberSites = incidence.shape[0]
         
         if randomize:
            # move rows around
            incidence = np.random.permutation(incidence)
         
         #######################
         
         if not randomize:
            if (numPredictors > (NumberSites -2)) or (len(np.where(np.var(predictors,axis=0) == 0)[0]) > 0):  # column-wise variance
            
               resultSemi = np.array([np.zeros(numPredictors)])
               resultSemiPartialMtx[iDictNode['y']] = resultSemi[0]
               return np.array([])
         
         TotalSum = np.sum(incidence)
         SumSites = np.sum(incidence,axis = 1)  # sum of the rows, alpha
         SumSpecies = np.sum(incidence,axis = 0)  # sum of the columns, omega
         NumberSpecies = incidence.shape[1]
         SiteWeights = np.diag(SumSites)   # Wn, used?
         SpeciesWeights = np.diag(SumSpecies) # Wk , used?
         
         try:
            # standardize Predictor, in this case Env matrix
            #Junk: Ones = np.array([np.ones(NumberSites)]).T
            #StdPredictorsNew = standardizeMatrix(SumSites, predictors)#, Ones, incidence)
            
            predOnes = np.ones((NumberSites, 1))
            StdPredictors = standardizeMatrixOld(SiteWeights, predictors, predOnes, incidence)
            
            ## P standardize 
            #Junk: Ones = np.array([np.ones(NumberSpecies)]).T
            
            nodeOnes = np.ones((NumberSpecies, 1))
            
            #StdNode = standardizeMatrix(SumSpecies, nodeCol[SpeciesPresentAtNode])#, Ones, incidence)
            StdNode = standardizeMatrixOld(SpeciesWeights, nodeCol[SpeciesPresentAtNode], nodeOnes, incidence)
              
         except Exception, e:
            print str(e)
            raise e
            resultSemi = np.array([np.zeros(numPredictors)])
         else:
            
            # PsigStd
            stdPSum = np.dot(incidence,StdNode)  
            
            # regression #############3
            #q,r = np.linalg.qr(np.dot(np.dot(StdPredictors.T,SiteWeights),StdPredictors))
            q,r = np.linalg.qr(np.dot(StdPredictors.T * SumSites, StdPredictors))

            RdivQT = np.linalg.lstsq(r,q.T)[0]
            
            StdPredRQ = np.dot(StdPredictors,RdivQT)
            
            
            # H is BetaAll
            #H = np.dot(np.dot(StdPredRQ,StdPredictors.T),SiteWeights)  # WON'T SCALE!!
            H_first = np.dot(StdPredRQ,StdPredictors.T)
            H = np.einsum('ij,j->ij',H_first,SumSites)
            
            Predicted =  np.dot(H,stdPSum)
            totalPSumRedidual = np.sum((stdPSum-Predicted)**2)
            
            stdPSumSqrs = np.sum(stdPSum**2)
            
            if  stdPSumSqrs != 0:
               ##### R Squared ####
               resultRsq = np.sum(Predicted**2)/stdPSumSqrs  
               ################################################3
               
               #% adjusted Rsq  (classic method) should be interpreted with some caution as the degrees of
               #% freedom for weighted models are different from non-weighted models
               #% adjustments based on effective degrees of freedom should be considered
               
               FGlobal = np.sum(Predicted**2)/totalPSumRedidual
               
               if not randomize:
                  if NumberSites-numPredictors-1 > 0:                  
                     RsqAdj = 1 - (((NumberSites-1)/(NumberSites-numPredictors-1))*(1-resultRsq))   
                  else:
                     RsqAdj = -999
                     
                  resultRsqAdjMtx[iDictNode['y']] = RsqAdj
                  resultFGlobalMtx[iDictNode['y']] = FGlobal
               else:
                  if 'FGlobal' in fResultsObserved:
                     if FGlobal >= fResultsObserved['FGlobal'][iDictNode['y']]:
                        vectProbRsq[iDictNode['y']] = 1 # vectProbRsq[iDictNode['y']] + 1
                        
               # semi partial correlations 
               d =  {'predictors' :predictors,'swDiagonoal': SumSites, 
                     'stdPSum':stdPSum,'resultRsq':resultRsq,'totalPSumRedidual':totalPSumRedidual,
                     'nodeNumber':iDictNode['y']}
               # sending whole Predictor mtx to predictorsFn func, and feeding it to apply_along_axis, feeds one col. at a time, 0 axis
               # 3 significance done: resultRsq, RsqAdj,FGlobal
               
               #resultSemi = np.apply_along_axis(predictorsFn, 0, predictors, **d)
               resultSemi = np.apply_along_axis(predictorsFn, 0, predictors, *(predictors, SumSites, stdPSum, resultRsq, totalPSumRedidual, iDictNode['y']))
               
                  
            else:
               resultSemi = np.array([np.zeros(numPredictors)])
      else:
         resultSemi = np.array([np.zeros(numPredictors)])
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

def appendENVtoBG(B,E):
   """
   @summary: appends E to B to control for E in B
   """
   B = B.astype(np.float)
   B = np.concatenate((B,E),axis=1)
   return B

def correctPValue(PValues):
   """
   @summary: correction routine
   @todo: need to test correcting vector
   """
   corrected = correct_pvalues_for_multiple_testing(PValues)
   return corrected

def calculateMCPA(pam, P, Pred, FGlobal=False, FSemiPartial=False, numPermute=0, numConcurrent=1, divisor=None): #calculateMCPA(pam, P, E, B, randomize=False, numPermute=0):
   """
   @summary: sends inputs to calculate
   """
   
   if FGlobal and FSemiPartial:
      fResultsObserved = {'FGlobal':FGlobal ,'FSemiPartial':FSemiPartial}
      tasks = []
      with concurrent.futures.ProcessPoolExecutor(
                                           max_workers=numConcurrent) as executor:
         for i in range(0,numPermute):
            tasks.append(executor.submit(semiPartCorrelation_Leibold_Vectorize, pam, Pred, P, 
                                         randomize=True, fResultsObserved=fResultsObserved))
      ProbSPtosum = [t.result()[0] for t in tasks]
      ProbRsqtosum = [t.result()[1] for t in tasks]
      SP_Result = sumProbabilities(ProbSPtosum,divisor)
      Rsq_Result = sumProbabilities(ProbRsqtosum,divisor)
      return SP_Result, Rsq_Result
   else:
      rSemiPartialMtx, rRsqAdjVct,rFSemiPartialMtx, rFGlobalMtx = semiPartCorrelation_Leibold_Vectorize(pam,Pred,P)
      return rSemiPartialMtx, rRsqAdjVct,rFSemiPartialMtx, rFGlobalMtx



if __name__ == "__main__":
   
   import cPickle
   import testWithData
   
   nodeMtx,pam = testWithData.makeInputsForTextTest()
   E = testWithData.getEnvTextMatrix()
   ########## Environmental ###########
   
   rSemiPartialMtx_E, rRsqAdjVct_E,rFSemiPartialMtx_E, rFGlobalMtx_E = calculateMCPA(pam, nodeMtx, E)
   fResultsObserved = {'FGlobal':rFGlobalMtx_E ,'FSemiPartial':rFSemiPartialMtx_E} # setting global
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
         tasks.append(executor.submit(semiPartCorrelation_Leibold_Vectorize, pam, E, nodeMtx, 
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
