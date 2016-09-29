import os,sys
import numpy as np
import simplejson as json
import warnings   #  for divide by zero
from multiprocessing import Process, Queue, Pool 
from Pedro_Analysis.MCPA.P_Value_Correction import correct_pvalues_for_multiple_testing
import testWithData
import concurrent.futures
import cPickle
warnings.filterwarnings('error')
np.seterr(all='warn')


############  Analysis ################

def stdMtx(W,M,OnesCol,I):
   """
   @param W: diagonal mtx
   @param M: mtx to be std
   @param OnesCol: column vector of ones, sites (n) or species (k)
   @param I: Incidence mtx, PAM 
   """
   TotalSum = float(I.sum())
   
   SiteWeights = W
   
   sPred = np.dot(np.dot(OnesCol.T,SiteWeights),M)
   sPred2 = np.dot(np.dot(OnesCol.T,SiteWeights),(M*M))
   
   MeanWeightedPred = sPred/TotalSum
   
   StdDevWeightedPred = ((sPred2-(sPred**2.0/TotalSum))/(TotalSum))**.5
   
   t = np.dot(OnesCol,StdDevWeightedPred)
   
   Std = ((np.dot(OnesCol,StdDevWeightedPred))**-1.0) * (M-np.dot(OnesCol,MeanWeightedPred))
   
   return Std



# ........................................
def semiPartCorrelation_Leibold_Vectorize(I, PredictorMtx, NodeMtx, randomize=False, 
                                          FresultsObserved={}): 
   """
   @summary: follows Pedro's matlab code as far as loops
   that are necessary for treating nodes individually. Exceptions to his code are mathematical changes
   for effeciency, and corrections for sps in tree but not in PAM, also fault checks.
   @param FresultsObserved: contains two F score matrices
   @param NodeMtx: Phylo Encoding (species (row) x node (column)
   @param I: Incidence Mtx  (PAM) 
   """
   
   IncidenceMtx = I
   NumberNodes = NodeMtx.shape[1] 
   NumberPredictors = PredictorMtx.shape[1]  
   iDictPred = {}
   iDictNode = {'y':0}
   
   if not randomize:
      # put results here
      resultSemiPartialMtx = np.zeros((NumberNodes,NumberPredictors))
      resultFSemiPartialMtx = np.zeros((NumberNodes,NumberPredictors))
      resultRsqAdjMtx = np.array([np.zeros(NumberNodes)]).T
      resultFGlobalMtx = np.array([np.zeros(NumberNodes)]).T                       
  
   else:
      
      MatrixProbSemiPartial = np.zeros((NumberNodes,NumberPredictors))
      VectorProbRsq = np.array([np.zeros(NumberNodes)]).T
      
   def predictors(predictorCol, **kwargs):
      """
      @summary: applied across column axis for predictor matrix.
      predictor matrix can be either env or hist biogeography
      """
      try:
         # needs the node number
         Predictors = kwargs['Predictors']
         swDiagonoal = kwargs['swDiagonoal']
         StdPSum  = kwargs['StdPSum']
         resultRsq = kwargs['resultRsq']
         TotalPSumResidual = kwargs['TotalPSumResidual']
         
         nodeNumber = kwargs['nodeNumber']
         predNumber = iDictPred['x']  # 'x' axis of results
         
         
         IthPredictor = np.array([predictorCol]).T
         WithoutIthPredictor = np.delete(Predictors,predNumber,axis=1)  
         
         # % slope for the ith predictor, Beta, regression coefficient
         Q,R = np.linalg.qr(np.dot(np.einsum('ij,j->ij',IthPredictor.T,swDiagonoal),IthPredictor))
         
         RdivQT = np.linalg.lstsq(R,Q.T)[0]
         
         IthsSlope_part = np.dot(RdivQT,IthPredictor.T)
         IthsSlope_second_part = np.einsum('ij,j->ij',IthsSlope_part,swDiagonoal)
         IthSlope = np.dot(IthsSlope_second_part,StdPSum)
         
         
         # % regression for the remaining predictors
         Q,R = np.linalg.qr(np.dot(np.einsum('ij,j->ij',WithoutIthPredictor.T,swDiagonoal),WithoutIthPredictor))
         RdivQT_r = np.linalg.lstsq(R,Q.T)[0]
         WithoutPredRQ_r = np.dot(WithoutIthPredictor,RdivQT_r)
         H_part = np.dot(WithoutPredRQ_r,WithoutIthPredictor.T)
         H = np.einsum('ij,j->ij',H_part,swDiagonoal)
         Predicted = np.dot(H,StdPSum)
         RemainingRsq = np.sum(Predicted**2)/np.sum(StdPSum**2)
         
         if (resultRsq - RemainingRsq) >= 0:
            resultSP = IthSlope * ((resultRsq - RemainingRsq)**.5) / np.absolute(IthSlope)
         else:
            resultSP = np.array([0.0])
            
         FSemiPartial = (resultRsq - RemainingRsq)/TotalPSumResidual
         if not randomize:
            resultFSemiPartialMtx[nodeNumber][predNumber] = FSemiPartial
         else:
            if 'FSemiPartial' in FresultsObserved:
               if FSemiPartial >= FresultsObserved['FSemiPartial'][nodeNumber][predNumber]:
                  MatrixProbSemiPartial[nodeNumber][predNumber] = 1 #MatrixProbSemiPartial[nodeNumber][predNumber] +1
                  
         iDictPred['x'] += 1
      except Exception, e:
         resultSP = np.array([0.0])

      return resultSP
   
   def nodes(nodeCol):
      """
      @summary: operation to be performed on each node column
      """
      
      iDictPred['x'] = 0
      
      SpeciesPresentAtNode = np.where(nodeCol != 0)[0]
      
      if randomize:
         # move columns around
         SpeciesPresentAtNodeRand = np.random.permutation(SpeciesPresentAtNode)
         Incidence = IncidenceMtx[:,SpeciesPresentAtNodeRand]
         
      else:
         Incidence = IncidenceMtx[:,SpeciesPresentAtNode]  # might want to use a take here
      
      # added Jeff, find if any of the columns in sliced Incidence are all zero
      bs = np.any(Incidence, axis=0)
      emptyCol = np.where(bs == False)[0]
      #############
      
     
      ###########
      # find rows in Incidence that are all zero
      bs = np.any(Incidence,axis=1)  # bolean selection row-wise logical OR
      EmptySites = np.where(bs == False)[0]  # position of deletes
      Incidence = np.delete(Incidence,EmptySites,0)  # delete rows
      
      if Incidence.shape[0] > 1:# and len(emptyCol) == 0: # might not need this last clause, get more good nodes for Tashi without it
         
         #print "node number ",NodeNumber
         Predictors = PredictorMtx
         Predictors = np.delete(Predictors,EmptySites,0) # delete rows
         NumberSites = Incidence.shape[0]
         
         if randomize:
            # move rows around
            Incidence = np.random.permutation(Incidence)
         
         #######################
         
         if not randomize:
            if (NumberPredictors > (NumberSites -2)) or (len(np.where(np.var(Predictors,axis=0) == 0)[0]) > 0):  # column-wise variance
            
               resultSemi = np.array([np.zeros(NumberPredictors)])
               resultSemiPartialMtx[iDictNode['y']] = resultSemi[0]
               return np.array([])
         
         TotalSum = np.sum(Incidence)
         SumSites = np.sum(Incidence,axis = 1)  # sum of the rows, alpha
         SumSpecies = np.sum(Incidence,axis = 0)  # sum of the columns, omega
         NumberSpecies = Incidence.shape[1]
         SiteWeights = np.diag(SumSites)   # Wn
         SpeciesWeights = np.diag(SumSpecies) # Wk
         
         try:
            # standardize Predictor, in this case Env matrix
            Ones = np.array([np.ones(NumberSites)]).T
            StdPredictors = stdMtx(SiteWeights, Predictors, Ones, Incidence)
            
            ## P standardize 
            Ones = np.array([np.ones(NumberSpecies)]).T
            StdNode = stdMtx(SpeciesWeights, nodeCol[SpeciesPresentAtNode], Ones, Incidence)
              
         except:
            resultSemi = np.array([np.zeros(NumberPredictors)])
         else:
            
            # PsigStd
            StdPSum = np.dot(Incidence,StdNode)  
            
            # regression #############3
            Q,R = np.linalg.qr(np.dot(np.dot(StdPredictors.T,SiteWeights),StdPredictors))
           
            RdivQT = np.linalg.lstsq(R,Q.T)[0]
            
            StdPredRQ = np.dot(StdPredictors,RdivQT)
            
            
            swDiagonoal = np.diagonal(SiteWeights)
            
            # H is BetaAll
            #H = np.dot(np.dot(StdPredRQ,StdPredictors.T),SiteWeights)  # WON'T SCALE!!
            H_first = np.dot(StdPredRQ,StdPredictors.T)
            H = np.einsum('ij,j->ij',H_first,swDiagonoal)
            
            Predicted =  np.dot(H,StdPSum)
            TotalPSumResidual = np.sum((StdPSum-Predicted)**2)
            
            StdPSumSqrs = np.sum(StdPSum**2)
            
            if  StdPSumSqrs != 0:
               ##### R Squared ####
               resultRsq = np.sum(Predicted**2)/StdPSumSqrs  
               ################################################3
               
               #% adjusted Rsq  (classic method) should be interpreted with some caution as the degrees of
               #% freedom for weighted models are different from non-weighted models
               #% adjustments based on effective degrees of freedom should be considered
               
               FGlobal = np.sum(Predicted**2)/TotalPSumResidual
               
               if not randomize:
                  if NumberSites-NumberPredictors-1 > 0:                  
                     RsqAdj = 1 - (((NumberSites-1)/(NumberSites-NumberPredictors-1))*(1-resultRsq))   
                  else:
                     RsqAdj = -999
                     
                  resultRsqAdjMtx[iDictNode['y']] = RsqAdj
                  resultFGlobalMtx[iDictNode['y']] = FGlobal
               else:
                  if 'FGlobal' in FresultsObserved:
                     if FGlobal >= FresultsObserved['FGlobal'][iDictNode['y']]:
                        VectorProbRsq[iDictNode['y']] = 1 # VectorProbRsq[iDictNode['y']] + 1
                        
               # semi partial correlations 
               d =  {'Predictors' :Predictors,'swDiagonoal': swDiagonoal, 
                     'StdPSum':StdPSum,'resultRsq':resultRsq,'TotalPSumResidual':TotalPSumResidual,
                     'nodeNumber':iDictNode['y']}
               # sending whole Predictor mtx to predictors func, and feeding it to apply_along_axis, feeds one col. at a time, 0 axis
               # 3 significance done: resultRsq, RsqAdj,FGlobal
               
               resultSemi = np.apply_along_axis(predictors, 0, Predictors, **d)
               
                  
            else:
               resultSemi = np.array([np.zeros(NumberPredictors)])
      else:
         resultSemi = np.array([np.zeros(NumberPredictors)])
      if not randomize:
         resultSemiPartialMtx[iDictNode['y']] = resultSemi[0]
      
      iDictNode['y'] += 1    
        
      return np.array([])      
   
   np.apply_along_axis(nodes, 0, NodeMtx)
   
   
   if randomize:
      return MatrixProbSemiPartial, VectorProbRsq
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

def calculateMCPA(I, P, Pred, FGlobal=False, FSemiPartial=False, numPermute=0, numConcurrent=1, divisor=None): #calculateMCPA(I, P, E, B, randomize=False, numPermute=0):
   """
   @summary: sends inputs to calculate
   """
   
   if FGlobal and FSemiPartial:
      FresultsObserved = {'FGlobal':FGlobal ,'FSemiPartial':FSemiPartial}
      tasks = []
      with concurrent.futures.ProcessPoolExecutor(
                                           max_workers=numConcurrent) as executor:
         for i in range(0,numPermute):
            tasks.append(executor.submit(semiPartCorrelation_Leibold_Vectorize, I, Pred, P, 
                                         randomize=True, FresultsObserved=FresultsObserved))
      ProbSPtosum = [t.result()[0] for t in tasks]
      ProbRsqtosum = [t.result()[1] for t in tasks]
      SP_Result = sumProbabilities(ProbSPtosum,divisor)
      Rsq_Result = sumProbabilities(ProbRsqtosum,divisor)
      return SP_Result, Rsq_Result
   else:
      rSemiPartialMtx, rRsqAdjVct,rFSemiPartialMtx, rFGlobalMtx = semiPartCorrelation_Leibold_Vectorize(I,Pred,P)
      return rSemiPartialMtx, rRsqAdjVct,rFSemiPartialMtx, rFGlobalMtx



if __name__ == "__main__":
   
   NodeMtx,I = testWithData.makeInputsForTextTest()
   E = testWithData.getEnvTextMatrix()
   ########## Environmental ###########
   
   rSemiPartialMtx_E, rRsqAdjVct_E,rFSemiPartialMtx_E, rFGlobalMtx_E = calculateMCPA(I, NodeMtx, E)
   FresultsObserved = {'FGlobal':rFGlobalMtx_E ,'FSemiPartial':rFSemiPartialMtx_E} # setting global
   cPickle.dump(FresultsObserved,open('/home/jcavner/compare_corr/concurrent/FScores.pkl','wb'))
   
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
         tasks.append(executor.submit(semiPartCorrelation_Leibold_Vectorize, I, E, NodeMtx, 
                                      randomize=True, FresultsObserved=FresultsObserved))
   #tasks.all()  # look at https://github.com/cjgrady/irksome-broccoli/blob/master/src/singleTile/parallelDijkstra.py#L73    
   ProbSPtosum_E = [t.result()[0] for t in tasks]
   ProbRsqtosum_E = [t.result()[1] for t in tasks]
   #pool.close()
   #pool.join()
   
   randSPSum = reduce(np.add,ProbSPtosum_E)
   P_SP_E = randSPSum/float(numPermute)
   
   randRSum = reduce(np.add,ProbRsqtosum_E)
   P_R_E = randRSum/float(numPermute)
