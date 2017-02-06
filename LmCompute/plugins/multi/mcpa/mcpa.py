"""
@summary: Module containing functions used to perform a MetaCommunity 
             Phylogenetics Analysis
@author: CJ Grady (originally from Jeff Cavner and converted from MATLAB code 
            in the referenced literature supplemental materials
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
@see: Leibold, m.A., E.P. Economo and P.R. Peres-Neto. 2010. Metacommunity
         phylogenetics: separating the roles of environmental filters and 
         historical biogeography. Ecology letters 13: 1290-1299.
@todo: Original method, randomize in method
@todo: New method, randomize first
"""
from math import sqrt
import numpy as np

from LmCommon.common.matrix import Matrix
from LmCommon.statistics.pValueCorrection import correctPValues

# .............................................................................
def getPValues(observedValue, testValues, numPermutations=None):
   """
   @summary: Gets an (1 or 2 dimension) array of P values where the P value for
                an array location is determined by finding the number of test
                values at corresponding locations are greater than or equal to
                that value and then dividing that number by the number of 
                permutations
   @param observedValue: An array of observed values to use as a reference
   @param testValues: A list of arrays generated from randomizations that will
                         be compared to the observed
   @param numPermutations: (optional) The total number of randomizations 
                              performed.  Divide the P-values by this if 
                              provided.
   @note: This method assumes the inputs are Matrix objects, not plain Numpy
   @todo: How to add metadata
   """
   # Create the P-Values matrix
   pVals = np.zeros(observedValue.data.shape, dtype=float)
   # For each matrix in test values
   for testMtx in testValues:
      # Add 1 where every value in the test matrix is greater than or equal to
      #    the value in the observed value.  Numpy comparisons will create a 
      #    matrix of boolean values for each cell, which when added to the 
      #    pVals matrix will be treated as 1 for True and 0 for False
      
      # If this is a stack
      if len(testMtx.shape) == 3:
         for i in xrange(len(testMtx.data.shape[2])):
            pVals += testMtx.data[:,:,i] >= observedValue.data
      else:
         pVals += testMtx.data >= observedValue.data
   # Scale and return the pVals matrix
   if numPermutations:
      return Matrix(pVals / numPermutations)
   else:
      return Matrix(pVals)

# .............................................................................
def standardizeMatrix(mtx, weights):
   """
   @summary: Standardizes either a phylogenetic or environment matrix
   @param mtx: The matrix to be standardized
   @param weights: A diagonal matrix of weights to use for standardization
   @note: Formula for standardization:
           Mstd = M - 1c.1r.W.M(1/trace(W)) ./ 1c(1r.W(M*M) 
                       - ((1r.W.M)*(1r.W.M))(1/trace(W))(1/trace(W)-1))^0.5
   @note: M - Matrix to be standardized
          W - A k by k diagonal matrix of weights, where each non-zero value is
                the column or row sum (depending on the M) for a incidence 
                matrix
          1r - A row of k ones
          1c - A column of k ones
          trace - Returns the sum of the input matrix
   @note: "./" indicates Hadamard division
   @note: "*" indicates Hadamard multiplication
   @see: Literature supplemental materials
   @note: Code adopted from supplemental material MATLAB code
   @note: This function assumes that mtx and weights are Numpy arrays
   """
   # Create a row of ones, we'll transpose for a column
   ones = np.ones((1, weights.shape[0]), dtype=float)
   # This maps to trace(W)
   totalSum = np.sum(weights)
   
   # s1 = 1r.W.M
   s1 = ones.dot(weights).dot(mtx)
   # s2 = 1r.W.(M*M)
   s2 = ones.dot(weights).dot(mtx*mtx)
   
   meanWeighted = s1 / totalSum
   
   stdDevWeighted = ((s2 - (s1**2.0 / totalSum)) / (totalSum))**0.5
   
   stdMtx = (ones.T.dot(stdDevWeighted)**-1.0) * (mtx - ones.T.dot(meanWeighted))
   
   return stdMtx

# .............................................................................
def mcpaRun(pam, predictorMtx, phyloMtx, randomize=False):
   """
   @summary: Perform an MCPA run on a PAM, predictor matrix, and phylo matrix
   @param pam: The PAM to use for this run (also referred to as an incidence 
                  matrix).  This is the entire PAM and not a subset.
   @param predictorMtx: A matrix of sites by predictors.  The predictors are 
                           environment predictors and can have biogeographic 
                           hypotheses appended
   @param phyloMtx: A species by clade (node) matrix encoded from a 
                       phylogenetic tree
   @param randomize: If this is true, randomize the incidence matrix so that
                        the observed values can be tested against different 
                        permutations to determine significance
   @note: Inputs are assumed to be Matrix objects and outputs are converted to
             Matrix
   @todo: Add metadata to outputs
   """
   # Initialization
   numPredictors = predictorMtx.data.shape[1]
   numNodes = phyloMtx.data.shape[1]
   
   # Adjusted R-squared
   adjRsq = np.zeros((numNodes, 1), dtype=float)
   
   # F Global
   fGlobal = np.zeros((numNodes, 1), dtype=float)

   # Semi partial
   semiPartialMtx = np.zeros((numNodes, numPredictors), dtype=float)
   
   # F Semi partial
   fSemiPartialMtx = np.zeros((numNodes, numPredictors), dtype=float)
   
   # For each node
   for j in xrange(numNodes):
      # Get species present in clade
      speciesPresentAtNode = np.where(phyloMtx.data[:,j] != 0)[0]
      
      if randomize:
         # Shuffle species around
         speciesPresentAtNode = np.random.permutation(speciesPresentAtNode)
         
      # Incidence full matrix is subset of PAM where clade species are present
      incidenceFull = pam.data[:,speciesPresentAtNode]
      
      # Check to see if a site has any presence values
      sitePresent = np.any(incidenceFull, axis=1)
      
      # Empty sites are sites that are not present
      emptySites = np.where(sitePresent == False)[0]
      
      # Remove the empty sites from the incidence matrix
      incidence = np.delete(incidenceFull, emptySites, axis=0)
      
      # Remove sites from predictor matrix
      predictors = np.delete(predictorMtx.data, emptySites, axis=0)
      
      # Get the number of sites and predictors from the shape of the predictors 
      #    matrix
      numSites = predictors.shape[0]
      
      # If we are supposed to randomize, shuffle the rows in the incidence mtx
      if randomize:
         incidence = np.random.permutation(incidence)
   
      # Get site weights
      siteWeights = np.diag(np.sum(incidence, axis=1))
      
      # Get species weights
      speciesWeights = np.diag(np.sum(incidence, axis=0))
      
      # Standardized environmental matrix
      predStd = standardizeMatrix(predictors, siteWeights)
      
      # Standardized P-matrix
      pStd = standardizeMatrix(phyloMtx.data[speciesPresentAtNode, j], 
                                                               speciesWeights)
      
      # Get standardized P-Sigma
      pSigmaStd = np.dot(incidence, pStd)
      
      # Regression
      #q, r = np.linalg.qr(np.dot(np.dot(predStd.T, siteWeights), predStd))
      q, r = np.linalg.qr(predStd.T.dot(siteWeights).dot(predStd))
      
      # r / q.T
      rDivQT = np.linalg.lstsq(r, q.T)[0]

      # H is Beta(all)
      # Jeff's code says this won't scale and instead break it up and do an 
      #    Einstein sum with eStd.dot(rDivQT).dot(predStd.T) and siteWeights
      # np.einsum('ij,j->ij', eStd.dot(rDivQT).dot(predStd.T), siteWeights
      # I haven't seen any documentation about that.  Switch if necessary
      h = predStd.dot(rDivQT).dot(predStd.T).dot(siteWeights)

      predicted = h.dot(pSigmaStd)
      totalPsigmaResidual = np.sum((pSigmaStd - predicted).T.dot(
                                                       pSigmaStd - predicted))
      
      # Calculate R-squared
      rSq = 1.0 * np.sum(predicted.T.dot(predicted)) / np.sum(
                                                   pSigmaStd.T.dot(pSigmaStd))
            
      # Calculate adjusted R-squared
      # (Classic method) Should be interpreted with some caution as the degrees
      #    of freedom for weighted models are different from non-weighted models
      #    adjustments based on effective degrees of freedom should be 
      #    considered
      adjRsq[j, 0] = 1 - ((numSites - 1)/(numSites - numPredictors - 1)
                                                                  ) * (1 - rSq)
      
      fGlobal[j, 0] = np.sum(predicted.T.dot(predicted)) / totalPsigmaResidual
      
      # For each predictor
      for i in range(numPredictors):
         # Get ith predictor, needs to be a column
         ithPredictor = predictors[:,i].reshape(predictors.shape[0], 1)
         # Get predictors without ith
         woIthPredictor = np.delete(predictors, i, axis=1)
         
         # Slope for ith predictor
         q, r = np.linalg.qr(ithPredictor.T.dot(siteWeights).dot(ithPredictor))
         ithRdivQT = np.linalg.lstsq(r, q.T)[0]
         ithSlope = ithRdivQT.dot(ithPredictor.T).dot(siteWeights).dot(
                                                                     pSigmaStd)
         
         # Regression for remaining predictors
         q, r = np.linalg.qr(woIthPredictor.T.dot(siteWeights).dot(
                                                               woIthPredictor))
         woIthRdivQT = np.linalg.lstsq(r, q.T)[0]
         h = woIthPredictor.dot(woIthRdivQT).dot(woIthPredictor.T).dot(
                                                                   siteWeights)
         predicted = h.dot(pSigmaStd)
         # Get remaining R squared
         remainingRsq = np.sum(predicted.T.dot(predicted)) / np.sum(
                                                   pSigmaStd.T.dot(pSigmaStd))
         
         # Calculate the semi-partial correlation
         try:
            semiPartialMtx[j, i] = (ithSlope * sqrt(rSq-remainingRsq)) / abs(
                                                                      ithSlope)
         except ValueError: # Square root of negative
            semiPartialMtx[j, i] = 0.0
         # Calculate F semi-partial
         fSemiPartialMtx[j, i] = (rSq - remainingRsq) / totalPsigmaResidual
                                 
   return Matrix(adjRsq), Matrix(fGlobal), Matrix(semiPartialMtx), Matrix(fSemiPartialMtx)

# .............................................................................
def mcpa(pam, phyloMtx, grim, bioGeoHypotheses=None, numPermutations=9999):
   """
   @summary: Performs a complete MCPA experiment
   @param pam: A presence / absence sites (rows) by species (columns) matrix to 
                  be used as the incidence matrix ("I" in the literature) 
   @param phyloMtx: A species (row) by internal node (column) matrix encoding 
                       of a phylogenetic tree ("P" in the literature)
   @param grim: A sites (rows) by environmental variable (columns) encoded 
                   matrix of environmental data ("E" in the literature)
   @param bioGeoHypotheses: (optional) A site (rows) by Biogeographic 
                               hypotheses (columns) encoded matrix of 
                               biogeographic hypotheses.  If this is provided, 
                               it is concatenated with the environmental matrix 
                               along the sites axis to create "E".
   @param numPermutations: The number of randomized MCPA runs to perform to 
                              determine significance of observed results
   @note: This function won't scale well, use tools to parallelize 
             randomizations, such as concurrent.futures, Pool, or 
             Makeflow / WorkQueue
   @note: This is really more of an example
   @note: This method assumes that inputs are Matrix objects
   """
   # If biogeographic hypotheses are provided, concatenate them with the grim
   if bioGeoHypotheses:
      predictorMtx = Matrix.concatenate([bioGeoHypotheses, grim], axis=1)
   else:
      # If not, just use the grim 
      predictorMtx = grim
   
   # First get the results of a run using the observed values
   obsMCPA = mcpaRun(pam, predictorMtx, phyloMtx)
   obsAdjRsq, obsFglobal, obsSemiPartialMtx, obsFsemiPartialMtx = obsMCPA
   
   # Randomizations
   randFglobals = []
   randFSemiPartials = []
   
   for i in xrange(numPermutations):
      _, fGlobal, _, fSemiPartial = mcpaRun(pam, predictorMtx, 
                                                      phyloMtx, randomize=True)
      randFglobals.append(fGlobal)
      randFSemiPartials.append(fSemiPartial)
   
   # Check to see which randomize f globals are greater than or equal to observed
   fGlobalPVals = getPValues(obsFglobal, randFglobals, numPermutations)
   # Check to see which semi partials are greater than or equal to observed
   fSemiPartialPVals = getPValues(obsFsemiPartialMtx, randFSemiPartials, 
                                        numPermutations)

   # P-Value correction
   corFGlobalPVals = correctPValues(fGlobalPVals)
   corFSemiPartialPVals = correctPValues(fSemiPartialPVals)
   
   # Return observed adjusted R squared, semi-partials, fGlobal P-values and
   #      fSemiPartial P-values
   return obsAdjRsq, obsSemiPartialMtx, corFGlobalPVals, corFSemiPartialPVals

