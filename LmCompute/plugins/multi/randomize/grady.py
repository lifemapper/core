"""
@summary: This module contains functions used to randomize a PAM using CJ's
             algorithm.  This algorithm can run in a parallel fashion and uses
             a fill-based approach so as to prevent a bias caused by starting
             with an initial condition of a populated matrix.
@author: CJ Grady
@version: 4.0

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
from random import random, choice, randint
import numpy as np

from LmCommon.common.matrix import Matrix

SEARCH_THRESHOLD = 100000 # Number of times to look for a match when fixing problems

# .............................................................................
def randPresAbs(threshold):
   """
   @summary: Randomly picks a number and returns 1 if it is less than the 
                threshold, else returns 0
   """
   if random() <= (threshold * 1.5):
      return 1
   else:
      return 0

# .............................................................................
def colAndRowPlusRbyC(rowTots, colTots, nRows, nCols):
   """
   @summary: This method treats the row and column as one array and uses the 
                proportional fill of the combination
   """
   rowWeights = rowTots
   colWeights = colTots
   rowWeights = rowWeights.reshape((nRows, 1))
   colWeights = colWeights.reshape((1, nCols))
   
   return ((rowWeights + colWeights) / (nRows + nCols -1)) + ((rowWeights*colWeights)/(nRows*nCols))

# .............................................................................
def maxColOrRow(rowTots, colTots, nRows, nCols):
   """
   @summary: This method returns a matrix of weights where the weight of each
                cell is the maximum between the proportions of the row and col
   """
   rowWeights = rowTots
   colWeights = colTots
   rowWeights = rowWeights.reshape((nRows, 1))
   colWeights = colWeights.reshape((1, nCols))
   
   # Could do this better / faster
   rowOnes = np.ones(rowWeights.shape)
   colOnes = np.ones(colWeights.shape)
   
   a = rowWeights * colOnes
   b = rowOnes * colWeights
   
   return np.maximum.reduce([a, b])

# .............................................................................
def gradyRandomize(mtx):
   """
   @summary: Main function for creating a random matrix
   @param mtx: A Matrix object representation of a PAM
   """
   mtxData = mtx.data
   mtxHeaders = mtx.getHeaders()
   
   # Step 1: Get the marginal totals of the matrix
   # ...........................
   rowTots = np.sum(mtxData, axis=1)
   colTots = np.sum(mtxData, axis=0)
   nRows, nCols = mtxData.shape
   
   #initialTotal = np.sum(rowTots)
   
   #weights = colAndRowPlusRbyC(rowTots, colTots, nRows, nCols)
   weights = maxColOrRow(rowTots, colTots, nRows, nCols)
   
   
   rowTots = rowTots.reshape((nRows, 1))
   colTots = colTots.reshape((1, nCols))
   
   # Step 2: Get initial random matrix
   # ...........................
   getInitialRandomMatrix = np.vectorize(randPresAbs)
   
   mtx1 = getInitialRandomMatrix(weights)
   
   # Step 3: Fix broken marginals
   # ...........................
   fixAttempts = 0
   numFixed = 0
   
   for i in xrange(nRows):
      while np.sum(mtx1[i,:]) > rowTots[i]:
         myChoice = choice(np.where(mtx1[i]==1)[0])
         fixAttempts += 1
         if mtx1[i, myChoice] == 1:
            mtx1[i, myChoice] = 0
            numFixed += 1
   
   for j in xrange(nCols):
      while np.sum(mtx1[:,j]) > colTots[0,j]:
         myChoice = choice(np.where(mtx1[:,j]==1)[0])
         fixAttempts += 1
         if mtx1[myChoice, j] == 1: # Can probably skip this
            mtx1[myChoice, j] = 0
            numFixed += 1
            
   #filledTotal = np.sum(mtx1)
   
   # Step 4: Fill
   # ...........................
   problemRows = []
   problemColumns = []
   
   rowSums = np.sum(mtx1, axis=1)
   colSums = np.sum(mtx1, axis=0)
   
   unfilledRows = np.where(rowSums < rowTots[:,0])[0].tolist()
   unfilledCols = np.where(colSums < colTots[0,:])[0].tolist()
   
   while unfilledRows:
      possibleCols = []
      r = choice(unfilledRows)
      possibleCols = list(set(np.where(mtx1[r,:] == 0)[0].tolist()).intersection(set(unfilledCols)))
      
      if len(possibleCols) == 0:
         unfilledRows.remove(r)
         problemRows.append(r)
      else:
         c = choice(possibleCols)
         mtx1[r,c] = 1
         rSum = np.sum(mtx1[r,:])
         cSum = np.sum(mtx1[:,c])
         
         if rSum == int(rowTots[r]):
            unfilledRows.remove(r)
         
         if cSum == int(colTots[0, c]):
            unfilledCols.remove(c)
   
   problemColumns = unfilledCols
   
   # Step 5: Fix problems
   # ...........................
   j = 0
   while problemRows:
      #shuffle(problemRows)
      #shuffle(problemColumns)
      #r = problemRows[0]
      #c = problemColumns[0]
      r = choice(problemRows)
      c = choice(problemColumns)
      i = 0
      r2 = randint(0, nRows-1)
      c2 = randint(0, nCols-1)
      while i < SEARCH_THRESHOLD and not (mtx1[r,c2] == 0 and mtx1[r2,c2] == 1 and mtx1[r2,c] == 0):
         i += 1
         r2 = randint(0, nRows-1)
         c2 = randint(0, nCols-1)
      if i < SEARCH_THRESHOLD:
         mtx1[r,c2] = 1
         mtx1[r2,c2] = 0
         mtx1[r2,c] = 1
      else:
         if j < SEARCH_THRESHOLD:
            j += 1
         else:
            raise Exception ("Couldn't fix row, col (%s, %s)" % (r, c))
      
      rSum = np.sum(mtx1[r,:])#, axis=1)
      cSum = np.sum(mtx1[:,c])#, axis=0)
         
      if rSum == int(rowTots[r]):
         problemRows.remove(r)
      if cSum == int(colTots[0,c]):
         problemColumns.remove(c)
   
   return Matrix(mtx1, headers=mtxHeaders)
   