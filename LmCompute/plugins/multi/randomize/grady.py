"""
@summary: This module contains functions used to randomize a PAM using CJ's
             algorithm.  This algorithm can run in a parallel fashion and uses
             a fill-based approach so as to prevent a bias caused by starting
             with an initial condition of a populated matrix.
@author: CJ Grady
@version: 4.0

@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
from random import random, choice, randint, shuffle
import numpy as np
import time

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
   
   return a+b
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
   aTime1 = time.time()
   rowTots = np.sum(mtxData, axis=1)
   colTots = np.sum(mtxData, axis=0)
   nRows, nCols = mtxData.shape
   
   #initialTotal = np.sum(rowTots)
   
   #weights = colAndRowPlusRbyC(rowTots, colTots, nRows, nCols)
   weights = maxColOrRow(rowTots, colTots, nRows, nCols)
   
   
   rowTots = rowTots.reshape((nRows, 1))
   colTots = colTots.reshape((1, nCols))
   
   bTime1 = time.time()
   print "Step 1 time: {}".format(bTime1-aTime1)
   
   # Step 2: Get initial random matrix
   # ...........................
   getInitialRandomMatrix = np.vectorize(randPresAbs)
   
   mtx1 = getInitialRandomMatrix(weights)
   
   bTime2 = time.time()
   print "Step 2 time: {}".format(bTime2 - bTime1)
   
   # Step 3: Fix broken marginals
   # ...........................
   fixAttempts = 0
   numFixed = 0
   
   for i in xrange(nRows):
      rowSum = np.sum(mtx1[i,:])
      if rowSum > rowTots[i]:
         rowChoices = np.where(mtx1[i] == 1)[0]
         shuffle(rowChoices)
         # for as many indecies as we are over the total
         for x in range(int(rowSum - rowTots[i])):
            mtx1[i, rowChoices[x]] = 0
            numFixed += 1 
      
   for j in xrange(nCols):
      colSum = np.sum(mtx1[:,j])
      if colSum > colTots[0,j]:
         colChoices = np.where(mtx1[:,j] == 1)[0]
         shuffle(colChoices)
         for y in range(int(colSum - colTots[0,j])):
            mtx1[colChoices[y], j] = 0
            numFixed += 1
      
            
   #filledTotal = np.sum(mtx1)
   
   bTime3 = time.time()
   print "Step 3 time: {}".format(bTime3 - bTime2)

   # Step 4: Fill
   # ...........................
   problemRows = []
   problemColumns = []
   
   rowSums = np.sum(mtx1, axis=1)
   colSums = np.sum(mtx1, axis=0)
   
   unfilledRows = np.where(rowSums < rowTots[:,0])[0].tolist()
   unfilledCols = np.where(colSums < colTots[0,:])[0].tolist()
   #unfilledRows = np.where(rowSums < rowTots[:,0])[0]
   #unfilledCols = np.where(colSums < colTots[0,:])[0]
   
   #print "Unfilled rows: {}, unfilled columns: {}".format(unfilledRows.shape, 
   #                                                       unfilledCols.shape)
   print "Unfilled rows: {}, unfilled columns: {}".format(len(unfilledRows), 
                                                          len(unfilledCols))
   
   while len(unfilledRows) > 0:
      possibleCols = []
      #r = np.random.choice(unfilledRows)
      r = choice(unfilledRows)
      possibleCols = list(set(np.where(mtx1[r,:] == 0)[0].tolist()).intersection(
         set(unfilledCols)))
      #possibleCols = np.intersect1d(np.where(mtx1[r,:] == 0)[0], unfilledCols)
      
      if len(possibleCols) == 0:
         #np.delete(unfilledRows, r)
         unfilledRows.remove(r)
         problemRows.append(r)
      else:
         c = choice(possibleCols)
         #c = np.random.choice(possibleCols)
         mtx1[r,c] = 1
         rSum = np.sum(mtx1[r,:])
         cSum = np.sum(mtx1[:,c])
         
         if rSum == int(rowTots[r]):
            unfilledRows.remove(r)
            #np.delete(unfilledRows, r)
         
         if cSum == int(colTots[0, c]):
            unfilledCols.remove(c)
            #np.delete(unfilledCols, c)
   
   #problemColumns = unfilledCols.tolist()
   problemColumns = unfilledCols
   
   bTime4 = time.time()
   print "Step 4 time: {}".format(bTime4 - bTime3)
   print "{} problem columns".format(len(problemColumns))

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
      
      cs = np.where(mtx1[r] == 0)[0]
      rs = np.where(mtx1[:,c] == 0)[0]
      
      numTries = 0
      found = False
      
      while not found and numTries < SEARCH_THRESHOLD:
         r2 = np.random.choice(rs)
         c2 = np.random.choice(cs)
         numTries += 1
         
         if mtx1[r2,c2] == 1:
            mtx1[r,c2] = 1
            mtx1[r2,c2] = 0
            mtx1[r2,c] = 1
            found = True
      
      if not found:
         raise Exception("Couldn't fix row, col ({}, {})".format(r, c))
      
      #r2 = randint(0, nRows-1)
      #c2 = randint(0, nCols-1)
      # 
      ## Can I do this with numpy?  maybe find all of the combinations and just pick from that?
      #
      #while i < SEARCH_THRESHOLD and not (
      #   mtx1[r,c2] == 0 and mtx1[r2,c2] == 1 and mtx1[r2,c] == 0):
      #   i += 1
      #   r2 = randint(0, nRows-1)
      #   c2 = randint(0, nCols-1)
      #if i < SEARCH_THRESHOLD:
      #   mtx1[r,c2] = 1
      #   mtx1[r2,c2] = 0
      #   mtx1[r2,c] = 1
      #else:
      #   if j < SEARCH_THRESHOLD:
      #      j += 1
      ##   else:
      #      raise Exception ("Couldn't fix row, col (%s, %s)" % (r, c))
      
      rSum = np.sum(mtx1[r,:])#, axis=1)
      cSum = np.sum(mtx1[:,c])#, axis=0)
         
      if rSum == int(rowTots[r]):
         problemRows.remove(r)
      if cSum == int(colTots[0,c]):
         problemColumns.remove(c)
   
   bTime5 = time.time()
   print "Step 5 time: {}".format(bTime5 - bTime4)
   print "Total time: {}".format(bTime5 - aTime1)

   return Matrix(mtx1, headers=mtxHeaders)
   