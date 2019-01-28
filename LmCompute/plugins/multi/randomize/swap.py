"""
@summary: Module containing functions to randomize a RAD PAM
@author: Lifemapper Team; lifemapper@ku.edu
@version: 4.0.0
@status: beta

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
from random import randrange

from LmCommon.common.matrix import Matrix

MAX_TRIES_WITHOUT_SWAP = 1000000

# .............................................................................
def swapRandomize(matrix, numSwaps, maxTries=MAX_TRIES_WITHOUT_SWAP):
   """
   @summary: Randomize a compressed matrix using the Swap method
   @param matrix: A PAM Matrix object
   @param numSwaps: The number of successful swaps to perform
   @param maxTries: The maximum number of tries to swap before failing
   """
   mtxHeaders = matrix.getHeaders()
   swappedMtx = matrix.data.copy().astype(int)
   counter = 0
   numTries = 0
   rowLen, colLen = matrix.data.shape

   #numTries is a safety to kill the loop if nothing is ever found
   while counter < numSwaps and numTries < MAX_TRIES_WITHOUT_SWAP: 
      numTries += 1
      column1 = randrange(0, colLen)
      column2 = randrange(0, colLen)
      row1 = randrange(0, rowLen)                   
      while column2 == column1:
         column2 = randrange(0, colLen)             
      firstcorner = swappedMtx[row1][column1]     
      if firstcorner ^ swappedMtx[row1][column2]:
         row2 = randrange(0, rowLen)              
         while row2 == row1:
               row2 = randrange(0, rowLen)
         if ((firstcorner ^ swappedMtx[row2][column1]) and 
             (not(firstcorner) ^ swappedMtx[row2][column2])):
               swappedMtx[row1][column2] = firstcorner
               swappedMtx[row2][column1] = firstcorner
               swappedMtx[row2][column2] = not(firstcorner)
               swappedMtx[row1][column1] = not(firstcorner)                 
               counter += 1
               numTries = 0
   if numTries >= MAX_TRIES_WITHOUT_SWAP:
      raise Exception("Reached maximum number of tries without finding suitable swap")
      
   return Matrix(swappedMtx, headers=mtxHeaders)
   