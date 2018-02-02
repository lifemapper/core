"""
@summary: Module containing methods to perform a splotch randomization of a PAM
@version: 4.0.0
@author: CJ Grady
@status: beta
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research
 
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
import numpy as np
import pysal
from random import choice, randrange

from LmCommon.common.matrix import Matrix

# .............................................................................
def splotchRandomize(mtx, shapegridFn, numSides):
   """
   @summary: Randomize a matrix using the Splotch method
   @param mtx: A Matrix object for a PAM
   @param shapegridFn: File location of shapegrid shapefile
   @param numSides: The number of sides for each shapegrid cell, used to build
                       connectivity matrix
   @note: Use Pysal to build an adjacency matrix for the cells in the shapegrid
   """
   mtxHeaders = mtx.getHeaders()
   columnSums = np.sum(mtx.data, axis=0)

   if numSides == 4:
      neighborMtx = pysal.rook_from_shapefile(shapegridFn)
   elif numSides == 6:
      neighborMtx = pysal.queen_from_shapefile(shapegridFn)
   else:
      raise Exception("Invalid number of cell sides")
   
   randomColumns = []
   for colSum in columnSums:
      randomColumns.append(_splotchColumn(neighborMtx, colSum))
   
   randPam = Matrix.concatenate(randomColumns)
   randPam.setHeaders(mtxHeaders)
   return randPam

# .............................................................................
def _splotchColumn(neighborMtx, numPresent):
   """
   @summary: Generates a splotch randomized column
   @param neighborMtx: A PySal generated adjacency matrix (n x n) where n is 
             number of cells
   @param numPresent: The number of cells to set as present
   """
   npCol = np.zeros((neighborMtx.n, 1), dtype=np.bool)
   
   # Need a connected set
   connected = set([])
   
   # Need an explored set
   explored = set([])
   
   # Pick a random start id
   rowId = randrange(0, neighborMtx.n)
   numExplored = 0
   
   # While we have more to explore
   while numExplored < numPresent:
      explored.add(rowId)
      numExplored += 1
      npCol[rowId, 0] = True
      
      # Add new ids to connected set
      newConnections = set(neighborMtx.neighbors[rowId]).difference(explored)
      connected = connected.union(newConnections)

      # Pick a new row id
      rowId = choice(tuple(connected))
      connected.remove(rowId)
      
   return Matrix(npCol)
   