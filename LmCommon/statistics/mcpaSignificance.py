"""
@summary: This module provides tools for determining which results from a
             Metacommunity Phylogenetics Analysis are significant
@author: CJ Grady
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
"""
import numpy as np

NODATA_VALUE = -999

# .............................................................................
def createMCPAReport(adjRsq, fGlobal, semiPartialMtx, fSemiPartialMtx, tree,
                     alpha=0.05, subsetSignificant=False):
   """
   @summary: Creates a report of two CSVs indicating significant results in the
                MCPA outputs
   @param adjRsq: A single column of adjusted R-squared values for each node
   @param fGlobal: A frequency column indicating what portion of the randomized
                      runs had an adjusted R-squared value greater than or 
                      equal to the observed
   @param semiPartialMtx: A matrix of observed semi-partial correlations
   @param fSemiPartialMtx: A frequency matrix of the portion of randomized runs
                              that had greater than or equal to the observed
                              semi-partial correlation for each cell
   @param tree: An LmTree object that corresponds to the MCPA outputs
   @param alpha: The significance value to use when evaluating if a result is
                    significant
   @param subsetSignificant: If this is true, remove all non-significant values
                                from the observed result
   @note: Each of the output matrices has a header row that looks like: 
             Node, [predictors], Adjusted R-squared
   @note: Each of the output matrices has a header column that has the id of 
             the node represented at each row.
   @note: The first matrix returned is the observed values, optionally 
             subsetted to only those that are significant for each of the 
             predictor columns and the adjusted R-squared column against the 
             node row
   @note: The data of the second matrix are boolean values indicating if that 
             cell is significant
   """
   # Get the observed values and a sorted version
   observedValues = np.concatenate([semiPartialMtx, adjRsq], axis=1)
   sortedObservedValues = sortRowsByNodeId(observedValues, tree)

   # Get the frequency values and sort them
   frequencyValues = np.concatenate([fSemiPartialMtx, fGlobal], axis=1)
   sortedFreqValues = sortRowsByNodeId(frequencyValues, tree)
   
   # Find the significant values
   significantValues = findSignificantValues(sortedFreqValues, alpha=alpha)
   
   # Check if we should subset
   if subsetSignificant:
      sortedObservedValues = np.where(significantValues, sortedObservedValues, 
                                      NODATA_VALUE)
   
   # TODO: Get this from metadata
   headerRow = ["Node Id"]
   headerRow.extend(["Predictor {0}".format(
                                   x) for x in range(semiPartialMtx.shape[1])])
   headerRow.append("Adj. Rsq")

   # Sorted list of node ids that have matrix indices   
   headerColumn = sorted(tree.getMatrixIndicesMapping().keys())
   
   decoratedObserved = makeDecoratedCSV(sortedObservedValues, headerRow, 
                                      headerColumn)
   decoratedSignificant = makeDecoratedCSV(significantValues, headerRow, 
                                         headerColumn)
   
   return decoratedObserved, decoratedSignificant

# .............................................................................
def findSignificantValues(freqMtx, alpha=0.05):
   """
   @summary: Return a boolean matrix indicating if a value is statistically 
                significant for a given alpha value
   @param freqMtx: For MCPA, this is a matrix or vector where each value 
                      represents the number of times this cell (semi-partial 
                      correlation or adjusted R-squared) was greater than the 
                      observed value, this value is then divided by the total 
                      number of randomizations to generate frequencies.
   @param alpha: The significance value to test against (statistical alpha for 
                    hypothesis testing)
   @note: Numpy will generate a boolean matrix when we apply a boolean operator 
             to a Numpy array 
   """
   return freqMtx <= alpha

# .............................................................................
def makeDecoratedCSV(mtx, headerRow, headerColumn):
   """
   @summary: Create a CSV text string from the provided inputs
   @param mtx: The matrix to use for the data of the CSV
   @param headerRow: A row (list) of headers for the CSV
   @param headerColumn: A column (list) of headers for each row of data 
   @todo: This could probably be done with Numpy directly by using structured 
             arrays
   @todo: Consider optionally including no data values
   """
   csvStr = ""
   headers = "{0}\n".format(','.join(["\"{0}\"".format(x) for x in headerRow]))
   csvStr.append(headers)
   
   if len(headerColumn) != mtx.shape[0]:
      raise Exception(
                   "Header column and matrix rows do not have the same length")
   
   for i in xrange(len(headerColumn)):
      rowStr = "\"{0}\",{1}\n".format(headerColumn[i],
                               ','.join(["\"{0}\"".format(x) for x in mtx[i]]))
      csvStr.append(rowStr)
   return csvStr

# .............................................................................
def sortRowsByNodeId(mtx, tree):
   """
   @summary: Sorts the rows in the provided matrix by the corresponding node id.
   @param mtx: The matrix to sort rows for
   @param tree: An LmTree object providing matrix index : node id mappings
   """
   nodeToMtxIdx = tree.getMatrixIndicesMapping()
   # Get the new order by sorting the path ids and creating a list of matching 
   # matrix indices
   rowOrder = [noeToMtxIdx[pathId] for pathId in sorted(nodeToMtxIdx.keys())]
   # Fancy index the matrix
   return mtx[rowOrder,:]
