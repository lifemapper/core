"""
@summary: Module containing a class for encoding a Phylogenetic tree into a 
             matrix
@author: CJ Grady (originally by Jeff Cavner)
@version: 1.0
@status: beta

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
@todo: Use method to get labels by matrix index when available from tree
"""
import numpy as np
from random import shuffle

from LmCommon.common.lmconstants import PhyloTreeKeys
from LmCommon.common.matrix import Matrix
from LmCommon.encoding.encodingException import EncodingException
from LmCommon.trees.lmTree import LmTree

# .............................................................................
class PhyloEncoding(object):
   """
   @summary: The PhyloEncoding class represents the encoding of a phylogenetic
                tree to match a PAM
   """
   # ..............................   
   def __init__(self, treeDict, pam):
      """
      @summary: Base constructor
      @param treeDict: A phylogenetic tree as a dictionary that will be 
                converted to an LmTree object
      @param pam: A Matrix for a PAM
      """
      if not isinstance(treeDict, LmTree):
         self.tree = LmTree(treeDict)
      else:
         self.tree = treeDict
      if isinstance(pam, Matrix):
         self.pam = pam
      else:
         self.pam = Matrix(pam)
   
   # ..............................   
   @classmethod
   def fromFile(cls, treeDLoc, pamDLoc):
      """
      @summary: Creates an instance of the PhyloEncoding class from tree and 
                   pam files
      @param treeDLoc: The location of the tree (in JSON format)
      @param pamDLoc: The location of the PAM (in numpy format)
      @raise IOError: If one or both of the files are not found
      """
      tree = LmTree.fromFile(treeDLoc)
      
      pam = Matrix.load(pamDLoc)
      
      return cls(tree, pam)
      
   # ..............................
   def encodePhylogeny(self):
      """
      @summary: Encode the phylogenetic tree into a matrix, P in the literature,
                   a tip (row) by internal node (column) matrix that needs to 
                   match the provided PAM.
      @todo: Consider providing options for correcting the tree / pam
      """
      if self.validate(): # Make sure that the PAM and tree match
         if self.tree.hasBranchLengths():
            pMtx = self._buildPMatrixWithBranchLengths()
         else:
            pMtx = self._buildPMatrixNoBranchLengths()
      else:
         raise EncodingException(
                  "PAM and Tree do not match, fix before encoding")
      
      return pMtx
   
   # ..............................
   def extendPamToMatchTree(self):
      """
      @summary: Extend the provided PAM with additional columns of aggregated 
                   presences for tips that didn't have matrix indices.  These 
                   columns are created by assembling all of the columns from 
                   the tips in the sister clade that have matrix indices and 
                   creating a column with a true presence value if any of the 
                   sister tip columns have true presence at that site.
      """
      _, newColumns = self._getSisterTipsForClade(self.tree.tree, 
                                                           self.pam.data.shape[1]-1)
      newColumnMtx = np.zeros((self.pam.data.shape[0], len(newColumns.keys())), 
                             dtype=np.int)
      # Extend the PAM by adding new columns to the right side
      self.pam.append(Matrix(newColumnMtx, axis=1))
      # TODO: Add header?

      for mtxIdx in newColumns.keys():
         # Get a matrix of all of the sister tips in the PAM
         tmp = np.take(self.pam.data, newColumns[mtxIdx], axis=1)
         # Set the presence value for each site to true if any of the sister 
         #    tips are present
         self.pam.data[:,mtxIdx] = np.any(tmp, axis=1)
   
   # ..............................
   def validate(self):
      """
      @summary: Validates the tree / PAM combination to make sure they can be
                   used to create an encoding
      """
      # check if tree is ultrametric
      if (not self.tree.hasBranchLengths() or self.tree.isUltrametric()) and \
           self.tree.isBinary(): 
         # Check that matrix indices in tree match PAM
         # List of matrix indices (based on PAM column count)
         pamMatrixIndices = range(self.pam.data.shape[1])
         # All matrix indices in tree
         treeMatrixIndices = self.tree.getMatrixIndicesInClade()
         
         # Find the intersection between the two lists by creating a set for 
         #    each and then checking which values are in both and making a list 
         intersection = list(set(pamMatrixIndices) & set(treeMatrixIndices))

         # This checks that there are no duplicates in either of the indices 
         #    lists and that they overlap completely
         if len(intersection) == len(pamMatrixIndices) and \
                len(pamMatrixIndices) == len(treeMatrixIndices):
            
            # If everything checks out to here, return true for valid
            return True
      # If anything does not validate, return false
      return False
   
   # ..............................
   def _buildPBranchLengthValues(self, clade):
      """
      @summary: Recurse through tree to get P matrix values for node / tip 
                   combinations
      @param clade: The current clade
      """
      blDict = {} # Dictionary of branch length lists bottom-up from tip to 
      #                current clade
      
      cladeBL = clade[PhyloTreeKeys.BRANCH_LENGTH] # To avoid lookups
      
      # This is the sum of all branch lengths in the clade and will be used as
      #    the denominator for the p value for each tip to this node.  See the
      #    literature for more information.  We will initialize with the branch
      #    length of this clade because it will always be added whether we are
      #    at a tip or an internal node
      blSum = cladeBL
      
      pValsDict = {} # Dictionary of dictionaries of P-values, first key is 
      #                   node path id, sub keys are tip matrix indices for 
      #                   that node
      if len(clade[PhyloTreeKeys.CHILDREN]) > 0: # Assume this is two
         
         cladePvals = {} # Initialize P-values for the clade dictionary
         multipliers = [-1.0, 1.0] # One branch should be positive, the other  
         #                              negative
         shuffle(multipliers)
         
         for child in clade[PhyloTreeKeys.CHILDREN]:
            
            childBlDict, childBlSum, childPvalDict = \
                                          self._buildPBranchLengthValues(child)
            multiplier = multipliers.pop(0)
            
            # Extend the P values dictionary
            pValsDict.update(childPvalDict)
            
            # Add this child's branch length sum to the clade's branch length 
            #    sum
            blSum += childBlSum

            childBL = child[PhyloTreeKeys.BRANCH_LENGTH]

            # We will add this value to the branch length list for all of the 
            #    tips in this clade.  It is the branch length of this clade 
            #    divided by the number of tips in the clade
            addVal = 1.0 * childBL / len(childBlDict.keys())
            
            # Process each of the tips in childBlDict
            for k in childBlDict.keys():
               
               # The formula for calculating the P-value is:
               # P(tip)(node) = (l1 + l2/2 + l3/3 + ... ln/n) / Sum of branch lengths to node
               #   This value is arbitrarily set to be positve for one child 
               #      and negative for the other (we will use "multiplier")
               # The l term is the length of a branch and it is divided by the
               #    number of tips that share that branch
               tipBLs = childBlDict[k] + [addVal]
               
               # Add the P-value to pValsDict
               cladePvals[k] = multiplier * sum(tipBLs) / childBlSum
               
               # Add to blDict with this branch length
               blDict[k] = tipBLs

         pValsDict[clade[PhyloTreeKeys.PATH_ID]] = cladePvals
         
      else: # We are at a tip
         blDict = {
            clade[PhyloTreeKeys.MTX_IDX] : []
         }
         
      
      return blDict, blSum, pValsDict
   
   # ..............................   
   def _buildPMatrixNoBranchLengths(self):
      """
      @summary: Creates a P matrix when no branch lengths are present
      @note: For this method, we assume that there is a total weight of -1 to 
                the left and +1 to the right for each node.  As we go down 
                (towards the tips) of the tree, we divide the proportion of 
                each previously visited node by 2.  We then recurse with this 
                new visited list down the tree.  Once we reach a tip, we can 
                return that list of proportions because it will match for that 
                tip for each of it's ancestors.
      @note: Example: 
               3
               +--2
               |  +-- 1
               |  |   +--0
               |  |   |  +-- A
               |  |   |  +-- B
               |  |   |
               |  |   +-- C
               |  |
               |  +-- D
               |
               +--4
                  +-- E
                  +-- F
                  
            Step 1: (Node 3) [] 
                      - recurse left with [(3,-1)] 
                      - recurse right with [(3,1)]
            Step 2: (Node 2) [(3,-1)]
                      - recurse left with [(3,-.5),(2,-1)] 
                      - recurse right with [(3,-.5),(2,1)]
            Step 3: (Node 1)[(3,-.5),(2,-1)] 
                      - recurse left with [(3,-.25),(2,-.5),(1,-1)]
                      - recurse right with [3,-.25),(2,-.5),(1,1)]
            Step 4: (Node 0)[(3,-.25),(2,-.5),(1,-1)]
                      - recurse left with [(3,-.125),(2,-.25),(1,-.5),(0,-1)]
                      - recurse right with [(3,-.125),(2,-.25),(1,-.5),(0,1)]
            Step 5: (Tip A) - Return [(3,-.125),(2,-.25),(1,-.5),(0,-1)]
            Step 6: (Tip B) - Return [(3,-.125),(2,-.25),(1,-.5),(0,1)]
            Step 7: (Tip C) - Return [(3,-.25),(2,-.5),(1,1)]
            Step 8: (Tip D) - Return [(3,-.5),(2,1)]
            Step 9: (Node 4) [(3,1)] - recurse left with [(3,.5),(4,-1)]
                                     - recurse right with [(3,.5),(4,1)]
            Step 10: (Tip E) - Return [(3,.5),(4,-1)]
            Step 11: (Tip F) - Return [(3,.5),(4,1)]
            
            Creates matrix:
                   0    1    2     3      4
               A -1.0 -0.5 -0.25 -0.125  0.0
               B  1.0 -0.5 -0.25 -0.125  0.0
               C  0.0  1.0 -0.5  -0.25   0.0
               D  0.0  0.0  1.0  -0.5    0.0
               E  0.0  0.0  0.0   0.5   -1.0
               F  0.0  0.0  0.0   0.5    1.0
      
      @see: Page 1293 of the literature
      """
      # .......................
      # Initialize the matrix
      allPathIds = self.tree.cladePaths.keys()
      
      # Internal path ids are the subset of path ids that are not tips
      internalPathIds = list(set(allPathIds) - set(self.tree.tips))
      matrix = np.zeros((len(self.tree.tips), len(internalPathIds)), 
                        dtype=np.float)

      # Get the list of tip proportion lists
      # Note: Which tip each of the lists belongs to doesn't really matter but
      #          recursion will start at the top and go to the bottom of the 
      #          tree tips
      tipProps = self._buildPMatrixTipProportionList(self.tree.tree, visited=[])
      
      # We need a mapping of node path id to matrix column.  I don't think 
      #    order matters
      nodeColumnIndex = dict(zip(internalPathIds, range(len(internalPathIds))))
      
      # The matrix index of the tip in the PAM maps to the row index of P
      for rowIdx, tipProp in tipProps:
         for nodePathId, val in tipProp:
            matrix[rowIdx][nodeColumnIndex[nodePathId]] = val
            
      labelPairs = self.tree.getMatrixIndexLabelPairs(useSquids=True, 
                                                      sort=True)
      labels = [label for _, label in labelPairs]
      return Matrix(matrix, headers={'1': labels})  
   
   # ..............................   
   def _buildPMatrixTipProportionList(self, clade, visited=[]):
      """
      @summary: Recurses through the tree to build a list of tip proportions to 
                   be used to build a P-matrix when no branch lengths are 
                   present
      @param clade: The current clade
      @param visited: A list of [(node path id, proportion)]
      @note: Proportion for each visited node is divided by two as we go 
                towards the tips at each hop
      """
      tipProps = []
      if len(clade[PhyloTreeKeys.CHILDREN]) > 0: # Assume this is two
         # First divide all existing visited by two
         newVisited = [(idx, val / 2.0) for idx, val in visited]
         # Recurse.  Left is negative, right is positive
         tipProps.extend(
            self._buildPMatrixTipProportionList(clade[PhyloTreeKeys.CHILDREN][0], 
                            newVisited + [(clade[PhyloTreeKeys.PATH_ID], -1.0)]))
         tipProps.extend(
            self._buildPMatrixTipProportionList(clade[PhyloTreeKeys.CHILDREN][1], 
                             newVisited + [(clade[PhyloTreeKeys.PATH_ID], 1.0)]))
      else: # We are at a tip
         tipProps.append((clade[PhyloTreeKeys.MTX_IDX], visited)) # Just return a list with one list
      return tipProps
   
   # ..............................   
   def _buildPMatrixWithBranchLengths(self):
      """
      @summary: Creates a P matrix when branch lengths are present
      @note: For this method, we assume that there is a total weight of -1 to 
                the left and +1 to the right for each node.  As we go down 
                (towards the tips) of the tree, we divide the proportion of 
                each previously visited node by 2.  We then recurse with this 
                new visited list down the tree.  Once we reach a tip, we can 
                return that list of proportions because it will match for that 
                tip for each of it's ancestors.
      @note: Example: 
               3
               +-- 2 (0.4)
               |   +-- 1 (0.15)
               |   |   +-- 0 (0.65)
               |   |   |   +-- A (0.2)
               |   |   |   +-- B (0.2)
               |   |   |
               |   |   +-- C (0.85)
               |   |
               |   +-- D (1.0)
               |
               +-- 4 (0.9)
                   +-- E (0.5)
                   +-- F (0.5)
                  
            Value for any cell (tip)(node) = (l1 + l2/2 + l3/3 + ... + ln/n)/
                                       (Sum of branch lengths in daughter clade)
            ln / n -> The length of branch n divided by the number of tips that
                         descend from that clade
             
            P(A)(2) = (0.2 + 0.65/2 + 0.15/3) / (0.2 + 0.2 + 0.65 + 0.85 + 0.15)
                    = (0.2 + 0.325 + 0.05) / (2.05)
                    = 0.575 / 2.05
                    = 0.280
                    
            P(D)(3) = (1.0 + 0.4/4) / (.2 + .2 + .65 + .85 + .15 + 1.0 + .4)
                    = 1.1 / 3.45
                    = 0.319
                  
            
            Creates matrix:
                    0       1       2       3       4
               A  -1.000  -0.500  -0.280  -0.196   0.000
               B   1.000  -0.500  -0.280  -0.196   0.000
               C   0.000   1.000  -0.439  -0.290   0.000
               D   0.000   0.000   1.000  -0.319   0.000
               E   0.000   0.000   0.000   0.500  -1.000
               F   0.000   0.000   0.000   0.500   1.000
      
      @see: Literature supplemental material
      """
      # We only need the P-values dictionary
      _, _, pValDict = self._buildPBranchLengthValues(self.tree.tree)
      
      
      # Initialize the matrix
      matrix = np.zeros((len(self.tree.tips), len(pValDict.keys())), 
                        dtype=np.float)

      # We need a mapping of node path id to matrix column.  I don't think 
      #    order matters
      nodeColumnIndex = dict(zip(pValDict.keys(), range(len(pValDict.keys()))))
      
      for nodePathId in pValDict.keys():
         for tipMtxIdx in pValDict[nodePathId].keys():
            matrix[tipMtxIdx][nodeColumnIndex[nodePathId]] = pValDict[
                                                         nodePathId][tipMtxIdx]
            
      labelPairs = self.tree.getMatrixIndexLabelPairs(useSquids=True, 
                                                      sort=True)
      labels = [label for _, label in labelPairs]
      return Matrix(matrix, headers={'1': labels})  
   
   # ..............................
   def _getSisterTipsForClade(self, clade, lastColumn):
      """
      @summary: Looks at the children of the clade to see if they are tips.  If 
                   so, check if it has a matrix index.  If not, get all of the
                   matrix indices in the sister clade and add to the dictionary
                   of new matrix columns to create.  Then recurse over children.
      @param clade: The clade to process
      @param lastColumn: The last column in the PAM.  Add to this as necessary
      @todo: Consider changing the name of this function
      """
      newColumns = {} # This is a dictionary with new matrix index key and a 
      #                    list of tips in the sister clade with matrix indices
      
      # This will fail if not binary
      leftChild, rightChild = clade[PhyloTreeKeys.CHILDREN]
      
      # We will put these into a list so code does not have to be duplicated
      for child, sister in [(leftChild, rightChild), (rightChild, leftChild)]:
         # Check child
         if len(child[PhyloTreeKeys.CHILDREN]) > 0: # Assume this is 2
            lastColumn, childColumns = self._getSisterTipsForClade(child, 
                                                                   lastColumn)
            newColumns.update(childColumns)
         else: # Tip
            if not child.has_key(PhyloTreeKeys.MTX_IDX): # No matrix index
               # Get the sister tips with matrix indices
               sisterTips = self.tree.getMatrixIndicesInClade(clade=sister)
               lastColumn += 1
               newColumns[lastColumn] = sisterTips
               child[PhyloTreeKeys.MTX_IDX] = lastColumn
      
      return lastColumn, newColumns
   
