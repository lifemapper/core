"""
@summary: Module containing LmTree class
@author: CJ Grady (originally from Jeff Cavner)
@version: 1.0
@status: release

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
@todo: Should we provide a method to collapse clades that only have one child?
@todo: Should pruning a tree collapse clades automatically?
"""
from copy import deepcopy
import json
import numpy as np
import os
from random import shuffle

from LmCommon.common.lmconstants import LMFormat, PhyloTreeKeys
from LmCommon.common.matrix import Matrix
from LmCommon.trees.convert.newickToJson import Parser

# .............................................................................
class LmTreeException(Exception):
   """
   @summary: Wrapper around the base Exception class for tree related errors
   """
   pass

# .............................................................................
class LmTree(object):
   """
   @summary: Class representing a phylogenetic tree in Lifemapper
   """
   
   # ..............................   
   def __init__(self, treeDict):
      """
      @summary: Constructor
      @param treeDict: A Phylogenetic tree dictionary with expected keys
      """
      self.tree = treeDict
      
      # Last clade id is used as a reference so that new clades have an unused
      #    identifier
      self._lastCladeId = None
      
      # The cladePaths dictionary contains paths (value) to clades (key)
      self.cladePaths = {}
      self.tips = [] # A list of path ids that are tips
      self._processTree()
      
   # ..............................
   @classmethod
   def createCopy(cls, oldTree):
      """
      @summary: Creates a new instance of the LmTree class that is a copy of
                   the provided tree
      @param oldTree: An instance of LmTree to copy
      """
      newTreeDict = deepcopy(oldTree.tree)
      return cls(newTreeDict)
      
   # ..............................   
   @classmethod
   def fromFile(cls, filename):
      """
      @summary: Create a new LmTree object from a file
      @param filename: The location of the file to read
      @raise IOError: Raised if the file does not exist
      """
      with open(filename) as inF:
         content = inF.read()
      
      ext = os.path.splitext(filename)[1]
      if ext in LMFormat.JSON.getExtensions(): # JSON
         return cls(json.loads(content))
      elif ext in LMFormat.NEWICK.getExtensions(): # Newick
         newickParser = Parser(content)
         jsonTree, _ = newickParser.parse()
         return cls(jsonTree)
      else: # Unknown
         raise LmTreeException(
            "LmTree does not know how to read %s files" % ext)

   # Public functions
   # ..........................................................................
   # ..............................
   def addMatrixIndices(self, pamMetadata):
      """
      @summary: Add matrix indices to the tree
      @param pamMetadata: A dictionary of (label, matrix index) pairs for a PAM
      @todo: Should this fail if not all of the columns are found?
      """
      self._addMatrixIndices(self.tree, pamMetadata)
   
   # ..............................
   def addSquidMatrixIndices(self, pamMetadata):
      """
      @summary: Add matrix indices to the tree
      @param pamMetadata: A dictionary of (squid, matrix index) pairs for a PAM
      @todo: Should this fail if not all of the columns are found?
      """
      self._addSquidMatrixIndices(self.tree, pamMetadata)
   
   # ..............................
   def addSQUIDs(self, squidDict):
      """
      @summary: Add Lifemapper SQUIDs to the tree
      @param squidDict: A dictionary with tip label keys and LM squid values
      @todo: Can this be combined with matrix indices?  Something like add 
                metadata?
      """
      self._addSQUIDs(self.tree, squidDict)
   
   # ..............................
   def getBranchLengths(self):
      """
      @summary: Return a dictionary of branch lengths for each clade
      @note: Path id is key, branch length is value
      """
      branchLengths = self._getBranchLengths(self.tree)
      return dict(branchLengths)
   
   # ..............................
   def getClade(self, pathId):
      """
      @summary: Get a clade by it's path id
      @param pathId: The path id of the clade to retrieve
      """
      if self.cladePaths.has_key(pathId):
         cladePath = self.cladePaths[pathId]
      else:
         raise LmTreeException("Path id: %s was not found" % pathId)
      
      clade = self.tree
      for cid in cladePath[1:]: # We can skip the root
         for child in clade[PhyloTreeKeys.CHILDREN]:
            if child[PhyloTreeKeys.PATH_ID] == cid:
               clade = child
               break
      if clade[PhyloTreeKeys.PATH_ID] == pathId:
         return clade
      else:
         raise LmTreeException("Could not find clade: %s" % pathId)

   # ..............................
   def getDistanceMatrix(self, squidLabels=False):
      """
      @summary: Build a Euclidean Distance Matrix for the clades in the tree
                   with matrix indices.  The values of each cell will be the 
                   distance between the two clades
      @todo: Consider expanding this method for the entire tree instead of only
                clades with matrix indices
      """
      # We can ignore distance to nodes since we are not adding
      _, distances, labelsDict = self._getDistanceDictionary(self.tree)
      
      numMtxIdxs = len(labelsDict.keys())
      # Assume that the n nodes with matrix indices are continuous from zero to 
      #    n-1

      # Create a numpy matrix full of zeros
      distMtx = np.zeros((numMtxIdxs, numMtxIdxs), dtype=float)
      for mtxIdx1 in distances.keys():
         for mtxIdx2 in distances[mtxIdx1].keys():
            distMtx[mtxIdx1, mtxIdx2] = distances[mtxIdx1][mtxIdx2]
      
      if squidLabels:
         labelKey = PhyloTreeKeys.SQUID
      else:
         labelKey = PhyloTreeKeys.NAME
      
      # Get labels by sorting the matrix index keys and getting the label
      labels = [labelsDict[k][labelKey] for k in sorted(labelsDict.keys())]

      # Create a Lifemapper Matrix object where the headers for both rows and 
      #    columns are the tip labels
      distanceMatrix = Matrix(distMtx, headers={0: labels, 1: labels})
      return distanceMatrix

   # ..............................
   def getMatrixIndicesInClade(self, clade=None):
      """
      @summary: Returns a list of all matrix indices in the tree
      @param clade: (optional) If not provided, use the root
      @note: Duplication is possible if matrix index is present in multiple 
                clades of the tree
      """
      if clade is None:
         clade = self.tree
         
      return self._getMatrixIndicesInClade(clade)

   # ..............................
   def getMatrixIndicesMapping(self):
      """
      @summary: Get a dictionary mapping of path id : matrix index 
      """
      return self._getMatrixIndicesMapping(self.tree)

   # ..............................
   def getLabels(self):
      """
      @summary: Get tip labels for a clade
      @note: Bottom-up order
      """
      labels = self._getLabels(self.tree)
      
      # Reverse so bottom-up ordering instead of top-down
      labels.reverse()
      
      return labels
      
   # ..............................
   def getMatrixIndexLabelPairs(self, useSquids=True, sort=True):
      """
      @summary: Returns a list of (matrix index, clade label) pairs
      @param useSquids: If true, use squids for labels, else use name
      @param sorted: If true, sort the list by matrix index before return
      """
      if useSquids:
         labelKey = PhyloTreeKeys.SQUID
      else:
         labelKey = PhyloTreeKeys.NAME
         
      pairs = self._getMatrixIndexLabelPairs(self.tree, labelKey)
      if sorted:
         return sorted(pairs)
      else:
         return pairs
      
   # ..............................
   def hasBranchLengths(self):
      """
      @summary: Returns boolean indicating if the tree has branch lengths for
                   every clade
      """
      return self._hasBranchLengths(self.tree)

   # ..............................
   def hasPolytomies(self):
      """
      @summary: Returns boolean indicating if the tree has polytomies
      """
      return self._hasPolytomies(self.tree)
   
   # ..............................
   def isBinary(self):
      """
      @summary: Returns a boolean indicating if the tree is binary
      @note: Checks that every clade has either zero or two children
      """
      return self._isBinary(self.tree)
   
   # ..............................
   def isUltrametric(self):
      """
      @summary: Check if the tree is ultrametric
      @note: To be ultrametric, the branch length from root to tip must be 
                equal for all tips
      """
      # Only possible if the tree has branch lengths
      if self.hasBranchLengths():
         # Get the branch lengths dictionary
         lengths = self.getBranchLengths()
         
         # Initialize the check sum.  All tips must have same sum
         checkSum = None
         for tip in self.tips:
            # Create a list of all of the branch lengths for a tip
            tipLengths = []
            for pathId in self.cladePaths[tip]:
               tipLengths.append(lengths[pathId])
            
            tipBL = round(sum(tipLengths), 3)
            
            if checkSum is None:
               checkSum = tipBL
            elif tipBL != checkSum:
               return False
         # If we made it through all of the tips, return true
         return True
      else:
         return False
   
   # ..............................
   def pruneTipsWithoutMatrixIndices(self):
      """
      @summary: Prunes the tree of any tips that don't have a matrix index
      """
      if self._pruneTipsWithoutMtxIdx(self.tree):
         # If true, prune root
         self.tree = {}
      # Clean up the tree after we prune
      self._processTree()
   
   # ..............................
   def pruneTree(self, labels, onlyTips=True):
      """
      @summary: Prunes the tree of any clade in the labels list
      @param labels: A list of labels to prune from the tree
      @param onlyTips: If true, only prune labels that are tips of the tree,
                          otherwise prune clades anywhere in the tree
      """
      # Will return boolean indicating if the root should be pruned
      pruneRoot = self._pruneTree(self.tree, labels, onlyTips)
      if pruneRoot:
         self.tree = {}
      # Clean up the tree after we prune
      self._cleanUpClade()
   
   # ..............................
   def removeMatrixIndices(self):
      """
      @summary: Remove all matrix indices from the tree
      @todo: Evaluate how useful this actually is
      """
      self._removeMatrixIndices(self.tree)
      
   # ..............................   
   def resolvePolytomies(self):
      """
      @summary: Resolve polytomies in a tree
      @note: It may be more efficient to fix all paths at the end rather than
                while we are resolving polytomies, but only if there are many
                to resolve.  Otherwise it is more efficient to only fix the
                clades that were changed
      """
      self._resolvePolytomies(self.tree)

   # ..............................
   def setBranchLengthForClade(self, pathId, branchLength):
      """
      @summary: Set the branch length for the specific clade
      @param pathId: The path id for the clade
      @param branchLength: The new branch length for the clade
      """
      clade = self.getClade(pathId)
      clade[PhyloTreeKeys.BRANCH_LENGTH] = branchLength
   
   # ..............................
   def writeTree(self, fn):
      """
      @summary: Writes the tree JSON to the specified file path
      @param fn: The file location to write the JSON tree
      @todo: Possibly remove.  Unless we decide that the trees written to disk
                should not include matrix index / path / maybe others.  We may
                do that because those things can vary between loads.  We are 
                already resetting the path on load
      """
      with open(fn, 'w') as outF:
         self.writeTreeToFlo(outF)
         
   # ..............................
   def writeTreeToFlo(self, flo):
      """
      @summary: Write the tree JSON to a file-like object
      @param flo: A file-like object to write to
      """
      json.dump(self.tree, flo, sort_keys=True, indent=3)
      
   
   # Helper methods
   # ..........................................................................
   # ..............................
   def _addMatrixIndices(self, clade, pamMetadata):
      """
      @summary: Recursively adds matrix indices to a clade
      @param clade: The clade to add matrix indices to
      @param pamMetadata: A dictionary of (label, matrix index) pairs for a PAM
      """
      # If the name of this clade is in the PAM metadata, add the matrix index
      if pamMetadata.has_key(clade[PhyloTreeKeys.NAME]):
         clade[PhyloTreeKeys.MTX_IDX] = pamMetadata[clade[PhyloTreeKeys.NAME]] 
   
      for child in clade[PhyloTreeKeys.CHILDREN]:
         self._addMatrixIndices(child, pamMetadata)

   # ..............................
   def _addSquidMatrixIndices(self, clade, pamMetadata):
      """
      @summary: Recursively adds matrix indices to a clade
      @param clade: The clade to add matrix indices to
      @param pamMetadata: A dictionary of (squid, matrix index) pairs for a PAM
      """
      # If the name of this clade is in the PAM metadata, add the matrix index
      if clade.has_key(PhyloTreeKeys.SQUID) and pamMetadata.has_key(clade[PhyloTreeKeys.SQUID]):
         clade[PhyloTreeKeys.MTX_IDX] = pamMetadata[clade[PhyloTreeKeys.SQUID]] 
   
      for child in clade[PhyloTreeKeys.CHILDREN]:
         self._addSquidMatrixIndices(child, pamMetadata)

   # ..............................
   def _addSQUIDs(self, clade, squidDict):
      """
      @summary: Recursively adds LM species SQUIDs to a clade
      @param clade: The clade to add SQUIDs to
      @param squidDict: A dictionary of label, squid pairs for the tree
      """
      # If the name of this clade is in the SQUID dictionary, add the squid
      if squidDict.has_key(clade[PhyloTreeKeys.NAME]):
         clade[PhyloTreeKeys.SQUID] = squidDict[clade[PhyloTreeKeys.NAME]]
   
      for child in clade[PhyloTreeKeys.CHILDREN]:
         self._addSQUIDs(child, squidDict)

   # ..............................
   def _cleanUpClade(self, clade=None, basePath=[]):
      """
      @summary: Recursively fixes the paths to each node and adds any missing 
                   keys
      @param clade: The clade to clean up
      @param basePath: The base path to use for this clade
      @todo: Clean up should probably ensure that the resulting tree is binary
      """
      if clade is None:
         clade = self.tree
         self.cladePaths = {} # Reset tip paths and calculate them all again
         self.tips = [] # Reset as well
      
      # Make sure that clade has children
      if not clade.has_key(PhyloTreeKeys.CHILDREN):
         clade[PhyloTreeKeys.CHILDREN] = []
         
      # Make sure clade has a name, even if it is blank
      if not clade.has_key(PhyloTreeKeys.NAME):
         clade[PhyloTreeKeys.NAME] = ''
         
      # Clade should have a path id
      if not clade.has_key(PhyloTreeKeys.PATH_ID) or \
            clade[PhyloTreeKeys.PATH_ID] is None:
         clade[PhyloTreeKeys.PATH_ID] = self._getNewPathId()
      
      # Path should be the path to the clade from the root
      cladePath = basePath + [clade[PhyloTreeKeys.PATH_ID]]
      # Set the path on this clade
      clade[PhyloTreeKeys.PATH] = cladePath
      # Recurse to children
      if len(clade[PhyloTreeKeys.CHILDREN]) > 0:
         for child in clade[PhyloTreeKeys.CHILDREN]:
            self._cleanUpClade(child, basePath=cladePath)
            
      else: # This is a tip, path id to tips list
         self.tips.append(clade[PhyloTreeKeys.PATH_ID])
         
      # Add clade path to dictionary
      self.cladePaths[clade[PhyloTreeKeys.PATH_ID]] = clade[PhyloTreeKeys.PATH]

   # ..............................
   def _findLargestPathId(self, clade):
      """
      @summary: Find the largest path id in the tree so that new path ids will
                   not collide
      """
      if clade is None:
         clade = self.tree

      pathIds = []
      # Get the current clade path id (if exists)
      try:
         pathIds.append(clade[PhyloTreeKeys.PATH_ID])
      except: # If path id key is missing
         pass
      
      # Get the children path ids
      if clade.has_key(PhyloTreeKeys.CHILDREN):
         for child in clade[PhyloTreeKeys.CHILDREN]:
            pathIds.append(self._findLargestPathId(child))

      try:
         return max(pathIds) # Could be None if all are None
      except: # Fails if empty list
         return None

   # ..............................
   def _getBranchLengths(self, clade):
      """
      @summary: Recursively get branch lengths for a clade
      @param clade: The clade to get branch lengths for
      @return: A list of (path id, branch length) pairs for this clade and all
                  sub-clades
      """
      branchLengths = []
      if clade.has_key(PhyloTreeKeys.BRANCH_LENGTH):
         branchLengths.append((clade[PhyloTreeKeys.PATH_ID], 
                               clade[PhyloTreeKeys.BRANCH_LENGTH]))
      else:
         raise LmTreeException("Clade {0} does not have branch length".format(
              clade[PhyloTreeKeys.PATH_ID]))
      for child in clade[PhyloTreeKeys.CHILDREN]:
         branchLengths.extend(self._getBranchLengths(child))
      return branchLengths
   
   # ..............................
   def _getDistanceDictionary(self, clade):
      """
      @summary: Recursively build a dictionary of distances between clades with
                   matrix indices
      """
      # Initialize dictionaries
      distanceToNodes = {}
      distanceBetweenNodes = {}
      labels = {}
      
      # Handle this node
      if clade.has_key(PhyloTreeKeys.MTX_IDX):
         cladeMtxIdx = clade[PhyloTreeKeys.MTX_IDX]
         # Add entry for label
         labels[cladeMtxIdx] = {
            PhyloTreeKeys.SQUID : clade[PhyloTreeKeys.SQUID] \
                    if clade.has_key(PhyloTreeKeys.SQUID) else '',
            PhyloTreeKeys.NAME: clade[PhyloTreeKeys.NAME]
         }
         distanceToNodes[cladeMtxIdx] = 0.0 # Distance to self is zero
         distanceBetweenNodes[cladeMtxIdx] = {
            cladeMtxIdx : 0.0 # Distance to self is zero
         }
      
      # Recurse into children
      for child in clade[PhyloTreeKeys.CHILDREN]:
         childToNodes, childBetweenNodes, \
                  childLabels = self._getDistanceDictionary(child)
         
         # Update distance to nodes with these from child
         distanceToNodes.update(childToNodes)

         # Update labels
         labels.update(childLabels)

         # Add to distances between nodes
         for k1 in distanceBetweenNodes.keys():
            # For each in this child
            for k2 in childBetweenNodes.keys():
               distK1K2 = distanceToNodes[k1] + distanceToNodes[k2]
               distanceBetweenNodes[k1][k2] = distK1K2
               childBetweenNodes[k2][k1] = distK1K2

         # Update dictionary
         distanceBetweenNodes.update(childBetweenNodes)
      
      # Need to add distance to this clade on the way up the tree
      for k in distanceToNodes.keys():
         distanceToNodes[k] += clade[PhyloTreeKeys.BRANCH_LENGTH]
      
      # Check if we have a matrix index
      return distanceToNodes, distanceBetweenNodes, labels

   # ..............................
   def _getLabels(self, clade):
      """
      @summary: Get tip labels for a clade
      @param clade: The clade to return labels for
      """
      localLabels = []
      
      if len(clade[PhyloTreeKeys.CHILDREN]) > 0: # Not a tip, so recurse
         for child in clade[PhyloTreeKeys.CHILDREN]:
            localLabels.extend(self._getLabels(child))
      else: # Tip, return (label, path id)
         localLabels.append((clade[PhyloTreeKeys.NAME], clade[PhyloTreeKeys.PATH_ID]))

      return localLabels
   
   # ..............................
   def _getMatrixIndexLabelPairs(self, clade, labelKey):
      """
      @summary: Recursively build a list of matrix index, label pairs
      @param clade: The clade to check for matrix index and recurse
      @param labelKey: The key to use as a label for this clade
      """
      pairs = []
      if clade.has_key(PhyloTreeKeys.MTX_IDX):
         pairs.append((clade[PhyloTreeKeys.MTX_IDX], clade[labelKey]))
      
      for child in clade[PhyloTreeKeys.CHILDREN]:
         pairs.extend(self._getMatrixIndexLabelPairs(child, labelKey))
      
      return pairs
   
   # ..............................
   def _getMatrixIndicesInClade(self, clade):
      """
      @summary: Recurses through the clade and builds a list of matrix indices
                   in the clade
      @param clade: The clade to look for matrix indices in
      """
      mtxIdxs = []
      if clade.has_key(PhyloTreeKeys.MTX_IDX):
         mtxIdxs.append(clade[PhyloTreeKeys.MTX_IDX])
      for child in clade[PhyloTreeKeys.CHILDREN]:
         mtxIdxs.extend(self._getMatrixIndicesInClade(child))
      return mtxIdxs

   # ..............................
   def _getMatrixIndicesMapping(self, clade):
      """
      @summary: Recursively build a dictionary of matrix index mapping to path 
                   id
      """
      mapping = {}
      if clade.has_key(PhyloTreeKeys.MTX_IDX):
         mapping[clade[PhyloTreeKeys.PATH_ID]] = clade[PhyloTreeKeys.MTX_IDX]
      for child in clade[PhyloTreeKeys.CHILDREN]:
         mapping.update(self._getMatrixIndicesMapping(child))
      return mapping
   
   # ..............................
   def _getNewPathId(self):
      """
      @summary: Gets an unused path id to use for a new clade
      """
      self._lastCladeId += 1
      return self._lastCladeId

   # ..............................
   def _hasBranchLengths(self, clade):
      """
      @summary: Returns boolean indicating if this clade and all sub-clades have
                   branch lengths
      @param clade: The clade to check
      """
      if not clade.has_key(PhyloTreeKeys.BRANCH_LENGTH) or \
            clade[PhyloTreeKeys.BRANCH_LENGTH] is None:
         return False
      
      hasBL = [True] # One true for this current clade
      for child in clade[PhyloTreeKeys.CHILDREN]:
         hasBL.append(self._hasBranchLengths(child))
      
      return all(hasBL) # Will return false if any are not true
   
   # ..............................
   def _hasPolytomies(self, clade):
      """
      @summary: Recursively look for polytomies in a clade
      @param clade: The clade to search
      """
      if clade.has_key(PhyloTreeKeys.CHILDREN):
         if len(clade[PhyloTreeKeys.CHILDREN]) > 2:
            return True
         else:
            for c in clade[PhyloTreeKeys.CHILDREN]:
               if self._hasPolytomies(c):
                  return True
      return False # Only if no polytomies here or branches

   # ..............................
   def _isBinary(self, clade):
      """
      @summary: Checks if a clade is binary
      @param clade: The clade to check
      @note: To be binary, each clade must have either zero (for tips) or two
                (for internal clades) children
      """
      if len(clade[PhyloTreeKeys.CHILDREN]) == 0:
         return True
      elif len(clade[PhyloTreeKeys.CHILDREN]) == 2:
         return self._isBinary(clade[PhyloTreeKeys.CHILDREN][0]) and \
                self._isBinary(clade[PhyloTreeKeys.CHILDREN][1])
      else:
         return False
      
   # ..............................
   def _mergeClades(self, clade1, clade2):
      """
      @summary: Merge clade2 with clade1 (clade 1 is primary)
      @param clade1: The primary clade
      @param clade2: The sub clade
      """
      try:
         # Add branch lengths
         clade1[PhyloTreeKeys.BRANCH_LENGTH] += clade2[
            PhyloTreeKeys.BRANCH_LENGTH]
      except:
         pass
      
      try:
         # Merge squid
         clade1[PhyloTreeKeys.SQUID] = clade2[PhyloTreeKeys.SQUID]
      except:
         pass
      
      try:
         # Matrix index
         clade1[PhyloTreeKeys.MTX_IDX] = clade2[PhyloTreeKeys.MTX_IDX]
      except:
         pass
      
      try:
         # Label
         if not clade1.has_key(PhyloTreeKeys.NAME) or not clade1[PhyloTreeKeys.NAME]:
            clade1[PhyloTreeKeys.NAME] = clade2[PhyloTreeKeys.NAME]
      except:
         pass
      
      try:
         clade1[PhyloTreeKeys.CHILDREN] = clade2[PhyloTreeKeys.CHILDREN]
      except:
         clade1[PhyloTreeKeys.CHILDREN] = []
      
      return clade1

   # ..............................
   def _processTree(self):
      """
      @summary: Process the provided tree, fill in missing information, and 
                   create clade paths dictionary
      """
      self._lastCladeId = self._findLargestPathId(self.tree)
      if self._lastCladeId is None:
         self._lastCladeId = -1
         
      # Fill in paths and populate tips
      self._cleanUpClade()

   # ..............................
   def _pruneTipsWithoutMtxIdx(self, clade):
      """
      @summary: Recursively prune the tree of any tips without a matrix index
                   and merge branches as necessary to maintain a binary, 
                   ultrametric tree
      @param clade: The clade to prune
      @return: Boolean indicating if this branch should be pruned
      """
      # If at a child, check to see if matrix index
      if not clade[PhyloTreeKeys.CHILDREN]: # Boolean like
         return not clade.has_key(PhyloTreeKeys.MTX_IDX)
      else:
         child0 = clade[PhyloTreeKeys.CHILDREN][0]
         child1 = clade[PhyloTreeKeys.CHILDREN][1]
         
         prune0 = self._pruneTipsWithoutMtxIdx(child0)
         prune1 = self._pruneTipsWithoutMtxIdx(child1)
         
         if prune0 or prune1:
            clade[PhyloTreeKeys.CHILDREN] = []
         
         if prune0 and prune1:
            return True
         elif prune0:
            # Merge child1 and node
            self._mergeClades(clade, child1)
         elif prune1:
            # Merge child0 and node
            self._mergeClades(clade, child0)
         return False

   # ..............................
   def _pruneTree(self, clade, labels, onlyTips):
      """
      @summary: Recursively prune the tree of any clades that have names in the
                   labels list
      @param clade: The clade to prune
      @param labels: A list of labels to prune
      @param onlyTips: If true, only prune tips and leave interior clades as 
                          they are
      @return: Boolean indicating if this branch should be pruned
      """
      # Only check this clade if not only tips or if this is a tip
      if not onlyTips or len(clade[PhyloTreeKeys.CHILDREN]) == 0:
         if clade[PhyloTreeKeys.NAME] in labels:
            return True

      # Check if children should be pruned (automatically pruned if this clade
      #    is pruned
      pruneChildren = []
      for i in range(len(clade[PhyloTreeKeys.CHILDREN])):
         if self._pruneTree(clade[PhyloTreeKeys.CHILDREN][i], labels, onlyTips):
            pruneChildren.append(i) # Append i to list if we should prune

      # Reverse list so that we can prune from the back first
      #    Otherwise indexes will be wrong
      pruneChildren.reverse()
      
      for j in pruneChildren:
         clade[PhyloTreeKeys.CHILDREN].pop(j)
      
      return False

   # ..............................
   def _removeMatrixIndices(self, clade):
      """
      @summary: Recursively remove matrix indices from the tree
      @param clade: The clade to remove matrix indices from
      """
      try:
         clade.pop(PhyloTreeKeys.MTX_IDX)
      except KeyError, ke: # Matrix index key was not present
         pass
      
      for child in clade[PhyloTreeKeys.CHILDREN]:
         self._removeMatrixIndices(child)

   # ..............................
   def _resolvePolytomies(self, clade):
      """
      @summary: Resolve polytomies in a clade
      @param clade: The clade to resolve polytomies in
      """
      while len(clade[PhyloTreeKeys.CHILDREN]) > 2:
         shuffle(clade[PhyloTreeKeys.CHILDREN])
         c1 = clade[PhyloTreeKeys.CHILDREN].pop(0)
         c2 = clade[PhyloTreeKeys.CHILDREN].pop(0)

         # Create a new clade dictionary with the two children
         #    We'll assign a path id in the cleanup step
         newClade = {
            PhyloTreeKeys.NAME: '',
            PhyloTreeKeys.CHILDREN: [c1, c2],
            PhyloTreeKeys.PATH: [],
            PhyloTreeKeys.BRANCH_LENGTH: 0.0
         }

         # Fix the paths of this clade and all children
         self._cleanUpClade(newClade, basePath=self.tree[PhyloTreeKeys.PATH])

         # Append the new clade
         clade[PhyloTreeKeys.CHILDREN].append(newClade)
      
      for child in clade[PhyloTreeKeys.CHILDREN]:
         self._resolvePolytomies(child)
