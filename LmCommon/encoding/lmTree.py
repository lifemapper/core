"""
@summary: Module containing LmTree class
@author: CJ Grady (originally from Jeff Cavner)
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
"""
import numpy as np
import os
from random import randint, shuffle
import json

from LmCommon.common.lmconstants import FileFormats, OutputFormat, PhyloTreeKeys
from LmCommon.encoding.newickToJson import Parser

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
      self.lastCladeId = None
      
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
      newTreeDict = oldTree.tree.copy()
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
      
      if ext in FileFormats.JSON.getExtensions(): # JSON
         return cls(content)
      elif ext in FileFormats.NEWICK.getExtensions(): # Newick
         newickParser = Parser(content)
         jsonTree, _ = newickParser.parse()
         return cls(jsonTree)
      else: # Unknown
         raise Exception, "LmTree does not know how to read %s files" % ext
      
   # ..............................   
   @classmethod
   def convertFromNewick(cls, filename):
      """
      @summary: Creates a new LmTree object by converting a Newick tree
      @param filename: The location of the Newick tree file
      @raise IOError: Raised if the file does not exist
      """
      with open(filename) as inF:
         newickString = inF.read()
      # Use the Newick parser to get JSON, then feed that into constructor
      newickParser = Parser(newickString)
      jsonTree, _ = newickParser.parse()
      return cls(jsonTree)
   
   # ..............................
   @classmethod
   def createRandomTree(cls, numTips):
      """
      @summary: Creates a random binary Phylogenetic tree with the specified 
                   number of tips
      @param numTips: The number of tips to include in this tree
      @note: Tips will have path ids 1 - numTips
      @note: Following convention in R package to number beginning with 1
      """
      # Create the root
      root = {
         PhyloTreeKeys.PATH_ID: 0,
         PhyloTreeKeys.PATH: [0],
         PhyloTreeKeys.CHILDREN: [],
         PhyloTreeKeys.NAME: '',
         PhyloTreeKeys.BRANCH_LENGTH: 0.0
      }
      
      n = 1
      for i in range(numTips):
         root[PhyloTreeKeys.CHILDREN].append({
                          PhyloTreeKeys.PATH_ID: i+1, 
                          PhyloTreeKeys.PATH: [], 
                          PhyloTreeKeys.CHILDREN: [], 
                          PhyloTreeKeys.NAME: str(i+1), 
                          PhyloTreeKeys.BRANCH_LENGTH: 0.0})
      
      n = numTips + 1
      
      newTree = cls(root)
      newTree.resolvePolytomies()
      return newTree

   # Public functions
   # ..........................................................................
   # ..............................
   def addMatrixIndices(self, pamMetadata):
      """
      @summary: Add matrix indices to the tree
      @param pamMetadata: A dictionary of (label, matrix index) pairs for a PAM
      """
      self._addMatrixIndices(self.tree, pamMetadata)
   
   # ..............................
   def checkUltraMetric(self):
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
            elif urs != checkSum:
               return False
         # If we made it through all of the tips, return true
         return True
      else:
         return False
   
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
      """
      if self.cladePaths.has_key(pathId):
         cladePath = self.cladePaths[pathId]
      else:
         raise Exception, "Path id: %s was not found" % pathId
      
      # Reverse path so we can process easier
      cladePath.reverse()
      
      clade = self.tree
      for cid in cladePath[1:]: # We can skip the root
         for child in clade[PhyloTreeKeys.CHILDREN]:
            if child[PhyloTreeKeys.PATH_ID] == cid:
               clade = child
               break
      if clade[PhyloTreeKeys.PATH_ID] == pathId:
         return clade
      else:
         raise Exception, "Could not find clade: %s" % pathId

   # ..............................
   def getCommonAncestor(self, pathIds):
      """
      @summary: Gets the common ancestor clade of the path ids specified
      @param pathIds: A list of path ids to find the common ancestor of
      @note: Assumes that all paths share root
      """
      paths = []
      for pathId in pathIds:
         path = self.cladePaths[pathId]
         path.reverse() # Reverse for easy comparison
         paths.append(path)
         
      i = 0
      try:
         while True: # Loop until the paths don't match
            items = set([]) # Create a set for the items
            for pth in paths:
               items.add(pth[i+1]) # Will fail out if goes past end of list 
            
            if len(items) == 1: # If all items are the same
               i += 1
            else:
               break
      except: # If we reach the end of a path most likely, i should be fine
         pass
      
      return self.getClade(paths[0][i])
 
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
   def getPathIdsWithoutBranchLengths(self):
      """
      @summary: Returns a list of path ids without branch lengths
      """
      return self._getPathIdsWithoutBranchLengths(self.tree)

   # ..............................
   def getTipsToPrune(self, pamMetadata):
      """
      @summary: Returns a list of path ids for tips that are not in the pam
                   metadata dictionary
      @param pamMetadata: A dictionary of (label, matrix index) pairs for a PAM
      @return: A list of labels that are in the tree but not the PAM
      """
      pamLabels = pamMetadata.keys()
      treeLabels = self.getLabels()
      # Create a list of the labels in treeLabels that are not in pamLabels
      labelsToPrune = list(set(treeLabels) - set(pamLabels))
      
      if len(labelsToPrune) == len(treeLabels):
         raise Exception, "Cannot prune all tips, PAM does not match tree"
      else:
         return labelsToPrune
   
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
      """
      with open(fn, 'w') as outF:
         self.writeTree(outF)
         
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
   def _cleanUpClade(self, clade=None, basePath=[]):
      """
      @summary: Recursively fixes the paths to each node and adds any missing 
                   keys
      @param clade: The clade to clean up
      @param basePath: The base path to use for this clade
      @todo: Should the path be reversed?
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
      
      # Path should be the path id of this clade followed by the path to the
      #    parent.  This creates a list starting at the node and then going
      #    up the tree to the root.
      cladePath = [clade[PhyloTreeKeys.PATH_ID]] + basePath
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
   def _findLargestPathId(self, clade=None):
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
         raise Exception, "Clade %s does not have branch length" % \
              clade[PhyloTreeKeys.PATH_ID]
      for child in clade[PhyloTreeKeys.CHILDREN]:
         branchLengths.extend(self._getBranchLengths(child))
   
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
   def _getNewPathId(self):
      """
      @summary: Gets an unused path id to use for a new clade
      """
      self.lastCladeId += 1
      return self.lastCladeId

   # ..............................
   def _getPathIdsWithoutBranchLengths(self, clade):
      """
      @summary: Recursively finds clades without branch lengths
      @param clade: The clade to recurse through
      """
      noBranchLengths = []
      if not clade.has_key(PhyloTreeKeys.BRANCH_LENGTH):
         noBranchLengths.append(clade[PhyloTreeKeys.PATH_ID])
      for child in clade[PhyloTreeKeys.CHILDREN]:
         noBranchLengths.extend(self._getBranchLengths(child))
      return noBranchLengths
   
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
   def _processTree(self):
      """
      @summary: Process the provided tree, fill in missing information, and 
                   create clade paths dictionary
      """
      self.lastCladeId = self._findLargestPathId(self, clade=None)
      if self.lastCladeId is None:
         self.lastCladeId = -1
         
      # Fill in paths and populate tips
      self._cleanUpClade()

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
         self._cleanUpClade(newClade, basePath=tree[PhyloTreeKeys.PATH])

         # Append the new clade
         clade[PhyloTreeKeys.CHILDREN].append(newClade)
      
      for child in clade[PhyloTreeKeys.CHILDREN]:
         self.resolvePolytomies(child)
