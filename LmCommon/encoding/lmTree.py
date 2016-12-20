"""
@summary: Module containing LmTree class
@author: Jeff Cavner (edited by CJ Grady)
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
"""
import numpy as np
import os
from random import randint, shuffle
import json

from LmCommon.common.lmconstants import FileFormats, OutputFormat, PhyloTreeKeys
from LmCommon.encoding.newickToJson import Parser

NO_BRANCH_LEN = 0
MISSING_BRANCH_LEN = 1
HAS_BRANCH_LEN = 2

# .............................................................................
class LmTree(object):
   """
   @summary: Class representing a phylogenetic tree in Lifemapper
   @todo: Add get lengths method that gets the latests lengths
   @todo: Add a get labels method if needed, return bottom up tip labels
   @todo: Add a drop matrix indices method
   @todo: What happens if mtxidx is in tree?
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
      
      # The tipPaths dictionary contians paths (value) to the tips (key)
      self.tipPaths = {}
      self._processTree()
      
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
   
   # Public functions
   # ..........................................................................

   # TODO: Get common ancestor?

   # TODO: Get node


   # TODO: Set branch length for node
   
   # TODO: Drop tips

   # TODO: Find nodes without branch lengths

   # TODO: Add matrix ids

   # TODO: Remove matrix id(s)

   # ..............................   
   def checkUltraMetric(self):
      """
      @summary: Check if the tree is ultrametric
      @note: To be ultrametric, the branch length from root to tip must be 
                equal for all tips
      @summary: check to see if tree is ultrametric, all the way to the root
      @todo: Return true for some scenario
      @todo: Rewrite
      """
      # Only possible if the tree has branch lengths
      if self.branchLengths == HAS_BRANCH_LEN:
         checkSum = None
         # TODO: Can we do this better?
         for tip in self.tipPaths:
            # We only need the tip path up the tree
            # TODO: Is that true?
            copytipPath = self.tipPaths[tip][0][1:]
            
            # Create a list of branch lengths from the tip to root, sum them, 
            #    and round to 3 decimal places
            urs = round(
               sum([self.lengths[pathId] for pathId in copytipPath]), 3)
            
            if checkSum is None:
               checkSum = urs
            elif urs != checkSum: 
               # We can fail the first time a time has a different branch 
               #    length to root
               return False
      else:
         return False
   
   # ..............................   
   def createRandomTree(self, numTips):
      """
      @summary: Creates a random binary Phylogenetic tree with the specified 
                   number of tips
      @param numTips: The number of tips to include in this tree
      @note: Tips will have path ids 1 - numTips
      @note: Following convention in R package to number beginning with 1
      @todo: This should be a class method and return a new object
      """
      clades = []
      for i in range(numTips):
         clades.append({
                          PhyloTreeKeys.PATH_ID: i+1, 
                          PhyloTreeKeys.PATH: [], 
                          PhyloTreeKeys.CHILDREN: [], 
                          PhyloTreeKeys.NAME: str(i+1), 
                          PhyloTreeKeys.BRANCH_LENGTH: 0.0})
      
      n = numTips + 1
      
      while len(clades) > 1:
         shuffle(clades)
         c1 = clades.pop(0)
         c2 = clades.pop(0)
         newClade = {
                       PhyloTreeKeys.PATH_ID: n,
                       PhyloTreeKeys.PATH: [],
                       PhyloTreeKeys.CHILDREN : [c1, c2],
                       PhyloTreeKeys.BRANCH_LENGTH: 0.0
                    }
         clades.append(newClade)
         n += 1
      
      return clades[0]

   # ..............................
   def getLabels(self, clade=None):
      """
      @summary: Get tip labels for a clade
      @param clade: The clade to return labels for, uses root if None
      @note: Bottom-up order
      """
      if clade is None:
         clade = self.tree
      
      labels = self._getLabels(clade)
      
      # Reverse so bottom-up ordering instead of top-down
      labels.reverse()
      
      return labels
      
   # ..............................
   def hasPolytomies(self):
      """
      @summary: Returns boolean indicating if the tree has polytomies
      """
      return self._hasPolytomies(self.tree)
   
   # ..............................   
   def resolvePolytomies(self, tree=None):
      """
      @summary: Resolve polytomies in a tree
      @param tree: (Optional) The tree to resolve.  If omitted, use tree root
      @todo: Do we need to do anything with branch lengths?
      @todo: Consider fixing all paths at the end
      @todo: Consider using clade instead of tree
      """
      if tree is None: # Start at the root
         tree = self.tree

      while len(tree[PhyloTreeKeys.CHILDREN]) > 2:
         shuffle(tree[PhyloTreeKeys.CHILDREN])
         c1 = tree[PhyloTreeKeys.CHILDREN].pop(0)
         c2 = tree[PhyloTreeKeys.CHILDREN].pop(0)

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
         tree[PhyloTreeKeys.CHILDREN].append(newClade)
      
      for i in range(len(tree[PhyloTreeKeys.CHILDREN])):
         self.resolvePolytomies(tree=tree[PhyloTreeKeys.CHILDREN][i])
      
   # ..............................   
   def writeTree(self, path):
      """
      @todo: Document
      @todo: Use json.dump?
      """
      #if os.path.exists(path):
      with open(path,'w') as f:
         f.write(json.dumps(self.tree, sort_keys=True, indent=4))
      
   
   # Helper methods
   # ..........................................................................
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
         self.tipPaths = {} # Reset tip paths and calculate them all again
      
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
            
      else: # This is a tip, add the path to the tipPaths dictionary
         self.tipPaths[clade[PhyloTreeKeys.PATH_ID]] = clade[PhyloTreeKeys.PATH]

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
   def _getLabels(self, clade):
      """
      @summary: Get tip labels for a clade
      @param clade: The clade to return labels for
      """
      localLabels = []
      
      if len(clade[PhyloTreeKeys.CHILDREN]) > 0: # Not a tip, so recurse
         for child in clade[PhyloTreeKeys.CHILDREN]:
            localLabels.extend(self._getLabels(child))
      else: # Tip, return label
         localLabels.append(clade[PhyloTreeKeys.NAME])

      return localLabels
   
   # ..............................
   def _getNewPathId(self):
      """
      @summary: Gets an unused path id to use for a new clade
      """
      self.lastCladeId += 1
      return self.lastCladeId

   # ..............................
   def _hasPolytomies(self, clade):
      if clade.has_key(PhyloTreeKeys.CHILDREN):
         if len(clade[PhyloTreeKeys.CHILDREN]) > 2:
            return True
         else:
            for c in clade[PhyloTreeKeys.CHILDREN]:
               if self._hasPolytomies(c):
                  return True
      return False # Only if no polytomies here or branches

   # ..............................
   def _processTree(self):
      """
      @summary: Process the provided tree, fill in missing information, and 
                   create tipPaths dictionary
      """
      self.lastCladeId = self._findLargestPathId(self, clade=None)
      if self.lastCladeId is None:
         self.lastCladeId = -1
         
      # Fill in paths and populate tips
      self._cleanUpClade()

   # Jeff's old code
   # ..........................................................................

   # ..............................   
   def findDropTipsDelMtx(self, dropTips):
      """
      @summary: find tips by name to drop and if there is a mx for tip removes all mx
      @todo: Document
      @todo: Remove inner function
      """     
      # ..............................   
      def takeOutMtx(clade):
         if clade.has_key(PhyloTreeKeys.CHILDREN):
            clade.pop(PhyloTreeKeys.MTX_IDX, None)
            for child in clade[PhyloTreeKeys.CHILDREN]:
               # TODO: This is not possible given Jeff's implementation
               takeOutMtx(child)
         else:
            clade.pop(PhyloTreeKeys.MTX_IDX, None)
      
      for n in dropTips:
         if n in self.tipNamesWithMX:
            takeOutMtx(self.tree)
            break
               
   # ..............................   
   def dropTips(self, tips):
      """
      @summary: drop tips from current tree returns new tree
      @param tips: list or array of tip (label) names to be removed
      @return: new tree obj
      @todo: In-line documentation
      """
      self.findDropTipsDelMtx(tips)
      if len(tips) < len(self.labelIds) - 1:
         edge = self._getEdges()
         nTips = len(self.labels) #Ntip
         nEdge = edge.shape[0]
         edge_1 = edge[:,0] 
         edge_2 = edge[:,1]
         
         #keep[match(tip, edge2)] <- FALSE
         labelmask = np.in1d(self.labels, tips)
         #TODO: Don't do this
         tips = self.labelIds[labelmask]
         tips = np.where(np.in1d(edge_2, tips))
         #print tips
         keep =  np.ones(nEdge, dtype=bool)
         keep[tips] = False 
         #print keep
         
         int_edge2 = [x for x in edge_2 if str(x) in self.internalPaths]
         ints = np.in1d(edge_2, int_edge2)
         #print ints
         
         e1Keep = edge_1[keep]
         e2WithE1Keep_not = np.logical_not(np.in1d(edge_2, e1Keep))
         
         while True:
            sel = reduce(np.logical_and, (e2WithE1Keep_not, ints, keep))
            if not(sum(sel)):
               break
            keep[sel] = False
         newEdges = edge[keep]
        
         if self.branchLengths == HAS_BRANCH_LEN:
            len_copy = self.lengths.copy()
         else:
            len_copy = False
         
         #######
         
         rowsToDelete = []
         #TODO: Better variable names
         for i, r in enumerate(newEdges):
            iN = r[0]  # internal node
            #TODO: Count seems unneccessary
            count = len(np.where(newEdges[:,0] == iN)[0])
            if count == 1:
               # then find if iN or r[1]? is in newEdges[:,1]
               if len(np.where(newEdges[:,1] == iN)[0]) == 0:
                  # then iN has no parent and can be deleted
                  rowsToDelete.append(i) 
               else:
                  tN = r[1]
                  if len_copy:
                     newLen = self.lengths[iN] + self.lengths[tN]
                     len_copy[tN] = newLen
                  parentI = np.where(newEdges[:,1] == iN)[0][0]
                  parent = newEdges[parentI][0]
                  find = np.array([parent, iN])
                  parentRow = np.where(np.all(newEdges==find, axis=1))[0][0]
                  newEdges[parentRow][1] = tN
                  rowsToDelete.append(i)
                  
         newEdges = np.delete(newEdges,np.array(rowsToDelete),axis=0)
         ###############
         
         eI = newEdges[:,0]
         eT = newEdges[:,1]
         tipPos = np.where(np.logical_not(np.in1d(eT, eI)))[0]
         newtips = eT[tipPos]
         
         #######
         tree = self._makeCladeFromEdges(newEdges, lengths=len_copy, tips=newtips)  # this relies on old data structures
         self.makePaths(tree)
         
         return Lmtree(tree)
      else:
         raise ValueError('Cannot remove all tips from a tree, or leave single tip')
      
   # ..............................   
   def _getEdges(self):
      """
      @summary: makes a (2 * No. internal Nodes) x 2 matrix representation of the tree
      @todo: Document 
      """
      
      edgeDict = {}
      #TODO: Evaluate
      def recurseEdge(clade):
         if clade.has_key(PhyloTreeKeys.CHILDREN):
            childIds = [int(c[PhyloTreeKeys.PATH_ID]) for c in clade[PhyloTreeKeys.CHILDREN]]
            if int(clade[PhyloTreeKeys.PATH_ID]) not in edgeDict:
               edgeDict[int(clade[PhyloTreeKeys.PATH_ID])] = childIds
            for child in clade[PhyloTreeKeys.CHILDREN]:
               recurseEdge(child)        
      recurseEdge(self.tree)
      
      edge_ll = []
      for e in edgeDict.items():
         for t in e[1]:
            edge_ll.append([e[0], t])
      edge = np.array(edge_ll)
      
      return edge

   #TODO: Document properties and move to end of class definition
   #TODO: Evaluate properties
   
   # ..............................   
   @property
   def tipNames(self):
      return self._tipNames
   
   # ..............................   
   @property
   def tipNamesWithMX(self):
      return self._tipNamesWithMX
   
   # ..............................   
   @property
   def polytomies(self):
      
      return self._polytomy
   
   # ..............................   
   @property
   def internalCount(self):
      
      return len(self.subTrees)   
   
   # ..............................   
   @property
   def subTrees(self):
      
      return self._subTrees
         
   # ..............................   
   @property
   def lengths(self):
     
      return self._lengths
     
   # ..............................   
   @property
   def tipCount(self):
       
      return len(self.tipPaths)
      
   # ..............................   
   @property
   def binary(self):
      #TODO: No need for doing this.  Will return 0 unless tipcount - internal count is 1, then 1
      #        Use length instead
      return bool(1//(self.tipCount - self.internalCount))
      
   # ..............................   
   @property
   def branchLengths(self):
      #TODO: Evalute and fix constants
      #if not self.tipPaths:
      #   self.subTrees = self.getTreeInfo(self.tree)[2] 
      if self._numberMissingLengths == 0:
         return HAS_BRANCH_LEN
      else:    
         if self._numberMissingLengths == (self.tipCount + self.internalCount -1):
            return NO_BRANCH_LEN
         else:
            return MISSING_BRANCH_LEN
   
   # ..............................   
   def _makeCladeFromEdges(self, edge, lengths=False, tips=False):
      """
      @summary: MORE GENERIC VERSION,makes a tree dict from a (2 * No. internal node) x 2 numpy matrix
      @param edge: numpy array of edges (integers)
      @param lengths: boolean for adding lengths
      @todo: Document
      @todo: Probably remove inner functions
      """
      
      iNodes = list(set(edge[:,0]))
      m = {}  # key is internal node, value is list of terminating nodes
      for iN in iNodes:
         dx = np.where(edge[:,0]==iN)[0]
         le = list(edge[dx][:,1])
         m[iN] = le
      #print m
      #m = {k[0]:list(k) for k in edge }
      tree = {PhyloTreeKeys.PATH_ID:str(0), PhyloTreeKeys.PATH:'', PhyloTreeKeys.CHILDREN:[]}  # will take out name for internal after testing
      def recurse(clade, l):
         for x in l:
            if clade.has_children(PhyloTreeKeys.CHILDREN):
               nc = {PhyloTreeKeys.PATH_ID:str(x), PhyloTreeKeys.PATH:''} # will take out name for internal after testing
               if lengths:
                  nc[PhyloTreeKeys.BRANCH_LENGTH] = lengths[x]
               if x not in tips:
                  nc[PhyloTreeKeys.CHILDREN] = []
                  #nc[PhyloTreeKeys.PATH] = ','.join([str(pI) for pI in self.internalPaths[str(x)]])
                  nc[PhyloTreeKeys.PATH] = ''
               else:
                  nc[PhyloTreeKeys.NAME] = self.tipPaths[str(x)][1]
                  #nc[PhyloTreeKeys.PATH] = ','.join([str(pI) for pI in self.tipPaths[str(x)][0]])
                  nc[PhyloTreeKeys.PATH] = ''
               clade[PhyloTreeKeys.CHILDREN].append(nc)
               if x not in tips:
                  recurse(nc, m[x])
      recurse(tree, m[edge[0][0]])
      
      return tree


# .............................................................................      
if __name__ == "__main__":
   #TODO: Remove
   p = "/home/jcavner/Charolettes_Data/Trees/RAxML_bestTree.12.15.14.1548tax.ultrametric.tre"
   
   p = "/home/jcavner/PhyloXM_Examples/test_polyWithoutLengths.json"
   
   to = LmTree.fromFile(p)
   
   print to.polyPos
   
   newTree = to.resolvePoly()
   
   newTree.writeTree("/home/jcavner/PhyloXM_Examples/resolvedPolyWithoutBranchLen.json")
   #treeDir = "/home/jcavner/PhyloXM_Examples/"
   #with open(os.path.join(treeDir,'resolvedPolyWithoutBranchLen.json'),'w') as f:
   #   f.write(json.dumps(newTree,sort_keys=True, indent=4))
   
   
   #newTree.writeTree("/home/jcavner/PhyloXM_Examples/resolvedPolyWithoutBranchLen.json")
   
   #print "first tips ", to.tipCount
   #print "first internal ",to.internalCount
   #print
   
   #rt = to.rTree(125)
   #to.makePaths(to.tree)
   #to._subTrees = False
   #print to.polytomies   
   
   #######################
   #to.resolvePoly()
   
   #to.subTrees = False
   #st = to.subTrees
   #####################
   
   #edges = to._getEdges()
   #tree = to._makeCladeFromEdges(edges,lengths=True)
   
   #p_t = to.dropTips(['B','D','H','I','J','K']) # 'A','C','D' # 'B','D'
   #p_t.writeTree("/home/jcavner/PhyloXM_Examples/test_drop.json")
   
   #treeDir = "/home/jcavner/PhyloXM_Examples/"
   #with open(os.path.join(treeDir,'tree_fromEdges.json'),'w') as f:
   #   f.write(json.dumps(tree,sort_keys=True, indent=4))
   #   
   #to2 = LmTree.fromFile(os.path.join(treeDir,'tree_withoutNewPaths_2_YES.json'))
   
   #print "after tips ", to.tipCount
   #print "after internal ",to.internalCount
   
         
   
   
         
   
