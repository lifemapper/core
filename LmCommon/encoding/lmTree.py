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
   """
   
   # ..............................   
   def __init__(self, treeDict):
      """
      @note: what happens if mtxIdx (mx) in tree?
      @todo: Document
      @param treeDict: 
      """
      
      self.tree = treeDict
      self._polytomy = False
      self._numberMissingLengths = 0
      self._subTrees = False
      self._lengths = False
      self.tipPaths = False
      self.internalPaths = False
      self.labels = False
      self.whichNPoly = []
      self._tipNames = []
      self._tipNamesWithMX = []
      self.getTreeInfo(self.tree)
      
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
   def getTreeInfo(self, clade):
      """
      @summary: performs one recursion for all tree info objects
      @todo: Document
      @todo: Better recursion
      """
      tipPaths = {}
      internalPaths = {}
      lengths =  {}
      subTrees = {}
      #TODO: We don't seem to need this with my changes to resolve polytomies
      self.polyPos = {}
      
      # ..............................   
      def recurseClade(clade):
         """
         @todo: Evaluate if this is really needed
         @todo: use constants for strings
         @todo: Evaluate casting
         """
         if clade.has_key(PhyloTreeKeys.CHILDREN):
            # do stuff in here
            if clade.has_key(PhyloTreeKeys.BRANCH_LENGTH):  # to control for pathId 0 not having length
               lengths[int(clade[PhyloTreeKeys.PATH_ID])] = float(clade[PhyloTreeKeys.BRANCH_LENGTH])
            else:
               if int(clade[PhyloTreeKeys.PATH_ID]) != 0:
                  self._numberMissingLengths +=1
            # TODO: Wasting compute
            internalPaths[clade[PhyloTreeKeys.PATH_ID]] = [int(x) for x in clade[PhyloTreeKeys.PATH].split(',')]
            subTrees[int(clade[PhyloTreeKeys.PATH_ID])] = clade[PhyloTreeKeys.CHILDREN]
            if len(clade[PhyloTreeKeys.CHILDREN]) > 2:
               self._polytomy = True
               self.whichNPoly.append(int(clade[PhyloTreeKeys.PATH_ID]))
               polydesc = {}
               for p in clade[PhyloTreeKeys.CHILDREN]:
                  if p.has_key(PhyloTreeKeys.BRANCH_LENGTH):
                     polydesc[int(p[PhyloTreeKeys.PATH_ID])] = p[PhyloTreeKeys.BRANCH_LENGTH]
                  else:
                     polydesc[int(p[PhyloTreeKeys.PATH_ID])] = ''
               self.polyPos[clade[PhyloTreeKeys.PATH_ID]] = {PhyloTreeKeys.PATH: clade[PhyloTreeKeys.PATH], PhyloTreeKeys.DESC: polydesc}
            for child in clade[PhyloTreeKeys.CHILDREN]:
               recurseClade(child)
         else:
            # tip
            if not clade.has_key(PhyloTreeKeys.BRANCH_LENGTH):
               self._numberMissingLengths +=1
            else:
               lengths[int(clade[PhyloTreeKeys.PATH_ID])] = float(clade[PhyloTreeKeys.BRANCH_LENGTH]) 
            tipPaths[clade[PhyloTreeKeys.PATH_ID]] = ([int(x) for x in clade[PhyloTreeKeys.PATH].split(',')], clade[PhyloTreeKeys.NAME])
            self._tipNames.append(clade[PhyloTreeKeys.NAME]) 
            if clade.has_key(PhyloTreeKeys.MTX_IDX):
               self._tipNamesWithMX.append(clade[PhyloTreeKeys.NAME])
      #...................................................
      #TODO: Document
      recurseClade(clade)
      self.tipPaths = tipPaths
      self.internalPaths = internalPaths
      self._lengths = lengths
      self.labelIds = np.array(sorted([int(tl) for tl in self.tipPaths.keys()], reverse=True))
      # this makes certain that labels are in in the order ape phy$labels presents them (bottom up)
      self.labels = np.array([self.tipPaths[str(li)][1] for li in self.labelIds])
      self._subTrees = subTrees
      #return tipPaths, lengths, subTrees

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

   # ..............................   
   def checkUltraMetric(self):
      """
      @summary: Check if the tree is ultrametric
      @note: To be ultrametric, the branch length from root to tip must be 
                equal for all tips
      @summary: check to see if tree is ultrametric, all the way to the root
      @todo: Return true for some scenario
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
   def makePaths(self, tree, takeOutBranches=False):
      """
      @summary: makes paths by recursing tree and appending parent to new pathId
      @todo: Document
      @todo: Probably remove inner functions
      @todo: Why are we resetting PATH_ID?
      """
      print "in make Paths"
      p = {PhyloTreeKeys.C:0}   
      # ..............................   
      def recursePaths(clade, parent):
         if clade.has_key(PhyloTreeKeys.CHILDREN):
            clade[PhyloTreeKeys.PATH].insert(0,str(p[PhyloTreeKeys.C]))
            clade[PhyloTreeKeys.PATH] = clade[PhyloTreeKeys.PATH] + parent 
            clade[PhyloTreeKeys.PATH_ID] = str(p[PhyloTreeKeys.C])
            #clade[PhyloTreeKeys.NAME] = str(p[PhyloTreeKeys.C])
            for child in clade[PhyloTreeKeys.CHILDREN]:
               p[PhyloTreeKeys.C] = p[PhyloTreeKeys.C] + 1
               recursePaths(child, clade[PhyloTreeKeys.PATH])
         else:
            # tips
            clade[PhyloTreeKeys.PATH].insert(0,str(p[PhyloTreeKeys.C]))
            clade[PhyloTreeKeys.PATH] = clade[PhyloTreeKeys.PATH] + parent
            clade[PhyloTreeKeys.PATH_ID] = str(p[PhyloTreeKeys.C])
            #clade[PhyloTreeKeys.NAME] = str(p[PhyloTreeKeys.C])  #take this out for real
      # ..............................   
      def takeOutBr(clade):
         
         if clade.has_key(PhyloTreeKeys.CHILDREN):
            clade.pop(PhyloTreeKeys.BRANCH_LENGTH, None)
            for child in clade[PhyloTreeKeys.CHILDREN]:
               takeOutBr(child)
         else:
            clade.pop(PhyloTreeKeys.BRANCH_LENGTH, None)
      # ..............................   
      def takeOutStrPaths(clade):
         
         if clade.has_key(PhyloTreeKeys.CHILDREN):
            clade[PhyloTreeKeys.PATH] = []
            clade[PhyloTreeKeys.PATH_ID] = ''
            for child in clade[PhyloTreeKeys.CHILDREN]:
               takeOutStrPaths(child)
         else:
            clade[PhyloTreeKeys.PATH] = []
            clade[PhyloTreeKeys.PATH_ID] = ''

      
      takeOutStrPaths(tree)    
      
      recursePaths(tree,[])
      
      if takeOutBranches:
         takeOutBr(tree)
      
      # ..............................
      # TODO: Why is this defined here?   
      def stringifyPaths(clade):
         if clade.has_key(PhyloTreeKeys.CHILDREN):
            clade[PhyloTreeKeys.PATH] = ','.join(clade[PhyloTreeKeys.PATH])
            for child in clade[PhyloTreeKeys.CHILDREN]:
               stringifyPaths(child)
         else:
            clade[PhyloTreeKeys.PATH] = ','.join(clade[PhyloTreeKeys.PATH])
            
      stringifyPaths(tree)
   
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

   # ..............................   
   def _makeCladeFromRandomEdges(self, edge, n):
      """
      @summary: makes a tree dict from a (2 * No. internal node) x 2 numpy matrix
      @param edge: numpy array of edges
      @param n: number of tips
      @todo: DOcument
      @todo: use constants
      @todo: Recurse better
      """
      tips = range(1,n+1)  # based on numbering convention in R
      iNodes = list(set(edge[:,0]))
      m = {}  # key is internal node, value is list of terminating nodes
      for iN in iNodes:
         dx = np.where(edge[:,0]==iN)[0]
         le = list(edge[dx][:,1])
         m[iN] = le
      #print m
      #m = {k[0]:list(k) for k in edge }
      tree = {PhyloTreeKeys.PATH_ID: str(n+1), PhyloTreeKeys.PATH: [], PhyloTreeKeys.CHILDREN: [], PhyloTreeKeys.NAME: str(n+1), PhyloTreeKeys.BRANCH_LENGTH:"0"}
      def recurse(clade, l):
         for x in l:
            if clade.has_key(PhyloTreeKeys.CHILDREN):
               nc = {PhyloTreeKeys.PATH_ID: str(x), PhyloTreeKeys.PATH: [], PhyloTreeKeys.NAME: str(x), PhyloTreeKeys.BRANCH_LENGTH: '0'}
               if x not in tips:
                  nc[PhyloTreeKeys.CHILDREN] = []
               clade[PhyloTreeKeys.CHILDREN].append(nc)
               if x not in tips:
                  recurse(nc, m[x])
      recurse(tree, m[n+1])
      
      return tree
      
   # ..............................   
   def _getRTips(self, rt):
      """
      @summary: recurses a random subtree and returns a list of its tips
      @todo: Document
      """
      tips = []
      # ..............................   
      def findTips(clade):
         if clade.has_key(PhyloTreeKeys.CHILDREN):
            clade[PhyloTreeKeys.NAME] = ''
            for child in clade[PhyloTreeKeys.CHILDREN]:
               findTips(child)
         else:
            # tips
            clade[PhyloTreeKeys.NAME] = ''
            tips.append(clade)
            
      findTips(rt)
      return tips
     
   # ..............................   
   def resolvePolytomies(self, tree=None):
      """
      @summary: Resolve polytomies in a tree
      @param tree: (Optional) The tree to resolve.  If omitted, use tree root
      @todo: Do we need to do anything with branch lengths?
      @todo: Fix paths as we edit
      """
      if tree is None: # Start at the root
         tree = self.tree
      while len(tree[PhyloTreeKeys.CHILDREN]) > 2:
         shuffle(tree[PhyloTreeKeys.CHILDREN])
         c1 = tree[PhyloTreeKeys.CHILDREN].pop(0)
         tree[PhyloTreeKeys.CHILDREN][0][PhyloTreeKeys.CHILDREN].append(c1)
      
      for i in range(len(tree[PhyloTreeKeys.CHILDREN])):
         self.resolvePolytomies(tree=tree[PhyloTreeKeys.CHILDREN][i])
      

   # ..............................   
   def resolvePoly(self):
      """
      @summary: resolves polytomies against tree object
      @return: new tree object
      @todo: Document
      @todo: Use constants 
      @todo: Rename this function to resolvePolytomies
      """ 
      if len(self.polyPos.keys()) > 0:
         
         st_copy = self.subTrees.copy()
         # loops through polys and makes rnd tree and attaches as children in subtree
         for k in self.polyPos.keys():
            pTips =  self.polyPos[k][PhyloTreeKeys.DESC].items()  # these are integers
            n = len(pTips)          
            rt = self.rTree(n)
            tips = self._getRTips(rt)
            for pt, t in zip(pTips, tips):
               #print pt," ",t
               t[PhyloTreeKeys.PATH_ID] = str(pt[0])  # might not need this
               t[PhyloTreeKeys.BRANCH_LENGTH] = pt[1]
               if str(pt[0]) not in self.tipPaths:
                  t[PhyloTreeKeys.CHILDREN] = self.subTrees[pt[0]]
               else:
                  t[PhyloTreeKeys.NAME] = self.tipPaths[str(pt[0])][1]
                  #print pt," ",t
            # now at this level get the two children of the random root
            c1 = rt[PhyloTreeKeys.CHILDREN][0]
            c2 = rt[PhyloTreeKeys.CHILDREN][1]
            
            st_copy[int(k)] = []
            st_copy[int(k)].append(c1)
            st_copy[int(k)].append(c2)
            
         removeList = list(self.polyPos.keys())
         # ..............................   
         def replaceInTree(clade):
            if clade.has_key(PhyloTreeKeys.CHILDREN):
               #if clade[PhyloTreeKeys.PATH_ID] in self.polyPos.keys():
               if clade[PhyloTreeKeys.PATH_ID] in removeList:
                  idx = removeList.index(clade[PhyloTreeKeys.PATH_ID] )
                  del removeList[idx]
               #if clade[PhyloTreeKeys.PATH_ID] == polyKey:
                  clade[PhyloTreeKeys.CHILDREN] = st_copy[int(clade[PhyloTreeKeys.PATH_ID])]
                  #return
               for child in clade[PhyloTreeKeys.CHILDREN]:
                  replaceInTree(child)
            else:
               pass    
         newTree = self.tree.copy()
         replaceInTree(newTree) 
         if self.branchLengths == NO_BRANCH_LEN:
            takeOutBr = True
         else: 
            takeOutBr = False
         self.makePaths(newTree, takeOutBranches=takeOutBr)
            
         return LmTree(newTree)
      
      else:
         return self
        
      
   # ..............................   
   def rTree(self, n, rooted=True):
      """
      @summary: given the number of tips generate a random binary tree by randomly splitting edges, 
      equal to foo branch in ape's rtree
      @param n: number of tips
      @note: this is just for >= 4 so far, but not be a problem
      @todo: Document
      @todo: Fix function name
      @todo: Constants
      """
      # ..............................   
      def generate(n, pos):
         n1 = randint(1,n-1)
         n2 = n - n1
         po2 = pos + 2 * n1 - 1
         edge[pos][0] = nod[PhyloTreeKeys.NC]
         edge[po2][0] = nod[PhyloTreeKeys.NC]
         nod[PhyloTreeKeys.NC] = nod[PhyloTreeKeys.NC] + 1
         if n1 > 2:
            edge[pos][1] = nod[PhyloTreeKeys.NC]
            generate(n1, pos+1)
         elif n1 == 2:
            edge[pos+1][0] = nod[PhyloTreeKeys.NC]
            edge[pos+2][0] = nod[PhyloTreeKeys.NC]
            edge[pos][1]   = nod[PhyloTreeKeys.NC]
            nod[PhyloTreeKeys.NC] = nod[PhyloTreeKeys.NC] + 1
         if n2 > 2:
            edge[po2][1] = nod[PhyloTreeKeys.NC]
            generate(n2, po2+1)
         elif n2 == 2:
            edge[po2 + 1][0] = nod[PhyloTreeKeys.NC]
            edge[po2 + 2][0] = nod[PhyloTreeKeys.NC]
            edge[po2][1]    = nod[PhyloTreeKeys.NC]
            nod[PhyloTreeKeys.NC] = nod[PhyloTreeKeys.NC] + 1
         
      nbr = (2 * n) - 3 + rooted
      edge =  np.array(np.arange(0, 2*nbr)).reshape(2, nbr).T
      edge.fill(-999)
      nod = {PhyloTreeKeys.NC: n + 1}
      generate(n, 0)
     
      idx = np.where(edge[:,1]==-999)[0]
      for i,x in enumerate(idx):
         edge[x][1] = i + 1
         
      rt = self._makeCladeFromRandomEdges(edge, n)
      return rt
   
   # ..............................   
   def createRandomTree(self, numTips):
      """
      @summary: Creates a random binary Phylogenetic tree with the specified 
                   number of tips
      @param numTips: The number of tips to include in this tree
      @note: Tips will have path ids 1 - numTips
      @note: Following convention in R package to number beginning with 1
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
   def writeTree(self, path):
      """
      @todo: Document
      @todo: Use json.dump?
      """
      #if os.path.exists(path):
      with open(path,'w') as f:
         f.write(json.dumps(self.tree, sort_keys=True, indent=4))
      
   # ..............................
   def hasPolytomies(self):
      """
      @summary: Returns boolean indicating if the tree has polytomies
      """
      return self._hasPolytomies(self.tree)
   
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
   
         
   
   
         
   