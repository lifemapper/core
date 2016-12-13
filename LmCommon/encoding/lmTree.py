"""
@summary: Module containing LMTree class
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
from operator import itemgetter #TODO: Really necessary?
import os
from random import randint
import json

from LmCommon.common.lmconstants import OutputFormat
from LmCommon.encoding.newickToJson import Parser

#TODO: Move to constants module and consider a FileTypes class
NHX_EXT = [".nhx", ".tre"]

# Dictionary keys
C_KEY = 'c' # TODO: What is this?  Document
CHILDREN_KEY = 'children' # Children of a node
DESC_KEY = 'desc' #TODO: What is this? Document.  Only used twice
LENGTH_KEY = 'length' # Branch length for that node
MTX_IDX_KEY = 'mx' # The matrix index for this node
NAME_KEY = 'name' # Name of the node
NC_KEY = 'nc' #TODO: Document
PATH_KEY = 'path' #TODO: Document
PATH_ID_KEY = 'pathId' # TODO: Document

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
   def fromFile(cls, dLoc):
      """
      @todo: Document
      """
      if os.path.exists(dLoc):
         fn,e = os.path.splitext(dLoc)
         if e == OutputFormat.JSON:
            #TODO: Fix duplication of effort. Should be able to load directly
            with open(dLoc, 'r') as f:
               jsonstr = f.read()
            return cls(json.loads(jsonstr))
         elif e in NHX_EXT:
            phyloDict = cls.convertFromNewick(dLoc)
            # TODO: Fix 
            if  isinstance(phyloDict, Exception):
               raise ValueError("Expected an python dictionary "+str(phyloDict))
            else:
               return cls(phyloDict)          
      else:
         #TODO: Raise exception
         pass # ?
      
   # ..............................   
   @classmethod
   def convertFromNewick(cls, dLoc):
      """
      @todo: Document
      """
      try:
         #TODO: Fix memory leak
         tree = open(dLoc, 'r').read()
         #TODO: Fix, recursive unnecessarily
         sh = Parser.from_string(tree)
         parser = Parser(sh)
         result, parentDicts = parser.parse()
      except Exception, e:
         #TODO: Do we want to return an exception as the result?  Should bubble up
         result = e
      return result
      
   # ..............................   
   def getTreeInfo(self, clade):
      """
      @summary: performs one recursion for all tree info objects
      @todo: Document
      """
      tipPaths = {}
      internalPaths = {}
      lengths =  {}
      subTrees = {}
      self.polyPos = {}
      
      # ..............................   
      def recurseClade(clade):
         """
         @todo: Evaluate if this is really needed
         @todo: use constants for strings
         @todo: Evaluate casting
         """
         if clade.has_key(CHILDREN_KEY):
            # do stuff in here
            if clade.has_key(LENGTH_KEY):  # to control for pathId 0 not having length
               lengths[int(clade[PATH_ID_KEY])] = float(clade[LENGTH_KEY])
            else:
               if int(clade[PATH_ID_KEY]) != 0:
                  self._numberMissingLengths +=1
            internalPaths[clade[PATH_ID_KEY]] = [int(x) for x in clade[PATH_KEY].split(',')]
            subTrees[int(clade[PATH_ID_KEY])] = clade[CHILDREN_KEY]
            if len(clade[CHILDREN_KEY]) > 2:
               self._polytomy = True
               self.whichNPoly.append(int(clade[PATH_ID_KEY]))
               polydesc = {}
               for p in clade[CHILDREN_KEY]:
                  if p.has_key(LENGTH_KEY):
                     polydesc[int(p[PATH_ID_KEY])] = p[LENGTH_KEY]
                  else:
                     polydesc[int(p[PATH_ID_KEY])] = ''
               self.polyPos[clade[PATH_ID_KEY]] = {PATH_KEY: clade[PATH_KEY], DESC_KEY: polydesc}
            for child in clade[CHILDREN_KEY]:
               recurseClade(child)
         else:
            # tip
            if not clade.has_key(LENGTH_KEY):
               self._numberMissingLengths +=1
            else:
               lengths[int(clade[PATH_ID_KEY])] = float(clade[LENGTH_KEY]) 
            tipPaths[clade[PATH_ID_KEY]] = ([int(x) for x in clade[PATH_KEY].split(',')], clade[NAME_KEY])
            self._tipNames.append(clade[NAME_KEY]) 
            if clade.has_key(MTX_IDX_KEY):
               self._tipNamesWithMX.append(clade[NAME_KEY])
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
      @todo: Use constants instead of strings
      """     
      # ..............................   
      def takeOutMtx(clade):
         if clade.has_key(CHILDREN_KEY):
            clade.pop(MTX_IDX_KEY, None)
            for child in clade[CHILDREN_KEY]:
               takeOutMtx(child)
         else:
            clade.pop(MTX_IDX_KEY, None)
      
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
         
         return LMtree(tree)
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
         if clade.has_key(CHILDREN_KEY):
            childIds = [int(c[PATH_ID_KEY]) for c in clade[CHILDREN_KEY]]
            if int(clade[PATH_ID_KEY]) not in edgeDict:
               edgeDict[int(clade[PATH_ID_KEY])] = childIds
            for child in clade[CHILDREN_KEY]:
               recurseEdge(child)        
      recurseEdge(self.tree)
      
      edge_ll = []
      for e in edgeDict.items():
         for t in e[1]:
            edge_ll.append([e[0], t])
      edge = np.array(edge_ll)
      
      return edge

   # ..............................   
   def _truncate(self, f, n):
      """
      @summary: Truncates/pads a float f to n decimal places without rounding
      @todo: Document
      @todo: Remove most likely, there are built-ins for this
      """
      s = '{}'.format(f)
      if 'e' in s or 'E' in s:
         return '{0:.{1}f}'.format(f, n)
      i, p, d = s.partition('.')
      return '.'.join([i, (d+'0'*n)[:n]])

   # ..............................   
   def checkUltraMetric(self):
      """
      @summary: check to see if tree is ultrametric, all the way to the root
      @todo: Use constants not class constants
      @todo: Document
      """
      #tipPaths,treeLengths,subTrees = self.getTreeInfo(self.tree)
      #self._subTrees = subTrees
      
      if self.branchLengths == HAS_BRANCH_LEN:
         toSet = []
         for tip in self.tipPaths:
            #copytipPath = list(tipPaths[tip][0])
            copytipPath = list(self.tipPaths[tip][0])
            copytipPath.pop()  # removes internal pathId from path list for root of tree
            toSum = []
            for pathId in copytipPath:
               toSum.append(self.lengths[pathId])
            urs = sum(toSum)
            s = self._truncate(urs, 3)
            toSet.append(s)
         #TODO: This can only be true if length is one, no need to make it complicated
         count = len(set(toSet))
         return bool(1//count)
      else:
         return NO_BRANCH_LEN  # need to think about this
   
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
      """
      print "in make Paths"
      p = {C_KEY:0}   
      # ..............................   
      def recursePaths(clade, parent):
         if clade.has_key(CHILDREN_KEY):
            clade[PATH_KEY].insert(0,str(p[C_KEY]))
            clade[PATH_KEY] = clade[PATH_KEY] + parent 
            clade[PATH_ID_KEY] = str(p[C_KEY])
            #clade[NAME_KEY] = str(p[C_KEY])
            for child in clade[CHILDREN_KEY]:
               p[C_KEY] = p[C_KEY] + 1
               recursePaths(child, clade[PATH_KEY])
         else:
            # tips
            clade[PATH_KEY].insert(0,str(p[C_KEY]))
            clade[PATH_KEY] = clade[PATH_KEY] + parent
            clade[PATH_ID_KEY] = str(p[C_KEY])
            #clade[NAME_KEY] = str(p[C_KEY])  #take this out for real
      # ..............................   
      def takeOutBr(clade):
         
         if clade.has_key(CHILDREN_KEY):
            clade.pop(LENGTH_KEY, None)
            for child in clade[CHILDREN_KEY]:
               takeOutBr(child)
         else:
            clade.pop(LENGTH_KEY, None)
      # ..............................   
      def takeOutStrPaths(clade):
         
         if clade.has_key(CHILDREN_KEY):
            clade[PATH_KEY] = []
            clade[PATH_ID_KEY] = ''
            for child in clade[CHILDREN_KEY]:
               takeOutStrPaths(child)
         else:
            clade[PATH_KEY] = []
            clade[PATH_ID_KEY] = ''

      
      takeOutStrPaths(tree)    
      
      recursePaths(tree,[])
      
      if takeOutBranches:
         takeOutBr(tree)
      
      # ..............................
      # TODO: Why is this defined here?   
      def stringifyPaths(clade):
         if clade.has_key(CHILDREN_KEY):
            clade[PATH_KEY] = ','.join(clade[PATH_KEY])
            for child in clade[CHILDREN_KEY]:
               stringifyPaths(child)
         else:
            clade[PATH_KEY] = ','.join(clade[PATH_KEY])
            
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
      tree = {PATH_ID_KEY:str(0), PATH_KEY:'', CHILDREN_KEY:[]}  # will take out name for internal after testing
      def recurse(clade, l):
         for x in l:
            if clade.has_children(CHILDREN_KEY):
               nc = {PATH_ID_KEY:str(x), PATH_KEY:''} # will take out name for internal after testing
               if lengths:
                  nc[LENGTH_KEY] = lengths[x]
               if x not in tips:
                  nc[CHILDREN_KEY] = []
                  #nc[PATH_KEY] = ','.join([str(pI) for pI in self.internalPaths[str(x)]])
                  nc[PATH_KEY] = ''
               else:
                  nc[NAME_KEY] = self.tipPaths[str(x)][1]
                  #nc[PATH_KEY] = ','.join([str(pI) for pI in self.tipPaths[str(x)][0]])
                  nc[PATH_KEY] = ''
               clade[CHILDREN_KEY].append(nc)
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
      tree = {PATH_ID_KEY: str(n+1), PATH_KEY: [], CHILDREN_KEY: [], NAME_KEY: str(n+1), LENGTH_KEY:"0"}
      def recurse(clade, l):
         for x in l:
            if clade.has_key(CHILDREN_KEY):
               nc = {PATH_ID_KEY: str(x), PATH_KEY: [], NAME_KEY: str(x), LENGTH_KEY: '0'}
               if x not in tips:
                  nc[CHILDREN_KEY] = []
               clade[CHILDREN_KEY].append(nc)
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
         if clade.has_key(CHILDREN_KEY):
            clade[NAME_KEY] = ''
            for child in clade[CHILDREN_KEY]:
               findTips(child)
         else:
            # tips
            clade[NAME_KEY] = ''
            tips.append(clade)
            
      findTips(rt)
      return tips
     
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
            pTips =  self.polyPos[k][DESC_KEY].items()  # these are integers
            n = len(pTips)          
            rt = self.rTree(n)
            tips = self._getRTips(rt)
            for pt, t in zip(pTips, tips):
               #print pt," ",t
               t[PATH_ID_KEY] = str(pt[0])  # might not need this
               t[LENGTH_KEY] = pt[1]
               if str(pt[0]) not in self.tipPaths:
                  t[CHILDREN_KEY] = self.subTrees[pt[0]]
               else:
                  t[NAME_KEY] = self.tipPaths[str(pt[0])][1]
                  #print pt," ",t
            # now at this level get the two children of the random root
            c1 = rt[CHILDREN_KEY][0]
            c2 = rt[CHILDREN_KEY][1]
            
            st_copy[int(k)] = []
            st_copy[int(k)].append(c1)
            st_copy[int(k)].append(c2)
            
         removeList = list(self.polyPos.keys())
         # ..............................   
         def replaceInTree(clade):
            if clade.has_key(CHILDREN_KEY):
               #if clade[PATH_ID_KEY] in self.polyPos.keys():
               if clade[PATH_ID_KEY] in removeList:
                  idx = removeList.index(clade[PATH_ID_KEY] )
                  del removeList[idx]
               #if clade[PATH_ID_KEY] == polyKey:
                  clade[CHILDREN_KEY] = st_copy[int(clade[PATH_ID_KEY])]
                  #return
               for child in clade[CHILDREN_KEY]:
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
            
         return LMtree(newTree)
      
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
         edge[pos][0] = nod[NC_KEY]
         edge[po2][0] = nod[NC_KEY]
         nod[NC_KEY] = nod[NC_KEY] + 1
         if n1 > 2:
            edge[pos][1] = nod[NC_KEY]
            generate(n1, pos+1)
         elif n1 == 2:
            edge[pos+1][0] = nod[NC_KEY]
            edge[pos+2][0] = nod[NC_KEY]
            edge[pos][1]   = nod[NC_KEY]
            nod[NC_KEY] = nod[NC_KEY] + 1
         if n2 > 2:
            edge[po2][1] = nod[NC_KEY]
            generate(n2, po2+1)
         elif n2 == 2:
            edge[po2 + 1][0] = nod[NC_KEY]
            edge[po2 + 2][0] = nod[NC_KEY]
            edge[po2][1]    = nod[NC_KEY]
            nod[NC_KEY] = nod[NC_KEY] + 1
         
      nbr = (2 * n) - 3 + rooted
      edge =  np.array(np.arange(0, 2*nbr)).reshape(2, nbr).T
      edge.fill(-999)
      nod = {NC_KEY: n + 1}
      generate(n, 0)
     
      idx = np.where(edge[:,1]==-999)[0]
      for i,x in enumerate(idx):
         edge[x][1] = i + 1
         
      rt = self._makeCladeFromRandomEdges(edge, n)
      return rt
   
   # ..............................   
   def writeTree(self, path):
      #if os.path.exists(path):
      with open(path,'w') as f:
         f.write(json.dumps(self.tree, sort_keys=True, indent=4))
      

# .............................................................................      
if __name__ == "__main__":
   #TODO: Remove
   p = "/home/jcavner/Charolettes_Data/Trees/RAxML_bestTree.12.15.14.1548tax.ultrametric.tre"
   
   p = "/home/jcavner/PhyloXM_Examples/test_polyWithoutLengths.json"
   
   to = LMtree.fromFile(p)
   
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
   #to2 = LMtree.fromFile(os.path.join(treeDir,'tree_withoutNewPaths_2_YES.json'))
   
   #print "after tips ", to.tipCount
   #print "after internal ",to.internalCount
   
         
   
   
         
   