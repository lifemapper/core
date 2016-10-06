import os
import cPickle
import simplejson as json
from itertools import combinations
from operator import itemgetter
#from lifemapperTools.common.NwkToJSON import Parser
from NwkToJSON import Parser
import numpy as np
from random import randint



class LMtree():
   
   NO_BRANCH_LEN = 0  # missing all branch lengths
   MISSING_BRANCH_LEN = 1 # missing some branch lengths
   HAS_BRANCH_LEN = 2
   JSON_EXT = ".json"
   NHX_EXT = [".nhx",".tre"]
   
   
   def __init__(self,treeDict):
      """
      @note: what happens if mtxIdx (mx) in tree?
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
      
   @classmethod
   def fromFile(cls,dLoc):
      if os.path.exists(dLoc):
         fn,e = os.path.splitext(dLoc)
         if e == cls.JSON_EXT:
            with open(dLoc,'r') as f:
               jsonstr = f.read()
            return cls(json.loads(jsonstr))
         elif e in cls.NHX_EXT:
            phyloDict = cls.convertFromNewick(dLoc) 
            if  isinstance(phyloDict,Exception):
               raise ValueError("Expected an python dictionary "+str(phyloDict))
            else:
               return cls(phyloDict)          
      else:
         pass # ?
      
   @classmethod
   def convertFromNewick(cls,dLoc):
      
      try:
         tree = open(dLoc,'r').read()
         sh = Parser.from_string(tree)
         parser = Parser(sh)
         result,parentDicts = parser.parse()
      except Exception, e:
         result = e
      return result
      
   def getTreeInfo(self,clade):
      """
      @summary: performs one recursion for all tree info objects
      """
      tipPaths = {}
      internalPaths = {}
      lengths =  {}
      subTrees = {}
      self.polyPos = {}
      
      def recurseClade(clade):
         if "children" in clade:
            # do stuff in here
            if "length" in clade:  # to control for pathId 0 not having length
               lengths[int(clade["pathId"])] = float(clade["length"])
            else:
               if int(clade['pathId']) != 0:
                  self._numberMissingLengths +=1
            internalPaths[clade['pathId']] = [int(x) for x in clade["path"].split(',')]
            subTrees[int(clade['pathId'])] = clade["children"]
            if len(clade["children"]) > 2:
               self._polytomy = True
               self.whichNPoly.append(int(clade["pathId"]))
               polydesc = {}
               for p in clade["children"]:
                  if 'length' in p:
                     polydesc[int(p["pathId"])] = p['length']
                  else:
                     polydesc[int(p["pathId"])] = ''
               self.polyPos[clade["pathId"]] = {'path':clade["path"],"desc":polydesc}
            for child in clade["children"]:
               recurseClade(child)
         else:
            # tip
            if "length" not in clade:
               self._numberMissingLengths +=1
            else:
               lengths[int(clade["pathId"])] = float(clade["length"]) 
            tipPaths[clade["pathId"]] = ([int(x) for x in clade["path"].split(',')],clade["name"])
            self._tipNames.append(clade["name"]) 
            if 'mx' in clade:
               self._tipNamesWithMX.append(clade["name"])
      #...................................................
      recurseClade(clade)
      self.tipPaths = tipPaths
      self.internalPaths = internalPaths
      self._lengths = lengths
      self.labelIds = np.array(sorted([int(tl) for tl in self.tipPaths.keys()],reverse=True))
      # this makes certain that labels are in in the order ape phy$labels presents them (bottom up)
      self.labels = np.array([self.tipPaths[str(li)][1] for li in self.labelIds])
      self._subTrees = subTrees
      #return tipPaths, lengths, subTrees
#...................................................   
   def findDropTipsDelMtx(self,dropTips):
      """
      @summary: find tips by name to drop and if there is a mx for tip removes all mx
      """     
      def takeOutMtx(clade):
         if "children" in clade:
            clade.pop('mx', None)
            for child in clade["children"]:
               takeOutMtx(child)
         else:
            clade.pop('mx', None)
      #...................................................
      for n in dropTips:
         if n in self.tipNamesWithMX:
            takeOutMtx(self.tree)
            break
               
#...................................................   
   def dropTips(self, tips ):
      """
      @summary: dropt tips from current tree returns new tree
      @param tips: list or array of tip (label) names to be removed
      @return: new tree obj
      """
      self.findDropTipsDelMtx(tips)
      if len(tips) < len(self.labelIds) - 1:
         edge = self._getEdges()
         nTips = len(self.labels) #Ntip
         nEdge = edge.shape[0]
         edge_1 = edge[:,0] 
         edge_2 = edge[:,1]
         
         #keep[match(tip, edge2)] <- FALSE
         labelmask = np.in1d(self.labels,tips)
         tips = self.labelIds[labelmask]
         tips = np.where(np.in1d(edge_2,tips))
         #print tips
         keep =  np.ones(nEdge,dtype=bool)
         keep[tips] = False 
         #print keep
         
         int_edge2 = [x for x in edge_2 if str(x) in self.internalPaths]
         ints = np.in1d(edge_2,int_edge2)
         #print ints
         
         e1Keep = edge_1[keep]
         e2WithE1Keep_not =  np.logical_not(np.in1d(edge_2,e1Keep))
         
         while True:
            sel = reduce(np.logical_and,(e2WithE1Keep_not,ints,keep))
            if not(sum(sel)):
               break
            keep[sel] = False
         newEdges = edge[keep]
        
         if self.branchLengths == self.HAS_BRANCH_LEN:
            len_copy = self.lengths.copy()
         else:
            len_copy = False
         
         #######
         
         rowsToDelete = []
         for i,r in enumerate(newEdges):
            iN = r[0]  # internal node
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
                  find = np.array([parent,iN])
                  parentRow = np.where(np.all(newEdges==find,axis=1))[0][0]
                  newEdges[parentRow][1] = tN
                  rowsToDelete.append(i)
                  
         newEdges =  np.delete(newEdges,np.array(rowsToDelete),axis=0)
         ###############
         
         eI = newEdges[:,0]
         eT = newEdges[:,1]
         tipPos = np.where(np.logical_not(np.in1d(eT,eI)))[0]
         newtips = eT[tipPos]
         
         #######
         tree = self._makeCladeFromEdges(newEdges, lengths=len_copy,tips=newtips)  # this relies on old data structures
         self.makePaths(tree)
         
         return LMtree(tree)
      else:
         raise ValueError('Cannot remove all tips from a tree, or leave single tip')
      
#...................................................         
   def _getEdges(self):
      """
      @summary: makes a (2 * No. internal Nodes) x 2 matrix representation of the tree 
      """
      
      edgeDict = {}
      def recurseEdge(clade):
         if "children" in clade:
            childIds = [int(c['pathId']) for c in clade['children']]
            if int(clade['pathId']) not in edgeDict:
               edgeDict[int(clade['pathId'])] = childIds
            for child in clade["children"]:
               recurseEdge(child)        
      recurseEdge(self.tree)
      
      edge_ll = []
      for e in edgeDict.items():
         for t in e[1]:
            edge_ll.append([e[0],t])
      edge = np.array(edge_ll)
      
      return edge
#...................................................   
   def _truncate(self,f, n):
      """
      @summary: Truncates/pads a float f to n decimal places without rounding
      """
      
      s = '{}'.format(f)
      if 'e' in s or 'E' in s:
         return '{0:.{1}f}'.format(f, n)
      i, p, d = s.partition('.')
      return '.'.join([i, (d+'0'*n)[:n]])

#...................................................   
   def checkUltraMetric(self):
      """
      @summary: check to see if tree is ultrametric, all the way to the root
      """
      #tipPaths,treeLengths,subTrees = self.getTreeInfo(self.tree)
      #self._subTrees = subTrees
      
      if self.branchLengths == self.HAS_BRANCH_LEN:
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
         count = len(set(toSet))
         return bool(1//count)
      else:
         return self.NO_BRANCH_LEN  # need to think about this
   
   @property
   def tipNames(self):
      return self._tipNames
   
   @property
   def tipNamesWithMX(self):
      return self._tipNamesWithMX
   
   @property
   def polytomies(self):
      
      return self._polytomy
   
   @property
   def internalCount(self):
      
      return len(self.subTrees)   
   
   @property
   def subTrees(self):
      
      return self._subTrees
         
   @property
   def lengths(self):
     
      return self._lengths
     
   @property
   def tipCount(self):
       
      return len(self.tipPaths)
      
   @property
   def binary(self):
      return bool(1//(self.tipCount - self.internalCount))
      
   @property
   def branchLengths(self):
      #if not self.tipPaths:
      #   self.subTrees = self.getTreeInfo(self.tree)[2] 
      if self._numberMissingLengths == 0:
         return self.HAS_BRANCH_LEN
      else:    
         if self._numberMissingLengths == (self.tipCount + self.internalCount -1):
            return self.NO_BRANCH_LEN
         else:
            return self.MISSING_BRANCH_LEN
   
# .........................................................................   
   def makePaths(self, tree, takeOutBranches=False):
      """
      @summary: makes paths by recursing tree and appending parent to new pathId
      """
      print "in make Paths"
      p = {'c':0}   
      def recursePaths(clade, parent):
         if "children" in clade:
            clade['path'].insert(0,str(p['c']))
            clade['path'] = clade['path'] + parent 
            clade['pathId'] = str(p['c'])
            #clade['name'] = str(p['c'])
            for child in clade["children"]:
               p['c'] = p['c'] + 1
               recursePaths(child,clade['path'])
         else:
            # tips
            clade['path'].insert(0,str(p['c']))
            clade['path'] = clade['path'] + parent
            clade['pathId'] = str(p['c'])
            #clade['name'] = str(p['c'])  #take this out for real
      # ................................
      def takeOutBr(clade):
         
         if "children" in clade:
            clade.pop('length', None)
            for child in clade["children"]:
               takeOutBr(child)
         else:
            clade.pop('length', None)
      # ................................      
      def takeOutStrPaths(clade):
         
         if "children" in clade:
            clade["path"] = []
            clade["pathId"] = ''
            for child in clade["children"]:
               takeOutStrPaths(child)
         else:
            clade["path"] = []
            clade["pathId"] = ''
      # ................................
      takeOutStrPaths(tree)    
      
      recursePaths(tree,[])
      
      if takeOutBranches:
         takeOutBr(tree)
      
      def stringifyPaths(clade):
         if "children" in clade:
            clade['path'] = ','.join(clade['path'])
            for child in clade["children"]:
               stringifyPaths(child)
         else:
            clade['path'] = ','.join(clade['path'])
            
      stringifyPaths(tree)
   
# .........................................................................   
   def _makeCladeFromEdges(self, edge, lengths=False, tips=False):
      """
      @summary: MORE GENERIC VERSION,makes a tree dict from a (2 * No. internal node) x 2 numpy matrix
      @param edge: numpy array of edges (integers)
      @param lengths: boolean for adding lengths
      """
      
      iNodes = list(set(edge[:,0]))
      m = {}  # key is internal node, value is list of terminating nodes
      for iN in iNodes:
         dx = np.where(edge[:,0]==iN)[0]
         le = list(edge[dx][:,1])
         m[iN] = le
      #print m
      #m = {k[0]:list(k) for k in edge }
      tree = {'pathId':str(0),'path':'','children':[]}  # will take out name for internal after testing
      def recurse(clade,l):
         for x in l:
            if 'children' in clade:
               nc = {'pathId':str(x),'path':''} # will take out name for internal after testing
               if lengths:
                  nc["length"] = lengths[x]
               if x not in tips:
                  nc['children'] = []
                  #nc["path"] = ','.join([str(pI) for pI in self.internalPaths[str(x)]])
                  nc["path"] = ''
               else:
                  nc["name"] = self.tipPaths[str(x)][1]
                  #nc["path"] = ','.join([str(pI) for pI in self.tipPaths[str(x)][0]])
                  nc["path"] = ''
               clade['children'].append(nc)
               if x not in tips:
                  recurse(nc,m[x])
      recurse(tree,m[edge[0][0]])
      
      return tree
# .........................................................................
   def _makeCladeFromEdges_tips(self, edge, lengths=False,tips=False):
      """
      @summary: MORE GENERIC VERSION,makes a tree dict from a (2 * No. internal node) x 2 numpy matrix
      @param edge: numpy array of edges (integers)
      @param lengths: boolean for adding lengths
      """
      sT = self.subTrees
      # these paths are for tips and internal for R sample tree 
      self.internalPaths = {
                            '7':[7],
                            '10':[10,7],
                            '8':[8,7],
                            '9':[9,8,7]
                            }
      self.tipPaths = {
                       '6':[6,10,7],
                       '5':[5,10,7],
                       '4':[4,9,8,7],
                       '3':[3,9,8,7],
                       '2':[2,8,7],
                       '1':[1,8,7]
                       }
      #tips = self.tipPaths.keys()
      iNodes = list(set(edge[:,0]))
      m = {}  # key is internal node, value is list of terminating nodes
      for iN in iNodes:
         dx = np.where(edge[:,0]==iN)[0]
         le = list(edge[dx][:,1])
         m[iN] = le
      print m
      #m = {k[0]:list(k) for k in edge }
      tree = {'pathId':str(0),'path':"7",'children':[],"name":str(0)}  # will take out name for internal after testing
      def recurse(clade,l):
         for x in l:
            if 'children' in clade:
               nc = {'pathId':str(x),'path':[],"name":str(x)} # will take out name for internal after testing
               if lengths:
                  #nc["length"] = self.lengths[x]
                  pass
               if x not in tips:
                  nc['children'] = []
                  nc["path"] = ','.join([str(pI) for pI in self.internalPaths[str(x)]])
               else:
                  #nc["name"] = self.tipPaths[str(x)][1]
                  nc["path"] = ','.join([str(pI) for pI in self.tipPaths[str(x)]])
                  pass
               clade['children'].append(nc)
               if x not in tips:
                  recurse(nc,m[x])
      recurse(tree,m[7])
      
      return tree
   
# .........................................................................        
   def _makeCladeFromRandomEdges(self, edge, n):
      """
      @summary: makes a tree dict from a (2 * No. internal node) x 2 numpy matrix
      @param edge: numpy array of edges
      @param n: number of tips
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
      tree = {'pathId':str(n+1),'path':[],'children':[],"name":str(n+1),"length":"0"}
      def recurse(clade,l):
         for x in l:
            if 'children' in clade:
               nc = {'pathId':str(x),'path':[],"name":str(x),"length":'0'}
               if x not in tips:
                  nc['children'] = []
               clade['children'].append(nc)
               if x not in tips:
                  recurse(nc,m[x])
      recurse(tree,m[n+1])
      
      return tree
      
   def makeClades(self, edge):
      """
      @deprecated: false start but has some good ideas in it
      """
      iNodes = list(set(edge[:,0])) #.sort()  # unique internal nodes from edges
      terminalEdges = [list(r) for r in edge if r[0] > r[1]]
      terminalLookUp = {}
      for row in terminalEdges:
         pt = row[0]
         child = {'pathId':row[1],'path':''}
         if pt not in terminalLookUp:
            terminalLookUp[pt] = {'pathId':pt,'path':'','children':[child]}   
         else:
            terminalLookUp[pt]['children'].append(child)
      
      le = [[x[0],x[1]] for x in edge] 
      le.sort(key=itemgetter(0)) 
      print le
   
   def _getRTips(self,rt):
      """
      @summary: recurses a random subtree and returns a list of its tips
      """
      tips = []
      def findTips(clade):
         if 'children' in clade:
            clade['name'] = ''
            for child in clade['children']:
               findTips(child)
         else:
            # tips
            clade['name'] = ''
            tips.append(clade)
            
      findTips(rt)
      return tips    
     
   def resolvePoly(self):
      """
      @summary: resolves polytomies against tree object
      @return: new tree object 
      """ 
      if len(self.polyPos.keys()) > 0:
         
         st_copy = self.subTrees.copy()
         # loops through polys and makes rnd tree and attaches as children in subtree
         for k in self.polyPos.keys():
            pTips =  self.polyPos[k]['desc'].items()  # these are integers
            n = len(pTips)          
            rt = self.rTree(n)
            tips = self._getRTips(rt)
            for pt, t in zip(pTips,tips):
               #print pt," ",t
               t['pathId'] = str(pt[0])  # might not need this
               t['length'] = pt[1]
               if str(pt[0]) not in self.tipPaths:
                  t['children'] = self.subTrees[pt[0]]
               else:
                  t['name'] = self.tipPaths[str(pt[0])][1]
                  #print pt," ",t
            # now at this level get the two children of the random root
            c1 = rt['children'][0]
            c2 = rt['children'][1]
            
            st_copy[int(k)] = []
            st_copy[int(k)].append(c1)
            st_copy[int(k)].append(c2)
            
         removeList = list(self.polyPos.keys())
         def replaceInTree(clade):
            if "children" in clade:
               #if clade["pathId"] in self.polyPos.keys():
               if clade["pathId"] in removeList:
                  idx = removeList.index(clade['pathId'] )
                  del removeList[idx]
               #if clade["pathId"] == polyKey:
                  clade["children"] = st_copy[int(clade["pathId"])]
                  #return
               for child in clade["children"]:
                  replaceInTree(child)
            else:
               pass    
         newTree = self.tree.copy()
         replaceInTree(newTree) 
         if self.branchLengths == self.NO_BRANCH_LEN:
            takeOutBr = True
         else: 
            takeOutBr = False
         self.makePaths(newTree,takeOutBranches=takeOutBr)
            
         return LMtree(newTree)
      
      else:
         return self
        
      
   def rTree(self, n, rooted=True):
      """
      @summary: given the number of tips generate a random binary tree by randomly splitting edges, 
      equal to foo branch in ape's rtree
      @param n: number of tips
      @note: this is just for >= 4 so far, but not be a problem
      """
      def generate(n, pos):
         n1 = randint(1,n-1)
         n2 = n - n1
         po2 = pos + 2 * n1 - 1
         edge[pos][0] = nod['nc']
         edge[po2][0] = nod['nc']
         nod['nc'] = nod['nc'] + 1
         if n1 > 2:
            edge[pos][1] = nod['nc']
            generate(n1, pos+1)
         elif n1 == 2:
            edge[pos+1][0] = nod['nc']
            edge[pos+2][0] = nod['nc']
            edge[pos][1]   = nod['nc']
            nod['nc'] = nod['nc'] + 1
         if n2 > 2:
            edge[po2][1] = nod['nc']
            generate(n2, po2+1)
         elif n2 == 2:
            edge[po2 + 1][0] = nod['nc']
            edge[po2 + 2][0] = nod['nc']
            edge[po2][1]    = nod['nc']
            nod['nc'] = nod['nc'] + 1
         
      nbr = (2 * n) - 3 + rooted
      edge =  np.array(np.arange(0,2*nbr)).reshape(2,nbr).T
      edge.fill(-999)
      nod = {'nc': n + 1}
      generate(n,0)
     
      idx = np.where(edge[:,1]==-999)[0]
      for i,x in enumerate(idx):
         edge[x][1] = i + 1
         
      rt = self._makeCladeFromRandomEdges(edge, n)
      return rt
   
   def writeTree(self, path):
      #if os.path.exists(path):
      with open(path,'w') as f:
         f.write(json.dumps(self.tree,sort_keys=True, indent=4))
      
      
if __name__ == "__main__":
   
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
   
         
   
   
         
   