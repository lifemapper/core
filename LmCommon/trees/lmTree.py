"""
@summary: Module containing LmTree class
@author: CJ Grady
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
@todo: Add method to remove annotations
"""
import dendropy
import numpy as np

from LmCommon.common.lmconstants import DEFAULT_TREE_SCHEMA, PhyloTreeKeys
from LmCommon.common.matrix import Matrix

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
   def __init__(self, filename, schema):
      """
      @summary: Wrapper around dendropy
      """
      self.tree = dendropy.Tree.get(path=filename, schema=schema)
   
   # Public functions
   # ..........................................................................
   
   # ..............................
   def addNodeLabels(self, prefix=None):
      """
      @summary: Add labels to the internal nodes of a tree
      @param prefix: If provided, this will be prepended to the node label
      @note: This labels nodes the way that R does
      """
      self._labelTreeNodes(self.tree.seed_node, len(self.getLabels()), 
                           prefix=prefix)
      
         
   # ..............................
   def annotateTree(self, attributeName, annotationPairs, 
                    labelAttribute='label', update=False):
      """
      @summary: Annotates the nodes of the tree
      @param attributeName: The name of the annotation attribute to add
      @param annotationPairs: A dictionary of label keys with annotation values
      @param labelAttribute: If this is provided, use this annotation attribute
                                as the key instead of the label
      """
      if labelAttribute == 'label':
         labelMethod = lambda taxon: taxon.label
      else:
         labelMethod = lambda taxon: taxon.annotations.get_value(labelAttribute)
         
      for taxon in self.tree.taxon_namespace:
         try:
            #label = getattr(taxon, labelAttribute)
            label = labelMethod(taxon)
            
            if taxon.annotations.get_value(attributeName) is not None:
               if update:
                  # Remove existing values
                  for ann in taxon.annotations.findall(name=attributeName):
                     taxon.annotations.remove(ann)
                  # Set new value
                  taxon.annotations.add_new(attributeName, annotationPairs[label])
            else:
               taxon.annotations.add_new(attributeName, annotationPairs[label])
         except KeyError:
            # Pass if a label is not found in the dictionary, otherwise fail
            pass
         except AttributeError:
            # Pass if taxon does not have attribute (may not have squid)
            pass
   
   # ..............................
   def getAnnotations(self, annotationAttribute):
      """
      @summary: Gets a list of (label, annotation) pairs
      @param annotationAttribute: The annotation attribute to retrieve
      """
      annotations = []
      for taxon in self.tree.taxon_namespace:
         try:
            att = taxon.annotations.get_value(annotationAttribute)
            annotations.append((taxon.label, att))
         except:
            pass
      return annotations
   
   # ..............................
   def getDistanceMatrix(self, labelAttribute='label', orderedLabels=None):
      """
      @summary: Get a Matrix object of phylogenetic distances between tips
      @param labelAttribute: The attribute of the tips to use as labels for 
                                the matrix
      @param orderedLabels: If provided, use this order of labels
      """
      if labelAttribute == 'label':
         labelFn = lambda taxon: taxon.label
      else:
         labelFn = lambda taxon: taxon.annotations.get_value(labelAttribute)
      # Get list of labels
      if orderedLabels is None:
         orderedLabels = []
         for taxon in self.tree.taxon_namespace:
            orderedLabels.append(labelFn(taxon))
      
      labelLookup = dict(
         [(orderedLabels[i], i) for i in range(len(orderedLabels))])
      
      
      distMtx = np.zeros((len(orderedLabels), len(orderedLabels)), dtype=float)
      pdm = self.tree.phylogenetic_distance_matrix()
      
      for taxon1 in self.tree.taxon_namespace:
         label = labelFn(taxon1)
         # Check for matrix index
         try:
            idx1 = labelLookup[label]
            
            for taxon2 in self.tree.taxon_namespace:
               try:
                  idx2 = labelLookup[labelFn(taxon2)]
                  #mrca = pdm.mrca(taxon1, taxon2)
                  dist = pdm.patristic_distance(taxon1, taxon2)
                  distMtx[idx1, idx2] = dist
               except:
                  pass
         except:
            pass
         
      distanceMatrix = Matrix(distMtx, headers={'0' : orderedLabels, 
                                                '1' : orderedLabels})
      return distanceMatrix
   
   # ..............................
   def getLabels(self):
      """
      @summary: Get tip labels for a clade
      @note: Bottom-up order
      """
      labels = []
      for taxon in self.tree.taxon_namespace:
         labels.append(taxon.label)
      
      labels.reverse()
      return labels
      
   # ..............................
   def getVarianceCovarianceMatrix(self, labelAttribute='label', orderedLabels=None):
      """
      @summary: Get a Matrix object of variance / covariance for tips in tree
      @param labelAttribute: The attribute of the tips to use as labels for 
                                the matrix
      @param orderedLabels: If provided, use this order of labels
      """
      if not self.hasBranchLengths():
         raise Exception, 'Cannot create VCV without branch lengths'
      
      if labelAttribute == 'label':
         labelFn = lambda taxon: taxon.label
      else:
         labelFn = lambda taxon: taxon.annotations.get_value(labelAttribute)
      # Get list of labels
      if orderedLabels is None:
         orderedLabels = []
         for taxon in self.tree.taxon_namespace:
            orderedLabels.append(labelFn(taxon))
      
      labelLookup = dict(
         [(orderedLabels[i], i) for i in range(len(orderedLabels))])
      
      n = len(orderedLabels)
         
      vcv = np.zeros((n, n), dtype=float)
      
      edges = []
      for edge in self.tree.postorder_edge_iter():
         edges.append(edge)
         
      edges.reverse()
      
      for edge in edges:
         try:
            el = edge.head_node.distance_from_root()
         except:
            el = None
         if el is not None:
            childNodes = edge.head_node.child_nodes()
            if len(childNodes) == 0:
               idx = labelLookup[labelFn(edge.head_node.taxon)]
               vcv[idx,idx] = edge.head_node.distance_from_root()
            else:
               leftChild, rightChild = edge.head_node.child_nodes()
               leftTips = [labelLookup[labelFn(tipNode.taxon)] for tipNode in leftChild.leaf_nodes()]
               rightTips = [labelLookup[labelFn(tipNode.taxon)] for tipNode in rightChild.leaf_nodes()]
               #if len(leftTips) > 1 and len(rightTips) > 1:
               for l in leftTips:
                  for r in rightTips:
                     vcv[l, r] = vcv[r, l] = el
 
         for node in self.tree.leaf_nodes():
            idx = labelLookup[labelFn(node.taxon)]
            vcv[idx,idx] = node.distance_from_root()

      vcvMatrix = Matrix(vcv, headers={'0' : orderedLabels,
                                       '1' : orderedLabels})
      return vcvMatrix

   # ..............................
   def hasBranchLengths(self):
      """
      @summary: Returns boolean indicating if the tree has branch lengths for
                   every clade
      """
      try:
         self.tree.minmax_leaf_distance_from_root()
         return True
      except:
         return False

   # ..............................
   def hasPolytomies(self):
      """
      @summary: Returns boolean indicating if the tree has polytomies
      """
      for n in self.tree.nodes():
         if len(n.child_nodes()) > 2:
            return True
      return False
   
   # ..............................
   def isBinary(self):
      """
      @summary: Returns a boolean indicating if the tree is binary
      @note: Checks that every clade has either zero or two children
      """
      for n in self.tree.nodes():
         if not len(n.child_nodes()) in [0, 2]:
            return False
      return True

   # ..............................
   def isUltrametric(self):
      """
      @summary: Check if the tree is ultrametric
      @note: To be ultrametric, the branch length from root to tip must be 
                equal for all tips
      """
      try:
         minBL, maxBL = self.tree.minmax_leaf_distance_from_root()
         return np.isclose(minBL, maxBL)
      except:
         pass
      return False
      
   # ..............................
   def pruneTipsWithoutAttribute(self, searchAttribute=PhyloTreeKeys.MTX_IDX):
      """
      @summary: Prunes the tree of any tips that don't have the specified 
                   attribute
      """
      pruneTaxa = []
      for taxon in self.tree.taxon_namespace:
         val = None
         try:
            val = taxon.annotations.get_value(searchAttribute)
         except:
            pass
         if val is None:
            pruneTaxa.append(taxon)
      
      self.tree.prune_taxa(pruneTaxa)
      self.tree.purge_taxon_namespace()
               
   # ..............................   
   def resolvePolytomies(self):
      """
      @summary: Resolve polytomies in a tree
      """
      self.tree.resolve_polytomies()
   
   # ..............................
   def writeTree(self, fn, schema=DEFAULT_TREE_SCHEMA):
      """
      @summary: Writes the tree JSON to the specified file path
      @param fn: The file location to write the JSON tree
      @param indent: Passed directly to json dump.  If provided and 
                        non-negative integer, pretty prints with this number of 
                        spaces per level
      @todo: Possibly remove.  Unless we decide that the trees written to disk
                should not include matrix index / path / maybe others.  We may
                do that because those things can vary between loads.  We are 
                already resetting the path on load
      """
      self.tree.write(path=fn, schema=schema)
         
   # ..............................
   def _labelTreeNodes(self, node, i, prefix=None):
      """
      @summary: Private function to do the work when labeling nodes
      @note: Recursive
      """
      cn = node.child_nodes()
      
      # If we have children, label and recurse
      if len(cn) > 0:
         if prefix is not None:
            node.label = '{}{}'.format(prefix, i)
         else:
            node.label = i
         # Loop through children and label nodes
         for child in cn:
            i = self._labelTreeNodes(child, i)
      # Return the current i value
      return i
   
