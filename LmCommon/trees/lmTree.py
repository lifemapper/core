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
   def annotateTree(self, attributeName, annotationPairs, 
                    labelAttribute='label'):
      """
      @summary: Annotates the nodes of the tree
      @param attributeName: The name of the annotation attribute to add
      @param annotationPairs: A dictionary of label keys with annotation values
      @param labelAttribute: If this is provided, use this annotation attribute
                                as the key instead of the label
      """
      for taxon in self.tree.taxon_namespace:
         try:
            label = getattr(taxon, labelAttribute)
            setattr(taxon, attributeName, annotationPairs[label])
            taxon.annotations.add_bound_attribute(attributeName)
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
            att = getattr(taxon, annotationAttribute)
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
      # Get list of labels
      if orderedLabels is None:
         orderedLabels = []
         for taxon in self.tree.taxon_namespace:
            orderedLabels.append(getattr(taxon, labelAttribute))
      
      labelLookup = dict(
         [(orderedLabels[i], i) for i in range(len(orderedLabels))])
      
      
      distMtx = np.zeros((len(orderedLabels), len(orderedLabels)), dtype=float)
      pdm = self.tree.phylogenetic_distance_matrix()
      
      for taxon1 in self.tree.taxon_namespace:
         label = getattr(taxon1, labelAttribute)
         # Check for matrix index
         try:
            idx1 = labelLookup[label]
            
            for taxon2 in self.tree.taxon_namespace:
               try:
                  idx2 = labelLookup[getattr(taxon2, labelAttribute)]
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
         if round(minBL, 7) == round(maxBL, 7):
            return True
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
            val = getattr(taxon, searchAttribute)
         except:
            pass
         if val is None:
            pruneTaxa.append(taxon)
      
      self.tree.prune_taxa(pruneTaxa)
               
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
         
