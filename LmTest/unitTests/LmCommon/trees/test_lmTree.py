"""
@summary: This module tests the LmCommon.encoding.lmTree module
@author: CJ Grady
@version: 1.0
@status: release

@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
from copy import deepcopy
import json
import logging
import os
from random import randint, shuffle
from StringIO import StringIO
from tempfile import gettempdir
import unittest

from LmCommon.common.lmconstants import PhyloTreeKeys
from LmCommon.trees.lmTree import LmTree, LmTreeException
from LmTest.helpers.testConstants import TREES_PATH

BASE_TEST_TREE = {
   "name": "0",
   "cladeId": 0,
   "length": 0.0,
   "children": [
      {
         "cladeId": 1,
         "length": .4,
         "children": [
            {
               "cladeId" : 2,
               "length": .15,
               "children": [
                  {
                     "cladeId" : 3,
                     "length" : .65,
                     "children": [
                        {
                           "name" : "4",
                           "cladeId" : 4,
                           "length" : .2,
                        },
                        {
                           "name" : "5",
                           "cladeId" : 5,
                           "length" : .2,
                        }
                     ]
                  },
                  {
                     "name" : "6",
                     "cladeId" : 6,
                     "length" : .85,
                  }
               ]
            },
            {
               "name" : "7",
               "cladeId" : 7,
               "length" : 1.0,
            }
         ]
      },
      {
         "cladeId" : 8,
         "length": .9,
         "children": [
            {
               "name" : "9",
               "cladeId" : 9,
               "length" : .5,
            },
            {
               "name" : "10",
               "cladeId" : 10,
               "length" : .5,
            }
         ]
      } 
   ]
}

# .............................................................................
class TestLmTree(unittest.TestCase):
   """
   @summary: This class tests the LmTree module
   """
   # ............................
   def setUp(self):
      """
      @summary: Prepare test
      """
      pass
   
   # ............................
   def tearDown(self):
      """
      @summary: Clean up after test
      """
      pass
   
   # ............................
   def test_create_copy(self):
      """
      @summary: This test checks that creating a copy of a tree returns a true
                   copy and not a reference to the previous tree
      """
      lmt1 = LmTree(BASE_TEST_TREE)
      lmt2 = LmTree.createCopy(lmt1)
      cladeId = 2
      
      # Update a branch length and check to see if the two corresponding clades
      #    have the same branch length afterwards
      clade1a = lmt1.getClade(cladeId)
      origBL = clade1a[PhyloTreeKeys.BRANCH_LENGTH]
      updateBL = origBL + .4
      lmt1.setBranchLengthForClade(cladeId, updateBL)
      
      clade1b = lmt1.getClade(cladeId)
      clade2b = lmt2.getClade(cladeId)
      
      newBL = clade1b[PhyloTreeKeys.BRANCH_LENGTH]
      testBL = clade2b[PhyloTreeKeys.BRANCH_LENGTH]

      assert newBL == updateBL

      assert newBL != testBL
      assert lmt1.tree != lmt2.tree
      
   # ............................
   def test_from_file_json(self):
      """
      @summary: Test that a tree can be successfully loaded from a file (JSON)
      """
      fn = os.path.join(TREES_PATH, 'generatedTreeWithBranchLengths.json')
      lmt = LmTree.fromFile(fn)
      
      # Assert that the tips list has at least one entry
      assert lmt.tips

   # ............................
   def test_from_file_newick(self):
      """
      @summary: Test that a tree can be successfully loaded from a file (Newick)
      """
      fn = os.path.join(TREES_PATH, 'testNewick.tre')
      lmt = LmTree.fromFile(fn)
      
      # Assert that the tips list has at least one entry
      assert lmt.tips

   # ............................
   def test_from_file_other(self):
      """
      @summary: Test that an exception is thrown when trying to open an 
                   unrecognized tree file type
      """
      fn = os.path.join(TREES_PATH, 'examplePhyloxml.xml')
      with self.assertRaises(LmTreeException):
         LmTree.fromFile(fn)

   # ............................
   def test_add_matrix_indices_matching(self):
      """
      @summary: Test that we can successfully add matrix indices
      """
      lmt = LmTree(BASE_TEST_TREE)
      # Randomly assign matrix indices to labels
      mtxIdxs = [i for i in range(len(lmt.tips))]
      shuffle(mtxIdxs)
      
      pamMetadata = {}
      i = 0
      for label, cladeId in lmt.getLabels():
         pamMetadata[label] = mtxIdxs[i]
         i += 1
      
      lmt.addMatrixIndices(pamMetadata)
      
      for tipId in lmt.tips:
         clade = lmt.getClade(tipId)
         cladeLabel = clade[PhyloTreeKeys.NAME]
         assert clade[PhyloTreeKeys.MTX_IDX] == pamMetadata[cladeLabel]
      
   # ............................
   def test_add_squids(self):
      """
      @summary: Test that we can successfully add LM species squids to a tree
      """
      lmt = LmTree(BASE_TEST_TREE)
      
      # Create a list from a string.  We'll shuffle this and then convert back
      #    to a string so we can have pseudo SQUIDs
      pseudoSquid = list("SomeSquidWellRandomize")
      
      # Randomly create "squids" for each label
      squidDict = {}
      for label, cladeId in lmt.getLabels():
         shuffle(pseudoSquid)
         squidDict[label] = ''.join(pseudoSquid)
      
      lmt.addSQUIDs(squidDict)
      
      for tipId in lmt.tips:
         clade = lmt.getClade(tipId)
         cladeLabel = clade[PhyloTreeKeys.NAME]
         assert clade[PhyloTreeKeys.SQUID] == squidDict[cladeLabel]
      
   # ............................
   def test_get_branch_lengths(self):
      """
      @summary: Test that a branch lengths dictionary is successfully returned
                   when requested and the tree has branch lengths.
      """
      lmt = LmTree(BASE_TEST_TREE)
      bls = lmt.getBranchLengths()
      assert bls # Assert that there is at least one branch length
      # Assert that every clade has a branch length
      assert len(bls) == len(lmt.cladePaths.keys()) 
      
   # ............................
   def test_get_branch_lengths_no_branch_lengths_fail(self):
      """
      @summary: Test that an exception is thrown when trying to retrieve branch
                   lengths from a tree that does not have them.
      """
      fn = os.path.join(TREES_PATH, 'generatedTreeNoBranchLengths.json')
      lmt = LmTree.fromFile(fn)
      with self.assertRaises(LmTreeException):
         bls = lmt.getBranchLengths()
      
   # ............................
   def test_get_clade_existing(self):
      """
      @summary: Test that an existing clade can be successfully returned
      """
      lmt = LmTree(BASE_TEST_TREE)
      for cladeId in lmt.cladePaths.keys():
         clade = lmt.getClade(cladeId)
         assert clade[PhyloTreeKeys.CLADE_ID] == cladeId

   # ............................
   def test_get_clade_nonexisting_fail(self):
      """
      @summary: Test that trying to retrieve a clade that does not exist fails
      """
      lmt = LmTree(BASE_TEST_TREE)
      with self.assertRaises(LmTreeException):
         clade = lmt.getClade(-9873) # Should not exist

   # ............................
   def test_get_distance_matrix(self):
      """
      @summary: Test that we can successfully generate a distance matrix
      """
      print BASE_TEST_TREE
      lmt = LmTree(BASE_TEST_TREE)
      mtx = lmt.getDistanceMatrix()
      print mtx.data
   
   # ............................
   def test_get_matrix_indices_in_existing_clade(self):
      """
      @summary: Test that we can successfully retrieve the matrix indices in an
                   existing clade
      """
      lmt = LmTree(BASE_TEST_TREE)
      
      # Randomly assign matrix indices to labels
      mtxIdxs = [i for i in range(len(lmt.tips))]
      shuffle(mtxIdxs)
      
      pamMetadata = {}
      i = 0
      for label, cladeId in lmt.getLabels():
         pamMetadata[label] = mtxIdxs[i]
         i += 1
      
      lmt.addMatrixIndices(pamMetadata)

      # Since we assigned a matrix index to every tip, every clade should have 
      #    matrix indices
      for cladeId in lmt.cladePaths.keys():
         clade = lmt.getClade(cladeId)
         # Check that there is at least one entry in each list
         assert lmt.getMatrixIndicesInClade(clade=clade)

   # ............................
   def test_get_matrix_indices_in_root(self):
      """
      @summary: Test that we can successfully retrieve the matrix indices in 
                   the entire tree from the root
      """
      lmt = LmTree(BASE_TEST_TREE)
      
      # Randomly assign matrix indices to labels
      mtxIdxs = [i for i in range(len(lmt.tips))]
      shuffle(mtxIdxs)
      
      pamMetadata = {}
      i = 0
      for label, cladeId in lmt.getLabels():
         pamMetadata[label] = mtxIdxs[i]
         i += 1
      
      lmt.addMatrixIndices(pamMetadata)

      # All of the matrix indices we created should be in the tree
      assert sorted(mtxIdxs) == sorted(lmt.getMatrixIndicesInClade())

   # ............................
   def test_get_labels(self):
      """
      @summary: Test that the get labels method returns all of the labels in 
                   the tree
      """
      lmt = LmTree(BASE_TEST_TREE)
      labels = lmt.getLabels()
      
      # All of our tips have labels in the test tree, so the labels list should
      #    be at least as long as the tips list
      assert len(labels) >= len(lmt.tips)
      
   # ............................
   def test_has_branch_lengths(self):
      """
      @summary: Test that the has branch lengths method works
      """
      blFn = os.path.join(TREES_PATH, 'generatedTreeWithBranchLengths.json')
      noBlFn = os.path.join(TREES_PATH, 'generatedTreeNoBranchLengths.json')
      
      # Check that a tree that we know has branch lengths reports that it does
      lmtWithBL = LmTree.fromFile(blFn)
      assert lmtWithBL.hasBranchLengths()
      
      # Check that a tree w/o branch lengths reports that it does not have them
      lmtNoBL = LmTree.fromFile(noBlFn)
      assert not lmtNoBL.hasBranchLengths()
   
   # ............................
   def test_has_polytomies(self):
      """
      @summary: Test the hasPolytomies method
      """
      # Check that a tree that has polytomies, reports as such
      treeDict = {
         "name" : "0",
         "children" : [
            {
               "name" : "1",
               "children" : []
            },
            {
               "name" : "2",
               "children" : []
            },
            {
               "name" : "3",
               "children" : []
            },
            {
               "name" : "4",
               "children" : []
            },
            {
               "name" : "5",
               "children" : []
            },
         ]
      }
      
      lmt = LmTree(treeDict)
      
      assert lmt.hasPolytomies()

      lmt2 = LmTree(BASE_TEST_TREE)
      
      # Check that a tree that should not have polytomies, does not report them
      assert not lmt2.hasPolytomies()
      
   # ............................
   def test_is_binary(self):
      """
      @summary: Test the isBinary method
      """
      # Not binary (polytomies)
      polyTreeDict = {
         "name" : "0",
         "children" : [
            {
               "name" : "1",
               "children" : []
            },
            {
               "name" : "2",
               "children" : []
            },
            {
               "name" : "3",
               "children" : []
            },
            {
               "name" : "4",
               "children" : []
            },
            {
               "name" : "5",
               "children" : []
            },
         ]
      }
      lmt1 = LmTree(polyTreeDict)
      assert not lmt1.isBinary()
      
      # Not binary (one child)
      oneChildTreeDict = {
         "name" : "0",
         "children" : [
            {
               "name" : "1",
               "children" : [
                  {
                     "name" : "3",
                     "children" : []
                  },
                  {
                     "name" : "4",
                     "children" : []
                  }
               ]
            },
            {
               "name" : "2",
               "children" : [ # Only one child
                  {
                     "name" : "5",
                     "children" : []
                  }
               ]
            },
         ]
      }
      lmt2 = LmTree(oneChildTreeDict)
      assert not lmt2.isBinary()
      
      # Binary
      tfn = os.path.join(TREES_PATH, 'generatedTreeNoBranchLengths.json')
      lmt3 = LmTree.fromFile(tfn)
      assert lmt3.isBinary()
      
   # ............................
   def test_is_ultrametric(self):
      """
      @summary: Test the isUltrametric method
      """
      # Not ultrametric
      nonUltrametricTreeDict = {
         "name": "0",
         "cladeId": 0,
         "length": 0.0,
         "children": [
            {
               "cladeId": 1,
               "length": .4,
               "children": [
                  {
                     "cladeId" : 2,
                     "length": .15,
                     "children": [
                        {
                           "cladeId" : 3,
                           "length" : .65,
                           "children": [ # Children don't have same lengths
                              {
                                 "name" : "4",
                                 "cladeId" : 4,
                                 "length" : .3,
                              },
                              {
                                 "name" : "5",
                                 "cladeId" : 5,
                                 "length" : .1,
                              }
                           ]
                        },
                        {
                           "name" : "6",
                           "cladeId" : 6,
                           "length" : .85,
                        }
                     ]
                  },
                  {
                     "name" : "7",
                     "cladeId" : 7,
                     "length" : 1.0,
                  }
               ]
            },
            {
               "cladeId" : 8,
               "length": .9,
               "children": [
                  {
                     "name" : "9",
                     "cladeId" : 9,
                     "length" : .5,
                  },
                  {
                     "name" : "10",
                     "cladeId" : 10,
                     "length" : .5,
                  }
               ]
            } 
         ]
      }
      lmt1 = LmTree(nonUltrametricTreeDict)
      assert not lmt1.isUltrametric()
      
      # Ultrametric
      tfn = os.path.join(TREES_PATH, 'generatedTreeWithBranchLengths.json')
      lmt2 = LmTree.fromFile(tfn)
      assert lmt2.isUltrametric()

   # ............................
   def test_prune_tree(self):
      """
      @summary: Test that we can successfully prune clades from a tree
      """
      treeDict = { # Not using base test tree because we need labels
         "name": "0",
         "cladeId": 0,
         "length": 0.0,
         "children": [
            {
               "name" : "1",
               "cladeId": 1,
               "length": .4,
               "children": [
                  {
                     "name" : "2",
                     "cladeId" : 2,
                     "length": .15,
                     "children": [
                        {
                           "name" : "3",
                           "cladeId" : 3,
                           "length" : .65,
                           "children": [
                              {
                                 "name" : "4",
                                 "cladeId" : 4,
                                 "length" : .2,
                              },
                              {
                                 "name" : "5",
                                 "cladeId" : 5,
                                 "length" : .2,
                              }
                           ]
                        },
                        {
                           "name" : "6",
                           "cladeId" : 6,
                           "length" : .85,
                        }
                     ]
                  },
                  {
                     "name" : "7",
                     "cladeId" : 7,
                     "length" : 1.0,
                  }
               ]
            },
            {
               "name" : "8",
               "cladeId" : 8,
               "length": .9,
               "children": [
                  {
                     "name" : "9",
                     "cladeId" : 9,
                     "length" : .5,
                  },
                  {
                     "name" : "10",
                     "cladeId" : 10,
                     "length" : .5,
                  }
               ]
            } 
         ]
      }
      lmt = LmTree(treeDict)
      tipId = ("7", 7) # label, tip id
      upperCladeId = ("8", 8) # Label , tip id
      testRemovedClades = [8, 9, 10] # These clades should not be in tree after
                                     #    removing the upper clade
                                     
      # Prune a tip, make sure that it is not present afterwards
      lmt.pruneTree([tipId[0], upperCladeId[0]], onlyTips=False)
      assert tipId[1] not in lmt.cladePaths.keys()
      with self.assertRaises(LmTreeException):
         tipClade = lmt.getClade(tipId[1])
         
      # Prune an upper clade, make sure that it and its children are gone
      for cladeId in testRemovedClades:
         assert cladeId not in lmt.cladePaths.keys()
         with self.assertRaises(LmTreeException):
            tipClade = lmt.getClade(cladeId)
      
   # ............................
   def test_prune_tree_only_tips(self):
      """
      @summary: Test that using the only tips options causes the pruneTree 
                   method to only prune tips
      """
      treeDict = { # Not using base test tree because we need labels
         "name": "0",
         "cladeId": 0,
         "length": 0.0,
         "children": [
            {
               "name" : "1",
               "cladeId": 1,
               "length": .4,
               "children": [
                  {
                     "name" : "2",
                     "cladeId" : 2,
                     "length": .15,
                     "children": [
                        {
                           "name" : "3",
                           "cladeId" : 3,
                           "length" : .65,
                           "children": [
                              {
                                 "name" : "4",
                                 "cladeId" : 4,
                                 "length" : .2,
                              },
                              {
                                 "name" : "5",
                                 "cladeId" : 5,
                                 "length" : .2,
                              }
                           ]
                        },
                        {
                           "name" : "6",
                           "cladeId" : 6,
                           "length" : .85,
                        }
                     ]
                  },
                  {
                     "name" : "7",
                     "cladeId" : 7,
                     "length" : 1.0,
                  }
               ]
            },
            {
               "name" : "8",
               "cladeId" : 8,
               "length": .9,
               "children": [
                  {
                     "name" : "9",
                     "cladeId" : 9,
                     "length" : .5,
                  },
                  {
                     "name" : "10",
                     "cladeId" : 10,
                     "length" : .5,
                  }
               ]
            } 
         ]
      }
      lmt = LmTree(treeDict)

      # Attempt to remove a tip and a clade with the only tips option
      #    The clade should remain but the tip should be removed
      #    Only the tip should be removed from the tree, none of the others
      
      innerClades = list(set(lmt.cladePaths.keys()) - set(lmt.tips))
      shuffle(innerClades)
      tipIds = deepcopy(lmt.tips) # Deep Copy so we have a new list
      shuffle(tipIds)
      
      removeClade = innerClades[0] # Random
      removeTip = tipIds.pop(0) # Random

      lmt.pruneTree([str(removeClade), str(removeTip)], onlyTips=True)
      
      # Check that tip has been removed
      assert removeTip not in lmt.cladePaths.keys()
      assert removeTip not in lmt.tips
      
      with self.assertRaises(LmTreeException):
         lmt.getClade(removeTip)
         
      # Check that the rest of the tips are present
      for tipId in tipIds: # We removed the tip from this list with pop earlier
         assert tipId in lmt.cladePaths.keys()
         assert tipId in lmt.tips
         lmt.getClade(tipId) # Should be successful
         
      # Check for clades, should all be present
      for cladeId in innerClades:
         assert cladeId in lmt.cladePaths.keys()
         lmt.getClade(cladeId) # Should be successful
      
   # ............................
   def test_remove_matrix_indices(self):
      """
      @summary: Test that the remove matrix indices method works correctly
      """
      lmt = LmTree(BASE_TEST_TREE)
      # Randomly assign matrix indices to labels
      mtxIdxs = [i for i in range(len(lmt.tips))]
      shuffle(mtxIdxs)
      
      pamMetadata = {}
      i = 0
      for label, cladeId in lmt.getLabels():
         pamMetadata[label] = mtxIdxs[i]
         i += 1
      
      lmt.addMatrixIndices(pamMetadata)
      
      # Check that we have matrix indices
      assert lmt.getMatrixIndicesInClade()
      
      # Remove them
      lmt.removeMatrixIndices()
      
      # Check that there are no longer matrix indices
      assert not lmt.getMatrixIndicesInClade()
      
   # ............................
   def test_resolve_polytomies(self):
      """
      @summary: Test that the resolve polytomies method works correctly
      """
      treeDict = {
         "name" : "0",
         "children" : [
            {
               "name" : "1",
               "children" : []
            },
            {
               "name" : "2",
               "children" : []
            },
            {
               "name" : "3",
               "children" : []
            },
            {
               "name" : "4",
               "children" : []
            },
            {
               "name" : "5",
               "children" : []
            },
         ]
      }
      
      lmt = LmTree(treeDict)
      
      assert lmt.hasPolytomies()
      
      lmt.resolvePolytomies()
      
      assert not lmt.hasPolytomies()
      
   # ............................
   def test_set_branch_length_for_clade_existing(self):
      """
      @summary: Test that setting the branch length for an existing clade works
      """
      treeDict = {
         "name": "0",
         "cladeId": 0,
         "length": 0.0,
         "children": [
            {
               "cladeId": 1,
               "length": .4,
               "children": [
                  {
                     "cladeId" : 2,
                     "length": .15,
                     "children": [
                        {
                           "cladeId" : 3,
                           "length" : .65,
                           "children": [
                              {
                                 "cladeId" : 4,
                                 "length" : .2,
                              },
                              {
                                 "cladeId" : 5,
                                 "length" : .2,
                              }
                           ]
                        },
                        {
                           "cladeId" : 6,
                           "length" : .85,
                        }
                     ]
                  },
                  {
                     "cladeId" : 7,
                     "length" : 1.0,
                  }
               ]
            },
            {
               "cladeId" : 8,
               "length": .9,
               "children": [
                  {
                     "cladeId" : 9,
                     "length" : .5,
                  },
                  {
                     "cladeId" : 10,
                     "length" : .5,
                  }
               ]
            } 
         ]
      }
      
      lmt1 = LmTree(treeDict)
      cladeId = 2
      
      # Update a branch length and check to see if the two corresponding clades
      #    have the same branch length afterwards
      clade1a = lmt1.getClade(cladeId)
      origBL = clade1a[PhyloTreeKeys.BRANCH_LENGTH]
      updateBL = origBL + .4
      lmt1.setBranchLengthForClade(cladeId, updateBL)
      
      clade1b = lmt1.getClade(cladeId)
      
      newBL = clade1b[PhyloTreeKeys.BRANCH_LENGTH]

      assert newBL == updateBL

      assert updateBL != origBL
      
   # ............................
   def test_set_branch_length_for_clade_nonexisting_fail(self):
      """
      @summary: Test that setting the branch length for a clade that does not
                   exist, fails
      """
      treeDict = {
         "name": "0",
         "cladeId": 0,
         "length": 0.0,
         "children": [
            {
               "cladeId": 1,
               "length": .4,
               "children": []
            },
            {
               "cladeId" : 8,
               "length": .9,
               "children": [
                  {
                     "cladeId" : 9,
                     "length" : .5,
                  },
                  {
                     "cladeId" : 10,
                     "length" : .5,
                  }
               ]
            } 
         ]
      }
      
      lmt1 = LmTree(treeDict)
      cladeId = 2
      
      with self.assertRaises(LmTreeException):
         clade1a = lmt1.getClade(cladeId)
         origBL = clade1a[PhyloTreeKeys.BRANCH_LENGTH]
         updateBL = origBL + .4
         lmt1.setBranchLengthForClade(cladeId, updateBL)
      
      
   # ............................
   def test_write_tree(self):
      """
      @summary: Test that a tree can be successfully written to a file
      """
      # Make a tree
      lmt = LmTree(BASE_TEST_TREE)
      # Get a random file name
      fn = os.path.join(gettempdir(), "tmp{0}tree.json".format(
                                                           randint(0, 100000)))
      # Write the tree to it
      lmt.writeTree(fn)
      # Get a new tree from the file
      lmt2 = LmTree.fromFile(fn)
      
      # Delete the temp file
      os.remove(fn)
      
      # Check that the trees are equal
      assert sorted(lmt.cladePaths.keys()) == sorted(lmt2.cladePaths.keys())
      assert sorted(lmt.tips) == sorted(lmt2.tips)
      
   # ............................
   def test_write_tree_flo(self):
      """
      @summary: Test that a tree can be written to a File-like object
      """
      # Make a tree
      lmt = LmTree(BASE_TEST_TREE)
      # Write the tree to a string io object
      flo = StringIO()
      lmt.writeTreeToFlo(flo)
      # Load the tree from the string io object
      flo.seek(0)
      treeJson = json.load(flo)
      lmt2 = LmTree(treeJson)
      # Check that the trees are equal
      assert sorted(lmt.cladePaths.keys()) == sorted(lmt2.cladePaths.keys())
      assert sorted(lmt.tips) == sorted(lmt2.tips)
   
# .............................................................................
def getTestSuites():
   """
   @summary: Gets the test suites for the LmCommon.common.systemMetadata module.
   @return: A list of test suites
   """
   loader = unittest.TestLoader()
   testSuites = []
   testSuites.append(loader.loadTestsFromTestCase(TestLmTree))
   return testSuites

# ============================================================================
# = Main                                                                     =
# ============================================================================

if __name__ == '__main__':
   #tests
   import logging
   logging.basicConfig(level = logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
