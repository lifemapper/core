"""
@summary: This module tests the LmCommon.encoding.lmTree module
@author: CJ Grady
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
import logging
import os
import unittest

from LmCommon.common.lmconstants import PhyloTreeKeys
from LmCommon.encoding.lmTree import LmTree, LmTreeException
from LmCommon.tests.helpers.testConstants import TREES_PATH

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
      treeDict = {
         "name": "0",
         "pathId": 0,
         "length": 0.0,
         "children": [
            {
               "pathId": 1,
               "length": .4,
               "children": [
                  {
                     "pathId" : 2,
                     "length": .15,
                     "children": [
                        {
                           "pathId" : 3,
                           "length" : .65,
                           "children": [
                              {
                                 "pathId" : 4,
                                 "length" : .2,
                              },
                              {
                                 "pathId" : 5,
                                 "length" : .2,
                              }
                           ]
                        },
                        {
                           "pathId" : 6,
                           "length" : .85,
                        }
                     ]
                  },
                  {
                     "pathId" : 7,
                     "length" : 1.0,
                  }
               ]
            },
            {
               "pathId" : 8,
               "length": .9,
               "children": [
                  {
                     "pathId" : 9,
                     "length" : .5,
                  },
                  {
                     "pathId" : 10,
                     "length" : .5,
                  }
               ]
            } 
         ]
      }
      
      lmt1 = LmTree(treeDict)
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
      assert False
      
   # ............................
   def test_add_matrix_indices_not_matching_fail(self):
      """
      @summary: Test that trying to add matrix indices that don't match, fails
      """
      assert False
      
   # ............................
   def test_get_branch_lengths(self):
      """
      @summary: Test that a branch lengths dictionary is successfully returned
                   when requested and the tree has branch lengths.
      """
      assert False
      
   # ............................
   def test_get_branch_lengths_no_branch_lengths_fail(self):
      """
      @summary: Test that an exception is thrown when trying to retrieve branch
                   lengths from a tree that does not have them.
      """
      assert False
      
   # ............................
   def test_get_clade_existing(self):
      """
      @summary: Test that an existing clade can be successfully returned
      """
      assert False

   # ............................
   def test_get_clade_nonexisting_fail(self):
      """
      @summary: Test that trying to retrieve a clade that does not exist fails
      """
      assert False

   # ............................
   def test_get_matrix_indices_in_existing_clade(self):
      """
      @summary: Test that we can successfully retrieve the matrix indices in an
                   existing clade
      """
      assert False
      
   # ............................
   def test_get_matrix_indices_in_nonexisting_clade_fail(self):
      """
      @summary: Test that requesting the matrix indices from an non-existing 
                   clade fails
      """
      assert False
      
   # ............................
   def test_get_matrix_indices_in_root(self):
      """
      @summary: Test that we can successfully retrieve the matrix indices in 
                   the entire tree from the root
      """
      assert False
      
   # ............................
   def test_get_labels(self):
      """
      @summary: Test that the get labels method returns all of the labels in 
                   the tree
      """
      assert False
      
   # ............................
   def test_has_branch_lengths(self):
      """
      @summary: Test that the has branch lengths method works
      """
      assert False
   
   # ............................
   def test_has_polytomies(self):
      """
      @summary: Test the hasPolytomies method
      """
      assert False
      
   # ............................
   def test_is_binary(self):
      """
      @summary: Test the isBinary method
      """
      assert False
      
   # ............................
   def test_is_ultrametric(self):
      """
      @summary: Test the isUltrametric method
      """
      assert False

   # ............................
   def test_prune_tree(self):
      """
      @summary: Test that we can successfully prune clades from a tree
      """
      assert False
      
   # ............................
   def test_prune_tree_only_tips(self):
      """
      @summary: Test that using the only tips options causes the pruneTree 
                   method to only prune tils
      """
      assert False
      
   # ............................
   def test_remove_matrix_indices(self):
      """
      @summary: Test that the remove matrix indices method works correctly
      """
      assert False
      
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
         "pathId": 0,
         "length": 0.0,
         "children": [
            {
               "pathId": 1,
               "length": .4,
               "children": [
                  {
                     "pathId" : 2,
                     "length": .15,
                     "children": [
                        {
                           "pathId" : 3,
                           "length" : .65,
                           "children": [
                              {
                                 "pathId" : 4,
                                 "length" : .2,
                              },
                              {
                                 "pathId" : 5,
                                 "length" : .2,
                              }
                           ]
                        },
                        {
                           "pathId" : 6,
                           "length" : .85,
                        }
                     ]
                  },
                  {
                     "pathId" : 7,
                     "length" : 1.0,
                  }
               ]
            },
            {
               "pathId" : 8,
               "length": .9,
               "children": [
                  {
                     "pathId" : 9,
                     "length" : .5,
                  },
                  {
                     "pathId" : 10,
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
         "pathId": 0,
         "length": 0.0,
         "children": [
            {
               "pathId": 1,
               "length": .4,
               "children": []
            },
            {
               "pathId" : 8,
               "length": .9,
               "children": [
                  {
                     "pathId" : 9,
                     "length" : .5,
                  },
                  {
                     "pathId" : 10,
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
      assert False
      
   # ............................
   def test_write_tree_flo(self):
      """
      @summary: Test that a tree can be written to a File-like object
      """
      assert False
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   #TODO: Remove
   #p = "/home/jcavner/Charolettes_Data/Trees/RAxML_bestTree.12.15.14.1548tax.ultrametric.tre"
   
   #p = "/home/jcavner/PhyloXM_Examples/test_polyWithoutLengths.json"
   
   #to = LmTree.fromFile(p)
   
   #print to.polyPos
   
   #newTree = to.resolvePoly()
   
   #newTree.writeTree("/home/jcavner/PhyloXM_Examples/resolvedPolyWithoutBranchLen.json")
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
   
         
   
   
         
   
#If tree is ultrametric and we resolve polytomies, it should remain ultrametric

#Get common ancestor?

#Get node

#Get labels?

#Set branch length for node

#Find nodes without branch lengths

#Add matrix ids

#Remove matrix id(s)



#200 lines docs
#642 total
#442 code


#61 lines docs
#747 total
#686 code






# When pruning tree, the result should be binary (maybe add an option to enforce binary)


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
