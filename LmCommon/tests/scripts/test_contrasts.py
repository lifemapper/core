"""
@summary: This module tests the LmCommon.encoding.contrasts module
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
import numpy as np
import os
import unittest

from LmCommon.common.unicode import toUnicode
from LmCommon.encoding.contrasts import (BioGeoEncoding, EncodingException, 
                                         PhyloEncoding)
from LmCommon.encoding.lmTree import LmTree

from LmCommon.tests.helpers.testConstants import (BIO_GEO_HYPOTHESES_PATH,
               OUTPUT_BIO_GEO_ENCODINGS_PATH, OUTPUT_PHYLO_ENCODINGS_PATH,
               SHAPEGRIDS_PATH, TREES_PATH)

# .............................................................................
class TestPhyloEncoding(unittest.TestCase):
   """
   @summary: This is a test class for the PhyloEncoding class
   """
   # ..............................   
   def test_consistent_results_branch_lengths(self):
      """
      @summary: Check that we have a deterministic result when encoding.  Rows
                   should always be in the same order but columns may change.
                   The value for a site may be negative or positive, but the 
                   absolute value should be the same.
      """
      self._consistent_results_helper(os.path.join(TREES_PATH, 
                                        'generatedTreeWithBranchLengths.json'))
      
   # ..............................   
   def test_consistent_results_no_branch_lengths(self):
      """
      @summary: Check that we have a deterministic result when encoding.  Rows
                   should always be in the same order but columns may change.
                   The value for a site may be negative or positive, but the 
                   absolute value should be the same.
      """
      self._consistent_results_helper(os.path.join(TREES_PATH, 
                                        'generatedTreeNoBranchLengths.json'))
   
   # ..............................   
   def test_lmtree_with_branch_lengths(self):
      """
      @summary: Test checks that encoding works properly when the tree has 
                   branch lengths and is already an LmTree object
      """
      treeDict = {
         "name": "0",
         "path":  [0],
         "pathId": 0,
         "length": 0.0,
         "children": [
            {
               "pathId": 1,
               "length": .4,
               "path": [1,0],
               "children": [
                  {
                     "pathId" : 2,
                     "length": .15,
                     "path": [9,5,0],
                     "children": [
                        {
                           "pathId" : 3,
                           "length" : .65,
                           "path": [3,2,1,0],
                           "children": [
                              {
                                 "pathId" : 4,
                                 "length" : .2,
                                 "path" : [4,3,2,1,0],
                                 "mx" : 0
                              },
                              {
                                 "pathId" : 5,
                                 "length" : .2,
                                 "path" : [5,3,2,1,0],
                                 "mx" : 1
                              }
                           ]
                        },
                        {
                           "pathId" : 6,
                           "length" : .85,
                           "path" : [6,2,1,0],
                           "mx" : 2
                        }
                     ]
                  },
                  {
                     "pathId" : 7,
                     "length" : 1.0,
                     "path" : [7,1,0],
                     "mx" : 3
                  }
               ]
            },
            {
               "pathId" : 8,
               "length": .9,
               "path": [8,0],
               "children": [
                  {
                     "pathId" : 9,
                     "length" : .5,
                     "path" : [9,8,0],
                     "mx" : 4
                  },
                  {
                     "pathId" : 10,
                     "length" : .5,
                     "path" : [10,8,0],
                     "mx" : 5
                  }
               ]
            } 
         ]
      }
      
      lmt = LmTree(treeDict)
      
      pam = np.random.choice(2, 24).reshape(4, 6)
      
      treeEncoder = PhyloEncoding(lmt, pam)
      
      pMtx = treeEncoder.encodePhylogeny()
      print pMtx
   
      assert round(np.sum(pMtx), 3) == 0.000
      
      # Check that sum of absolute values is 2*columns
      assert round(np.sum(np.abs(pMtx)), 3) == 2.000 * pMtx.shape[1]

      # Approximate expected result
      #    Absolute values should be equal but order can vary (column-wise)
      #    Rows should be in same order
      approximateExpectedResult = [
         [ 0.000,  0.280,  0.500, -1.000,  0.196],
         [ 0.000,  0.280,  0.500,  1.000,  0.196],
         [ 0.000,  0.439, -1.000,  0.000,  0.290],
         [ 0.000, -1.000,  0.000,  0.000,  0.319],
         [-1.000,  0.000,  0.000,  0.000, -0.500],
         [ 1.000,  0.000,  0.000,  0.000, -0.500]
      ]
      
      # Get a set of the absolute values of both the expected values and the 
      #    actual values and make sure they match
      expectedValues = set([])
      for row in approximateExpectedResult:
         for item in row:
            expectedValues.add(abs(round(item, 3)))
      
      testValues = set([])
      for row in pMtx:
         for item in row:
            testValues.add(abs(round(item, 3)))

      assert testValues == expectedValues
   
   # ..............................   
   def test_lmtree_without_branch_lengths(self):
      """
      @summary: Test that checks that encoding an LmTree object works when it
                   does not have branch lengths
      """
      treeDict = {
         "name": "0",
         "pathId": 0,
            "children": [
            {
               "pathId": 1,
               "children": [
                  {
                     "pathId" : 2,
                     "children": [
                        {
                           "pathId" : 3,
                           "children": [
                              {
                                 "pathId" : 4,
                                 "mx" : 0
                              },
                              {
                                 "pathId" : 5,
                                 "mx" : 1
                              }
                           ]
                        },
                        {
                           "pathId" : 6,
                           "mx" : 2
                        }
                     ]
                  },
                  {
                     "pathId" : 7,
                     "mx" : 3
                  }
               ]
            },
            {
               "pathId" : 8,
               "children": [
                  {
                     "pathId" : 9,
                     "mx" : 4
                  },
                  {
                     "pathId" : 10,
                     "mx" : 5
                  }
               ]
            } 
         ]
      }
      
      pam = np.random.choice(2, 24).reshape(4, 6)
   
      lmt = LmTree(treeDict)
      treeEncoder = PhyloEncoding(lmt, pam)
      pMtx = treeEncoder.encodePhylogeny()
      print pMtx
   
      assert round(np.sum(pMtx), 3) == 0.000
      
      # Check that sum of absolute values is 2*columns
      assert round(np.sum(np.abs(pMtx)), 3) == 2.000 * pMtx.shape[1]

      # Approximate expected result
      #    Absolute values should be equal but order can vary (column-wise)
      #    Rows should be in same order
      approximateExpectedResult = [
         [-0.125, -0.250, -0.500, -1.000,  0.000],
         [-0.125, -0.250, -0.500,  1.000,  0.000],
         [-0.250, -0.500,  1.000,  0.000,  0.000],
         [-0.500,  1.000,  0.000,  0.000,  0.000],
         [ 0.500,  0.000,  0.000,  0.000, -1.000],
         [ 0.500,  0.000,  0.000,  0.000,  1.000]
       ]
      
      # Get a set of the absolute values of both the expected values and the 
      #    actual values and make sure they match
      expectedValues = set([])
      for row in approximateExpectedResult:
         for item in row:
            expectedValues.add(abs(round(item, 3)))
      
      testValues = set([])
      for row in pMtx:
         for item in row:
            testValues.add(abs(round(item, 3)))

      assert testValues == expectedValues
   
   
   # ..............................   
   def test_nonbinary_fail(self):
      """
      @summary: Test that encoding fails when the tree is not binary
      """
      treeDict = {
         "name": "0",
         "pathId": 0,
         "length": 0.0,
         "children": [
            {
               "length": .4,
               "children": [
                  {
                     "length": .15,
                     "children": [
                        {
                           "length" : .85,
                           "mx" : 0
                        },
                     ]
                  },
                  {
                     "length" : 1.0,
                     "mx" : 3
                  }
               ]
            },
            {
               "length": .9,
               "children": [
                  {
                     "length" : .5,
                     "mx" : 4
                  },
                  {
                     "length" : .5,
                     "mx" : 5
                  }
               ]
            } 
         ]
      }
      
      pam = np.random.choice(2, 24).reshape(4, 6)
      
      treeEncoder = PhyloEncoding(treeDict, pam)
      
      with self.assertRaises(EncodingException):
         pMtx = treeEncoder.encodePhylogeny()
   
   # ..............................   
   def test_nonultrametric_fail(self):
      """
      @summary: Test that encoding fails when the tree is not ultrametric
      """
      treeDict = {
         "name": "0",
         "path":  [0],
         "pathId": 0,
         "length": 0.0,
         "children": [
            {
               "pathId": 1,
               "length": .4,
               "path": [1,0],
               "children": [
                  {
                     "pathId" : 2,
                     "length": .15,
                     "path": [9,5,0],
                     "children": [
                        {
                           "pathId" : 3,
                           "length" : .65,
                           "path": [3,2,1,0],
                           "children": [
                              {
                                 "pathId" : 4,
                                 "length" : .25,
                                 "path" : [4,3,2,1,0],
                                 "mx" : 0
                              },
                              {
                                 "pathId" : 5,
                                 "length" : .3,
                                 "path" : [5,3,2,1,0],
                                 "mx" : 1
                              }
                           ]
                        },
                        {
                           "pathId" : 6,
                           "length" : .85,
                           "path" : [6,2,1,0],
                           "mx" : 2
                        }
                     ]
                  },
                  {
                     "pathId" : 7,
                     "length" : 1.0,
                     "path" : [7,1,0],
                     "mx" : 3
                  }
               ]
            },
            {
               "pathId" : 8,
               "length": .9,
               "path": [8,0],
               "children": [
                  {
                     "pathId" : 9,
                     "length" : .5,
                     "path" : [9,8,0],
                     "mx" : 4
                  },
                  {
                     "pathId" : 10,
                     "length" : .5,
                     "path" : [10,8,0],
                     "mx" : 5
                  }
               ]
            } 
         ]
      }
      
      pam = np.random.choice(2, 24).reshape(4, 6)
      
      treeEncoder = PhyloEncoding(treeDict, pam)
      
      with self.assertRaises(EncodingException):
         pMtx = treeEncoder.encodePhylogeny()
   
   # ..............................   
   def test_polytomy_fail(self):
      """
      @summary: Test that encoding fails when the tree has polytomies
      """
      treeDict = {
         "name": "0",
         "pathId": 0,
         "length": 0.0,
         "children": [
            {
               "length": .4,
               "children": [
                  {
                     "length": .15,
                     "children": [
                        {
                           "length" : .85,
                           "mx" : 0
                        },
                        {
                           "length" : .85,
                           "mx" : 1
                        },
                        {
                           "length" : .85,
                           "mx" : 2
                        }
                     ]
                  },
                  {
                     "length" : 1.0,
                     "mx" : 3
                  }
               ]
            },
            {
               "length": .9,
               "children": [
                  {
                     "length" : .5,
                     "mx" : 4
                  },
                  {
                     "length" : .5,
                     "mx" : 5
                  }
               ]
            } 
         ]
      }
      
      pam = np.random.choice(2, 24).reshape(4, 6)
      
      treeEncoder = PhyloEncoding(treeDict, pam)
      
      with self.assertRaises(EncodingException):
         pMtx = treeEncoder.encodePhylogeny()
   
   # ..............................   
   def test_extend_pam_to_match_tree(self):
      """
      @summary: Test to check that PAM can be expanded to match tips in tree
      """
      treeDict = {
         "name": "0",
         "path":  [0],
         "pathId": 0,
         "length": 0.0,
         "children": [
            {
               "pathId": 1,
               "length": .4,
               "path": [1,0],
               "children": [
                  {
                     "pathId" : 2,
                     "length": .15,
                     "path": [9,5,0],
                     "children": [
                        {
                           "pathId" : 3,
                           "length" : .65,
                           "path": [3,2,1,0],
                           "children": [
                              {
                                 "pathId" : 4,
                                 "length" : .2,
                                 "path" : [4,3,2,1,0],
                                 "mx" : 0
                              },
                              {
                                 "pathId" : 5,
                                 "length" : .2,
                                 "path" : [5,3,2,1,0],
                                 "mx" : 1
                              }
                           ]
                        },
                        {
                           "pathId" : 6,
                           "length" : .85,
                           "path" : [6,2,1,0],
                           "mx" : 2
                        }
                     ]
                  },
                  {
                     "pathId" : 7,
                     "length" : 1.0,
                     "path" : [7,1,0],
                  }
               ]
            },
            {
               "pathId" : 8,
               "length": .9,
               "path": [8,0],
               "children": [
                  {
                     "pathId" : 9,
                     "length" : .5,
                     "path" : [9,8,0],
                     "mx" : 3
                  },
                  {
                     "pathId" : 10,
                     "length" : .5,
                     "path" : [10,8,0],
                  }
               ]
            } 
         ]
      }
      
      pam = np.random.choice(2, 16).reshape(4, 4)
      
      treeEncoder = PhyloEncoding(treeDict, pam)
      treeEncoder.extendPamToMatchTree()
      
      pMtx = treeEncoder.encodePhylogeny()
      
      # Check that PAM has extra columns
      assert treeEncoder.pam.shape == (4, 6)
      
      # Check the values of those columns in the encoding
      # Expected absolute values for the last two columns
      expectedValues = np.round(np.array([
         [1.0, 0.19565],
         [1.0, 0.19565],
         [0.0, 0.28985],
         [0.0, 0.50000],
         [0.0, 0.31884],
         [0.0, 0.50000]
      ]), 3)
      
      cmpMtx = np.round(np.abs(pMtx[:,-2:]), 3)
      
      assert np.all(np.sort(cmpMtx) == np.sort(expectedValues))
   
   # ..............................   
   def test_tips_missing_matrix_idx(self):
      """
      @summary: Test to check that encoding fails if the matrix indices in the
                   tips do not match the PAM
      """
      treeDict = {
         "name": "0",
         "path":  [0],
         "pathId": 0,
         "length": 0.0,
         "children": [
            {
               "pathId": 1,
               "length": .4,
               "path": [1,0],
               "children": [
                  {
                     "pathId" : 2,
                     "length": .15,
                     "path": [9,5,0],
                     "children": [
                        {
                           "pathId" : 3,
                           "length" : .65,
                           "path": [3,2,1,0],
                           "children": [
                              {
                                 "pathId" : 4,
                                 "length" : .2,
                                 "path" : [4,3,2,1,0],
                              },
                              {
                                 "pathId" : 5,
                                 "length" : .2,
                                 "path" : [5,3,2,1,0],
                                 "mx" : 1
                              }
                           ]
                        },
                        {
                           "pathId" : 6,
                           "length" : .85,
                           "path" : [6,2,1,0],
                           "mx" : 2
                        }
                     ]
                  },
                  {
                     "pathId" : 7,
                     "length" : 1.0,
                     "path" : [7,1,0],
                     "mx" : 3
                  }
               ]
            },
            {
               "pathId" : 8,
               "length": .9,
               "path": [8,0],
               "children": [
                  {
                     "pathId" : 9,
                     "length" : .5,
                     "path" : [9,8,0],
                     "mx" : 4
                  },
                  {
                     "pathId" : 10,
                     "length" : .5,
                     "path" : [10,8,0],
                     "mx" : 5
                  }
               ]
            } 
         ]
      }
      
      pam = np.random.choice(2, 24).reshape(4, 6)
      
      treeEncoder = PhyloEncoding(treeDict, pam)
      
      with self.assertRaises(EncodingException):
         pMtx = treeEncoder.encodePhylogeny()
   
   # ..............................   
   def test_tree_dict_with_branch_lengths(self):
      """
      @summary: This test checks that phylogenetic encoding works properly with
                   a tree with branch lengths
      """
      treeDict = {
         "name": "0",
         "path":  [0],
         "pathId": 0,
         "length": 0.0,
         "children": [
            {
               "pathId": 1,
               "length": .4,
               "path": [1,0],
               "children": [
                  {
                     "pathId" : 2,
                     "length": .15,
                     "path": [9,5,0],
                     "children": [
                        {
                           "pathId" : 3,
                           "length" : .65,
                           "path": [3,2,1,0],
                           "children": [
                              {
                                 "pathId" : 4,
                                 "length" : .2,
                                 "path" : [4,3,2,1,0],
                                 "mx" : 0
                              },
                              {
                                 "pathId" : 5,
                                 "length" : .2,
                                 "path" : [5,3,2,1,0],
                                 "mx" : 1
                              }
                           ]
                        },
                        {
                           "pathId" : 6,
                           "length" : .85,
                           "path" : [6,2,1,0],
                           "mx" : 2
                        }
                     ]
                  },
                  {
                     "pathId" : 7,
                     "length" : 1.0,
                     "path" : [7,1,0],
                     "mx" : 3
                  }
               ]
            },
            {
               "pathId" : 8,
               "length": .9,
               "path": [8,0],
               "children": [
                  {
                     "pathId" : 9,
                     "length" : .5,
                     "path" : [9,8,0],
                     "mx" : 4
                  },
                  {
                     "pathId" : 10,
                     "length" : .5,
                     "path" : [10,8,0],
                     "mx" : 5
                  }
               ]
            } 
         ]
      }
      
      pam = np.random.choice(2, 24).reshape(4, 6)
      
      treeEncoder = PhyloEncoding(treeDict, pam)
      
      pMtx = treeEncoder.encodePhylogeny()
      print pMtx
   
      assert round(np.sum(pMtx), 3) == 0.000
      
      # Check that sum of absolute values is 2*columns
      assert round(np.sum(np.abs(pMtx)), 3) == 2.000 * pMtx.shape[1]

      # Approximate expected result
      #    Absolute values should be equal but order can vary (column-wise)
      #    Rows should be in same order
      approximateExpectedResult = [
         [ 0.000,  0.280,  0.500, -1.000,  0.196],
         [ 0.000,  0.280,  0.500,  1.000,  0.196],
         [ 0.000,  0.439, -1.000,  0.000,  0.290],
         [ 0.000, -1.000,  0.000,  0.000,  0.319],
         [-1.000,  0.000,  0.000,  0.000, -0.500],
         [ 1.000,  0.000,  0.000,  0.000, -0.500]
      ]
      
      # Get a set of the absolute values of both the expected values and the 
      #    actual values and make sure they match
      expectedValues = set([])
      for row in approximateExpectedResult:
         for item in row:
            expectedValues.add(abs(round(item, 3)))
      
      testValues = set([])
      for row in pMtx:
         for item in row:
            testValues.add(abs(round(item, 3)))

      assert testValues == expectedValues
   
   # ..............................   
   def test_tree_dict_without_branch_lengths(self):
      """
      @summary: Test to check that encoding works properly when the tree does
                   not have branch lengths
      """
      treeDict = {
         "name": "0",
         "pathId": 0,
            "children": [
            {
               "pathId": 1,
               "children": [
                  {
                     "pathId" : 2,
                     "children": [
                        {
                           "pathId" : 3,
                           "children": [
                              {
                                 "pathId" : 4,
                                 "mx" : 0
                              },
                              {
                                 "pathId" : 5,
                                 "mx" : 1
                              }
                           ]
                        },
                        {
                           "pathId" : 6,
                           "mx" : 2
                        }
                     ]
                  },
                  {
                     "pathId" : 7,
                     "mx" : 3
                  }
               ]
            },
            {
               "pathId" : 8,
               "children": [
                  {
                     "pathId" : 9,
                     "mx" : 4
                  },
                  {
                     "pathId" : 10,
                     "mx" : 5
                  }
               ]
            } 
         ]
      }
      
      pam = np.random.choice(2, 24).reshape(4, 6)
   
      treeEncoder = PhyloEncoding(treeDict, pam)
      pMtx = treeEncoder.encodePhylogeny()
      print pMtx
   
      assert round(np.sum(pMtx), 3) == 0.000
      
      # Check that sum of absolute values is 2*columns
      assert round(np.sum(np.abs(pMtx)), 3) == 2.000 * pMtx.shape[1]

      # Approximate expected result
      #    Absolute values should be equal but order can vary (column-wise)
      #    Rows should be in same order
      approximateExpectedResult = [
         [-0.125, -0.250, -0.500, -1.000,  0.000],
         [-0.125, -0.250, -0.500,  1.000,  0.000],
         [-0.250, -0.500,  1.000,  0.000,  0.000],
         [-0.500,  1.000,  0.000,  0.000,  0.000],
         [ 0.500,  0.000,  0.000,  0.000, -1.000],
         [ 0.500,  0.000,  0.000,  0.000,  1.000]
       ]
      
      # Get a set of the absolute values of both the expected values and the 
      #    actual values and make sure they match
      expectedValues = set([])
      for row in approximateExpectedResult:
         for item in row:
            expectedValues.add(abs(round(item, 3)))
      
      testValues = set([])
      for row in pMtx:
         for item in row:
            testValues.add(abs(round(item, 3)))

      assert testValues == expectedValues
   
   # ..............................
   def _consistent_results_helper(self, fn):
      """
      @summary: This is a helper function to make sure that results are 
                   consistent.  It is used for both branch lengths method and
                   non-branch lengths method
      """
      lmt = LmTree.fromFile(os.path.join(fn))
      
      numTips = len(lmt.tips)
      numSites = 10 # Just picked a random number
      numIterations = 10 # Number of times to check that results are the same
      
      # Add matrix indices (just set the matrix index to be the label
      # Note: We started tip labels at 1
      pamMetadata = dict([(toUnicode(str(i+1)), i) for i in xrange(numTips)])
      
      lmt.addMatrixIndices(pamMetadata)
      
      # Create a PAM
      pam = np.random.choice(2, (numTips*numSites)).reshape(numSites, numTips)

      treeEncoder = PhyloEncoding(lmt, pam)
      
      cmpMtx = treeEncoder.encodePhylogeny()
      
      # Check that the sum of the matrix is zero
      assert round(np.sum(cmpMtx), 3) == 0.000
      
      # Check that sum of every column is zero
      assert np.all(np.round(np.sum(cmpMtx, axis=0), 3) == 0.000)
      
      # Check that the sum of the absolute values is 2 * columns
      assert round(np.sum(np.abs(cmpMtx)), 3) == 2.000 * cmpMtx.shape[1]
      
      # Get the absolute values and sort.  We'll test that results are the same
      sortedMtx = np.sort(np.abs(cmpMtx))
      
      for i in xrange(numIterations):
         # NOTE: Probably only need to check that sorted values are the same 
         #          and that the columns sum to zero.  The other tests are 
         #          redundant but may thrown an error more quickly
         pMtx = treeEncoder.encodePhylogeny()
         
         # Check that sum is zero
         assert round(np.sum(pMtx), 3) == 0.000
         
         # Check that the sum of the absolute values is 2*columns
         assert round(np.sum(np.abs(pMtx)), 3) == 2.000 * pMtx.shape[1]
         
         # Check that column sums are zero
         assert np.all(np.round(np.sum(pMtx, axis=0), 3) == 0.000)
         
         # Get the absolute value and sort, then check if the values are the
         #    same as our compare matrix
         sortedPMtx = np.sort(np.abs(pMtx))
         assert np.all(sortedMtx == sortedPMtx)
      
# .............................................................................
class TestBioGeoEncoding(unittest.TestCase):
   """
   @summary: This class tests the BioGeoEncoding class
   @note: Accuracy for an encoding is determined by assessing if the absolute
             value of the sum of each column is what is expected.  Absolute
             value is used because the sign assigned to a feature used for
             encoding the hypothesis can be either 1 or -1 and is arbitrary.
             However, any site that should be 'A' will be 'A' (if correct), no
             matter if 'A' = 1 or -1.  Thus, the absolute value of the sum will
             be consistent.
   """
   # ..............................   
   def setUp(self):
      """
      @summary: Set up the test by initializing the encoder
      """
      gridDLoc = os.path.join(SHAPEGRIDS_PATH, "TenthDegree_Grid_FL-2462.shp")
      self.bgEncoder = BioGeoEncoding(gridDLoc)
   
   # ..............................   
   def tearDown(self):
      """
      @summary: Tear down the test by setting the encoder to None
      """
      self.bgEncoder = None
   
   # ..............................
   def test_consistent(self):
      """
      @summary: This test checks that the results produced are consistent.  Not
                   necessarily identical each time as the positive and negative
                   value for any hypothesis can vary, but the absolute value
                   of the sums of each column should be the same.
      """
      mergedLayerDL = os.path.join(BIO_GEO_HYPOTHESES_PATH, 
                                   "MergedContrasts_Florida.shp")
      self.bgEncoder.addLayers(mergedLayerDL, eventField="event")
      
      lyrList = []
      for lyrFn in ["ApalachicolaRiver.shp",
                    "GulfAtlantic.shp",
                    "Pliocene.shp"]:
         lyrList.append(os.path.join(BIO_GEO_HYPOTHESES_PATH, lyrFn))
      self.bgEncoder.addLayers(lyrList)
      
      # Create the expected result matrix
      testMerged = np.load(os.path.join(OUTPUT_BIO_GEO_ENCODINGS_PATH,
                                        "test_mergedFA.npy"))
      testCollection = np.load(os.path.join(OUTPUT_BIO_GEO_ENCODINGS_PATH,
                                            "test_collectionFA.npy"))
      testEncoding = np.concatenate([testMerged, testCollection], axis=1)

      # Encode the hypotheses 10 times and check that they are what is expected
      for i in xrange(10):
         bg1 = self.bgEncoder.encodeHypotheses()
         assert np.all(abs(np.sum(testEncoding, axis=0)) == abs(np.sum(bg1, 
                                                                       axis=0)))
   
   # ..............................
   def test_every_feature_a_hypothesis(self):
      """
      @summary: Test that a layer defined as having every feature as a separate 
                   hypothesis is encoded successfully
      @todo: Check output 
      """
      layerDL = os.path.join(BIO_GEO_HYPOTHESES_PATH, "threeFeatures.shp")
      # Treat every feature as a hypothesis since we don't provide event field
      self.bgEncoder.addLayers(layerDL, eventField='id')
      bg1 = self.bgEncoder.encodeHypotheses()

   # ..............................
   def test_layer_does_not_exist_fail(self):
      """
      @summary: Test that a file not found (IOError) exception is thrown when
                   trying to encode a hypothesis layer that does not exist
      """
      missingLayer = os.path.join(BIO_GEO_HYPOTHESES_PATH, 
                                  "nonExistingLayer.shp")
      self.bgEncoder.addLayers(missingLayer, eventField="event")
      with self.assertRaises(IOError):
         bg1 = self.bgEncoder.encodeHypotheses()
   
   # ..............................
   def test_merged(self):
      """
      @summary: Test that the BioGeo encoding works for a single, merged, 
                   hypothesis shapefile
      """
      mergedLayerDL = os.path.join(BIO_GEO_HYPOTHESES_PATH, 
                                   "MergedContrasts_Florida.shp")
      self.bgEncoder.addLayers(mergedLayerDL, eventField="event")
      bg1 = self.bgEncoder.encodeHypotheses()
      testMerged = np.load(os.path.join(OUTPUT_BIO_GEO_ENCODINGS_PATH,
                                        "test_mergedFA.npy"))
      assert np.all(abs(np.sum(testMerged, axis=0)) == abs(np.sum(bg1, axis=0)))
   
   # ..............................   
   def test_multiple_layers(self):
      """
      @summary: Tests that bio geo encoding works properly when adding multiple
                   layers of differing methods
      """
      mergedLayerDL = os.path.join(BIO_GEO_HYPOTHESES_PATH, 
                                   "MergedContrasts_Florida.shp")
      self.bgEncoder.addLayers(mergedLayerDL, eventField="event")
      
      lyrList = []
      for lyrFn in ["ApalachicolaRiver.shp",
                    "GulfAtlantic.shp",
                    "Pliocene.shp"]:
         lyrList.append(os.path.join(BIO_GEO_HYPOTHESES_PATH, lyrFn))
      for lyrFn in lyrList:
         print os.path.exists(lyrFn)
      self.bgEncoder.addLayers(lyrList)
      
      
      bg1 = self.bgEncoder.encodeHypotheses()
      testMerged = np.load(os.path.join(OUTPUT_BIO_GEO_ENCODINGS_PATH,
                                        "test_mergedFA.npy"))
      testCollection = np.load(os.path.join(OUTPUT_BIO_GEO_ENCODINGS_PATH,
                                            "test_collectionFA.npy"))
      testEncoding = np.concatenate([testMerged, testCollection], axis=1)

      assert np.all(abs(np.sum(testEncoding, axis=0)) == abs(np.sum(bg1, axis=0)))
   
   # ..............................
   def test_missing_eventfield_column(self):
      """
      @summary: Test that an EncodingException is thrown when trying to encode
                   a layer with an event field that does not exist
      """
      layerDL = os.path.join(BIO_GEO_HYPOTHESES_PATH, 
                             "MergedContrasts_Florida.shp")
      # Add with event field instead of mutually exclusive
      self.bgEncoder.addLayers(layerDL, eventField='bad_field')
      with self.assertRaises(EncodingException):
         bg1 = self.bgEncoder.encodeHypotheses()
      
   # ..............................
   def test_one_hypothesis(self):
      """
      @summary: Test that encoding is successful when the layer includes one
                   hypothesis
      """
      layerDL = os.path.join(BIO_GEO_HYPOTHESES_PATH, "oneFeature.shp")
      self.bgEncoder.addLayers(layerDL, eventField='event')
      bg1 = self.bgEncoder.encodeHypotheses()
   
   # ..............................   
   def test_layer_too_many_features_fail(self):
      """
      @summary: Test that BioGeo encoding fails when adding a layer with too
                   many features.
      @note: Too many features is defined as more than two for a particular 
                event
      """
      layerDL = os.path.join(BIO_GEO_HYPOTHESES_PATH, "threeFeatures.shp")
      # Add with event field instead of mutually exclusive
      self.bgEncoder.addLayers(layerDL, eventField='event')
      with self.assertRaises(EncodingException):
         bg1 = self.bgEncoder.encodeHypotheses()
   
   # ..............................   
   def test_one_layer_two_features(self):
      """
      @summary: Test that encoding is successful when the layer has two features
      """
      layerDL = os.path.join(BIO_GEO_HYPOTHESES_PATH, "GulfAtlantic.shp")
      self.bgEncoder.addLayers(layerDL, eventField='event')
      bg1 = self.bgEncoder.encodeHypotheses()
   
   # ..............................
   def test_shapegrid_does_not_exist_fail(self):
      """
      @summary: Tests that an IOError is thrown if the shapegrid file does not
                   exist
      """
      with self.assertRaises(IOError):
         bgEncoder2 = BioGeoEncoding(os.path.join(SHAPEGRIDS_PATH, 
                                                  "missing.shp"))
   
   # ..............................
   def test_zero_features_fail(self):
      """
      @summary: Test that encoding fails when there are zero features for a 
                   layer
      """
      layerDL = os.path.join(BIO_GEO_HYPOTHESES_PATH, "zeroFeatures.shp")
      self.bgEncoder.addLayers(layerDL, eventField='event')
      with self.assertRaises(EncodingException):
         bg1 = self.bgEncoder.encodeHypotheses()
   
# .............................................................................
def getTestSuites():
   """
   @summary: Gets the test suites for the LmCommon.common.systemMetadata module.
   @return: A list of test suites
   """
   loader = unittest.TestLoader()
   testSuites = []
   testSuites.append(loader.loadTestsFromTestCase(TestBioGeoEncoding))
   testSuites.append(loader.loadTestsFromTestCase(TestPhyloEncoding))
   return testSuites

# ============================================================================
# = Main                                                                     =
# ============================================================================

if __name__ == '__main__':
   #tests
   logging.basicConfig(level = logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
