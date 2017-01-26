"""
@summary: This module is used to test the MCPA module
@author: CJ Grady
@version: 4.0.0
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
@note: This currently is not a great tests but tests for basic functionality
@todo: Thoroughly test
"""
import logging
import numpy as np
import unittest

from LmCompute.plugins.multi.mcpa.mcpa import standardizeMatrix, mcpaRun

# .............................................................................
class TestMCPA(unittest.TestCase):
   """
   @summary: This is a test class for MCPA
   """
   # ..............................
   def test_valid(self):
      """
      @summary: Test with data that we expect will operate correctly and just 
                   checks that it does not fail
      """
      # Incidence matrix (PAM; I in literature)
      I = np.array([[1, 0, 0, 1, 0, 0],
                    [0, 0, 1, 1, 0, 0],
                    [1, 0, 0, 1, 0, 1],
                    [0, 0, 1, 1, 0, 1],
                    [0, 1, 0, 1, 0, 1],
                    [0, 0, 0, 0, 1, 0],
                    [1, 0, 0, 0, 1, 0],
                    [0, 1, 0, 0, 1, 0]])
   
      # Encoded phylogeny (P in literature)
      P = np.array([[-1.   , -0.5  , -0.25 , -0.125,  0.   ],
                    [ 1.   , -0.5  , -0.25 , -0.125,  0.   ],
                    [ 0.   ,  1.   , -0.5  , -0.25 ,  0.   ],
                    [ 0.   ,  0.   ,  1.   , -0.5  ,  0.   ],
                    [ 0.   ,  0.   ,  0.   ,  0.5  , -1.   ],
                    [ 0.   ,  0.   ,  0.   ,  0.5  ,  1.   ]])
      
      # Environment data (E - maps to GRIM)
      E = np.array([[1.3,  13.0, 100.0], 
                    [.78,  12.4, 121.0], 
                    [.85,  1.2,  99.0], 
                    [1.0,  0.98, 11.2], 
                    [4.8,  0.45,  21.23], 
                    [3.89, 0.99,  21.11], 
                    [3.97, 1.2,  12.01], 
                    [3.23, 1.0,  10.12] ])

      totalSum = np.sum(I)
      sumSites = np.sum(I, axis=1)
      sumSpecies = np.sum(I, axis=0)
      numSites, numSpecies = I.shape
      siteWeights = np.diag(sumSites)
      speciesWeights = np.diag(sumSpecies)
   
      stdP = standardizeMatrix(P, speciesWeights)
      
      assert np.round(np.sum(stdP), 3) == 0.000
      
      stdE = standardizeMatrix(E, siteWeights)
      
      rSq, fG, sp, fSP = mcpaRun(I, E, P)
      ## random calculations
      numPermute = 9999
   
      # MCPA CJ
      obsAdjRsq, obsSP, fGlobalP, fSPPVals = mcpa2.mcpa(I, P, E, numPermutations=numPermute)
      
   
# .............................................................................
def getTestSuites():
   """
   @summary: Gets the test suites for the LmCommon.common.systemMetadata module.
   @return: A list of test suites
   """
   loader = unittest.TestLoader()
   testSuites = []
   testSuites.append(loader.loadTestsFromTestCase(TestMCPA))
   return testSuites

# ============================================================================
# = Main                                                                     =
# ============================================================================

if __name__ == '__main__':
   #tests
   logging.basicConfig(level = logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
