"""
@summary: This script runs Lifemapper web tests
@author: CJ Grady
@contact: cjgrady [at] ku [dot] edu
@organization: Lifemapper (http://lifemapper.org)
@version: 4.0.0
@status: alpha

@license: Copyright (C) 2017, University of Kansas Center for Research

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
import argparse
import os
import sys
import unittest

from LmTest.webTestsLite.api.v2 import (envLayer, globalPam, gridset, layer, 
                                        matrix, occurrence, ogc, scenario, 
                                        scenPackage, sdmProject, shapegrid, 
                                        snippet, speciesHint, tree)
from LmTest.webTestsLite.common.userUnitTest import UserTestCase

# .............................................................................
def get_test_classes():
   """
   @summary: Return a list of the available test classes in this module.  This 
                should be returned to a test suite builder that will 
                parameterize tests appropriately
   """
   tcs = []
   tcs.extend(envLayer.get_test_classes())
   tcs.extend(globalPam.get_test_classes())
   tcs.extend(gridset.get_test_classes())
   tcs.extend(layer.get_test_classes())
   tcs.extend(matrix.get_test_classes())
   tcs.extend(occurrence.get_test_classes())
   tcs.extend(ogc.get_test_classes())
   tcs.extend(scenario.get_test_classes())
   tcs.extend(scenPackage.get_test_classes())
   tcs.extend(sdmProject.get_test_classes())
   tcs.extend(shapegrid.get_test_classes())
   tcs.extend(snippet.get_test_classes())
   tcs.extend(speciesHint.get_test_classes())
   tcs.extend(tree.get_test_classes())
   return tcs

# .............................................................................
def get_test_suite(userId=None, pwd=None):
   """
   @summary: Get test suite for this module.  Always get public tests and get
                user tests if user information is provided
   @param userId: The id of the user to use for tests
   @param pwd: The password of the specified user
   """
   suite = unittest.TestSuite()
   testClasses = get_test_classes()
   
   for tc in testClasses:
      suite.addTest(UserTestCase.parameterize(tc))
   
   if userId is not None:
      for tc in testClasses:
         suite.addTest(UserTestCase.parameterize(tc, userId=userId, pwd=pwd))
      
   return suite

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Run web service tests')
   parser.add_argument('-u', '--user', type=str, 
                 help='If provided, run tests for this user (and anonymous)' )
   parser.add_argument('-p', '--pwd', type=str, help='Password for user')
   
   if os.geteuid() == 0:
      print "Error: This script should not be run as root"
      sys.exit(2)
   args = parser.parse_args()
   suite = get_test_suite(userId=args.user, pwd=args.pwd)
   unittest.TextTestRunner(verbosity=2).run(suite)

   