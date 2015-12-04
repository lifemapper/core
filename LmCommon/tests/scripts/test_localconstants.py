"""
@summary: This module contains unit tests for the 
             LmCommon.common.localconstants module
@author: CJ Grady
@version: 1.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
import re
import unittest

from LmCommon.common.localconstants import ARCHIVE_USER, ENCODING, \
                                           OGC_SERVICE_URL, WEBSERVICES_ROOT

# .............................................................................
# Module-wide list of tested items from LmCommon.common.lmconstants
# As tests are written for each constant, the constant should be added to this 
#    list
testedItems = ['ARCHIVE_USER', 'ENCODING', 'OGC_SERVICE_URL', 'WEBSERVICES_ROOT']

# .............................................................................
def isValidUrl(url):
   regex = re.compile(
      r'^(?:http|ftp)s?://' # http:// or https://
      r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domain...
      r'localhost|' # localhost...
      r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|' # ...or ipv4
      r'\[?[A-F0-9]*:[A-F0-9:]+\]?)' # ...or ipv6
      r'(?::\d+)?' # optional port
      r'(?:/?|[/?]\S+)$', re.IGNORECASE)
   return regex.match(url)

# .............................................................................
class TestConstantTestCoverage(unittest.TestCase):
   """
   @summary: Test class to ensure that all constants are tested
   """
   # ...................................
   def test_constant_test_coverage(self):
      """
      @summary: This function attempts to determine if all of the constants 
                   available in localconstants have been tested.  It does this 
                   by looking at the testedItems list and seeing if the constant 
                   is included.  If there are any constants that
                   exist in the LmCommon.common.localconstants module that have
                   not been imported directly, this will throw an error.  
      @note: Constants with a leading '_' will be ignored.
      """
      global testedItems
      
      import LmCommon.common.localconstants as allConstants
      for item in dir(allConstants):
         if not item.startswith('_') and item not in ['Config']:
            self.assertIn(item, testedItems)

# .............................................................................
class TestLocalConstants(unittest.TestCase):
   """
   @summary: Test class for local constants
   """
   # ...................................
   def test_encoding(self):
      """
      @summary: Tests that ENCODING is a string and that this Python 
                   installation recognizes it as a valid encoding.
      """
      self.assertIsInstance(ENCODING, str)
      
      # Test to see if Python can use it
      u'Test'.encode(ENCODING)
      
   # ...................................
   def test_ogc_service_url(self):
      """
      @summary: Tests that OGC_SERVICE_URL is a string and a valid URL.
      """
      self.assertIsInstance(OGC_SERVICE_URL, str)
      self.assertTrue(isValidUrl(OGC_SERVICE_URL))
      
   # ...................................
   def test_WEBSERVICES_ROOT(self):
      """
      @summary: Tests that WEBSERVICES_ROOT is a string and a valid URL.
      """
      self.assertIsInstance(WEBSERVICES_ROOT, str)
      self.assertTrue(isValidUrl(OGC_SERVICE_URL))

# .............................................................................
class TestUserConstants(unittest.TestCase):
   """
   @summary: Class to test user constants
   """
   # ...................................
   def test_archive_user(self):
      """
      @summary: Tests that ARCHIVE_USER is a string
      """
      self.assertIsInstance(ARCHIVE_USER, str)
   
# .............................................................................
def getTestSuites():
   """
   @summary: Gets the test suites for the 
                common.jobs.processes.sdm.omExperiment.omRequest module
   @return: A list of test suites
   """
   loader = unittest.TestLoader()
   testSuites = []
   testSuites.append(loader.loadTestsFromTestCase(TestConstantTestCoverage))
   testSuites.append(loader.loadTestsFromTestCase(TestLocalConstants))
   testSuites.append(loader.loadTestsFromTestCase(TestUserConstants))
   return testSuites

# ============================================================================
# = Main                                                                     =
# ============================================================================

if __name__ == '__main__':
   #tests
   logging.basicConfig(level = logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
