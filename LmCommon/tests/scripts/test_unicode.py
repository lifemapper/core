# coding=utf-8
"""
@summary: This module contains unit tests for the LmCommon.common.unicode module
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
import unittest

from LmCommon.common.unicode import fromUnicode, toUnicode

# .............................................................................
# Module-wide list of tested items from LmCommon.common.unicode
# As tests are written for each import, the import should be added to this list
testedItems = ["fromUnicode", "toUnicode"]

# .............................................................................
class TestImportTestCoverage(unittest.TestCase):
   """
   @summary: Test class to ensure that all imports are tested
   """
   # ...................................
   def test_coverage(self):
      """
      @summary: This function attempts to determine if all of the imports 
                   available in unicode have been tested.  It does this by
                   looking at the testedItems list and seeing if the import is
                   included.  If there are any import that exist in the 
                   LmCommon.common.unicode module that have not been included
                   in the testedItems list, this will throw an error.  
      @note: Constants with a leading '_' will be ignored.
      """
      global testedItems
      
      import LmCommon.common.unicode as allImports
      for item in dir(allImports):
         if not item.startswith('_') and item not in ['ENCODING']:
            self.assertIn(item, testedItems)
            
# .............................................................................
class TestFromUnicode(unittest.TestCase):
   """
   @summary: This class performs tests on the fromUnicode function
   """
   # ...................................
   def test_ascii_only(self):
      """
      @summary: Tests that the fromUnicode function works with ascii characters
      """
      fromUnicode(u'test string')
      fromUnicode(u'test string', encoding='ascii')
      fromUnicode(u'test string', encoding='utf-16')
      
   # ...................................
   def test_utf8_characters(self):
      """
      @summary: Tests the the fromUnicode function works with uf8 characters
      """
      testString = u'ŰƬòsae'
      fromUnicode(testString)
      fromUnicode(testString, encoding='utf8')
      fromUnicode(testString, encoding='utf16')

# .............................................................................
class TestToUnicode(unittest.TestCase):
   """
   @summary: This class performs tests on the toUnicode function
   """
   # ...................................
   def test_non_strings(self):
      """
      @summary: Tests that the toUnicode function works with non-strings.  In 
                   each case, the item should be converted to a unicode string
      """
      self.assertIsInstance(toUnicode(1), unicode)
      self.assertIsInstance(toUnicode(None), unicode)
      self.assertIsInstance(toUnicode(3.2), unicode)
      self.assertIsInstance(toUnicode({'test' : 'val'}), unicode)
      self.assertIsInstance(toUnicode([1, 'abd', 8.2332]), unicode)
      
   # ...................................
   def test_strings(self):
      """
      @summary: Tests the the toUnicode function works with strings
      """
      self.assertIsInstance(toUnicode(u'ŰƬòsae'), unicode)
      self.assertIsInstance(toUnicode(u'some string'), unicode)
      self.assertIsInstance(toUnicode(str('non-unicode on entry')), unicode)
      self.assertIsInstance(toUnicode("some other ascii string"), unicode)
   
# .............................................................................
def getTestSuites():
   """
   @summary: Gets the test suites for the LmCommon.common.unicode module.
   @return: A list of test suites
   """
   loader = unittest.TestLoader()
   testSuites = []
   testSuites.append(loader.loadTestsFromTestCase(TestImportTestCoverage))
   testSuites.append(loader.loadTestsFromTestCase(TestFromUnicode))
   testSuites.append(loader.loadTestsFromTestCase(TestToUnicode))
   return testSuites

# ============================================================================
# = Main                                                                     =
# ============================================================================

if __name__ == '__main__':
   #tests
   logging.basicConfig(level = logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
