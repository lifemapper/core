"""
@summary: This module contains unit tests for the LmCommon.common.config module
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
from ConfigParser import NoOptionError, NoSectionError
import logging
import os
import unittest

from LmCommon.common.config import Config, CONFIG_FILENAME

testConfigFn = "../../data/testConfig.ini"

# .............................................................................
# Module-wide list of tested items from LmCommon.common.config
# As tests are written for each import, the import should be added to this list
testedItems = ['Config', 'CONFIG_FILENAME', 'singleton']

# .............................................................................
class TestConfigFilename(unittest.TestCase):
   """
   @summary: Test class for the CONFIG_FILENAME variable
   """
   # ...................................
   def test_config_filename(self):
      """
      @summary: This test ensures that CONFIG_FILENAME is an existing file path 
                   on this system
      """
      self.assertTrue(os.path.exists(CONFIG_FILENAME))

# .............................................................................
class TestImportTestCoverage(unittest.TestCase):
   """
   @summary: Test class to ensure that all imports are tested
   """
   # ...................................
   def test_coverage(self):
      """
      @summary: This function attempts to determine if all of the imports 
                   available in config have been tested.  It does this by
                   looking at the testedItems list and seeing if the import is 
                   included.  If there are any imports that exist in the 
                   LmCommon.common.config module that have not been included
                   in the testedItems list, this will throw an error.
      @note: Constants with a leading '_' will be ignored.
      """
      global testedItems
      
      import LmCommon.common.config as allImports
      for item in dir(allImports):
         if not item.startswith('_') and item not in ['ConfigParser', 'hashlib', 'os']:
            self.assertIn(item, testedItems)

# .............................................................................
class TestConfig(unittest.TestCase):
   """
   @summary: Tests the Config class
   """
   # ...................................
   def setUp(self):
      self.config = Config(fn=testConfigFn)
      
   # ...................................
   def test_singleton(self):
      """
      @summary: This test attempts to create a second instance of the Config 
                   class.  If this class is truly a singleton, it will return
                   the same object.
      """
      config2 = Config(fn=testConfigFn)
      self.assertIs(self.config, config2)
      
   # ...................................
   def test_get_valid(self):
      """
      @summary: This test attempts to retrieve a valid item from the config 
                   file.  The expected response is that a string is returned.
      """
      self.assertIsInstance(self.config.get('test get', 'valid'), str)
   
   # ...................................
   def test_get_missing(self):
      """
      @summary: This test attempts to retrieve an item from the config file 
                   that does not exist.  The expected response is for a 
                   'ConfigParser.NoOptionError' to be raised.
      """
      self.assertRaises(NoOptionError, self.config.get, 'test get', 'missing')
   
   # ...................................
   def test_get_boolean_valid(self):
      """
      @summary: This test attempts to retrieve a valid boolean value from the 
                   config file.  The expected response is that a boolean will 
                   be returned.
      """
      self.assertIsInstance(
                     self.config.getboolean('test get boolean', 'valid'), bool)
   
   # ...................................
   def test_get_boolean_missing(self):
      """
      @summary: This test attempts to retrieve a boolean value for an option 
                   that does not exist.  The expected response is that a 
                   ConfigParser.NoOptionError will be raised.
      """
      self.assertRaises(NoOptionError, self.config.getboolean, 
                                          'test get boolean', 'missing')
   
   # ...................................
   def test_get_boolean_missing_section(self):
      """
      @summary: This test attempts to retrieve a boolean value for an option in
                   a section that does not exist.  The expected response is for
                   a ConfigParser.NoSectionError to be raised.
      """
      self.assertRaises(NoSectionError, self.config.get, 
                                           'missing section', 'some value')

   # ...................................
   def test_get_boolean_nonboolean(self):
      """
      @summary: This test attempts to retrieve a boolean value for a 
                   non-boolean option in the configuration file.  The expected
                   response is that a ValueError will be raised.
      """
      self.assertRaises(ValueError, self.config.getboolean, 
                                       'test get boolean', 'nonboolean')
   
   # ...................................
   def test_get_float_valid(self):
      """
      @summary: This test attempts to retrieve a float for a valid option.  The
                   expected response is a float object will be returned.
      """
      self.assertIsInstance(self.config.getfloat('test get float', 'valid'), 
                            float)
   
   # ...................................
   def test_get_float_missing(self):
      """
      @summary: This test attempts to retrieve a float for a value that does 
                   not exist in the configuration file.  The expected response 
                   is for a ConfigParser.NoOptionError to be raised.
      """
      self.assertRaises(NoOptionError, self.config.getfloat, 
                                          'test get float', 'missing')
   
   # ...................................
   def test_get_float_missing_section(self):
      """
      @summary: This test attempts to retrieve a float value for an option in
                   a section that does not exist.  The expected response is for
                   a ConfigParser.NoSectionError to be raised.
      """
      self.assertRaises(NoSectionError, self.config.get, 
                                           'missing section', 'some value')

   # ...................................
   def test_get_float_nonfloat(self):
      """
      @summary: This test attempts to retrieve a float for a value in the 
                   configuration file that is not a float.  The expected 
                   response is for a ValueError to be raised.
      """
      self.assertRaises(ValueError, self.config.getfloat, 
                                       'test get float', 'nonfloat')
   
   # ...................................
   def test_get_int_valid(self):
      """
      @summary: This test attempts to retrieve an integer for a valid option in
                   the configuration file.  The expected response is for an 
                   integer object to be returned.
      """
      self.assertIsInstance(self.config.getint('test get int', 'valid'), int)
   
   # ...................................
   def test_get_int_missing(self):
      """
      @summary: This test attempts to retrieve an integer for an option that 
                   does not exist in the configuration file.  The expected 
                   response is that a ConfigParser.NoOptionError will be 
                   raised.
      """
      self.assertRaises(NoOptionError, self.config.getint, 
                                          'test get int', 'missing')
   
   # ...................................
   def test_get_int_missing_section(self):
      """
      @summary: This test attempts to retrieve a integer value for an option in
                   a section that does not exist.  The expected response is for
                   a ConfigParser.NoSectionError to be raised.
      """
      self.assertRaises(NoSectionError, self.config.get, 
                                           'missing section', 'some value')

   # ...................................
   def test_get_int_nonint(self):
      """
      @summary: This test attempts to retrieve an integer for an option in the
                   configuration file that is not an integer.  The expected 
                   response is that a ValueError will be raised.
      """
      self.assertRaises(ValueError, self.config.getint, 
                                       'test get int', 'nonint')
   
   # ...................................
   def test_get_list_valid(self):
      """
      @summary: This test attempts to retrieve a list for a valid option in the
                   configuration file.  The expected response is for a list to
                   be returned. 
      """
      self.assertIsInstance(self.config.getlist('test get list', 'valid'), 
                            list)
   
   # ...................................
   def test_get_list_missing(self):
      """
      @summary: This test attempts to retrieve a list for an option that is not
                   present in the configuration file.  The expected response is
                   for a ConfigParser.NoOptionError to be raised.
      """
      self.assertRaises(NoOptionError, self.config.getlist, 'test get list', 'missing')
   
   # ...................................
   def test_get_list_missing_section(self):
      """
      @summary: This test attempts to retrieve a list value for an option in
                   a section that does not exist.  The expected response is for
                   a ConfigParser.NoSectionError to be raised.
      """
      self.assertRaises(NoSectionError, self.config.get, 
                                           'missing section', 'some value')

   # ...................................
   def test_config_fn_is_none(self):
      """
      @summary: This test attempts to instantiate a Config object with None for
                   the config file name.  The expected response is for a 
                   ValueError to be raised.
      """
      self.assertRaises(ValueError, Config, fn=None)

   # ...................................
   def test_config_fn_is_empty_string(self):
      """
      @summary: This test attempts to instantiate a Config object with an empty
                   string for the config file name.  The expected response is
                   for a ValueError to be raised.
      """
      self.assertRaises(ValueError, Config, fn='')

   # ...................................
   def test_get_multiline(self):
      """
      @summary: This test attempts to retrieve a valid multi-line option from
                   the config file.  The expected response is for a string to 
                   be returned with the same value as the compare string.
      """
      cmpStr = """This is a longer string
that is continued in the next line"""
      testStr = self.config.get('test get', 'multiline')
      self.assertIsInstance(testStr, str)
      self.assertEqual(cmpStr, testStr)
   
   # ...................................
   def test_get_include_local_reference(self):
      """
      @summary: This test attempts to retrieve a valid option from the config
                   file that makes a reference to another option in the same
                   section.  The expected response is for a string to be 
                   returned with the same value as the compare string.
      """
      cmpStr = "This is a local reference test"
      testStr = self.config.get('test get', 'local reference')
      self.assertIsInstance(testStr, str)
      self.assertEqual(cmpStr, testStr)
   
   # ...................................
   def test_get_include_default_reference(self):
      """
      @summary: This test attempts to retrieve a valid option from the config 
                   file that makes a reference to another option in the DEFAULT 
                   section of the config file.  The expected response is for a 
                   string to be returned with the same value as the compare 
                   string.
      """
      cmpStr = "This is a reference test in default"
      testStr = self.config.get('test get', 'default reference')
      self.assertEqual(cmpStr, testStr)
   
   # ...................................
   def test_missing_section(self):
      """
      @summary: This tests attempts to reference an option in a section that 
                   does not exist in the configuration file.  The expected 
                   response is for a ConfigParser.NoSectionError to be raised.
      """
      self.assertRaises(NoSectionError, self.config.get, 
                                           'missing section', 'some value')
   
# .............................................................................
def getTestSuites():
   """
   @summary: Gets the test suites for the 
                common.jobs.processes.sdm.omExperiment.omRequest module
   @return: A list of test suites
   """
   testSuites = []
   testSuites.append(unittest.TestLoader().loadTestsFromTestCase(TestImportTestCoverage))
   testSuites.append(unittest.TestLoader().loadTestsFromTestCase(TestConfig))
   return testSuites

# ============================================================================
# = Main                                                                     =
# ============================================================================

if __name__ == '__main__':
   #tests
   logging.basicConfig(level = logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
