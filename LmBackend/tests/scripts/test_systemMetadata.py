"""
@summary: Module containing unit tests for the LmCommon.common.systemMetadata
             module.
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
import os
import unittest

from LmBackend.common.systemMetadata import CPU_INFO_FN, \
                                           getSystemConfigurationDictionary, \
                                           MEM_INFO_FN
import LmBackend.common.systemMetadata as SysMeta # Used for monkey patching

# .............................................................................
# Module-wide list of tested items from LmCommon.common.systemMetadata
# As tests are written for each import, the import should be added to this list
testedItems = ['CPU_INFO_FN', 'getSystemConfigurationDictionary', 'MEM_INFO_FN']

# .............................................................................
class TestImportTestCoverage(unittest.TestCase):
   """
   @summary: Test class to ensure that all imports are tested
   """
   # ...................................
   def test_coverage(self):
      """
      @summary: This function attempts to determine if all of the imports 
                   available in systemMetadata have been tested.  It does this 
                   by looking at the testedItems list and seeing if the import 
                   is included.  If there are any imports that exist in the 
                   LmCommon.common.systemMetadata module that have not been 
                   included in the testedItems list, this will throw an error. 
      @note: Constants with a leading '_' will be ignored.
      """
      global testedItems
      
      import LmCommon.common.systemMetadata as allImports
      for item in dir(allImports):
         if not item.startswith('_') and item not in ['OrderedDict', 
                                                      'platform', 'socket']:
            self.assertIn(item, testedItems)

# .............................................................................
class TestConstants(unittest.TestCase):
   """
   @summary: Tests the constants in LmCommon.common.systemMetadata
   """
   # ...................................
   def test_cpu_info_fn(self):
      """
      @summary: Test to make sure that CPU_INFO_FN is a valid file path
      """
      self.assertIsInstance(CPU_INFO_FN, str)
      self.assertTrue(os.path.exists(CPU_INFO_FN))
   
   # ...................................
   def test_mem_info_fn(self):
      """
      @summary: Test to make sure that MEM_INFO_FN is a valid file path
      """
      self.assertIsInstance(MEM_INFO_FN, str)
      self.assertTrue(os.path.exists(MEM_INFO_FN))
   
# .............................................................................
class TestGetSystemConfigurationDictionary(unittest.TestCase):
   """
   @summary: Test class for getSystemConfigurationDictionary
   """
   
   # ...................................
   def test_machine_name(self):
      """
      @summary: Test that the 'machine name' key is returned is a string
      """
      self.assertIsInstance(getSystemConfigurationDictionary()['machine name'], 
                            str)
   
   # ...................................
   def test_machine_ip(self):
      """
      @summary: Test that the 'machine ip' key is returned is a string
      """
      self.assertIsInstance(getSystemConfigurationDictionary()['machine ip'],  
                            str)
   
   # ...................................
   def test_architecture(self):
      """
      @summary: Test that the 'architecture' key is returned is a string
      """
      self.assertIsInstance(getSystemConfigurationDictionary()['architecture'], 
                            str)
   
   # ...................................
   def test_os(self):
      """
      @summary: Test that the 'os' key is returned is a string
      """
      self.assertIsInstance(getSystemConfigurationDictionary()['os'], str)
   
   # ...................................
   def test_total_memory(self):
      """
      @summary: Test that the 'total memory' key is returned is a string
      """
      self.assertIsInstance(getSystemConfigurationDictionary()['total memory'], 
                            str)
   
   # ...................................
   def test_cpus(self):
      """
      @summary: Test that the 'cpus' key is returned is a string
      """
      self.assertIsInstance(getSystemConfigurationDictionary()['cpus'], str)
   
   # ...................................
   def test_python_version(self):
      """
      @summary: Test that the 'python version' key is returned is a string
      """
      self.assertIsInstance(
               getSystemConfigurationDictionary()['python version'], str)
   
   # ...................................
   def test_linux_version(self):
      """
      @summary: Test that the 'linux version' key is returned is a string
      """
      self.assertIsInstance(
               getSystemConfigurationDictionary()['linux version'], str)
   
# .............................................................................
class TestMonkeyPatches(unittest.TestCase):
   """
   @summary: Tests the constants in LmCommon.common.systemMetadata
   """
   # ...................................
   def setUp(self):
      """
      @summary: Since we are only temporarily monkey patching the module we 
                   need to store the original constants so they can be restored
      """
      self.origCpuInfo = SysMeta.CPU_INFO_FN
      self.origMemInfo = SysMeta.MEM_INFO_FN
      
   # ...................................
   def tearDown(self):
      """
      @summary: Restore the original constants
      """
      SysMeta.CPU_INFO_FN = self.origCpuInfo
      SysMeta.MEM_INFO_FN = self.origMemInfo
      
   # ...................................
   def test_bad_cpu_info_fn(self):
      """
      @summary: Tests that an IOError is raised when the CPU_INFO_FN constant 
                   does not match the system file
      """
      SysMeta.CPU_INFO_FN = '/someBadFilePath'
      self.assertRaises(IOError, getSystemConfigurationDictionary)
      
   # ...................................
   def test_bad_mem_info_fn(self):
      """
      @summary: Tests that an IOError is raised when the MEM_INFO_FN constant 
                   does not match the system file
      """
      SysMeta.MEM_INFO_FN = '/someBadFilePath'
      self.assertRaises(IOError, getSystemConfigurationDictionary)
      
# .............................................................................
def getTestSuites():
   """
   @summary: Gets the test suites for the LmCommon.common.systemMetadata module.
   @return: A list of test suites
   """
   loader = unittest.TestLoader()
   testSuites = []
   testSuites.append(loader.loadTestsFromTestCase(TestConstants))
   testSuites.append(loader.loadTestsFromTestCase(TestGetSystemConfigurationDictionary))
   testSuites.append(loader.loadTestsFromTestCase(TestImportTestCoverage))
   testSuites.append(loader.loadTestsFromTestCase(TestMonkeyPatches))
   return testSuites

# ============================================================================
# = Main                                                                     =
# ============================================================================

if __name__ == '__main__':
   #tests
   logging.basicConfig(level = logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
