# coding=utf-8
"""
@summary: Module containing unit tests for the 
             LmCommon.common.subprocessManager module.
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
import time
import unittest

from LmCommon.common.lmXml import deserialize, fromstring
from LmBackend.common.subprocessManager import MAX_CONCURRENT_PROCESSES, \
                                SubprocessManager, VariableContainer, WAIT_TIME

# .............................................................................
# Module-wide list of tested items from LmCommon.common.subprocessManager
# As tests are written for each import, the import should be added to this list
testedItems = ['MAX_CONCURRENT_PROCESSES', 'SubprocessManager', 
               'VariableContainer', 'WAIT_TIME']

# .............................................................................
class TestImportTestCoverage(unittest.TestCase):
   """
   @summary: Test class to ensure that all imports are tested
   """
   # ...................................
   def test_coverage(self):
      """
      @summary: This function attempts to determine if all of the imports 
                   available in subprocessManager have been tested.  It does 
                   this by looking at the testedItems list and seeing if the 
                   import is included.  If there are any imports that exist in 
                   the LmCommon.common.subprocessManager module have not been 
                   included in the testedItems list, this will throw an error. 
      @note: Constants with a leading '_' will be ignored.
      """
      global testedItems
      
      import LmCommon.common.subprocessManager as allImports
      for item in dir(allImports):
         if not item.startswith('_') and item not in ['Popen', 'serialize', 
                                                      'sleep', 'tostring']:
            self.assertIn(item, testedItems)

# .............................................................................
class TestConstants(unittest.TestCase):
   """
   @summary: Tests the constants of LmCommon.common.subprocessManager
   """
   # ...................................
   def test_max_concurrent_processes(self):
      """
      @summary: Tests that the MAX_CONCURRENT_PROCESSES constant is an integer 
                   greater than 0
      """
      self.assertIsInstance(MAX_CONCURRENT_PROCESSES, int)
      self.assertGreater(MAX_CONCURRENT_PROCESSES, 0)

   # ...................................
   def test_wait_time(self):
      """
      @summary: Tests that the WAIT_TIME constant is an integer greater than 0
      """
      self.assertIsInstance(WAIT_TIME, int)
      self.assertGreater(WAIT_TIME, 0)

# .............................................................................
class TestSubprocessManager(unittest.TestCase):
   """
   @summary: Tests the SubprocessManager class
   """
   # ...................................
   def test_constructor_default_parameters(self):
      """
      @summary: Tests that the SubprocessManager constructor works with the 
                   default parameters
      """
      spMgr = SubprocessManager()

   # ...................................
   def test_constructor_with_parameters(self):
      """
      @summary: Tests that the SubprocessManager constructor works when 
                   parameters are supplied.
      """
      cl = [
            'echo 3',
            'uptime'
           ]
      spMgr = SubprocessManager(commandList=cl, maxConcurrent=3)
   
   # ...................................
   def test_add_process_commands(self):
      """
      @summary: Tests that process commands can be successfully added to an 
                   existing SubprocessManager object.
      """
      spMgr = SubprocessManager()
      numExistingCommands = len(spMgr.procs)
      addCommands = ['echo test', 'uptime', 'clear']
      spMgr.addProcessCommands(addCommands)
      self.assertEqual(len(spMgr.procs), numExistingCommands + len(addCommands))
   
   # ...................................
   def test_get_number_of_running_processes(self):
      """
      @summary: Tests that SubprocessManager.getNumberOfRunningProcesses
                   returns an active count of the number of running processes.
      """
      spMgr = SubprocessManager()
      startNumProc = spMgr.getNumberOfRunningProcesses()
      # Run 2 additional processes
      spMgr.launchProcess("sleep 30")
      spMgr.launchProcess("sleep 30")
      self.assertEqual(spMgr.getNumberOfRunningProcesses(), startNumProc+2)
   
   # ...................................
   def test_launch_process(self):
      spMgr = SubprocessManager()
      startNumProc = spMgr.getNumberOfRunningProcesses()
      # Run 2 additional processes
      spMgr.launchProcess("sleep 30")
      spMgr.launchProcess("sleep 30")
      self.assertEqual(spMgr.getNumberOfRunningProcesses(), startNumProc+2)
   
   # ...................................
   def test_run_processes(self):
      """
      @summary: Tests that SubprocessManager.runProcesses runs all of the
                   processes in the queue.
      """
      cmds = ['sleep 30', 'sleep 30', 'sleep 20', 'echo 3']
      spMgr = SubprocessManager(commandList=cmds)
      self.assertEqual(len(spMgr.procs), len(cmds))
      self.assertGreater(len(spMgr.procs), 0)
      spMgr.runProcesses()
      self.assertEqual(len(spMgr.procs), 0)
   
   # ...................................
   def test_wait(self):
      """
      @summary: Tests the SubprocessManager.wait function
      """
      spMgr = SubprocessManager()
      startTime1 = time.time()
      spMgr.wait()
      endTime1 = time.time()
   
      startTime2 = time.time()
      spMgr.wait(10)
      endTime2 = time.time()
      
      # Test that we waited long enough
      self.assertGreaterEqual(endTime1 - startTime1, WAIT_TIME)
      self.assertGreaterEqual(endTime2 - startTime2, 10)
   
# .............................................................................
class TestVariableContainer(unittest.TestCase):
   """
   @summary: Tests the VariableContainer class
   """
   # ...................................
   def test_atomic_values(self):
      """
      @summary: Tests that a VariableContainer works correctly with atomic 
                   objects
      """
      # Integer
      intVal = 3
      vc1 = VariableContainer(intVal)
      self.assertEqual(intVal, int(deserialize(fromstring(str(vc1))).values))

      # Float
      floatVal = 3
      vc2 = VariableContainer(floatVal)
      self.assertEqual(floatVal, float(deserialize(fromstring(str(vc2))).values))

      # String
      strVal = 'some string'
      vc3 = VariableContainer(strVal)
      self.assertEqual(strVal, deserialize(fromstring(str(vc3))).values)

      # Unicode 1
      uni1Val = u'some string'
      vc4 = VariableContainer(uni1Val)
      self.assertEqual(uni1Val, deserialize(fromstring(unicode(vc4))).values)

      # Unicode 2
      uni2Val = u'ŰƬòsae'
      vc5 = VariableContainer(uni2Val)
      self.assertEqual(uni2Val, deserialize(fromstring(unicode(vc5))).values)

# .............................................................................
def getTestSuites():
   """
   @summary: Gets the test suites for the LmCommon.common.subprocessManager 
                module.
   @return: A list of test suites
   """
   loader = unittest.TestLoader()
   testSuites = []
   testSuites.append(loader.loadTestsFromTestCase(TestImportTestCoverage))
   testSuites.append(loader.loadTestsFromTestCase(TestConstants))
   testSuites.append(loader.loadTestsFromTestCase(TestSubprocessManager))
   testSuites.append(loader.loadTestsFromTestCase(TestVariableContainer))
   return testSuites

# ============================================================================
# = Main                                                                     =
# ============================================================================

if __name__ == '__main__':
   #tests
   logging.basicConfig(level = logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
