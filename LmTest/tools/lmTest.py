"""
@summary: Module containing Lifemapper test classes
@author: CJ Grady
@version: 1.0
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
import subprocess
from time import sleep

# .............................................................................
class LMTest(object):
   """
   @summary: Base class for Lifemapper tests
   """
   # ...............................
   def __init__(self, name, description, testLevel):
      self.name = name
      self.description = description
      self.testLevel = testLevel
      self.status = None
      self.message = None
      
   # ...............................
   def runTest(self):
      """
      @summary: Method called to run the test
      """
      pass
      
   # ...............................
   def cleanup(self):
      """
      @summary: Method to perform any necessary cleanup
      """
      pass
   
   # ...............................
   def setLogger(self, log):
      """
      @summary: Sets the standard output logger for the test
      """
      self.log = log
   
   # ...............................
   def setErrorLogger(self, log):
      """
      @summary: Sets the error logger for the test
      """
      self.eLog = log
   
   # ...............................
   def evaluateTest(self):
      """
      @summary: The evaluateTest method should determine if the test completed
                   successfully
      """
      self.status = 2
      self.message = "Base class should not be instantiated"
   
# .............................................................................
class LMTestBuilder(object):
   """
   @summary: The LMTestBuilder class is a base class for all test builders.
                The test builder subclasses will take in a deserialized XML 
                object as input and build a LMTest object
   """
   name = "BASE"
   
   # ...............................
   def __init__(self, **kwargs):
      self.kwargs = dict(kwargs)
   
   # ...............................
   def buildTests(self, testObj):
      name = testObj.name
      description = testObj.description
      testLevel = testObj.testLevel
      return [LMTest(name, description, testLevel)]
   
# .............................................................................   
class LMSystemTest(LMTest):
   """
   @summary: LM System tests check something on the system, like resource usage 
                or file permissions.
   """
   # ...............................
   def __init__(self, name, description, testLevel, command, cleanupCmd=None, 
                      successMessage="Success", warningMessage="Warning", 
                      failMessage="Failure"):
      self.name = name
      self.description = description
      self.testLevel = testLevel
      self.status = None
      self.message = None
      self.command = command
      self.cleanupCommand = cleanupCmd
      self.successMsg = successMessage
      self.warnMsg = warningMessage
      self.failMsg = failMessage
      self.log = None
      self.eLog = None
      
   # ...............................
   def runTest(self):
      """
      @summary: Method called to run the test
      """
      self.p = subprocess.Popen(self.command, shell=True)
      
      while self.p.poll() is None:
         sleep(3)
      
   # ...............................
   def cleanup(self):
      """
      @summary: Method to perform any necessary cleanup
      """
      if self.cleanupCommand is not None:
         cleanupProc = subprocess.Popen(self.cleanupCommand, shell=True)
         while cleanupProc.poll() is None:
            sleep(3)
   
   # ...............................
   def evaluateTest(self):
      """
      @summary: Evaluate the results of the test.  Commands used to run system
                   tests should produce exit values of 0 for success, 1 for 
                   warning, and 2 for error
      """
      self.status = int(self.p.poll())
      if self.status == 0:
         self.message = self.successMsg
      elif self.status == 1:
         self.message = self.warnMsg
      else:
         self.message = self.failMsg

# .............................................................................
class LMSystemTestBuilder(LMTestBuilder):
   """
   @summary: Builder for creating system tests for things like checking 
                directory permissions and memory usage
   """
   name = "system"
   # ...........................
   def buildTests(self, testObj):
      """
      @summary: Build a LMSystemTest objects
      @param testObj: Deserialized XML with system test information
      """
      name = testObj.name
      description = testObj.description
      testLevel = int(testObj.testLevel)
      cmd = testObj.command
      try:
         cleanupCmd = testObj.cleanupCommand
      except:
         cleanupCmd = None
         
      try:
         sMsg = testObj.passMsg
      except:
         sMsg = "Success"
         
      try:
         wMsg = testObj.warnMsg
      except:
         wMsg = "Warning"
         
      try:
         fMsg = testObj.failMsg
      except:
         fMsg = "Fail"
      
      return [LMSystemTest(name, description, testLevel, cmd, 
                          cleanupCmd=cleanupCmd, successMessage=sMsg, 
                          warningMessage=wMsg, failMessage=fMsg)]
# Client library test (remote test?)
# Unit test
# Local test
# Compute test (script test?)

# Plugins like compute?


