"""
@summary: Base Lifemapper testing suite
@author: CJ Grady
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
import glob
import logging
import os
import sys
import time

from LmCommon.common.log import LmLogger
from LmCommon.common.lmXml import deserialize, fromstring
from LmCommon.tools.testing.lmTest import LMSystemTestBuilder
from LmCommon.tools.testing.lmTestFactory import LMTestFactory

# .............................................................................
class LMTestSuite(object):
   """
   @summary: This is the base LMTestSuite.  It can be subclassed if necessary.
   """
   name = "Lifemapper Testing Suite"
   description = "Performs a suite of common Lifemapper tests"
   version = "1.0"
   testBuilders = [LMSystemTestBuilder]

   # ...........................
   def __init__(self):
      self.tests = []
      self.successful = []
      self.warnings = []
      self.failed = []
      self.parser = argparse.ArgumentParser(prog=self.name, 
                                            description=self.description, 
                                            version=self.version)
      self._addBaseArguments()
      self.addArguments()
      self.kwargs = {}
      
      # Run argparser parser
      self.args = self.parser.parse_args()
      
      self._parseBaseArguments()
      self.parseArguments()
      
   # ...........................
   def _addBaseArguments(self):
      """
      @summary: Add some basic arguments.  These should probably be present in
                   any subclass, but possibly not.  If these should not be 
                   included, override this method in a subclass.  Additional
                   arguments should be added in addArguments
      """
      # Log file / standard out
      self.parser.add_argument("-o", "--output", 
         help="""File to store log output from test suite.  Ignored if \
output directory is specified for individual logs.  If omitted, use standard \
out""")
      
      # Error file / standard out
      self.parser.add_argument("-e", "--error", 
         help="""File to store error output from test suite.  Ignored \
if output directory is specified for individual logs.  If omitted, use \
standard error""")
      
      # Log directory
      self.parser.add_argument("-d", "--outDir", 
         help="""Write individual log files to this directory.  \
If omitted, uses output path / std out""")
      
      # Pedantic mode
      self.parser.add_argument("--pedantic", 
                 help="Run tests in pedantic mode", action="store_true")

      # Stop on first failure
      self.parser.add_argument("--quickStop", 
                     help="Stop on first failure", action="store_true")

      # Summarize
      self.parser.add_argument("--summarize", 
                  help="Generate a summary report", action="store_true")

      # Test level
      self.parser.add_argument("--testLevel", type=int,
         help="Run tests less than or equal to this level (default: 10)", 
         default=10)
      
      # Test directories
      self.parser.add_argument("-t", "--testDir", nargs="*",
                               help="Add tests from this directory")
      
   # ...........................
   def _parseBaseArguments(self):
      """
      @summary: Parse the basic arguments from the base class. This should 
                   probably not be modified in a subclass, but there may be a
                   situation where it should be.  If it is modified, these 
                   parameter values may need to be initialized.  Additional 
                   arguments should be parsed in the parseArguments method
      """
      self.output = self.args.output
      self.error = self.args.error
      self.outDir = self.args.outDir
      self.pedantic = self.args.pedantic
      self.quickStop = self.args.quickStop
      self.summarize = self.args.summarize
      self.testLevel = self.args.testLevel
      
      # Process test directories
      for tDir in self.args.testDir:
         self.addTestsFromDirectory(tDir)
      
   # ...........................
   def addArguments(self):
      """
      @summary: Override this method in a subclass to add any extra arguments   
      """
      pass
   
   # ...........................
   def parseArguments(self):
      """
      @summary: Override this method in a subclass to parse any extra arguments
      """
      pass
      
   # ...........................
   def addTestsFromFile(self, filepath, testFactory=None):
      """
      @summary: Adds all of the tests found in a test configuration file
      @param filepath: The path to the file to find tests in
      """
      if testFactory is None:
         testFactory = LMTestFactory(self.testBuilders, **self.kwargs)
      
      if os.path.exists(filepath) and os.path.isfile(filepath):
         with open(filepath) as inF:
            testStr = inF.read()
         testObj = deserialize(fromstring(testStr))
         self.tests.extend(testFactory.getTests(testObj))
      else:
         raise Exception, "%s does not exist or is not a file" % filepath
      
   # ...........................
   def addTestsFromDirectory(self, dirPath):
      """
      @summary: Iterates through the test files to get tests to run
      """
      testFactory = LMTestFactory(self.testBuilders)
      if os.path.exists(dirPath) and os.path.isdir(dirPath):
         for fn in glob.glob(os.path.join(dirPath, "*")):
            self.addTestsFromFile(fn, testFactory=testFactory)
   
   # ...........................
   def getOutputLogger(self, logName):
      """
      @summary: Get a logger object for a test's output
      """
      if self.outDir is not None:
         log = LmLogger(logName, logging.DEBUG)
         log._addFileHandler(os.path.join(self.outDir, 
                                          "{logName}.log".format(logName)))
      else:
         log = LmLogger(self.name, logging.DEBUG)
         if self.output is not None:
            log._addFileHandler(self.output)
         else:
            log._addConsoleHandler()
      return log
   
   # ...........................
   def getErrorLogger(self, logName):
      """
      @summary: Get a logger object for a test's erroroutput
      """
      if self.outDir is not None:
         log = LmLogger(logName, logging.DEBUG)
         log._addFileHandler(os.path.join(self.outDir, 
                                          "{logName}.log".format(logName)))
      else:
         log = LmLogger(self.name, logging.DEBUG)
         if self.output is not None:
            log._addFileHandler(self.error)
         else:
            log._addConsoleHandler()
      return log
   
   # ...........................
   def getTime(self):
      """
      @summary: Using a function to get the time allows us to change the timing
                   mechanism later easily
      """
      return time.time()
   
   # ...........................
   def runTests(self):
      """
      @summary: Run all of the tests in the suite with appropriate test level
      """
      try:
         for t in self.tests:
            # Check test level
            if t.testLevel <= self.testLevel:
               outputLog = self.getOutputLogger(t.name)
               errorLog = self.getErrorLogger(t.name)
   
               t.setLogger(outputLog)
               t.setErrorLogger(errorLog)
               
               startTime = self.getTime()
               t.runTest()
               endTime = self.getTime()
               
               t.evaluateTest()
               
               t.cleanup()
   
               # Success
               if t.status == 0:
                  self.successful.append((t.name, startTime, endTime))
               # Pedantic failure
               elif t.status == 1 and not self.pedantic:
                  self.warnings.append((t.name, t.message))
               # Failure
               else:
                  self.failed.append((t.name, t.message))
               
               # Quick stop, on error or pedantic error
               if self.quickStop and (
                     t.status == 2 or (self.pedantic and t.status == 1)):
                  break
      except Exception, e:
         print "An exception occurred, aborting"
         print str(e)
         sys.exit(2)
         
      
      if self.summarize:
         self.summary()
         
      if len(self.failed) > 0:
         sys.exit(2)
      else:
         sys.exit(0)

   # ...........................
   def summary(self):
      """
      @summary: Summarize the results of the test suite
      """
      log = self.getOutputLogger("summary")
      log.info("Successful Tests")
      log.info("---------------------")
      for tName, tStart, tEnd in self.successful:
         log.info("{testName} in {timeDiff} seconds".format(testName=tName, 
                                                     timeDiff=(tEnd - tStart)))
      log.info("")
      log.info("Warnings")
      log.info("---------------------")
      for tName, msg in self.warnings:
         log.info("{testName}, {msg}".format(testName=tName, msg=msg))
      log.info("")

      log.info("Failures")
      log.info("---------------------")
      for tName, msg in self.failed:
         log.info("{testName}, {msg}".format(testName=tName, msg=msg))
      
   
# .............................................................................
if __name__ == "__main__":
   if os.geteuid() == 0:
      print "Error: This script should not be run as root"
      sys.exit(2)
   testSuite = LMTestSuite()
   testSuite.runTests()
   