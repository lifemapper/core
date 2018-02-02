"""
@summary: Module containing Lifemapper compute test classes
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
from LmCommon.common.lmXml import deserialize, fromstring
from LmCompute.environment.testEnv import TestEnv
from LmCompute.jobs.runners.factory import getJobRunnerForJob
from LmCommon.common.unicode import fromUnicode, toUnicode

from LmCommon.tools.testing.lmTest import LMTest, LMTestBuilder

# .............................................................................
class LMComputeTest(LMTest):
   """
   @summary: Class for LmCompute tests
   """
   # ...............................
   def __init__(self, name, description, testLevel, jobXml, expectedStatus):
      """
      @summary: Constructor for compute test
      @param name: The name of this test
      @param description: A description of this test
      @param testLevel: What level this test runs at
      @param jobXml: The XML string representing the job request
      @param expectedStatus: The expected job status at the end of the test
      """
      LMTest.__init__(self, name, description, testLevel)
      self.jobXml = jobXml
      self.job = None
      self.expectedStatus = int(expectedStatus)
      self.resultStatus = None

   # ...............................
   def runTest(self):
      """
      @summary: Method called to run the test
      """
      try:
         self.job = deserialize(fromstring(fromUnicode(toUnicode(self.jobXml))))
         env = TestEnv(self.job)
         jr = getJobRunnerForJob(self.job, env)
         jr.run()
         self.resultStatus = int(jr.status)
      except Exception, e:
         self.message = str(e)
         print self.message
         self.status = 2

   # ...............................
   def evaluateTest(self):
      """
      @summary: Evaluate the results of the test.  Commands used to run system
                   tests should produce exit values of 0 for success, 1 for 
                   warning, and 2 for error
      """
      if self.resultStatus is not None:
         if self.resultStatus == self.expectedStatus:
            self.status = 0
            self.message = "%s completed correctly" % self.name
         else:
            self.status = 2
            self.message = "%s did not complete with expected status: (%s != %s" % (
                            self.name, self.resultStatus, self.expectedStatus)
      else:
         pass # This will be taken care of when running the test
         #self.status = 2
         #self.message = "%s did not produce a result status"

# .............................................................................
class LMComputeTestBuilder(LMTestBuilder):
   """
   @summary: Builder for creating compute tests
   """
   name = "compute"
   # ...........................
   def buildTests(self, testObj):
      """
      @summary: Build a a LMComputeTest object
      @param testObj: Deserialized XML with compute test information
      """
      name = testObj.name
      description = testObj.description
      testLevel = int(testObj.testLevel)
      jobXml = testObj.jobXml
      expectedStatus = testObj.expectedStatus
      
      return [LMComputeTest(name, description, testLevel, jobXml, 
                            expectedStatus)]
   