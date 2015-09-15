"""
@summary: This script runs all of the available job tests to make sure an 
             LmCompute instance is working properly
@author: CJ Grady
@version: 3.0
@status: beta
"""
import glob
import os
import time
import traceback
import urllib2

from LmBackend.common.urllib2mock import addUrlMappingFromFile, installMockOpener
from LmCommon.common.lmXml import deserialize, fromstring, parse
from LmCompute.common.localconstants import SAMPLE_JOBS_PATH, SAMPLE_DATA_PATH
from LmCompute.environment.testEnv import TestEnv
from LmCompute.jobs.runners.factory import getJobRunnerForJob
from LmCommon.common.unicode import fromUnicode, toUnicode

MOCK_URLS_FN = os.path.join(SAMPLE_DATA_PATH, "jobData.csv")


# .............................................................................
class JobTest(object):
   """
   @summary: Each instance of this class represents a particular LmCompute job
                test.
   """
   # ............................
   def __init__(self, jobXml, expectedStatus, name, description, enabled):
      """
      @summary: Constructor
      @param jobXml: The XML string representing the job request
      @param expectedStatus: The expected job status at the end of the test
      @param name: The name of this test
      @param description: A description of what this test is testing
      @param enabled: Indicates if this test is enabled or not
      """
      self.jobXml = jobXml
      self.job = None
      self.expectedStatus = int(expectedStatus)
      self.name = name
      self.description = description
      self.enabled = enabled.lower() == 'true'
      self.resultStatus = None
      
   # ............................
   def runTest(self):
      """
      @summary: Runs the job and checks that the output job status matches what
                   is expected
      """
      if self.enabled:
         self.job = deserialize(fromstring(fromUnicode(toUnicode(self.jobXml))))
         env = TestEnv(self.job)
         jr = getJobRunnerForJob(self.job, env)
         jr.run()
         self.resultStatus = jr.status
         if jr.status == self.expectedStatus:
            return True
         else:
            return False
      else:
         print("Test: %s is disabled" % self.name)
         return None

# .............................................................................
def getEnabledTestsInDirectory():
   """
   @summary: Gets the tests out of the specified directory
   """
   searchString = os.path.join(SAMPLE_JOBS_PATH, "*andom*")
   jobTests = []
   
   for fn in glob.iglob(searchString):
      try:
         obj = deserialize(parse(fn).getroot())
         jobTests.append(JobTest(obj.jobXml, obj.expectedStatus, obj.name, obj.description, obj.enabled))
      except Exception, e:
         print("Could not add test for %s" % fn)
         print(traceback.format_exc())
   return jobTests   

# .............................................................................
def runTests(jobTests):
   """
   @summary: The function will run all of the tests provided
   @param jobTests: A list of JobTest objects
   """
   passedTests = []
   failedTests = []
   disabledTests = []
   
   startTime = time.clock()
   for jTest in jobTests:
      try:
         t1 = time.clock()
         print("--------------------------------------------------------------")
         print("Starting test: %s, expected result: %s" % (jTest.name, jTest.expectedStatus))
         print(jTest.description)
         print("\n")
         
         testSuccess = jTest.runTest()
         
         t2 = time.clock()
         print("Test time: %s" % (t2-t1))
         if testSuccess is None:
            disabledTests.append(jTest.name)
         elif testSuccess:
            passedTests.append(jTest.name)
         else:
            msg = "Expected status: %s, does not match result status: %s" % (jTest.expectedStatus, jTest.resultStatus)
            failedTests.append((jTest.name, msg))
      except Exception, e:
         msg = traceback.format_exc()
         failedTests.append((jTest.name, msg))
         print("Test failed: %s" % str(e))
         print(traceback.format_exc())
      print("--------------------------------------------------------------")
   endTime = time.clock()
   print("Total time: %s" % (endTime - startTime))
   return (passedTests, failedTests, disabledTests)

# .............................................................................
if __name__ == "__main__":
   
   # Set up mock HTTP opener
   # This is used to return local files instead of trying to fulfill invalid 
   #   HTTP requests
   print("Installing mock opener")
   installMockOpener()
   addUrlMappingFromFile(MOCK_URLS_FN, basePath=SAMPLE_DATA_PATH)
   
   jobTests = getEnabledTestsInDirectory()
   passed, failed, disabled = runTests(jobTests)
   
   print("\n-----------------------------------------------")
   print("-                   Summary                   -")
   print("-----------------------------------------------")
   print("Successful Tests:")
   for t in passed:
      print("   %s" % t)
   print("\nFailed Tests:")
   for t, msg in failed:
      print("   %s - %s" % (t, msg))
   print("\nDisabled Tests:")
   for t in disabled:
      print("   %s" % t)
      