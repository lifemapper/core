"""
@summary: Lifemapper web test suite
"""
import glob
import os
import sys

from LmCommon.tools.testing.testSuite import LMTestSuite
from LmWebServer.tools.testing.lmTests.webTest import LMWebTestBuilder

from LmCommon.common.lmXml import deserialize, fromstring
from LmCommon.tools.testing.lmTestFactory import LMTestFactory

# .............................................................................
class LMWebTestSuite(LMTestSuite):
   """
   @summary: This is the base LMTestSuite.  It can be subclassed if necessary.
   """
   name = "Lifemapper Web Testing Suite"
   description = "Performs a suite of Lifemapper web tests"
   version = "1.0"
   testBuilders = [LMWebTestBuilder]

   def addTestsFromDirectory(self, dirPath):
      """
      @summary: Iterates through the test files to get tests to run
      """
      testFactory = LMTestFactory(self.testBuilders)
      for fn in glob.glob(os.path.join(dirPath, "*")):
         with open(fn) as inF:
            testStr = inF.read() 
         testObjs = deserialize(fromstring(testStr))
         for t in testObjs.lmTest:
            self.tests.append(testFactory.getTest(t))
   

# .............................................................................
if __name__ == "__main__":
   if os.geteuid() == 0:
      print "Error: This script should not be run as root"
      sys.exit(2)
   testSuite = LMWebTestSuite()
   testSuite.runTests()
   