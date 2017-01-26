"""
@summary: Lifemapper web test suite
"""
import os
import sys

from LmCommon.tools.testing.testSuite import LMTestSuite
from LmWebServer.tools.testing.envMethods.server import LmServerEnv
from LmWebServer.tools.testing.lmTests.webTest import LMWebTestBuilder
from LmWebServer.tools.testing.lmTests.webServiceTest import LMWebServiceTestBuilder

# .............................................................................
class LMWebTestSuite(LMTestSuite):
   """
   @summary: This is the base LMTestSuite.  It can be subclassed if necessary.
   """
   name = "Lifemapper Web Testing Suite"
   description = "Performs a suite of Lifemapper web tests"
   version = "1.0"
   testBuilders = [LMWebTestBuilder, LMWebServiceTestBuilder]
   
   # ...........................
   def addArguments(self):
      LMTestSuite.addArguments(self)
      self.parser.add_argument("userId", type=str, help="The user to use for credentialed requests")
      self.parser.add_argument("pwd", type=str, help="The user's password")
      
      #TODO: Add argument for other environment types
      
   # ...........................
   def parseArguments(self):
      LMTestSuite.parseArguments(self)
      self.userId = self.args.userId
      self.pwd = self.args.pwd
      #TODO: Add ability to use other environments
      self.env = LmServerEnv(userId=self.userId)
      
      self.kwargs['userId'] = self.userId
      self.kwargs['pwd'] = self.pwd
      self.kwargs['env'] = self.env


# .............................................................................
if __name__ == "__main__":
   if os.geteuid() == 0:
      print "Error: This script should not be run as root"
      sys.exit(2)
   testSuite = LMWebTestSuite()
   testSuite.runTests()
   