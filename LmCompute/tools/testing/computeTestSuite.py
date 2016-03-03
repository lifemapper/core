"""
@summary: Lifemapper compute test suite
@author: CJ Grady
@version: 1.0
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
import os
import sys

from LmCommon.tools.testing.testSuite import LMTestSuite
from LmCompute.tools.testing.computeTest import LMComputeTestBuilder

from LmBackend.common.urllib2mock import (addUrlMappingFromFile, 
                                          installMockOpener)
from LmCompute.common.localconstants import SAMPLE_DATA_PATH

MOCK_URLS_FN = os.path.join(SAMPLE_DATA_PATH, "jobData.csv")

# .............................................................................
class LMComputeTestSuite(LMTestSuite):
   """
   @summary: This is Lifemapper compute test suite.
   """
   name = "Lifemapper Compute Testing Suite"
   description = "Performs a suite of Lifemapper compute tests"
   version = "1.0"
   testBuilders = [LMComputeTestBuilder]
   
   # ...........................
   def addArguments(self):
      LMTestSuite.addArguments(self)
      self.parser.add_argument("-sdp", "--sampleData",
                               help="""Path to sample data directory""")
      self.parser.add_argument("-murls", "--mockUrlFilePath",
                         help="""Path to CSV file with mock URL mappings""")
      
   # ...........................
   def parseArguments(self):
      LMTestSuite.parseArguments(self)
      if self.args.sampleData is not None:
         self.sdp = self.args.sampleData
      else:
         self.sdp = SAMPLE_DATA_PATH
      
      if self.args.mockUrlFilePath is not None:
         self.mufp = self.args.mockUrlFilePath
      else:
         self.mufp = MOCK_URLS_FN

      # Set up mock HTTP opener
      # This is used to return local files instead of trying to fulfill invalid 
      #   HTTP requests
      print("Installing mock opener")
      installMockOpener()
      addUrlMappingFromFile(self.mufp, basePath=self.sdp)


# .............................................................................
if __name__ == "__main__":
   if os.geteuid() == 0:
      print "Error: This script should not be run as root"
      sys.exit(2)
   testSuite = LMComputeTestSuite()
   testSuite.runTests()
   
