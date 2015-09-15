"""
@summary: This module contains a factory method that will determine which type 
             of job runner to use by using introspection to determine which is 
             appropriate for the job specified by the xml file
@author: CJ Grady
@version: 3.0.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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

from LmCommon.common.lmconstants import JobStatus
from LmCommon.common.lmXml import deserialize, fromstring
from LmCommon.common.unicode import fromUnicode, toUnicode

from LmCompute.common.jobTypes import JOB_TYPES
from LmCompute.common.lmObj import LmException

from LmCompute.environment.controlledPushEnv import ControlledPushEnv
from LmCompute.environment.localEnv import LocalEnv
from LmCompute.environment.testEnv import TestEnv

# .............................................................................
def getJobRunnerForJob(job, env):
   """
   @summary: Gets the appropriate job runner for a job
   @param job: The deserialized job object to use as input
   @param env: The environment methods to use when running this job
   """
   jr = JOB_TYPES[int(job.processType)]['constructor'](job, env)
   return jr

# .............................................................................
def runJobFromXmlFilename(jobXmlFn, envClass, removeJobFile=True):
   """
   @summary: Get the appropriate job runner and run it based on the job 
                information specified in the xml file specified by jobXmlFn
   @param jobXmlFn: The name of the xml file containing job information
   @param envClass: The constructor method for the environment to use.  It will
                       be supplied with a job object to construct
   """
   if not os.path.exists(jobXmlFn):
      raise LmException(JobStatus.IO_NOT_FOUND, 
                        "The job request file: %s, was not found" % jobXmlFn)
   else:
      job = deserialize(fromstring(open(jobXmlFn).read()))
      
      # Create an environment object for the given job
      env = envClass(job)
      
      jr = getJobRunnerForJob(job, env)
      jr.run()
      if removeJobFile:
         os.remove(jobXmlFn)

# .............................................................................
if __name__ == "__main__":
   
   usage = "Usage: python factory.py [test] jobXmlFile [jobXmlFile*]"
   
   if len(sys.argv) > 1:
      i = 1
      # Look for test mode
      if sys.argv[1].lower() == "test":
         # Test mode
         envClass = TestEnv
         # Move to next entry
         i = i+1
         
         # Check to see if there is more in list
         if len(sys.argv) <= 2:
            # Fail with usage message about test mode
            print(usage)
            sys.exit()
      else:
         envClass = LocalEnv
         #envClass = ControlledPushEnv

      # For each file name specified
      for fn in sys.argv[i:]:
         try:
            runJobFromXmlFilename(fn, envClass)
         except LmException, e:
            print(fromUnicode(toUnicode(e)))
            raise e
         except Exception, e:
            print("An unhandled exception occurred: %s" % fromUnicode(toUnicode(e)))
            raise e
   else:
      print(usage)
      sys.exit()
