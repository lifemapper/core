"""
@summary: This module contains a class to submit jobs to a cluster.  This is a 
             very similar process for multiple cluster operating systems / 
             schedulers and the specifics can be handled by modifying the 
             commands in the configuration file.
@author: CJ Grady
@version: 3.0
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

from LmCommon.common.lmXml import deserialize, parse

from LmCompute.common.localconstants import JOB_REQUEST_PATH
from LmCompute.common.log import SubmitterLogger
from LmCompute.jobs.submitters.base import JobSubmitter

# .............................................................................
class ClusterSubmitter(JobSubmitter):
   """
   @summary: This class submits jobs to a cluster.
   """
   # .............................
   def __init__(self, capacity, numJobsCmd, submitCmd):
      self.capacity = capacity
      self.numJobsCmd = numJobsCmd
      self.submitCmd = submitCmd
      self.log = SubmitterLogger()
      
   # .............................
   def getNumberOfEmptySlots(self):
      """
      @summary: This gets the number of jobs that can be submitted by 
                   subtracting the number of running jobs from the capacity of 
                   the submitter.
      """
      numAvail = self.capacity - self.getNumberOfRunningJobs()
      self.log.info("Number of empty slots: %s" % numAvail)
      return numAvail
   
   # .............................
   def getNumberOfRunningJobs(self):
      """
      @summary: Query the job scheduler to find how many jobs are currently 
                   running
      """
      res = os.popen(self.numJobsCmd)
      resp = res.read()
      if len(resp) == 0:
         num = 0
      else:
         num = int(resp)
      self.log.info("%s running jobs" % num)
      return num
   
   # .............................
   def submitJobs(self, jobXmlFns):
      """
      @summary: This function will submit the jobs specified
      @param jobXmlFns: A list of xml file names with job content
      """
      for xmlFn in jobXmlFns:

         try:
            el = parse(xmlFn).getroot()
            j = deserialize(el)
            
            fn = os.path.join(JOB_REQUEST_PATH, "%s-%s.xml" % (j.processType, j.jobId))
            
            self.log.info("Submitting job: %s - %s" % (j.processType, j.jobId))
            
            # Move file
            os.rename(xmlFn, fn)
            
            cmd = self.fillInSubmissionCommand(j, fn)
                     
            res = os.popen(cmd)
            res.close()
            j = None
         except Exception, e:
            self.log.error("Could not submit job %s: %s" % (xmlFn, str(e)))
   
   # .............................
   def updateCapacity(self, newCapacity):
      """
      @summary: Updates the capacity of the job submitter
      """
      self.capacity = newCapacity
