"""
@summary: This is a job submitter subclass that will run jobs on a local machine
@author: CJ Grady
@version: 1.0
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
import traceback
from LmCommon.common.lmXml import deserialize, parse

from LmCompute.common.localconstants import JOB_REQUEST_DIR
from LmCompute.jobs.submitters.base import JobSubmitter

from LmBackend.common.subprocessManager import SubprocessManager

class LocalSubmitter(JobSubmitter):
   """
   @summary: This is the JobSubmitter base class.  It defines the methods that
                should be implemented in the sub classes.
   """
   # .............................
   def __init__(self, capacity, submitCmd):
      """
      @summary: Constructor sets the capacity and instantiates a subprocess
                   manager to handle the processes.
      """
      JobSubmitter.__init__(self, capacity)
      self.spm = SubprocessManager(maxConcurrent=capacity)
      self.submitCmd = submitCmd
   
   # .............................
   def getNumberOfEmptySlots(self):
      """
      @summary: This gets the number of jobs that can be submitted by 
                   subtracting the number of running jobs from the capacity of 
                   the submitter.
      """
      return self.capacity - self.spm.getNumberOfRunningProcesses()
   
   # .............................
   def submitJobs(self, jobXmlFns):
      """
      @summary: This function will submit the jobs specified
      @param jobXmlFns: A list of xml file names with job content
      """
      procCmds = []
      for xmlFn in jobXmlFns:

         try:
            print(xmlFn)
            el = parse(xmlFn).getroot()
            j = deserialize(el)
            fn = os.path.join(JOB_REQUEST_DIR, "%s-%s.xml" % (j.processType, j.jobId))
         
            # Move file
            os.rename(xmlFn, fn)

            cmd = self.fillInSubmissionCommand(j, fn)
            print("Submitting job: %s" % fn)
            procCmds.append(cmd)
            j = None
         except Exception, e:
            tb = traceback.format_exc()
            print(str(e))
            print(tb)
            # If parse error, need to remove the file probably
            #os.remove(xmlFn)
   
      self.spm.addProcessCommands(procCmds)
      self.spm.runProcesses()
   