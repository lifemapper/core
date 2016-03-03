"""
@summary: Module containing the python job runner base class
@author: CJ Grady
@version: 3.0.0
@status: beta

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
import datetime
import sys

from LmCompute.jobs.runners.base import JobRunner
from LmCommon.common.lmconstants import JobStatus
from LmCompute.common.lmObj import LmException

class PythonRunner(JobRunner):
   """
   @summary: This is the base class for Job Runners that run python code to 
                work on a job.
   """
   # ...................................
   def __init__(self, job, env):
      self.job = job
      JobRunner.__init__(self, job, env)
      self.progress = 0
      
   # ...................................
   def run(self):
      try:
         self.startTime = datetime.datetime.now()
         self._initializeJob()
         self.status = JobStatus.COMPUTE_INITIALIZED
         self._update()
         self.status = JobStatus.RUNNING
         self._update()
         self._doWork()
         self.status = JobStatus.COMPUTED
         self._update()
         self.status = JobStatus.COMPLETE
         self._update()
         self._push()
         if self.status < JobStatus.GENERAL_ERROR:
            self.status = JobStatus.PUSH_COMPLETE
#          self.status = JobStatus.PUSH_COMPLETE
#          self._update()
         self.endTime = datetime.datetime.now()
         self.metrics['start time'] = str(self.startTime)
         self.metrics['end time'] = str(self.endTime)
         self.metrics['elapsed time'] = str(self.endTime - self.startTime)
         self._cleanUp()
         self.env.finalizeJob(self.job.processType, self.job.jobId)
#          self.status = JobStatus.COMPLETE
#          self._update()
         if self.status < JobStatus.GENERAL_ERROR:
            self.status = JobStatus.COMPLETE
      except LmException, lme:
         self.status = lme.code
         self._update()
         try:
            self.log.error(str(lme))
         except: # Log not initialized (early failure)
            print("Logging not initialized (early fail)")
            print(str(lme))
      except Exception, e:
         self.status = JobStatus.UNKNOWN_CLUSTER_ERROR
         self._update()
         try:
            self.log.error(str(e))
         except: # Log not initialized (early failure)
            print("Logging not initialized (early fail)")
            print(str(e))
         raise e # This will prevent the job request from being deleted.  Catch this and handle job request properly
   
   # ...................................
   def _doWork(self):
      """
      @summary: Does the work for the process
      """
      pass
   
   # ...................................
   def _push(self):
      """
      @summary: Pushes the results of the job to the job server
      """
      pass
   
   # ...................................
   def _update(self):
      """
      @summary: Updates the job object in storage
      """
      print(self.env.updateJob(self.job.processType, self.job.jobId, self.status, self.progress))
      
   # ..................................
   def remoteStop(self):
      """
      @summary: This is called when a job is asked to stop remotely with 
                   SIGTERM.  The job should do whatever it can to stop its 
                   operation cleanly.  This signal will likely be followed by a 
                   SIGKILL if emitted by a scheduler and that cannot be caught.
      """
      self.status = JobStatus.REMOTE_KILL
      self._update()
      sys.exit(1)
