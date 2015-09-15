"""
@summary: Module containing the application job runner class
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
from StringIO import StringIO
from subprocess import Popen, PIPE
import sys
from time import sleep

from LmCompute.jobs.runners.base import JobRunner

from LmCommon.common.lmconstants import JobStatus
from LmCompute.common.lmObj import LmException

# Other
# ............................
WAIT_SECONDS = 30 # The number of seconds to wait, by default, between polling
HEARTBEAT_SECONDS = 4 * 60 * 60 # Four hours

# .............................................................................
class ApplicationRunner(JobRunner):
   """
   @summary: This class is a job runner that spawns off a 3rd party application
                to work on a job.
   """
   # ...................................
   def __init__(self, job, env):
      """
      @summary: Constructor
      @param jobType: The type of job to be run
      @param jobId: The id of the job to run
      @param env: The environment to run in
      """
      if self.__class__ == ApplicationRunner:
         raise Exception("Abstract class ApplicationRunner should not be instantiated.")
      JobRunner.__init__(self, job, env)
      self.status = JobStatus.PULL_COMPLETE
      self.progress = 0
   
   # ...................................
   def run(self):
      """
      @summary: Runs the application and periodically checks it
      """
      try:
         self.startTime = datetime.datetime.now()
         self._initializeJob()
         self._update()
         cmd = self._buildCommand()
         self._startApplication(cmd)
         self.status = JobStatus.RUNNING
         hbTime = 0
         while not self._poll():
            self._checkApplication()
            if hbTime <= 0:
               hbTime = HEARTBEAT_SECONDS
               self._update()
            else:
               hbTime = hbTime - WAIT_SECONDS
            self._wait()
         self._checkApplication()
         self._checkOutput()
         self._update()
         self.endTime = datetime.datetime.now()
         self.metrics['start time'] = str(self.startTime)
         self.metrics['end time'] = str(self.endTime)
         self.metrics['elapsed time'] = str(self.endTime - self.startTime)
         self._push()
         if self.status < JobStatus.GENERAL_ERROR:
            self.status = JobStatus.PUSH_COMPLETE
         self._cleanUp()
         self.writeMetrics(self.PROCESS_TYPE, self.job.jobId)
         self.env.finalizeJob(self.job.processType, self.job.jobId)
         if self.status < JobStatus.GENERAL_ERROR:
            self.status = JobStatus.COMPLETE
      except LmException, lme:
         try:
            self.log.debug(str(lme))
         except: # Logger not initialized
            pass
         self.status = lme.code
         self._update()
      except Exception, e:
         try:
            self.log.debug(str(e))
         except: # Logger not initialized
            pass
         self.status = JobStatus.UNKNOWN_CLUSTER_ERROR
         self._update()
         raise e

   # ...................................
   def _buildCommand(self):
      """
      @summary: Builds a command to be ran for the job
      """
      return ""
   
   # ...................................
   def _checkApplication(self):
      """
      @summary: Checks a running application to get an updated progress and 
                   status
      """
      return True
   
   # ...................................
   def _checkOutput(self):
      """
      @summary: Checks the output of an application to see if any unexpected
                   errors occurred.
      """
      pass
   
   # ...................................
   def _getMetricsAsStringIO(self):
      """
      @summary: Return all of the metrics as a StringIO object that can be 
                   written to a zip file for return
      """
      val = '\n'.join(["{key}: {value}".format(key=k, value=self.metrics[k]
                                               ) for k in self.metrics.keys()])
      print(val)
      s = StringIO(val)
      s.seek(0)
      return s
      
   # ...................................
   def _poll(self):
      """
      @summary: Polls the active subprocess running the application
      """
      if self.subprocess.poll() is not None:
         return True
      else:
         return False
   
   # ...................................
   def _push(self):
      """
      @summary: Posts the results of the job back to the server
      """
      pass
   
   # ...................................
   def _startApplication(self, cmd):
      """
      @summary: This method takes a bash command and starts up an application.
      """
      self.subprocess = Popen(cmd, shell=True, stderr=PIPE)
      sleep(WAIT_SECONDS)
   
   # ...................................
   def _update(self):
      """
      @summary: Updates the job object in storage
      """
      print(self.env.updateJob(self.job.processType, self.job.jobId, self.status, self.progress))
      
   # ...................................
   def _wait(self):
      """
      @summary: Waits some amount of time so that the job runner isn't 
                   constantly polling the application and updating files.
      """
      sleep(WAIT_SECONDS)
      
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
