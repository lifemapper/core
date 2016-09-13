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
from subprocess import Popen, PIPE
from time import sleep

from LmCompute.jobs.runners.base import JobRunner


# Other
# ............................
WAIT_SECONDS = 30 # The number of seconds to wait, by default, between polling

# TODO: Add mechanism to catch hung jobs
HEARTBEAT_SECONDS = 4 * 60 * 60 # Four hours


# .............................................................................
class ApplicationRunner(JobRunner):
   """
   @summary: This class is a job runner that spawns off a 3rd party application
                to work on a job.
   """
   # ...................................
   def _doWork(self):
      """
      @summary: Runs the application and periodically checks it
      """
      cmd = self._buildCommand()
      self._startApplication(cmd)
      hbTime = 0
      while not self._poll():
         self._wait()
      self._checkOutput()

   # ...................................
   def _buildCommand(self):
      """
      @summary: Builds a command to be ran for the job
      """
      return ""
   
   # ...................................
   def _checkOutput(self):
      """
      @summary: Checks the output of an application to see if any unexpected
                   errors occurred.
      """
      pass
   
   # ...................................
   def _poll(self):
      """
      @summary: Polls the active subprocess running the application
      """
      if self.subprocess.poll() is not None:
         if self.subprocess.stderr is not None:
            self.log.error(self.subprocess.stderr.read())
         return True
      else:
         return False
   
   # ...................................
   def _startApplication(self, cmd):
      """
      @summary: This method takes a bash command and starts up an application.
      """
      self.subprocess = Popen(cmd, shell=True, stderr=PIPE)
      sleep(WAIT_SECONDS)
   
   # ...................................
   def _wait(self):
      """
      @summary: Waits some amount of time so that the job runner isn't 
                   constantly polling the application and updating files.
      """
      sleep(WAIT_SECONDS)
      
