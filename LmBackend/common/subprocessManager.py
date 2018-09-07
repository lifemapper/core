"""
@summary: Module containing SubprocessManager module
@author: CJ Grady
@status: alpha
@version: 1.0

@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
from subprocess import Popen, PIPE
import signal
from time import sleep
import multiprocessing
CONCURRENT_PROCESSES = max(1, multiprocessing.cpu_count() - 2)

from LmCommon.common.lmXml import serialize, tostring

WAIT_TIME = 10
MAX_RUN_TIME = 60 * 60 * 4 # 4 hours

# .............................................................................
class VariableContainer(object):
   """
   @summary: Creates a container for variables to be passed to a subprocess
   """
   # .............................
   def __init__(self, values):
      self.values = values
   
   # .............................
   def __str__(self):
      return tostring(serialize(self))

   # .............................
   def __unicode__(self):
      return tostring(serialize(self))

# .............................................................................
class SubprocessManager(object):
   """
   @summary: This class manages subprocesses
   """
   # .............................
   def __init__(self, commandList=[], maxConcurrent=CONCURRENT_PROCESSES):
      """
      @param commandList: A list of commands to run as subprocesses
      @param maxConcurrent: (optional) The maximum number of subprocesses to 
                               run concurrently
      """
      self.procs = commandList
      self.maxConcurrent = maxConcurrent
      self._runningProcs = []
   
   # .............................
   def addProcessCommands(self, commandList):
      """
      @summary: Adds a list of commands to the list to run 
      """
      self.procs.extend(commandList)
   
   # .............................
   def runProcesses(self):
      """
      @summary: Runs the processes in self.procs
      """
      while len(self.procs) > 0:
         numRunning = self.getNumberOfRunningProcesses()
         num = min((self.maxConcurrent - numRunning), len(self.procs))
         
         while num > 0:
            proc = self.procs.pop(0)
            self.launchProcess(proc)
            num = num - 1
         
         self.wait()
      
      while self.getNumberOfRunningProcesses() > 0:
         self.wait()
      
   # .............................
   def wait(self, waitSeconds=WAIT_TIME):
      """
      @summary: Waits the specified amount of time
      """
      sleep(waitSeconds)
   
   # .............................
   def launchProcess(self, cmd):
      """
      @summary: Launches a subprocess for the command provided
      """
      self._runningProcs.append(Popen(cmd, shell=True))
   
   # .............................
   def getNumberOfRunningProcesses(self):
      """
      @summary: Returns the number of running processes
      """
      numRunning = 0
      for idx in xrange(len(self._runningProcs)):
         if self._runningProcs[idx].poll() is None:
            numRunning = numRunning +1
         else:
            self._runningProcs[idx] = None
      self._runningProcs = filter(None, self._runningProcs)
      return numRunning

# .............................................................................
class SubprocessRunner(object):
   """
   @summary: This class manages a subprocess
   """
   # .............................
   def __init__(self, cmd, waitSeconds=WAIT_TIME, killTime=MAX_RUN_TIME):
      """
      @summary: Constructor for single command runner
      @param cmd: The command to run
      @param waitSeconds: The number of seconds to wait between polls
      """
      self.cmd = cmd
      self.waitTime = waitSeconds
      self.killTime = killTime
      self.myProc = None
   
   # .............................
   def run(self):
      """
      @summary: Run the command
      @return: exit status code, standard error
      """
      stdErr = None
      self.myProc = Popen(self.cmd, shell=True, stderr=PIPE, 
                          preexec_fn=os.setsid)
      self._wait()
      runTime = 0
      while self.myProc.poll() is None and runTime < self.killTime:
         self._wait()
         runTime += self.waitTime
         
      if runTime >= self.killTime:
         self.signal(signal.SIGTERM)
         raise Exception, 'Killed long running process'
         
      # Get output
      exitCode = self.myProc.poll()
      if self.myProc.stderr is not None:
         stdErr = self.myProc.stderr.read()
      return exitCode, stdErr
   
   # .............................
   def signal(self, signum):
      """
      @summary: Signal the running process
      """
      if self.myProc is not None:
         os.killpg(os.getpgid(self.myProc.pid), signum)
            
   # .............................
   def _wait(self):
      """
      @summary: Sleeps the specified amount of time
      """
      sleep(self.waitTime)
