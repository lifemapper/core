"""
@summary: Module containing SubprocessManager module
@author: CJ Grady
@status: alpha
@version: 1.0

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

from subprocess import Popen
from time import sleep

from LmCommon.common.lmXml import serialize, tostring
from LmCommon.common.localconstants import CONCURRENT_PROCESSES

WAIT_TIME = 10

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
