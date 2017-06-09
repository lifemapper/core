"""
@summary: This module contains the job controller daemon that will run as a 
             daemon process and continually provide Makeflow processes that
             workers can connect to
@author: CJ Grady
@version: 1.0.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import argparse
import glob
import os
import shutil
import signal
from subprocess import Popen
import subprocess
import sys
from time import sleep
import traceback

from LmServer.base.utilities import isCorrectUser
from LmBackend.common.daemon import Daemon, DaemonCommands
from LmCommon.common.lmconstants import JobStatus
#from LmServer.db.scribe import Scribe
from LmServer.db.borgscribe import BorgScribe
from LmServer.common.lmconstants import (CATALOG_SERVER_BIN, MAKEFLOW_BIN,
                  MAKEFLOW_WORKSPACE, MATT_DAEMON_PID_FILE, WORKER_FACTORY_BIN)
from LmServer.common.localconstants import (CATALOG_SERVER_OPTIONS, 
                  MAKEFLOW_OPTIONS, MAX_MAKEFLOWS, WORKER_FACTORY_OPTIONS)
from LmServer.common.log import LmServerLogger

# .............................................................................
class MattDaemon(Daemon):
   """
   @summary: The MattDaemon class manages a pool of Makeflow subprocesses
                that workers connect to.   Once one of the Makeflow processes
                completes, it is replaced by the next available job chain.
                Workers can be located anywhere, but at least one should 
                probably run locally for local processes like database updates. 
   """
   # .............................
   def initialize(self):
      """
      @summary: Initialize the job controller
      """
      # Makeflow pool
      self._mfPool = []
      self.csProc = None
      self.wfProc = None

      self.sleepTime = 30
      self.maxMakeflows = MAX_MAKEFLOWS
      self.workspace = MAKEFLOW_WORKSPACE
      
      # Establish db connection
      self.scribe = BorgScribe(self.log)
      self.scribe.openConnections()
      
      # Start catalog server
      self.startCatalogServer()
      
      # Start worker factory
      self.startWorkerFactory()
   
   # .............................
   def run(self):
      """
      @summary: This method will continue to run while the self.keepRunning 
                   attribute is true
      """
      try:
         self.log.info("Running")
         
         while self.keepRunning and os.path.exists(self.pidfile):
            
            # Check if catalog server and factory are running
            # TODO: Should we attempt to restart these if they are stopped?
            if self.csProc.poll() is not None:
               raise Exception, "Catalog server has stopped"
            
            if self.wfProc.poll() is not None:
               raise Exception, "Worker factory has stopped"
            
            # Check if there are any empty slots
            numRunning = self.getNumberOfRunningProcesses()
            
            #  Add mf processes for empty slots
            for mfObj, mfDocFn in self.getMakeflows(self.maxMakeflows - numRunning):
               
               if os.path.exists(mfDocFn):
                  cmd = self._getMakeflowCommand("lifemapper-{0}".format(
                                             mfObj.getId()), mfDocFn)
                  self.log.debug(cmd)
                  self._mfPool.append([mfObj, mfDocFn, Popen(cmd, shell=True)])
               else:
                  self._cleanupMakeflow(mfObj, mfDocFn, exitStatus=2, 
                                        lmStatus=JobStatus.IO_GENERAL_ERROR)
            # Sleep
            self.log.info("Sleep for {0} seconds".format(self.sleepTime))
            sleep(self.sleepTime)
            
         self.log.debug("Exiting")
      except Exception, e:
         tb = traceback.format_exc()
         self.log.error("An error occurred")
         self.log.error(str(e))
         self.log.error(tb)
         self.stopWorkerFactory()
         self.stopCatalogServer()
   
   # .............................
   def getMakeflows(self, count):
      """
      @summary: Use the scribe to get available makeflow documents and moves 
                   DAG files to workspace
      @param count: The number of Makeflows to retrieve
      @note: If the DAG exists in the workspace, assume that things failed and
                we should try to continue
      """
      rawMFs = self.scribe.findMFChains(count)
      
      mfs = []
      for mfObj in rawMFs:
         # New filename
         origLoc = mfObj.getDLocation()
         newLoc = os.path.join(self.workspace, os.path.basename(origLoc))
         # Move to workspace if it does not exist (see note)
         if not os.path.exists(newLoc):
            shutil.copyfile(origLoc, newLoc)
         # Add to mfs list
         mfs.append((mfObj, newLoc))

      return mfs
      
   # .............................
   def getNumberOfRunningProcesses(self):
      """
      @summary: Returns the number of running processes
      """
      numRunning = 0
      for idx in xrange(len(self._mfPool)):
         if self._mfPool[idx] is not None:
            result = self._mfPool[idx][2].poll()
            if result is None:
               numRunning += 1
            else:
               mfObj = self._mfPool[idx][0]
               mfDocFn = self._mfPool[idx][1]
               self._mfPool[idx] = None
               
               self._cleanupMakeflow(mfObj, mfDocFn, result)

      self._mfPool = filter(None, self._mfPool)
      return numRunning

   # .............................
   def onUpdate(self):
      """
      @summary: Called on Daemon update request
      """
      # Read configuration
      self.readConfiguration()
      
      self.log.debug("Update signal caught!")
      
   # .............................
   def onShutdown(self):
      """
      @summary: Called on Daemon shutdown request
      """
      self.log.debug("Shutdown signal caught!")
      self.scribe.closeConnections()
      Daemon.onShutdown(self)

      # Wait for makeflows to finish
      maxTime = 60 * 3
      timeWaited = 0
      numRunning = self.getNumberOfRunningProcesses()
      while numRunning > 0 and timeWaited < maxTime:
         self.log.debug(
            "Waiting on {} makeflow processes to finish".format(numRunning))
         sleep(self.sleepTime)
         timeWaited += self.sleepTime
         try:
            numRunning = self.getNumberOfRunningProcesses()
         except:
            numRunning = 0
         
      if timeWaited > maxTime:
         self.log.debug("Waited for {} seconds.  Stopping.".format(timeWaited))
      
      # Stop worker factory
      try:
         self.stopWorkerFactory()
      except:
         pass
      
      # Stop catalog server
      try:
         self.stopCatalogServer()
      except:
         pass
      
      
   # .............................
   def startCatalogServer(self):
      """
      @summary: Start the local catalog server
      """
      cmd = "{csBin} {csOptions}".format(csBin=CATALOG_SERVER_BIN, 
                                         csOptions=CATALOG_SERVER_OPTIONS)
      self.csProc = Popen(cmd, shell=True, preexec_fn=os.setsid, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
   
   # .............................
   def stopCatalogServer(self):
      """
      @summary: Stop the local catalog server
      """
      os.killpg(os.getpgid(self.csProc.pid), signal.SIGTERM)
   
   # .............................
   def startWorkerFactory(self):
      """
      @summary: Start worker factory
      """
      cmd = "{wfBin} {wfOptions}".format(wfBin=WORKER_FACTORY_BIN, 
                                         wfOptions=WORKER_FACTORY_OPTIONS)
      self.wfProc = Popen(cmd, shell=True, preexec_fn=os.setsid)
   
   # .............................
   def stopWorkerFactory(self):
      """
      @summary: Kill worker factory
      """
      os.killpg(os.getpgid(self.wfProc.pid), signal.SIGTERM)
   
   # .............................
   def _getMakeflowCommand(self, name, mfDocFn):
      """
      @summary: Assemble Makeflow command
      @param name: The name of the Makeflow job
      @param mfDocFn: The Makeflow file to run
      """
      mfCmd = "{mfBin} {mfOptions} -N {mfName} {mfDoc}".format(
                           mfBin=MAKEFLOW_BIN, mfOptions=MAKEFLOW_OPTIONS, 
                           mfName=name, mfDoc=mfDocFn)
      return mfCmd

   # .............................
   def _cleanupMakeflow(self, mfObj, mfDocFn, exitStatus, lmStatus=None):
      """
      @summary: Clean up a makeflow that has finished, by completion, error, or
                   signal
      @param mfObj: Makeflow chain object
      @param mfDocFn: The file location of the DAG (in the workspace)
      @param exitStatus: Unix exit status (negative: killed by signal, 
                                           zero: successful, positive: error)
      @param lmStatus: If provided, update the database with this status
      """
      # If success, delete
      if exitStatus == 0:
         self.scribe.deleteObject(mfObj)
      else:
         # Either killed by signal or error
         if lmStatus is None:
            lmStatus = JobStatus.GENERAL_ERROR
         # Check if killed by signal
         if exitStatus < 0:
            lmStatus = JobStatus.INITIALIZE
         
         # Update
         mfObj.updateStatus(lmStatus)
         self.scribe.updateObject(mfObj)
      
      # Remove files from workspace
      delFiles = glob.glob("{0}*".format(mfDocFn))
      for fn in delFiles:
         os.remove(fn)
   
# .............................................................................
if __name__ == "__main__":
   if not isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)
      
   if os.path.exists(MATT_DAEMON_PID_FILE):
      pid = open(MATT_DAEMON_PID_FILE).read().strip()
   else:
      pid = os.getpid()
   
   parser = argparse.ArgumentParser(prog="Lifemapper Makeflow Daemon (Matt Daemon)",
                           description="Controls a pool of Makeflow processes",
                           version="1.0.0")
   
   parser.add_argument('cmd', choices=[DaemonCommands.START, 
                                       DaemonCommands.STOP, 
                                       DaemonCommands.RESTART],
              help="The action that should be performed by the makeflow daemon")

   args = parser.parse_args()

   mfDaemon = MattDaemon(MATT_DAEMON_PID_FILE, 
               log=LmServerLogger("mattDaemon", addConsole=True, addFile=True))

   if args.cmd.lower() == DaemonCommands.START:
      print "Start"
      mfDaemon.start()
   elif args.cmd.lower() == DaemonCommands.STOP:
      print "Stop"
      mfDaemon.stop()
   elif args.cmd.lower() == DaemonCommands.RESTART:
      mfDaemon.restart()
   else:
      print "Unknown command:", args.cmd.lower()
      sys.exit(2)
   
