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
@todo: Need to delete all files on delete
@todo: Move documents to staging area? Or create extra files in place (delete them?)
@todo: If documents are stored in staging area, do we need to check for existing on startup?
"""
import argparse
import os
import signal
from subprocess import Popen
import sys
from time import sleep
import traceback

from LmBackend.common.daemon import Daemon, DaemonCommands
from LmServer.db.scribe import Scribe
from LmServer.common.lmconstants import (CATALOG_SERVER_BIN, MAKEFLOW_BIN,
                                    MATT_DAEMON_PID_FILE, WORKER_FACTORY_BIN)
from LmServer.common.localconstants import (ARCHIVE_USER, CATALOG_SERVER_OPTIONS, 
                  MAKEFLOW_OPTIONS, MAX_MAKEFLOWS, WORKER_FACTORY_OPTIONS)
from LmCommon.common.lmconstants import JobStatus

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
      
      # Establish db connection
      self.scribe = Scribe(self.log)
      self.scribe.openConnections()
      
      # Read configuration
      self.readConfiguration()
      
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
            for mfId, mfDocFn in self.getMakeflows(self.maxMakeflows - numRunning):
               
               if os.path.exists(mfDocFn):
                  cmd = self._getMakeflowCommand("lifemapper-{0}".format(mfId), 
                                                 mfDocFn)
                  self.log.debug(cmd)
                  self._mfPool.append([mfId, Popen(cmd, shell=True)])
               else:
                  # TODO: Replace with correct function
                  self.scribe.updateMakeflow(mfId, JobStatus.IO_GENERAL_ERROR)
            # Sleep
            self.log.info("Sleep for {0} seconds".format(self.sleepTime))
            sleep(self.sleepTime)
            
         self.log.debug("Exiting")
      except Exception, e:
         tb = traceback.format_exc()
         self.log.error("An error occurred")
         self.log.error(str(e))
         self.log.error(tb)
   
   # .............................
   def getMakeflows(self, count):
      """
      @summary: Use the scribe to get available makeflow documents
      @param count: The number of Makeflows to retrieve
      @todo: Change scribe function
      @todo: Make sure the response is a list of makeflow id, makeflow filename 
                pairs
      """
      mfs = self.scribe.moveAndReturnJobChains(count, ARCHIVE_USER)
      
      # TODO: These need to be makeflow id, makeflow document file name pairs
      
      
      return mfs
      
   # .............................
   def getNumberOfRunningProcesses(self):
      """
      @summary: Returns the number of running processes
      @todo: Catch errors and update makeflow
      """
      numRunning = 0
      for idx in xrange(len(self._mfPool)):
         result = self._mfPool[idx][1].poll()
         if result is None:
            numRunning += 1
         else:
            mfId = self._mfPool[idx][0]
            self._mfPool[idx] = None
            
            # Check output
            # Standard result codes are: negative for killed by signal, 
            #                            zero for success, positive for error
            
            if result == 0:
               # Success
               # TODO: Replace with correct command
               self.scribe.deleteJobChain(mfId)
            elif result < 0:
               # Killed by signal, reset most likely
               # TODO: Replace with correct method
               self.scribe.updateMakeflow(mfId, JobStatus.INITIALIZE)
            else:
               # Error, update Makeflow status
               # TODO: Replace with correct method
               self.scribe.updateMakeflow(mfId, JobStatus.GENERAL_ERROR)

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
      @todo: Check that makeflows are stopped?  Or force shutdown?
      """
      self.log.debug("Shutdown signal caught!")
      self.scribe.closeConnections()
      
      # Stop worker factory
      self.stopWorkerFactory()
      
      # Stop catalog server
      self.stopCatalogServer()
      
      Daemon.onShutdown(self)
      
   # .............................
   def readConfiguration(self):
      """
      @summary: Get the maximum number of Makeflow processes for pool
      @todo: Read these from a configuration file
      """
      self.sleepTime = 30
      self.maxMakeflows = MAX_MAKEFLOWS

   # .............................
   def startCatalogServer(self):
      """
      @summary: Start the local catalog server
      """
      cmd = "{csBin} {csOptions}".format(csBin=CATALOG_SERVER_BIN, 
                                         csOptions=CATALOG_SERVER_OPTIONS)
      self.csProc = Popen(cmd, shell=True, preexec_fn=os.setsid)
   
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

# .............................................................................
if __name__ == "__main__":
   
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

   mfDaemon = MattDaemon(MATT_DAEMON_PID_FILE)

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
   