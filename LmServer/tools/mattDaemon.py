"""
@summary: This module contains the job controller daemon that will run as a 
             daemon process and continually provide Makeflow processes that
             workers can connect to
@author: CJ Grady
@version: 1.0.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
#TODO: Number of mfs constant
#TODO: Find existing MFs
#TODO: MF_DAEMON_PID_FILE
#TODO: Logger
#TODO: Something other than a list for pool?
#TODO: What if document does not exist?

import argparse
import os
from subprocess import Popen
import sys
from time import sleep
import traceback

from LmBackend.common.daemon import Daemon
from LmCompute.common.log import MediatorLogger
from LmServer.db.scribe import Scribe
from LmServer.common.localconstants import ARCHIVE_USER, CATALOG_SERVER_BIN, \
       CATALOG_SERVER_OPTIONS, CATALOG_SERVER_PID_FILE, MAKEFLOW_BIN, \
       MAKEFLOW_OPTIONS, MATT_DAEMON_PID_FILE, MAX_MAKEFLOWS, \
       WORKER_FACTORY_BIN, WORKER_FACTORY_OPTIONS

# .............................................................................
class MattDaemon(Daemon):
   """
   @summary: The JobController class manages a pool of Makeflow subprocesses
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
            
            #TODO: Check if catalog server and factory are running
            
            
            
            # Check if there are any empty slots
            numRunning = self.getNumberOfRunningProcesses()
            
            #   Add mf processes for empty slots
            for jid, mfDoc in self.getMakeflowDocs(MAX_MAKEFLOWS - numRunning):
               cmd = self._getMakeflowCommand("lifemapper-{0}".format(jid), 
                                              mfDoc)
               self.log.debug(cmd)
               self._mfPool.append([jid, Popen(cmd, shell=True)])
            # Sleep
            self.log.info("Sleep for {0} seconds".format(self.sleepTime))
            sleep(self.sleepTime)
            
            #TODO: Keep a cache of mf docs?
            
         self.log.debug("Exiting")
      except Exception, e:
         tb = traceback.format_exc()
         self.log.error("An error occurred")
         self.log.error(str(e))
         self.log.error(tb)
   
   # .............................
   def getMakeflowDocs(self, count):
      """
      @summary: Use the scribe to get available makeflow documents
      """
      jcs = self.scribe.moveAndReturnJobChains(count, ARCHIVE_USER)
      #mfDocs = [mf for _, mf in jcs]
      #return mfDocs
      return jcs
      
   # .............................
   def getNumberOfRunningProcesses(self):
      """
      @summary: Returns the number of running processes
      """
      numRunning = 0
      for idx in xrange(len(self._mfPool)):
         if self._mfPool[idx][1].poll() is None:
            numRunning = numRunning +1
         else:
            jid = self._mfPool[idx][0]
            self._mfPool[idx] = None
            self.scribe.deleteJobChain(jid)
      self._mfPool = filter(None, self._mfPool)
      return numRunning

   # .............................
   def onUpdate(self):
      # Read configuration
      self.readConfiguration()
      
      self.log.debug("Update signal caught!")
      
   # .............................
   def onShutdown(self):

      self.log.debug("Shutdown signal caught!")
      self.scribe.closeConnections()
      
      #TODO: Check that makeflows are stopped? or force shutdown
      
      # Stop worker factory
      self.stopWorkerFactory()
      
      # Stop catalog server
      self.stopCatalogServer()
      
      
      Daemon.onShutdown(self)
      
   # .............................
   def readConfiguration(self):
      """
      @summary: Get the maximum number of Makeflow processes for pool
      """
      # NOTE: 
      self.mfCmd = "{mfBin} {mfOptions}"
      # TODO: Get this from a constant and / or argument
      self.mfCmd = "{mfBin} -T wq -N {mfName} -t 600 -u 600 {mfDoc}"
      self.sleepTime = 30

   # .............................
   def startCatalogServer(self):
      cmd = "{csBin} {csOptions}".format(csBin=CATALOG_SERVER_BIN, 
                                         csOptions=CATALOG_SERVER_OPTIONS)
      pass
   
   # .............................
   def stopCatalogServer(self):
      pass
   
   # .............................
   def startWorkerFactory(self):
      cmd = "{wfBin} {wfOptions}".format(wfBin=WORKER_FACTORY_BIN, 
                                         wfOptions=WORKER_FACTORY_OPTIONS)
   
   # .............................
   def stopWorkerFactory(self):
      pass
   
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
   
   parser.add_argument('cmd', choices=['start', 'stop', 'restart'],
              help="The action that should be performed by the makeflow daemon")

   args = parser.parse_args()

   mfDaemon = MattDaemon(MF_DAEMON_PID_FILE, log=MediatorLogger(pid))

   if args.cmd.lower() == 'start':
      print "Start"
      mfDaemon.start()
   elif args.cmd.lower() == 'stop':
      print "Stop"
      mfDaemon.stop()
   elif args.cmd.lower() == 'restart':
      mfDaemon.restart()
   else:
      print "Unknown command:", args.cmd.lower()
      sys.exit(2)
   