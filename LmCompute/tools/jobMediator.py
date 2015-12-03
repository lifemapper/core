"""
@summary: This module contains the Job Mediator Daemon that will run as a 
             daemon process and submit jobs for computation
@author: CJ Grady
@version: 3.0.0
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
import sys
from time import sleep
import traceback

from LmCompute.common.localconstants import ADMIN_EMAIL, ADMIN_NAME, \
                                        JM_INACTIVE_TIME, \
                                        JM_SLEEP_TIME, JOB_MEDIATOR_PID_FILE, \
                                        LOCAL_MACHINE_ID
from LmCompute.common.log import MediatorLogger
from LmCompute.jobs.retrievers.factory import getJobRetrieversDictFromConfig
from LmCompute.jobs.submitters.factory import getJobSubmitterFromConfig

from LmBackend.common.daemon import Daemon

# .............................................................................
class JobMediator(Daemon):
   """
   @summary: The JobMediator class contains a list of JobRetrievers that get 
                jobs via some mechanism and then the mediator will pass these 
                jobs to a jobSubmitter object that will run the jobs in the 
                current environment.
   """
   # .............................
   def initialize(self):
      # Job retrievers
      self.retrievers = {}
      self.inactives = {}
      # Read configuration
      self.readConfiguration()
   
   # .............................
   def run(self):
      """
      @summary: This method will continue to run while the self.keepRunning 
                   attribute is true
      """
      try:
         self.log.info("Running")
         while self.keepRunning and os.path.exists(self.pidfile):
            # See if any inactives need to be reinstated
            for iKey in self.inactives.keys():
               i = self.inactives[iKey]
               i['timeLeft'] -= self.sleepTime
               if i['timeLeft'] <= 0:
                  self.inactives.pop(iKey, None)
                  self.addRetriever(iKey, i['retriever'])
            
            # If there are retrievers
            if len(self.retrievers.keys()) > 0:
               # Do we need more jobs?
               numToSubmit = self.submitter.getNumberOfEmptySlots()
               # How many each (equally divide for now)
               numEach = numToSubmit / len(self.retrievers.keys())
               
               if numEach > 0:
                  # Build a jobs to submit list
                  jobsToSubmit = []
                  
                  for retrieverKey in self.retrievers.keys():
                     retriever = self.retrievers[retrieverKey]
                     retJobs = retriever.getJobs(numEach)
                     
                     # If there are no jobs left for a retriever, see if it should go away, or sleep
                     if len(retJobs) == 0: 
                        self.log.debug("No jobs for %s, moving to inactive list" % retrieverKey)
                        self.retrievers.pop(retrieverKey, None) # Remove from active list
                        
                        # If the retriever should persist, add it to the inactives
                        if retriever.PERSIST:
                           self.inactives[retrieverKey] = {
                                                  'retriever' : retriever,
                                                  'timeLeft' : self.inactiveTime
                                                }
         
                     jobsToSubmit.extend(retJobs)
                  self.log.debug("Submitting %s jobs" % len(jobsToSubmit))
                  self.submitter.submitJobs(jobsToSubmit)
            else:
               # If there are no retrievers that are active or inactive, stop
               if len(self.inactives.keys()) == 0:
                  #self.keepRunning = False
                  self.log.message.critical("Out of job retrievers, shutting down")
                  self.onShutdown()
                  
            sleep(self.sleepTime)
         self.log.debug("Exiting")
      except Exception, e:
         tb = traceback.format_exc()
         # Something happened, email developers
         recipients = ADMIN_EMAIL
         subject = "The %s job mediator has failed" % LOCAL_MACHINE_ID
         message = """\
Hi {adminName},

The Lifemapper job mediator at {machineId} has failed with the following message:

{errMsg}

{tb}""".format(adminName=ADMIN_NAME, machineId=LOCAL_MACHINE_ID, 
                   errMsg=str(e), tb=tb)
         try:
            notifier = EmailNotifier()
            notifier.sendMessage(recipients, subject, message)
         except Exception, e:
            self.log.error('Failed to notify %s about %s, \n message: %s' 
                        % (str(recipients), subject, message))

   
   # .............................
   def onUpdate(self):
      # Read configuration
      self.readConfiguration()
      
      self.log.debug("Update signal caught!")
      
   # .............................
   def onShutdown(self):

      self.log.debug("Shutdown signal caught!")
      
      Daemon.onShutdown(self)
      
   # .............................
   def readConfiguration(self):
      """
      @summary: Get the mediators configuration options
      """
      self.sleepTime = JM_SLEEP_TIME
      self.inactiveTime = JM_INACTIVE_TIME

      self.submitter = getJobSubmitterFromConfig()

      self.retrievers = getJobRetrieversDictFromConfig()

   # .............................
   def addRetriever(self, key, retriever):
      """
      @summary: Adds a job retriever to the list of job retrievers
      """
      self.retrievers[key] = retriever


# .............................................................................
if __name__ == "__main__":
   
   if os.path.exists(JOB_MEDIATOR_PID_FILE):
      pid = open(JOB_MEDIATOR_PID_FILE).read().strip()
   else:
      pid = os.getpid()
   
   jobMediator = JobMediator(JOB_MEDIATOR_PID_FILE, log=MediatorLogger(pid))
   
   if len(sys.argv) == 2:
      if sys.argv[1].lower() == 'start':
         jobMediator.start()
      elif sys.argv[1].lower() == 'stop':
         jobMediator.stop()
      #elif sys.argv[1].lower() == 'update':
      #   jobMediator.update()
      elif sys.argv[1].lower() == 'restart':
         jobMediator.restart()
      else:
         print("Unknown command")
         self.log.error("Unknown command: %s" % sys.argv[1].lower())
         sys.exit(2)
   else:
      print("usage: %s start|stop|update" % sys.argv[0])
      sys.exit(2)
