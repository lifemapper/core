"""
@summary: Module containing job runner base class
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
import logging
from logging.handlers import RotatingFileHandler
import os
import signal

from LmCommon.common.lmconstants import JobStatus

from LmCompute.common.localconstants import (ADMIN_EMAIL, ADMIN_NAME, 
                        INSTITUTION_NAME, LOCAL_MACHINE_ID, LOG_LOCATION, 
                        STORE_LOGS, METRICS_PATH, STORE_METRICS)

from LmBackend.common.systemMetadata import getSystemConfigurationDictionary

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
# Date format for log dates
LOG_DATE_FORMAT = '%d %b %Y %H:%M'

# .............................................................................
class JobRunner(object):
   """
   @summary: Job Runner base class. Job runners are responsible for running a 
                process required to execute some job.
   """
   # ..................................
   def __init__(self, job, env):
      self.job = job
      self.env = env
      self.metrics = {}
      self.log = None # This will be initialized after the job is initialized 
                      #    and the log directory is created.
      signal.signal(signal.SIGTERM, self._receiveStopSignal) # Stop signal
      
   # ..................................
   def run(self):
      raise Exception, "Run method must be declared in a sub-class"
   
   # ...................................
   def _cleanUp(self, removeOutput=True):
      """
      @summary: Cleans up after a job has completed.  This should deleting all
                   files created by the job that do not need to be kept on the
                   node.
      @param removeOutput: (optional) Should output directory be removed at the 
                              end of the job
      """
      try: # Output some extra logging information
         self.log.debug("Job end time: %s" % self.endTime)
      except Exception, e:
         print str(e)
         #pass
      try:
         import shutil
         if STORE_LOGS:
            shutil.move(self.jobLogFile, os.path.join(LOG_LOCATION, os.path.basename(self.jobLogFile)))
         if removeOutput:
            shutil.rmtree(self.outputPath)#, ignore_errors=True)
      except:
         pass
   
   # ..................................
   def _initLogger(self, name, filename):
      self.log = logging.getLogger(name)
      self.log.setLevel(logging.DEBUG)
      fileLogHandler = RotatingFileHandler(self.jobLogFile)
      fileLogHandler.setLevel(self.log.level)
      formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
      fileLogHandler.setFormatter(formatter)
      self.log.addHandler(fileLogHandler)

   # ...................................
   def _initializeJob(self):
      """
      @summary: This method initializes a job.  This may include writing out 
                   parameter files to the file system or other initialization
                   tasks.
      """
      self.outputPath = os.path.join(self.env.getJobOutputPath(), 
                       "job-{0}-{1}".format(self.PROCESS_TYPE, self.job.jobId))

      if not os.path.exists(self.outputPath):
         os.makedirs(self.outputPath)

      self.jobLogFile = "%s/jobLog-%s.log" % (self.outputPath, self.job.jobId)
      self._initLogger('job-%s-%s' % (self.PROCESS_TYPE, self.job.jobId), self.jobLogFile)
      self.log.debug("Job start time: %s" % self.startTime)
      self.log.debug("-------------------------------------------------------")
      self.log.debug("Job Id: %s" % self.job.jobId)
      self.log.debug("User Id: %s" % self.job.userId)
      try:
         self.log.debug("Parent URL: %s" % self.job.parentUrl)
      except:
         pass
      try:
         self.log.debug("Object URL: %s" % self.job.url)
      except:
         pass
      self.log.debug("Institution: %s" % INSTITUTION_NAME)
      self.log.debug("Admin: %s" % ADMIN_NAME)
      self.log.debug("Admin Email: %s" % ADMIN_EMAIL)
      self.log.debug("Local Machine ID: %s" % LOCAL_MACHINE_ID)
      self.log.debug("-------------------------------------------------------")
      self.log.debug("Process Id: %s" % os.getpid())
      self.log.debug("-------------------------------------------------------")
      self._logSystemInformation()
      
      self._processJobInput()
      
      self.status = JobStatus.COMPUTE_INITIALIZED

   # ..................................
   def _processJobInput(self):
      """
      @summary: Process job inputs
      """
      pass      
   
   # ..................................
   def _logSystemInformation(self):
      sysConfig = getSystemConfigurationDictionary()
      try:
         self.metrics['machine id'] = LOCAL_MACHINE_ID
         self.metrics['ip address'] = sysConfig['machine ip']
         self.metrics['uname'] = sysConfig['machine name']
         self.log.debug("-------------------------------------------------------")
         self.log.debug("Machine Name: {0}".format(sysConfig["machine name"]))
         self.log.debug("Machine IP: {0}".format(sysConfig["machine ip"]))
         self.log.debug("Architecture: {0}".format(sysConfig["architecture"]))
         self.log.debug("OS: {0}".format(sysConfig["os"]))
         self.log.debug("Total Memory: {0}".format(sysConfig["total memory"]))
         self.log.debug("CPU Info: {0}".format(sysConfig['cpus']))
         self.log.debug("Python Version: {0}".format(sysConfig["python version"]))
         self.log.debug("Linux Version: {0}".format(sysConfig["linux version"]))
         self.log.debug("-------------------------------------------------------")
      except:
         pass

   # .............................
   def _receiveStopSignal(self, sigNum, stack):
      """
      @summary: Handler used to receive signals
      @param sigNum: The signal received
      @param stack: The stack at the time of signal
      """
      if sigNum == signal.SIGTERM:
         self.remoteStop()

   # ..................................
   def remoteStop(self):
      """
      @summary: This is called when a job is asked to stop remotely with 
                   SIGTERM.  The job should do whatever it can to stop its 
                   operation cleanly.  This signal will likely be followed by a 
                   SIGKILL if emitted by a scheduler and that cannot be caught.
      """
      pass
   
   # ..................................
   def writeMetrics(self, jobType, jobId):
      """
      @summary: Writes out the metrics of the job
      """
      if STORE_METRICS:
         fn = os.path.join(METRICS_PATH,
                           "job-%s-%s.metrics" % (jobType, jobId))
         if len(self.metrics.keys()) > 0:
            with open(fn, 'w') as outFile:
               for key in self.metrics.keys():
                  outFile.write("%s: %s\n" % (key, str(self.metrics[key])))
      