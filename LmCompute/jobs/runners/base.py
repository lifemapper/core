"""
@summary: Module containing job runner base class
@author: CJ Grady
@version: 4.0.0
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
import argparse
import datetime
from hashlib import md5
import logging
from logging.handlers import RotatingFileHandler
import os
import signal
import sys

from LmCommon.common.lmXml import deserialize, parse

from LmCompute.common.localconstants import (ADMIN_EMAIL, ADMIN_NAME, 
                        INSTITUTION_NAME, LOCAL_MACHINE_ID)

from LmBackend.common.systemMetadata import getSystemConfigurationDictionary

#TODO: This are also defined in LmServer.common.lmconstants, consider moving to LmBackend
LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
# Date format for log dates
LOG_DATE_FORMAT = '%d %b %Y %H:%M'

# .............................................................................
class JobRunner(object):
   """
   @summary: Job Runner base class. Job runners are responsible for running a 
                process required to execute some job.
   """
   prog = ''
   processType = ''
   
   # ..................................
   def __init__(self, jobXmlFn, jobName=None, outDir=None, workDir=None, 
                      metricsFn=None, logFn=None, logLevel=None):
      """
      @summary: Constructor for base class, should be called by subclasses or
                   inherited
      @param jobXmlFn: A file name of an XML file with job parameters
      @param jobName: (optional) The name of the job (used for work directory 
                                    and logging
      @param outDir: (optional) A directory where outputs should be written.  
                                   This can be the final location or a shared 
                                   directory with the front end.
      @param workDir: (optional) A workspace directory where the work directory
                                    should be created.  If omitted, uses the
                                    current directory
      @param metricsFn: (optional) A file name to store metrics from the job run
      @param logFn: (optional) A file name to write the job log to.  If this is
                       not an absolute path, it will be relative to the work
                       directory.  If None, only log to console
      @param logLevel: (optional) What level to write the logs
      """
      self.metrics = {}
      self.jobXmlFn = jobXmlFn
      
      # If job name is None, generate one
      if jobName is not None:
         self.jobName = jobName
      else:
         self.jobName = md5().hexdigest()[:8]
      self.outDir = outDir

      # Get the cwd if workDir is None
      if workDir is None:
         workDir = os.getcwd()
      self.workDir = os.path.join(workDir, self.jobName)
      self.metricsFn = metricsFn
      
      # If log path is absolute, use it.  If not, assume relative to work dir
      if os.path.isabs(logFn):
         self.logFn = logFn
      else:
         self.logFn = os.path.join(self.workDir, logFn)
      
      # Make sure that we have a file and not a directory, if directory add file
      if len(os.path.basename(self.logFn)) == 0: # Path ended with /
         self.logFn = os.path.join(self.logFn, 'jobLog.log')
      self.logFn = logFn
      self.logLevel = logLevel
      
      signal.signal(signal.SIGTERM, self._receiveStopSignal) # Stop signal
   
   # ..................................
   def _cleanUp(self):
      if self.args.cleanUp:
         if self.createdWS:
            shutil.rmtree(self.args.work_dir)
         else:
            self.log.debug("Workspace directory was not created by this process.  Don't delete.")
   
   # ..................................
   def _doWork(self):
      """
      @summary: Do the work associated with this job
      """
      pass
   
   # ..................................
   def _finishJob(self):
      """
      @summary: Finish the job by moving outputs to the specified location and
                   finalizing whatever else needs to be done before cleanup
      """
      pass
   
   # ..................................
   def _initializeJob(self):
      """
      @summary: Prepares the work directory, logs some metadata and reads the 
                   job xml file
      """
      self.job = deserialize(parse(self.jobXmlFn))
      
      self.createdWS = False
      # Create the work directory if it does not exist
      if not os.path.exists(self.workDir):
         #TODO: What to do if this fails?
         os.makedirs(self.workDir)
         self.createdWS = True
      
      # Set up the log after the word directory is created
      self._initializeLog()
   
      self.log.debug("Job start time: %s" % str(datetime.datetime.now()))
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
      
      self._processJobInput()
      
   # ..................................
   def _initializeLog(self):
      """
      @summary: Initialize the log file, if specified, for this job
      """
      self.log = logging.getLogger(self.jobName)
      
      if self.logLevel is not None and self.logLevel.lower() in ['info', 'debug', 'warn', 'error', 'critical']:
         if self.logLevel == 'info':
            ll = logging.INFO
         elif self.logLevel == 'debug':
            ll = logging.DEBUG
         elif self.logLevel == 'warn':
            ll = logging.WARNING
         elif self.logLevel == 'error':
            ll = logging.ERROR
         else:
            ll = logging.CRITICAL
      else:
         ll = logging.NOTSET
      self.log.setLevel(ll)
      
      # Only set file handler if should log to file, else omit
      if self.logFn is not None:
         fileLogHandler = RotatingFileHandler(self.logFn)
         fileLogHandler.setLevel(self.log.level)
         formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
         fileLogHandler.setFormatter(formatter)
         self.log.addHandler(fileLogHandler)
   
   # .............................
   def _processJobInput(self):
      """
      @summary: Process the job inputs and set up anything necessary
      """
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
   def _writeMetrics(self):
      if self.metricsFn is not None:
         if len(self.metrics.keys()) > 0:
            with open(self.metricsFn, 'w') as outFile:
               for key in self.metrics.keys():
                  outFile.write("%s: %s\n" % (key, str(self.metrics[key])))

   # ..................................
   def remoteStop(self):
      """
      @summary: This is called when a job is asked to stop remotely with 
                   SIGTERM.  The job should do whatever it can to stop its 
                   operation cleanly.  This signal will likely be followed by a 
                   SIGKILL if emitted by a scheduler and that cannot be caught.
      """
      sys.exit(1)
   
   # ..................................
   def run(self):
      """
      @summary: Runs the job.  This method probably doesn't need to be 
                   overwritten in subclasses unless a different behavior is 
                   needed for some reason
      """
      # Initialization time
      initTime = datetime.datetime.now()
      self._initializeJob()
      # Job start time
      startTime = datetime.datetime.now()
      self._doWork()
      # job end time
      endTime = datetime.datetime.now()
      self._finishJob()
      # Finalize time
      finalizeTime = datetime.datetime.now()
      self._cleanUp()
      # Clean up time
      cleanupTime = datetime.datetime.now()

      self.metrics['init clock'] = str(initTime)
      self.metrics['init time'] = str(startTime - initTime)
      self.metrics['run time'] = str(endTime - startTime)
      self.metrics['finalize time'] = str(finalizeTime - endTime)
      self.metrics['cleanup time'] = str(cleanupTime - finalizeTime)
      self.metrics['elapsed time'] = str(cleanupTime - initTime)
      self.metrics['end clock'] = str(cleanupTime)
      self._writeMetrics()
      
