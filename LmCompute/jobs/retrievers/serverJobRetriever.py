"""
@summary: 
@author: CJ Grady
@version: 
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
import glob
from httplib import BadStatusLine
import os
import socket
from urllib2 import HTTPError, URLError

from LmCommon.common.lmconstants import HTTPStatus

from LmCompute.common.jobClient import LmJobClient
from LmCompute.common.jobTypes import JOB_TYPES
from LmCompute.common.log import RetrieverLogger
from LmCompute.jobs.retrievers.base import JobRetriever

# .............................................................................
class ServerRetriever(JobRetriever):
   """
   @summary: This class will connect to a job server to retrieve jobs to run
   """
   PERSIST = True

   # .............................
   def __init__(self, jobXmlDir, jobServer, numToPull, threshold, jobTypes=JOB_TYPES, users=None):
      """
      @summary: Constructor
      @param jobXmlDir: A directory where job xml files can be stored
      @param jobServer: The job server to connect to
      @param numToPull: The number of jobs to pull with each pull request
      @param threshold: Pull jobs when the number of jobs in the queue is less 
                           than this threshold
      @param jobTypes: Pull jobs of these types
      @param users: Pull jobs for these users
      """
      self.cl = LmJobClient(jobServer)
      # Look for existing jobs
      self.jobXmlFns = glob.glob(os.path.join(jobXmlDir, "*"))
      
      self.jobDir = jobXmlDir
      
      if not os.path.exists(self.jobDir):
         os.mkdir(self.jobDir)
      
      self.jobTypes = jobTypes
      self.users = users
      self.numToPull = numToPull
      self.threshold = threshold
      name = '%s-retriever' % jobServer.replace('http://', '').replace('/', '_')
      self.log = RetrieverLogger(name)
   
   # .............................
   def getJobs(self, num):
      """
      @summary: Return this number of jobs
      """
      self.log.info("%s jobs requested, %s in queue" % (num, len(self.jobXmlFns)))
      if len(self.jobXmlFns) < self.threshold:
         try:
            if self.cl.availableJobs(jobTypes=self.jobTypes, users=self.users):
               try:
                  newJobs = self.cl.requestJobs(self.jobDir,
                                                jobTypes=self.jobTypes, 
                                                parameters={"users" : self.users}, 
                                                numJobs=self.numToPull)
                  self.log.debug("Pulled %s jobs" % len(newJobs))
               except BadStatusLine, bErr:
                  # This happens if Apache is reset
                  # Try again
                  try:
                     newJobs = self.cl.requestJobs(self.jobDir,
                                                jobTypes=self.jobTypes, 
                                                parameters={"users" : self.users}, 
                                                numJobs=self.numToPull)
                  except Exception, e:
                     # return empty list on second fail
                     self.log.error("Failed to pull jobs after Apache reset")
                     self.log.error(str(e))
                     newJobs = []
               except HTTPError, err:
                  if err.code == HTTPStatus.SERVICE_UNAVAILABLE:
                     # No jobs, return empty list
                     self.log.debug("Service unavailable, returning no jobs")
                     newJobs = []
                  else:
                     # try again
                     try:
                        newJobs = self.cl.requestJobs(self.jobDir,
                                                jobTypes=self.jobTypes, 
                                                parameters={"users" : self.users}, 
                                                numJobs=self.numToPull)
                     except Exception, e2:
                        # return empty list on second fail
                        self.log.error("Failed to pull jobs after Apache reset")
                        self.log.error(str(e))
                        newJobs = []
               except Exception, e: # Catch any other exception
                  self.log.error("Failed to pull jobs")
                  self.log.error(str(e))
                  newJobs = []
               
               self.jobXmlFns.extend(newJobs)
         except socket.error, se:
            self.log.error("Socket error while attempting to pull jobs or checking for jobs")
            self.log.error(str(se))
            # Don't add new jobs
            self.log.error("These (queued) jobs may fail because of this error:")
            for j in self.jobXmlFns:
               self.log.error(j)
         except URLError, urlE:
            self.log.error("Error when trying to comminicate with server")
            self.log.error(str(urlE))
            # Don't add new jobs
            self.log.error("These (queued) jobs may fail because of this error:")
            for j in self.jobXmlFns:
               self.log.error(j)
         except Exception, e:
            self.log.error("An unknown error occurred")
            self.log.error(str(e))
            # Don't add new jobs
            self.log.error("These (queued) jobs may fail because of this error:")
            for j in self.jobXmlFns:
               self.log.error(j)
      if num >= len(self.jobXmlFns):
         ret = self.jobXmlFns
         self.jobXmlFns = []
         return ret
      else:
         ret = self.jobXmlFns[:num]
         self.jobXmlFns = self.jobXmlFns[num:]
         return ret
   