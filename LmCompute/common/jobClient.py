"""
@summary: Lifemapper computational job client library
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
import os
from StringIO import StringIO
import urllib
import urllib2
import zipfile

from LmCommon.common.lmconstants import JobStatus
from LmCommon.common.lmXml import deserialize, fromstring

from LmCompute.common.jobTypes import JOB_TYPES
from LmCompute.common.lmconstants import LM_CLIENT_VERSION_URL
from LmCompute.common.localconstants import LOCAL_MACHINE_ID
from LmCompute.common.log import JobClientLogger

class RemoteKillException(Exception):
   """
   @summary: Signaled by Lifemapper job server to indicate that there is a 
                problem (server side, out of date client, other) and that the
                controlling code should stop until it is resolved
   """
   pass

class LmJobClient(object):
   """
   @summary: Communicates with the Lifemapper job server to retrieve, update,
                and post jobs
   """
   __version__ = "3.0.0 beta"
   
   # .................................
   def __init__(self, server):
      self.id = LOCAL_MACHINE_ID
      self.jobServer = server
      self.log = JobClientLogger()
   
   # .................................
   def availableJobs(self, jobTypes=JOB_TYPES.keys(), 
                           users=None,
                           threshold=1):
      """
      @summary: Checks to see if there are some threshold of jobs available 
                   that match the given criteria
      @param jobTypes: A list of process types (integers) to check for
      @param users: A comma separated list of user ids
      @param threshold: Return true if there are at least this many jobs that
                           match the criteria
      """
      params = [
                ("request", "existJobs"),
                ("jobTypes", ','.join([str(i) for i in jobTypes])),
                ("users", users),
                ("threshold", threshold)
               ]
      res = self.makeRequest(self.jobServer, parameters=params)
      if str(res).strip().lower() == "false":
         return False
      else:
         return True
   
   # .................................
   def _getVersionParts(self, versionString):
      major = 0
      minor = 0
      revision = 0
      status = "zzzz"
      vStr = versionString.strip().split(' ')

      if len(vStr) > 1:
         status = vStr[1]
      
      mmrList = vStr[0].split('.') # Split on '.'
      
      try: # If not all parts are specified, specifies as many as possible
         major = int(mmrList[0])
         minor = int(mmrList[1])
         revision = int(mmrList[2])
      except:
         pass
      
      return (major, minor, revision, status)
      
   # .................................
   def checkVersion(self):
      """
      @summary: Checks the version number of this client against those that 
                   are supported by the server.
      """
      res = self.makeRequest(LM_CLIENT_VERSION_URL, objectify=True)
      for client in res:
         if client.name == "lmCompute":
            minVersionStr = client.versions.minimum
            minVersion = self._getVersionParts(minVersionStr)
            myVersion = self._getVersionParts(self.__version__)
            
            if myVersion < minVersion:
               raise RemoteKillException("My version of client (%s) is no longer supported.  Minimum required = %s" % (myVersion, minVersion))
   
   # .................................
   def postJob(self, jobType, jobId, content, contentType, component):
      """
      @summary: Posts the results of a job to the job server
      @param jobType: The process type id of the job to post
      @param jobId: The id of the job to post
      @param content: The content of the post (this could be a file, string, etc)
      @param contentType: The mime-type of the content being posted
      @param component: What part of the job is being posted.  
                           Examples: package, model, projection
      """
      params = [
                ("request", "PostJob"),
                ("jobType", jobType),
                ("jobId", jobId),
                ("component", component)
               ]
      headers = {"Content-Type": contentType}
      body = content
      url = self.jobServer
      ret = self.makeRequest(url, method="POST", parameters=params, body=body, 
                             headers=headers, objectify=False)
      return ret

   # .................................
   def requestJobs(self, jobDir, jobTypes=JOB_TYPES.keys(), parameters={}, numJobs=1):
      """
      @summary: This is like requestJob, but instead of returning a job object,
                   it returns a list of jobs in xml format.
      @param jobDir: A directory to store the job xml files in
      @param jobTypes: A list of job types to request
      @param parameters: A dictionary of parameters to pass to the job server.
      @param numJobs: The number of jobs to request (response will be <= to 
                         this number)
      """
      params = [
                ("request", "GetJob"),
                ("jobTypes", ','.join([str(i) for i in jobTypes])),
                ("numJobs", numJobs)
               ]
      params.extend([(key, parameters[key]) for key in parameters.keys()])
      jobXmlFns = []
      try:
         ret = self.makeRequest(self.jobServer, parameters=params)
         self.log.debug("Len ret: %s" % str(len(ret)))
         jobStream = StringIO()
         jobStream.write(ret)
         jobStream.seek(0)
         
         self.log.debug("Reading zip file")
         with zipfile.ZipFile(jobStream, 'r', allowZip64=True) as z:
            self.log.debug("Namelist: %s" % str(z.namelist()))
            for zname in z.namelist():
               jXml = z.read(zname)
               try:
                  j = deserialize(fromstring(jXml)) # This will fail if not xml
                  self.updateJob(j.processType, j.jobId, JobStatus.PULL_COMPLETE, "1")
                  
                  # Write the xml
                  fn = os.path.join(jobDir, "%s-%s.xml" % (j.processType, j.jobId))
                  with open(fn, 'w') as xmlF:
                     xmlF.write(jXml)
                  
                  # Clear memory of job object
                  j = None
                  
                  jobXmlFns.append(fn)
               except Exception, e:
                  self.log.debug("Failed for %s because %s" % (zname, str(e)))
                  pass # Not xml
               jXml = None
         
      except urllib2.HTTPError, e:
         self.log.error(str(e))
         if e.code == 503:
            self.log.warning("No jobs are available that match request")
         elif e.code == 500:
            self.log.error("An unknown server error occurred")
         elif e.code == 403:
            self.log.error("Authentication error occurred")
         elif e.code == 404:
            self.log.error("The request job url was not found")
      return jobXmlFns

   # .................................
   def requestPost(self, jobType, jobId, component):
      """
      @summary: Asks the job server if the job results can be posted
      """
      params = [
                ("request", "RequestPost"),
                ("jobType", jobType),
                ("jobId", jobId),
                ("component", component)
               ]
      resp = self.makeRequest(self.jobServer, parameters=params)
      return bool(resp)
   
   # .................................
   def updateJob(self, jobType, jobId, status, progress):
      """
      @summary: Updates job status and progress metadata on the server
      @param jobType: The process type for this job
      @param jobId: The id of this job
      @param status: The job's status
      @param progress: The job's progress
      """
      params = [
                ("request", "UpdateJob"),
                ("jobType", jobType),
                ("jobId", jobId),
                ("status", status),
                ("progress", progress)
               ]
      return self.makeRequest(self.jobServer, method="POST", parameters=params)
   
   # .........................................
   def makeRequest(self, url, method="GET", parameters=[], body=None, 
                         headers={}, objectify=False):
      """
      @summary: Performs an HTTP request
      @param url: The url endpoint to make the request to
      @param method: (optional) The HTTP method to use for the request
      @param parameters: (optional) List of url parameters
      @param body: (optional) The payload of the request
      @param headers: (optional) Dictionary of HTTP headers
      @param objectify: (optional) Should the response be turned into an object
      @return: Response from the server
      """
      try:
         parameters = removeNonesFromTupleList(parameters)
         urlparams = urllib.urlencode(parameters)
         
         if body is None and len(parameters) > 0 and method.lower() == "post":
            body = urlparams
         else:
            url = "%s?%s" % (url, urlparams)
         req = urllib2.Request(url, data=body, headers=headers)
         ret = urllib2.urlopen(req)
         resp = ret.read()
         if objectify:
            return self.objectify(resp)
         else:
            return resp
      except urllib2.HTTPError, e:
         self.log.error("HTTPError on url: %s" % url)
         raise e
      except Exception, e:
         self.log.error("Exception on url: %s (%s)" % (url, str(e)))
         raise e
      
   # .........................................
   def objectify(self, xmlString):
      """
      @summary: Takes an XML string and processes it into a python object
      @param xmlString: The xml string to turn into an object
      @note: Uses LmAttList and LmAttObj
      @note: Object attributes are defined on the fly
      """
      try:
         return deserialize(fromstring(xmlString))
      except Exception, e:
         self.log.error(xmlString)
         raise e

# .............................................................................
def removeNonesFromTupleList(paramsList):
   """
   @summary: Removes parameter values that are None
   @param paramsList: List of parameters (name, value) [list of tuples]
   @return: List of parameters that are not None [list of tuples]
   """
   ret = []
   for param in paramsList:
      if param[1] is not None:
         ret.append(param)
   return ret

