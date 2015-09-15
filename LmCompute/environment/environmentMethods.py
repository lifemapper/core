"""
@summary: Contains the base environment class defining environment methods
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
# .............................................................................
class _EnvironmentMethods(object):
   """
   @summary: Base class for environment methods.  These methods are passed to a
                job runner and allow it to be portable since the environment 
                which it runs can be changed by passing in a different subclass
                of _EnvironmentMethods.
   """
   # ..................................
   def __init__(self, job):
      """
      @summary: Constructor
      @note: This must be overwritten in a sub class
      """
      pass
   
   # ..................................
   def createLink(self, fromPath, toPath):
      """
      @summary: Creates a link between two paths
      @param fromPath: The new file / directory
      @param toPath: The file / directory that it points to
      """
      raise Exception("createLink has not been implemented.")
   
   # ..................................
   def finalizeJob(self, jobType, jobId):
      """
      @summary: Finalizes a job and does any necessary cleanup
      """
      raise Exception("finalizeJob has not been implemented")
   
   # ..................................
   def getApplicationPath(self):
      """
      @summary: Gets the application path for this environment
      @return: The base path for applications in this environment
      @rtype: String
      """
      raise Exception("getApplicationPath has not been implemented.")
   
   # ..................................
   def getJob(self, jobTypes):
      """
      @summary: Gets a job for this environment
      @return: A job object
      """
      raise Exception("getJob has not been implemented")

   # ..................................
   def getJobDataPath(self):
      """
      @summary: Gets the job input data path for this environment
      @return: The base path for job input data in this environment
      @rtype: String
      """
      raise Exception("getJobDataPath has not been implemented.")
   
   # ..................................
   def getJobOutputPath(self):
      """
      @summary: Gets the job output data path for this environment
      @return: The base path for job output data in this environment
      @rtype: String
      """
      raise Exception("getJobOutputPath has not been implemented.")
   
   # ..................................
   def getLogger(self):
      """
      @summary: Gets a logger for this environment
      @return: A logger
      @rtype: Logger
      """
      raise Exception("getLogger has not been implemented")

   # ..................................
   def getPluginsPath(self):
      """
      @summary: Gets the path to the plugins directory
      @return: The base path for Lifemapper compute plugins
      @rtype: String
      """
      raise Exception("getPluginsPath has not been implemented")
   
   # ..................................
   def getPythonCmd(self):
      """
      @summary: Gets the command to run python for this machine
      @return: Path to the python executable
      @rtype: String
      """
      raise Exception("getPythonCmd has not been implemented")

   # ..................................
   def getTemporaryFilename(self, extension, base=None):
      """
      @summary: Gets a filename for a temporary file
      @return: A temporary filename
      @rtype: String
      """
      raise Exception("getTemporaryFilename has not been implemented")
   
   # ..................................
   def postJob(self, jobType, jobId, content, contentType, component):
      """
      @summary: Posts (part of) a job via the environment
      @param jobType: The type of job being posted
      @param jobId: The id of the job being posted
      @param content: The content of the post
      @param contentType: The MIME-type of the content
      @param component: The part of the job being posted (data, log, error, etc)
      """
      raise Exception("postJob has not been implemented")
   
   # ..................................
   def requestJob(self, validTypes=[], parameters={}):
      """
      @summary: Requests a job to run
      @param validTypes: A list of the job types this environment can process
      @param parameters: An optional dictionary of parameters specifying a 
                            subset of jobs that this environment is willing to 
                            compute
      """
      raise Exception("requestJob has not been implemented")

   # ..................................
   def updateJob(self, jobType, jobId, status, progress):
      """
      @summary: Updates the job status information in whatever manages it for
                   this environment
      @param status: The new status of the job
      @param progress: The new progress of the job
      @return: Value indicating success
      @rtype: Boolean
      """
      raise Exception("updateJob has not been implemented.")
   
   