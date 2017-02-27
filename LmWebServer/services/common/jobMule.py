"""
@summary: The jobMule module contains the class 'JobMule' that is used to 
             communicate with the server about job information
@author: CJ Grady
@version: 4.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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

@note: Formerly named JobManager
@todo: Move constants to constants module
"""
from random import choice
import StringIO
import zipfile


from LmCommon.common.lmconstants import JobStatus, HTTPStatus, ProcessType

from LmServer.base.lmobj import LmHTTPError, LMError
from LmServer.common.lmconstants import DbUser, JobFamily, RECOVERABLE_ERRORS
from LmServer.common.localconstants import PUBLIC_USER, TROUBLESHOOTERS
from LmServer.common.log import JobMuleLogger

from LmServer.db.scribe import Scribe

from LmServer.sdm.sdmJob import SDMProjectionJob

from LmWebServer.formatters.jobFormatter import JobFormatter

# ALL_USERS = 'all'

# @todo: This should be moved to a constants file or a config file
PROCESSES = {
             ProcessType.ATT_MODEL : {
                "job family" : JobFamily.SDM,
                "weight" : 1
             },
             ProcessType.ATT_PROJECT : {
                "job family" : JobFamily.SDM,
                "weight" : 2
             },
             ProcessType.OM_MODEL : {
                "job family" : JobFamily.SDM,
                "weight" : 1
             },
             ProcessType.OM_PROJECT : {
                "job family" : JobFamily.SDM,
                "weight" : 2
             },
             ProcessType.GBIF_TAXA_OCCURRENCE : {
                "job family" : JobFamily.SDM,
                "weight" : 1
             },
             ProcessType.BISON_TAXA_OCCURRENCE : {
                "job family" : JobFamily.SDM,
                "weight" : 1
             },
             ProcessType.IDIGBIO_TAXA_OCCURRENCE : {
                "job family" : JobFamily.SDM,
                "weight" : 1
             },
             ProcessType.USER_TAXA_OCCURRENCE : {
                "job family" : JobFamily.SDM,
                "weight" : 1
             },
             ProcessType.RAD_BUILDGRID : {
                "job family" : JobFamily.RAD,
                "weight" : 1
             },
             ProcessType.RAD_INTERSECT : {
                "job family" : JobFamily.RAD,
                "weight" : 10
             },
             ProcessType.RAD_COMPRESS : {
                "job family" : JobFamily.RAD,
                "weight" : 10
             },
             ProcessType.RAD_SWAP : {
                "job family" : JobFamily.RAD,
                "weight" : 10
             },
             ProcessType.RAD_SPLOTCH : {
                "job family" : JobFamily.RAD,
                "weight" : 10
             },
             ProcessType.RAD_GRADY : {
                "job family" : JobFamily.RAD,
                "weight" : 10
             },
             ProcessType.RAD_CALCULATE : {
                "job family" : JobFamily.RAD,
                "weight" : 10
             }
            }

# .............................................................................
class JobMule(object):
   """
   @summary: The JobServer class acts as a gate-keeper of sorts to handle jobs
                going in and out of the database and file system
   """
   # .........................................
   def __init__(self, developers=TROUBLESHOOTERS):
      """
      @summary: Constructor
      """
      self.developers = developers
      self.log = JobMuleLogger()
      self.scribe = Scribe(self.log, dbUser=DbUser.Job)
      self.scribe.openConnections()

   # .........................................
   def close(self):
      """
      @summary: Closes the connection to the database
      """
      self.scribe.closeConnections()
   
   # .........................................
   def areJobsAvailable(self, jobTypes, userIds=[], threshold=1):
      """
      @summary: Returns True if there are jobs available, False if not
      @summary: Returns the number of available jobs for each of the specified
                   job types
      @param jobTypes: List of desired job types (LmCommon.common.lmconstants.ProcessType)
      @param userIds: List of desired user ids (None for all)
      @param threshold: Minimum number requested
      """
      count = self.scribe.countJobs(jobTypes, JobStatus.INITIALIZE, userIdLst=userIds) 
      self.log.debug("Are jobs (%s) for %s available: %s" % (str(jobTypes), str(userIds), count))
      
      if count >= threshold:
         return True
      else:
         return False
   
   # .........................................
   def requestJobs(self, computeIP, count=1, 
                   processTypes=[ProcessType.ATT_MODEL, ProcessType.ATT_PROJECT,
                                 ProcessType.OM_MODEL, ProcessType.OM_PROJECT,
                                 ProcessType.GBIF_TAXA_OCCURRENCE, 
                                 ProcessType.BISON_TAXA_OCCURRENCE,
                                 ProcessType.USER_TAXA_OCCURRENCE], 
                   userIds=[], inputTypes=[]):
      """
      @summary: Requests a job packet to work on
      @param computeIP: Identifier for the ComputeResource requesting jobs
      @param count: The number of jobs to move from startStat to endStat 
                    and return. 
      @param processTypes: filter jobs by all acceptable required software; will 
                           take any if empty list; 
                           uses @type LmCommon.common.lmconstants.ProcessType
      @param userIds: filter jobs by all acceptable userIds; will take any if 
                      empty list
      @param inputTypes: filter jobs by all acceptable input data; will 
                         take any if empty list; 
                         uses @type LmCommon.common.lmconstants.InputDataType 
      @return: a list of Job packets
      """
      # Create a weighted list of choices
      weightedProcessTypes = []
      for pType in processTypes:
         weightedProcessTypes.extend(PROCESSES[pType]["weight"]*[pType])
      
      jobs = []
      
      # Repeat until a job can be found or there are no types left
      while len(jobs) < count and len(weightedProcessTypes) > 0:
         pTypes = [choice(weightedProcessTypes)]

         jobs.extend(self.scribe.pullJobs(count - len(jobs), computeIP, pTypes, userIds, 
                                  inputTypes))

         # Remove the type that didn't return a job (could have multiple entries)
         weightedProcessTypes = [x for x in weightedProcessTypes if x not in pTypes]
      
      
      if len(jobs) < 1:
         raise LmHTTPError(HTTPStatus.SERVICE_UNAVAILABLE, 
                           msg="No jobs need to be computed for your request")
      
      # Start zip file
      outStream = StringIO.StringIO()
      zf = zipfile.ZipFile(outStream, 'w', compression=zipfile.ZIP_DEFLATED, 
                              allowZip64=True)
      for j in jobs:
         fn = "%s-%s.xml" % (j.processType, j.getId())
         try:
            jf = JobFormatter(j)
            zf.writestr(fn, str(jf.format()))
         except Exception, e:
            self.log.error("Failed to add job to zip file: %s" % fn)
            self.log.error(str(e))
            j.status = JobStatus.MODEL_ERROR
            self.scribe.updateJobAndObjectStatus(j.jobFamily, j.getId(),
                                            computeIP, JobStatus.MODEL_ERROR, 0)
            
      zf.close()
      outStream.seek(0)
      return outStream.getvalue()
      
# ...............................................
   def _experimentComplete(self, job):
      complete = False
      if isinstance(job, (SDMProjectionJob)):
         #proj = job.projection
         proj = job.outputObj
         mdlId = proj.getModel().getId()
         unfinishedCount = self.scribe.countProjections(job.getUserId(), 
                                                        inProcess=True, 
                                                        mdlId=mdlId)
         if unfinishedCount == 0:
            # Take advantage of listing services returning results with last 
            #    modified first
            lastDoneId = self.scribe.listProjections(0, 10, userId=job.getUserId(), 
                                                             mdlId=mdlId)[0].id
            if lastDoneId == proj.getId():
               complete = True

      return complete
   
   # .........................................
   def updateJob(self, processType, jobId, status, computeIP=None, progress=None):
      """
      @summary: Updates the status / progress of a job in the database
      @note: Also serves as a "heartbeat" function of sorts so we know 
                something is still being worked on
      @param jobType: The ProcessType of job to be updated (since ids can be redundant)
      @param jobId: The id of the job to be updated
      @param computeName: The unique name of the ComputeResource running the job
      @param status: The new status of the job
      @param progress: The new progress of the job
      """
      if status in RECOVERABLE_ERRORS:
         status = JobStatus.INITIALIZE
         
      if PROCESSES.has_key(processType):
         jobFamily = PROCESSES[processType]['job family']
      else:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                           msg="Unknown process type: %s" % processType)

      # This will change to scribe.updateJob, which will also update job and
      # object, move dependent jobs (including Notifications), and  
      # write the mapfile on experiment completion
      ret = self.scribe.updateJobAndObjectStatus(jobFamily, jobId, 
                                                 computeIP, status, 
                                                 progress)

      # @todo: Handle this in scribe or via triggers
      if processType in [ProcessType.OM_PROJECT, ProcessType.ATT_PROJECT] \
             and status >= JobStatus.GENERAL_ERROR:
         job = self.scribe.getJob(jobFamily, jobId)
         expComplete = self._experimentComplete(job)
         if expComplete and job.getUserId() == PUBLIC_USER:
            self.scribe.updateExperimentMapfile(proj=job.outputObj)
      return ret

   # .........................................
   def postJob(self, processType, jobId, content, component, contentType=None):
      """
      @summary: Posts a job result to the database / file system
      @param processType: The type of job to be updated (lmconstants.ProcessType)
      @param jobId: The id of the job to be updated
      @param content: The content to save
      @param component: The type of the content (job specific such as error log, model, stats, etc)
      @contentType: The mime-type of the content
      @note: The package will be posted after the model or projection so we can
                wait until the package is posted before trying to delete a job
                or mark it as complete
      @note: The compute client should decide when the job is complete and job 
                deletion / movement of dependent jobs should take place in the 
                updateJob function or in some other call
      @todo: Make sure that content type and component come back?
      """
      try:
         if PROCESSES.has_key(processType):
            job = self.scribe.getJob(PROCESSES[processType]['job family'], jobId)
         else:
            raise LMError(["Unknown process type: %s" % str(processType)])
         job.write(component, content, None)
         self.scribe.updateJob(job)
         try:
            self.scribe.moveDependentJobs(job)
            expComplete = self._experimentComplete(job)
            if expComplete and job.getUserId() == PUBLIC_USER:
               self.scribe.updateExperimentMapfile(proj=job.dataObj)
         except LMError, e1:
            self.log.debug("-------------------------------------------------")
            self.log.debug("Error moving dependent jobs and checking if experiment is complete.  May not be a bug")
            self.log.debug(e1.getTraceback())
            self.log.debug(e1)
            self.log.debug("-------------------------------------------------")
      except Exception, e:
         e2 = LMError(e)
         self.log.debug(e2.getTraceback())
         self.log.debug(str(e))
         raise LmHTTPError(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))
   
   # .........................................
   def requestPost(self, jobType, jobId, responseType):
      """
      @summary: Asks if the computational unit can send back the result of a job
      @param jobType: The type of job to be updated (since ids can be redundant)
      @param jobId: The id of the job to be updated
      @param responseType: The type of content that the unit wants to post.  This could make a difference if the unit wants to post an error log or the actual data
      """
      return "True"
   
   
# .............................................................................
if __name__ == "__main__":
   # ..........................................................................
   userId = 'unitTest'
   scribe = Scribe(JobMuleLogger())
   scribe.openConnections()

   jbs = scribe.pullJobs(1, 'test', [ProcessType.GBIF_TAXA_OCCURRENCE], 
                         ['unitTest'], [None])
   
   print jbs[0].mdlname
   
   scribe.closeConnections()
