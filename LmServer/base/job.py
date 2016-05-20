"""
@summary: Module containing the base class definition for Job objects
@author: CJ Grady
@contact: cjgrady@ku.edu
@version: 0.1
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
"""
import mx.DateTime as DT

from LmCommon.common.lmconstants import JobStatus
from LmCommon.common.lmXml import serialize, tostring 

from LmServer.base.lmobj import LMError, LMAbstractObject, LMObject

# .............................................................................
class _JobData(LMObject):
   """
   @summary: The base class for JobData objects.  JobData objects are 
             containers for the data required by an application to 
             perform a computation.  They are instantiated only by the 
             JobMule (LM server side), sent to the ComputeResource, and
             deserialized there. They are subclassed by _JobObjects which adds
             information required by the server side database LMJob table, 
             for tracking readiness to be computed, compute status, progress.
   """

   # .........................................
   def __init__(self, obj, jid, processType, userid, objUrl, parentUrl, inputType=None):
      """
      @summary: Constructor
      @param obj: data object, generally a dictionary of key-value pairs,
                  used for primary calculations
      @param jid: The job database id 
      @param processType: Type of calculations to be performed on this object
      @param userid: user for this job/data
      @param objUrl: URL for requesting data/metadata for this object
      @param parentUrl: URL for requesting data/metadata for this object's parent
      @param inputType: Code (lmconstants.InputDataType) for input data required 
                        for this job
      """
      if self.__class__ == _JobData:
         raise LMError(["Abstract class _JobData should not be instantiated."])
      self._dataObj = obj
      # TODO: Migrate from jid to jobId as it is clearer when serialized
      self.jid = jid
      self.jobId = jid
      self.processType = processType
      self.inputType = inputType
      self.userId = userid
      self.objectUrl = objUrl
      self.parentUrl = parentUrl

   # .........................................
   def getId(self):
      """
      @summary: Returns the job id
      @return: Job id
      @rtype: integer
      """
      return self.jobId

# .............................................................................
# .............................................................................
# .............................................................................

# .............................................................................
class _Job(LMAbstractObject):
   """
   @summary: The abstract base class for Job objects.  Job objects are 
             containers for 1) Job information required by the JobMule (in 
             the Management module) which serves out jobs when they are ready; 
             and 2) data required by an application (in the Compute module) 
             to perform a computation (_JobData).
   """

   # .........................................
   def __init__(self, jobData, outputObj, objType, jobFamily=None, computeId=None, 
                email=None, status=None, statusModTime=None, priority=None, 
                lastHeartbeat=None, createTime=None, retryCount=0):
      """
      @summary: Constructor
      @param jobData: Lifemapper module for this job
      @param outputObj: new/existing data object receiving output calculations
      @param objType: type of object used receiving output calculations
      @param computeId: The db id of the Compute Resource working on this job
      @param email: user email to be used for notification
      @param status: status of this job
      @param statusModTime: timestamp of status change
      @param priority: priority of this job
      @param lastHeartbeat: timestamp of latest communication for this job on 
                            Compute Resource
      @param createTime: timestamp of creation time 
      @param retryCount: Number of times this job has been started 
                         (including the initial try)
      """
      if self.__class__ == _Job:
         raise LMError(["Abstract class _Job should not be instantiated."])
      self.jobData = jobData
      self.jobFamily = jobFamily
         
      # For LMJob table values
      self._outputObj = outputObj
      self._outputObjType = objType
      self.status = status
      self.statusModTime = statusModTime
      self.stageModTime = createTime
      self._computeResourceId = computeId
      self.priority = priority
      self.progress = None
      self.createTime = createTime   
      self.lastHeartbeat = lastHeartbeat
      self.email = email
      self.retryCount = retryCount
               
      self.pickledParts = []
      self.stringParts = ["node"]
      self.intParts = ["progress", "status"]
      self.defaultPart = "status"

   # .........................................
   def getId(self):
      """
      @summary: Returns the job id
      @return: Job id
      @rtype: integer
      """
      return self.jobData.jid
   
   # .........................................
   def setId(self, jid):
      """
      @summary: Sets the job id on the jobData object
      @param jid: Job id
      """
      self.jobData.jid = jid

# ...............................................
   @property
   def makeflowFilename(self):
      dloc = self._outputObj.createLocalDLocation(makeflow=True)
      return dloc

   # .........................................
   @property
   def processType(self):
      return self.jobData.processType
   
   # .........................................
   @property
   def inputType(self):
      return self.jobData.inputType

   # .........................................
   @property
   def dataObj(self):
      return self.jobData._dataObj
   
   # .........................................
   @property
   def outputObj(self):
      return self._outputObj
   
   def resetOutputObject(self, outobj):
      if type(self._outputObj) == type(outobj):
         self._outputObj = outobj
         
   def getUserId(self):
      return self._outputObj.getUserId()

   # .........................................
   @property  
   def outputObjType(self):
      return self._outputObjType
   
   # ....................................
   @property
   def metadataUrl(self):
      return self._outputObj.metadataUrl
   
   # .........................................
   @property  
   def computeResourceId(self):
      return self._computeResourceId

   # .........................................
   def postLocal(self):
      """
      @summary: Post locally
      """
      try:
         # Updates status
         self._postLocal()
         return True
      except Exception, e:
         print('Failed post: %s' % str(e))
         self.update(status=JobStatus.LM_PIPELINE_DISPATCH_ERROR)
         return False
      
   # ....................................
   def _getLocal(self):
      """
      @summary: Gets job status and progress from the cluster, sets these 
                attributes on the object.
      """
      try:
         stat = self._write()
      except Exception, e:
         print 'Error in _getLocal: %s' + str(e)
         stat = JobStatus.LM_PIPELINE_WRITEFILE_ERROR
      self.update(status=stat)

   # .........................................
   def serialize(self):
      """
      @note: This must be implemented by subclasses
      """
      return tostring(serialize(self.jobData))
      #raise LMError(['serialize must be implemented by subclass'])
   
   # .........................................
   def update(self, status=None, stage=None):
      """
      @summary Updates status (and stage) on radJob dataObject
      @param status: (optional) The new status for the object
      @param stage: (optional) The new processing stage for the object
      """
      currtime = DT.gmt().mjd
      self._outputObj.updateStatus(status, modTime=currtime, stage=stage)
      if status is not None:
         self.status = status
         self.statusModTime = currtime
         
   # .........................................
   def incrementRetry(self):
      self.retryCount += 1
      
   # .........................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      @note: Must be implemented in the subclass (for now)
      """
      raise LMError(["Write must be implemented by subclass"])


