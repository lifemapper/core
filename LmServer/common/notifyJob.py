"""
@summary: Module containing the base class definition for Job objects
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

from LmCommon.common.lmconstants import JobStage, ProcessType

from LmServer.base.job import _Job, _JobData
from LmServer.base.lmobj import LMMessage

from LmServer.notifications.email import EmailNotifier

# .............................................................................
class NotifyJobData(_JobData):
   """
   @summary: Job class shared between SDM and RAD sub-systems for user 
             email notification 
   @note: Since job readiness depends on different object/stage completion, 
          depending on the notification type, object/object type will we will 
          send stage as a parameter.
   """
   # ....................................
   def __init__(self, email, jid, processType, userid, objUrl, parentUrl):
      """
      @summary: Email notification job constructor
      """
      subject = 'Lifemapper jobs complete'
      body =('All requested jobs for experiment %s (%s) are complete;' 
             % (parentUrl, objUrl) +
             'Visit the above URLs to retrieve data or error messages')
      msg = LMMessage(body, [email], subject=subject)
      _JobData.__init__(self, msg, jid, ProcessType.SMTP, userid, 
                       objUrl, parentUrl)
# .............................................................................
class NotifyJob(_Job):
   """
   @summary: Job class shared between SDM and RAD sub-systems for user 
             email notification 
   @note: Since job readiness depends on different object/stage completion, 
          depending on the notification type, object/object type will we will 
          send stage as a parameter.
   """
   # ....................................
   software = ProcessType.SMTP
   stage = JobStage.NOTIFY

   # ....................................
   def __init__(self, obj=None, objType=None, parentUrl=None, jobFamily=None, 
                computeId=None, email=None, status=None, statusModTime=None, 
                priority=None, lastHeartbeat=None, createTime=None, jid=None, 
                retryCount=None):
      """
      @summary: Email notification job constructor
      @copydoc LmServer.base.job._Job::__init__()
      """
      jobData = NotifyJobData(email, jid, ProcessType.SMTP, obj.getUserId(), 
                              obj.metadataUrl, parentUrl)
      _Job.__init__(self, jobData, obj, objType, 
                       jobFamily=jobFamily, computeId=computeId, 
                       status=status, statusModTime=statusModTime, 
                       priority=priority, lastHeartbeat=lastHeartbeat, 
                       createTime=createTime, retryCount=retryCount)

   # .........................................
   def update(self, status=None):
      """
      @summary Updates status on NotifyJob
      @param status: new status for the object
      """
      currtime = DT.gmt().mjd
      if status is not None:
         self.status = status
         self.statusModTime = currtime

   # .........................................
   def run(self):
      notifier = EmailNotifier()
      notifier.sendMessage(self.dataObj.toAddresses, self.dataObj.subject, self.dataObj.body)

