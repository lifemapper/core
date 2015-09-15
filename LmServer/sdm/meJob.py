"""
@summary: Module containing classes for MaxEnt jobs
@author: CJ Grady
@version: 1.0
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
from LmCommon.common.lmconstants import JobStage, ProcessType

from LmServer.base.lmobj import LMError
from LmServer.sdm.sdmJob import SDMModelJob, SDMProjectionJob

# .............................................................................
class MeModelJob(SDMModelJob):
   stage = JobStage.MODEL
   """
   @summary: Model job that should be run by MaxEnt
   """
   def __init__(self, model, computeId=None, lastHeartbeat=None, 
                createTime=None, jid=None):
      """
      @summary: Model job constructor
      @param model: Lifemapper model object that contains parameters for the
                       model to be generated.
      @param pid: (optional) The id of the pipeline that initiated this job.
      @param jid: (optional) The unique job id, currently the same as the modelId 
      """
      if model.algorithmCode != 'ATT_MAXENT':
         raise LMError('Model %s is not an MaxEnt job' % model.algorithmCode)
      SDMModelJob.__init__(self, model, processType=ProcessType.ATT_MODEL, 
                           computeId=computeId, lastHeartbeat=lastHeartbeat, 
                           createTime=createTime, jid=jid)


# .............................................................................
class MeProjectionJob(SDMProjectionJob):
   software = ProcessType.ATT_PROJECT
   stage = JobStage.PROJECT
   """
   @summary: Projection job that should be run by MaxEnt
   """
   def __init__(self, projection, computeId=None, 
                lastHeartbeat=None, createTime=None, jid=None):
      """
      @summary: Projection job constructor
      @param projection: Lifemapper projection object containing parameters
                            for the projection to be generated.
      @param pid: (optional) The id of the pipeline that initiated this job.
      @param jid: (optional) The unique job id, currently the same as the projectionId 
      """
      if projection.getModel().algorithmCode != 'ATT_MAXENT':
         raise LMError('Model %s is not an MaxEnt job' % 
                         projection.getModel().algorithmCode)
      SDMProjectionJob.__init__(self, projection, 
                                processType=ProcessType.ATT_PROJECT,
                                computeId=computeId, 
                                lastHeartbeat=lastHeartbeat, 
                                createTime=createTime, jid=jid)
