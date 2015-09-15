"""
@summary: Module containing classes for openModeller jobs
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

@todo: Raw model validation should check against the openModeller xsd
"""
from LmCommon.common.lmconstants import JobStage, ProcessType

from LmServer.base.lmobj import LMError
from LmServer.sdm.sdmJob import SDMModelJob, SDMProjectionJob

# .............................................................................
class OmModelJob(SDMModelJob):
   stage = JobStage.MODEL
   """
   @summary: Model job to be run by openModeller
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
      if model.algorithmCode == 'ATT_MAXENT':
         raise LMError('Model %s is not an openModeller job' % model.algorithmCode)
      SDMModelJob.__init__(self, model, processType=ProcessType.OM_MODEL,
                           computeId=computeId, lastHeartbeat=lastHeartbeat, 
                           createTime=createTime, jid=jid)


# .............................................................................
class OmProjectionJob(SDMProjectionJob):
   stage = JobStage.PROJECT
   """
   @summary: Projection job to be run by openModeller
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
      if projection.getModel().algorithmCode == 'ATT_MAXENT':
         raise Exception('Model %s is not an openModeller job' % 
                         projection.algorithmCode)
      SDMProjectionJob.__init__(self, projection, 
                                processType=ProcessType.OM_PROJECT, 
                                computeId=computeId, lastHeartbeat=lastHeartbeat, 
                                createTime=createTime, jid=jid)

      
