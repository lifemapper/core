"""
@summary: This module contains the base JobSubmitter class
@author: CJ Grady
@version: 3.0.0
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
from LmCommon.common.lmconstants import ProcessType

from LmCompute.common.resourcePredictor import predictResourcesNeeded

# .............................................................................
class JobSubmitter(object):
   """
   @summary: This is the JobSubmitter base class.  It defines the methods that
                should be implemented in the sub classes.
   """
   # .............................
   def __init__(self, capacity):
      """
      @summary: Base class constructor
      """
      self.capacity = capacity
      
   # .............................
   def getNumberOfEmptySlots(self):
      """
      @summary: This gets the number of jobs that can be submitted by 
                   subtracting the number of running jobs from the capacity of 
                   the submitter.
      """
      raise Exception, "getNumberOfEmptySlots should be implemented in subclass"
   
   # .............................
   def fillInSubmissionCommand(self, job, jobFn):
      """
      @summary: Fills in a job submission command
      @param job: The job object to use for information to fill in command
      @param jobFn: The filename containing the serialized job for calculation
      """
      cmd = self.submitCmd
      
      processName = "lmJob"
      pType = int(job.processType)
      if pType == ProcessType.ATT_MODEL:
         processName = "MEMDL"
      elif pType == ProcessType.ATT_PROJECT:
         processName = "MEPRJ"
      elif pType == ProcessType.OM_MODEL:
         processName = "OMMDL"
      elif pType == ProcessType.OM_PROJECT:
         processName = "OMPRJ"
      elif pType == ProcessType.RAD_BUILDGRID:
         processName = "RADBS"
      elif pType == ProcessType.RAD_INTERSECT:
         processName = "RADINT"
      elif pType == ProcessType.RAD_COMPRESS:
         processName = "RADCMP"
      elif pType == ProcessType.RAD_SWAP:
         processName = "RADSWP"
      elif pType == ProcessType.RAD_SPLOTCH:
         processName = "RADSPL"
      elif pType == ProcessType.RAD_CALCULATE:
         processName = "RADCALC"
      elif pType == ProcessType.GBIF_TAXA_OCCURRENCE:
         processName = "GBIF"
      elif pType == ProcessType.BISON_TAXA_OCCURRENCE:
         processName = "BISON"

      # Replace job file and name with job information
      jobName = "%s-%s" % (processName, job.jobId)
      cmd = cmd.replace('#JOB_NAME#', jobName)
      cmd = cmd.replace('#JOB_FILE#', jobFn)
      
      # Get predicted values
      nodes, coresPerNode, memoryPerNode, walltime, jobQueue = predictResourcesNeeded(job)
      
      # Replace resource strings in command with resource values
      cmd = cmd.replace('#NODES#', str(nodes))
      cmd = cmd.replace('#CORES_PER_NODE#', str(coresPerNode))
      cmd = cmd.replace('#MEMORY_PER_NODE#', str(memoryPerNode))
      cmd = cmd.replace('#WALLTIME#', str(walltime))
      cmd = cmd.replace('#JOB_QUEUE#', str(jobQueue))
      
      return cmd
   
   # .............................
   def submitJobs(self, jobs):
      """
      @summary: This function will submit the jobs specified
      @param jobs: A list of jobs to submit
      """
      raise Exception, "submitJobs should be implemented in subclass"
   
   # .............................
   def updateCapacity(self, newCapacity):
      """
      @summary: Updates the capacity of the job submitter
      """
      self.capacity = newCapacity
      