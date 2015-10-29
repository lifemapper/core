"""
@summary: This module contains functions that attempt to predict the resources needed for a job
@author: CJ Grady
@version: 1.0
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

# TODO: Move this to localconstants
BIG_JOB_QUEUE = "-q big.q"


# .............................................................................
def predictResourcesNeeded(job):
   """
   @summary: Predict the resources needed for this job.
   @param job: The job to predict needed resources for
   @note: This is a very crude prediction, this is the place where Yuan's 
             code could be inserted for better prediction
   """
   nodes = 1
   coresPerNode = 1
   memoryPerNode = "1024MB"
   walltime = "01:30:00"
   jobQueue = ""
   
   if int(job.processType) == ProcessType.ATT_MODEL:
      nodes = 1
      coresPerNode = 1
      
      try:
         if job.layers.large.lower() == "true":
            memoryPerNode = "4096MB"
            walltime = "04:00:00"
            jobQueue = BIG_JOB_QUEUE
         else:
            memoryPerNode = "1024MB"
            walltime = "00:30:00"
      except: # If large is not indicated, assume small
         memoryPerNode = "1024MB"
         walltime = "00:30:00"
   elif int(job.processType) == ProcessType.OM_MODEL:
      nodes = 1
      coresPerNode = 1

      try:
         if job.layers.large.lower() == "true":
            memoryPerNode = "4096MB"
            walltime = "04:00:00"
            jobQueue = BIG_JOB_QUEUE
         else:
            memoryPerNode = "1024MB"
            walltime = "01:30:00"
      except: # If large is not indicated, assume small
         memoryPerNode = "1024MB"
         walltime = "01:30:00"
   elif int(job.processType) == ProcessType.ATT_PROJECT:
      nodes = 1
      coresPerNode = 1
      
      try:
         if job.layers.large.lower() == "true":
            memoryPerNode = "4096MB"
            walltime = "04:00:00"
            jobQueue = BIG_JOB_QUEUE
         else:
            memoryPerNode = "1024MB"
            walltime = "01:30:00"
      except: # If large is not indicated, assume small
         memoryPerNode = "1024MB"
         walltime = "01:30:00"
   elif int(job.processType) == ProcessType.OM_PROJECT:
      nodes = 1
      coresPerNode = 1

      try:
         if job.layers.large.lower() == "true":
            memoryPerNode = "4096MB"
            walltime = "04:00:00"
            jobQueue = BIG_JOB_QUEUE
         else:
            memoryPerNode = "1024MB"
            walltime = "00:15:00"
      except: # If large is not indicated, assume small
         memoryPerNode = "1024MB"
         walltime = "00:15:00"
   elif int(job.processType) == ProcessType.RAD_INTERSECT:
      nodes = 1
      coresPerNode = 10
      memoryPerNode = "10GB"
      numLyrs = len(job.layerSet.layer)
      
      numChunks = 2 + (numLyrs / coresPerNode)
      totalMinutes = int(numChunks * 10 * 1.20)
      
      numHours = totalMinutes / 60
      numMinutes = totalMinutes % 60
      
      walltime = "%02d:%02d:00" % (numHours, numMinutes)
   elif int(job.processType) in [ProcessType.RAD_CALCULATE, 
                                 ProcessType.RAD_COMPRESS, 
                                 ProcessType.RAD_SWAP, 
                                 ProcessType.RAD_SPLOTCH]:
      nodes = 1
      coresPerNode = 1
      memoryPerNode = "4096MB"
      walltime = "04:00:00"

   return nodes, coresPerNode, memoryPerNode, walltime, jobQueue
