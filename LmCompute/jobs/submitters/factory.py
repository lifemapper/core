"""
@summary: This module contains a factory for creating instances of job 
             submitter objects based on the options specified in a 
             configuration object.
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
from LmCompute.common.localconstants import JOB_SUBMITTER_TYPE, JOB_CAPACITY, \
               LOCAL_SUBMIT_COMMAND, SGE_SUBMIT_COMMAND, SGE_COUNT_JOBS_COMMAND

# .............................................................................
def getJobSubmitterFromConfig():
   """
   @summary: Uses configured variables to instantiate correct job submitter
   """
   if JOB_SUBMITTER_TYPE.lower() == 'local':
      from LmCompute.jobs.submitters.localSubmitter import LocalSubmitter
      return LocalSubmitter(JOB_CAPACITY, LOCAL_SUBMIT_COMMAND)

   elif JOB_SUBMITTER_TYPE.lower() == 'cluster':
      from LmCompute.jobs.submitters.clusterSubmitter import ClusterSubmitter
      return ClusterSubmitter(JOB_CAPACITY, SGE_COUNT_JOBS_COMMAND, SGE_SUBMIT_COMMAND)
   
   else:
      raise Exception, "Unknown submitter type: %s" % JOB_SUBMITTER_TYPE

#    JOB_SUBMITTER_SECTION = "LmCompute - Job Submitter"
#    
#    capacity = config.getint(JOB_SUBMITTER_SECTION, 'CAPACITY')
#    jsType = config.get(JOB_SUBMITTER_SECTION, 'JOB_SUBMITTER_TYPE')
#    
#    if jsType.lower() == 'local':
#       from LmCompute.jobs.submitters.localSubmitter import LocalSubmitter
#       subJobCmd = config.get(JOB_SUBMITTER_SECTION, 'LOCAL_SUBMIT_COMMAND')
#       return LocalSubmitter(capacity, subJobCmd)
# 
#    elif jsType.lower() == 'cluster':
#       from LmCompute.jobs.submitters.clusterSubmitter import ClusterSubmitter
#       numJobsCmd = config.get(JOB_SUBMITTER_SECTION, 'NUM_JOBS_COMMAND')
#       subJobCmd = config.get(JOB_SUBMITTER_SECTION, 'SGE_SUBMIT_COMMAND')
#       return ClusterSubmitter(capacity, numJobsCmd, subJobCmd)
#    
#    else:
#       raise Exception, "Unknown submitter type: %s" % jsType
