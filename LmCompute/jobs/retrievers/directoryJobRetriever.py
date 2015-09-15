"""
@summary: This module contains a job retriever subclass that retrieves jobs 
             from static files in a directory
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
import glob
import os

from LmCompute.jobs.retrievers.base import JobRetriever

# .............................................................................
class DirectoryRetriever(JobRetriever):
   """
   @summary: This class will find all of the job files in a directory and make 
                them available to run.
   """
   PERSIST = False

   # .............................
   def __init__(self, jobDir):
      """
      @summary: Constructor
      @param jobDir: The directory containing job files
      """
      if not os.path.exists(jobDir):
         raise Exception, "Job directory %s does not exist" % jobDir

      self.jobsFns = glob.glob("%s/*" % jobDir)
   
   # .............................
   def getJobs(self, num):
      """
      @summary: Return this number of jobs
      """
      if num >= len(self.jobFns):
         ret = self.jobFns
         self.jobFns = []
         return ret
      else:
         ret = self.jobFns[:num]
         self.jobFns = self.jobFns[num:]
         return ret
   