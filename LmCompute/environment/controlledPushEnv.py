"""
@summary: Contains the Lifemapper cluster environment class with a controlled 
             push mechanism
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

@note: This is just a subclass of LocalEnv that writes outputs to a directory
          rather than pushing back to the job server directly.
"""
import os
from uuid import uuid4

from LmCompute.common.localconstants import LOCKFILE_NAME, METAFILE_NAME, \
                                            PUSH_JOBS_DIR
from LmCompute.environment.localEnv import LocalEnv

# .............................................................................
class ControlledPushEnv(LocalEnv):
   """
   @summary: This class inherits from LocalEnv and overrides the post job 
                method so that job output is sent to a directory and it can be 
                pushed back by a separate process.
   """
   # ..................................
   def _getPushJobDirectory(self, jobType, jobId):
      """
      @summary: Gets the output location for job data so that it can be pushed
                   by an outside script
      """
      escapedServerDir = self.cl.jobServer.replace('http://', '').replace('https://', '').replace('/', '_').replace('?', '')
      return os.path.join(PUSH_JOBS_DIR, escapedServerDir, '%s-%s' % (jobType, jobId))

   # ..................................
   def _getLockfilePath(self, jobType, jobId):
      """
      @summary: Returns the lockfile file path
      """
      return os.path.join(self._getPushJobDirectory(jobType, jobId), LOCKFILE_NAME)
      
   # ..................................
   def finalizeJob(self, jobType, jobId):
      """
      @summary: Finalizes a job and does any necessary cleanup
      """
      print "Finalizing job"
      lockfile = self._getLockfilePath(jobType, jobId)
      print "Does log file,", lockfile, "exist?", os.path.exists(lockfile)
      if os.path.exists(lockfile):
         print "Removing lock file"
         os.remove(lockfile)
      else:
         print "Lockfile did not exist"
   
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
      # See if the output directory already exists, if not, create it and add 
      #    lock file
      print "Posting", jobType, jobId, contentType, component
      lockfile = self._getLockfilePath(jobType, jobId)
      outDir = self._getPushJobDirectory(jobType, jobId)
      print "Does log file,", lockfile, "exist?", os.path.exists(lockfile)
      if not os.path.exists(lockfile):
         os.makedirs(outDir)
         with open(lockfile, 'w') as f:
            f.write(str(os.getpid()))
            
      
      #print "Job type:", jobType
      #print jobType, jobId, contentType, component
      # Write file to directory
      fn = str(uuid4())
      fullPath = os.path.join(outDir, fn)
      with open(fullPath, 'w') as outFile:
         outFile.write(content)
         
      # Add entry to meta file
      with open(os.path.join(outDir, METAFILE_NAME), 'a') as metaFile:
         metaFile.write('%s,%s,%s,%s,%s\n' % (jobType, jobId, fn, contentType, component))
      
      return True
   
