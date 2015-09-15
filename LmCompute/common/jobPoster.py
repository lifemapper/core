"""
@summary: Module containing job poster class
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
          
          
===============================================================================
=                                                                             =
=                         This is not currently used!                         =
=                        I just don't want to lose it                         =
=                                                                             =
===============================================================================
          
          
"""

try: # So none of these imports break stuff until we decide to use this
   import concurrent.futures

   import threading
   from time import sleep
   import os
   import sys
   
   from LmCompute.common.jobClient import LmJobClient
   from LmCompute.common.localconstants import LOCKFILE_NAME, METAFILE_NAME
except:
   pass

USE_METHOD = 1

# .............................................................................   
def jobPosterThread(jobServer, jobsDirectory, numConcurrent):
   """
   @summary: This is a job poster thread.  It should stop when the main 
                application stops.  However, any data that is being posted at
                that time should be allowed to finish.
   @param jobServer: The job server end-point to post back to
   @param jobsDirectory: The directory containing job results sub-directories
   @param numConcurrent: Allow this many jobs to be posted at a time
   @note: This should be run as a daemon thread
   """
   # Establish job client
   jobCl = LmJobClient(jobServer)
   
   
   if USE_METHOD == 1:
      # Method 1: This method will be harder stop but will achieve greater 
      #              cocurrency when posting because the pool will be larger  
      #              and it won't wait for a group to be done to start the next
      while True: # Loop until main thread stops
         # Get list of all direct subdirectories
         dataDirs = next(os.walk(jobsDirectory))[1]
         
         with concurrent.futures.ProcessPoolExecutor(
                                        max_workers=numConcurrent) as executor:
            for ddir in dataDirs: # All sub directories
               executor.submit(postJobProcess, jobCl, ddir)
   else:
      # Method 2: This method will be easier to stop but will not post as much 
      #              data back at a time since each slot will wait for the  
      #              others to finish before starting another process
      # Loop until the main thread stops
      while True:
         # Get list of all direct subdirectories
         dataDirs = next(os.walk(jobsDirectory))[1]
         
         while len(dataDirs) > 0:
            # Select a subset of the directories to work on at a time
            workDirs = dataDirs[:numConcurrent]
            del dataDirs[:numConcurrent]
            
            with concurrent.futures.ProcessPoolExecutor(
                                        max_workers=numConcurrent) as executor:
               for ddir in workDirs:
                  executor.submit(postJobProcess, jobCl, ddir)
   

# .............................................................................
def postJobProcess(jc, jobDir):
   """
   @summary: Process to post job data.  This will be run in parallel to post 
                multiple jobs at once.  By executing it as a process instead of
                a thread, it will not be interrupted if the calling thread ends.
   @param jc: This is the job client object to use to post the job data.  It is
                 established in the calling thread and passed to each of the
                 process functions so it doesn't need to be created each time.
   @param jobDir: The directory containing data to post.  It will be checked to 
                     ensure that it is ready before posting back results.
   """
   # Look for lock file, if it is present, end
   if not os.path.exists(os.path.join(jobDir, LOCKFILE_NAME)):
      # Look for meta file
      if os.path.exists(os.path.join(jobDir, METAFILE_NAME)):
         with open(os.path.join(jobDir, METAFILE_NAME)) as metaIn:
            # For each file listed in the meta file
            for line in metaIn:
               # Split the line on commas
               procType, jobId, fn, contentType, component = line.strip().split(',')
               print "Posting:", procType, jobId, component
               jc.postJob(procType, jobId, open(fn).read(), contentType, component)
      else:
         print "No meta file for job dir", jobDir
   else: # Lockfile is present, end
      pass
      
# .............................................................................
def getPosterThreads(daemons=True):
   """
   @summary: Returns a list of threads that will post job results.  These are 
                determined by reading configuration file
   @param daemons: (optional) Boolean value indicating if these should be 
                                 daemon threads
   """
   posterThreads = []
   
   return posterThreads





# Need a main process, it should be converted into a daemon
# Need daemon threads that will determine what work to do as long as they are running and will sleep if nothing to do
# Need to run processes to do some simple task that should not be interrupted

# class MyApplication(object):
#    """
#    @summary: This is the main application that will be started
#    @note: Should be a Daemon subclass
#    """
#    def __init__(self, numDthreads):
#       self.numDthreads = numDthreads
#       
#    def run(self):
#       ts = []
#       for i in xrange(self.numDthreads):
#          ts.append(threading.Thread(name='thread-%s' % i, target=myDaemonThread, args=[i]))
#          ts[i].setDaemon(True)
#       
#       for t in ts:
#          t.start()
#       
#       while not os.path.exists('killfile'):
#          print "killfile doesn't exist yet"
#          sleep(30)
#       print "Killfile existed"
#       sys.exit(0)
      
      