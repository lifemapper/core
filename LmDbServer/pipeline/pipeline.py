"""
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
import faulthandler
import mx.DateTime
import os
import socket
import subprocess
import threading
from time import sleep
from types import ListType, TupleType

from LmServer.base.lmobj import LMObject, LMError
from LmServer.common.localconstants import APP_PATH, TROUBLESHOOTERS
from LmServer.common.lmconstants import LOG_PATH
from LmServer.common.log import ScriptLogger

from LmBackend.notifications.email import EmailNotifier

TIMEOUT = 30.0

# .............................................................................
class _Worker(LMObject, threading.Thread):

   def __init__(self, lock, pipelineName, 
                threadSuffix=None, developers=TROUBLESHOOTERS):
      thrdname = self.__class__.__name__.lower()
      if threadSuffix is not None:
         thrdname = thrdname + threadSuffix
         
      LMObject.__init__(self)
      threading.Thread.__init__(self, name=thrdname)
      self.developers = developers
      self.log = ScriptLogger(thrdname)
      self._lock = lock
      self.lockOwner = False
      self._sleeptime = 2
      self.hostname = socket.gethostname().lower()
      self.lastError = None
      self._pipelineKillFile = _Pipeline.getKillfilename(pipelineName)
      self.threadKillFile = os.path.join(APP_PATH, '%s_%s.die' 
                                                   % (pipelineName, thrdname))
      self.threadInjectFile = os.path.join(APP_PATH, '%s_%s.inject.txt' 
                                                      % (pipelineName, thrdname))
      if os.path.exists(self.threadKillFile):
         success, msg = self._deleteFile(self.threadKillFile)
         self.log.debug('Deleted %s (success=%s, %s)' % 
                        (self.threadKillFile, str(success), msg))

      
# ...............................................
   def _getLock(self):
      if self._lock is not None: 
         isLocked = self._lock.acquire()
         self.lockOwner = isLocked
         if not isLocked:
            self.log.debug('Tried unsuccessfully to get lock')
      else:
         raise LMError('Invalid lock: None')
      return isLocked
      
# ...............................................
   def _freeLock(self):
      if self._lock is not None: 
         if self._lock._is_owned():
            self._lock.release()
            self.lockOwner = False
            sleep(self._sleeptime)
      else:
         raise LMError('Invalid lock: None')
      
# ...............................................
   def _isLocked(self):
      if self._lock is not None: 
         return self._lock.locked()
      else:
         raise LMError('Invalid lock: None')
      
# ...............................................
   def _existKillFile(self):
      if (os.path.exists(self._pipelineKillFile) or 
          os.path.exists(self.threadKillFile)):
         return True
      else:
         return False
      
# ...............................................
   def signalKill(self, all=False, msg=None):
      if all:
         killfile = self._pipelineKillFile
      else:
         killfile = self.threadKillFile
      try:
         f = open(killfile, 'w')
         f.write('%s says DIE' % self.name)
         if msg:
            f.write(msg)
         f.close()
      except Exception, e: 
         print 'Failed to write %s: %s' % (killfile, str(e)) 
      
# ...............................................
   def _wait(self, waittime=None):
      if waittime is None:
         waittime = self._sleeptime
      timeslept = 0
      while timeslept <= waittime:
         if self._existKillFile():
            break
         else:
            sleep(self._sleeptime)
            timeslept += self._sleeptime
      
# ...............................................
   def _failGracefully(self, lmerr, allFail=True):
      self.lastError = lmerr
      logmsg = None

      if lmerr is not None:
         logmsg = str(lmerr)
         logmsg += '\n Error catch location: %s' % lmerr.location
         logmsg += '\n Traceback: %s' % lmerr.getTraceback()
         self.log.error(logmsg)
         self._notifyPeople('LM Pipeline error', logmsg)

      if not self._existKillFile():
         self.signalKill(all=allFail, msg=logmsg)
      
      if self._scribe.isOpen:
         try:
            self._getLock()
            self._scribe.closeConnections()
         except Exception, e:
            self.log.warning('Error while failing: %s' % str(e))
         finally:
            self._freeLock()
            
      if lmerr is not None:
         self._notifyPeople("The Lifemapper pipeline has failed", 
               'The %s Lifemapper pipeline has crashed with the message:\n\n%s' 
               % (self.hostname, str(lmerr)))
                     
      self.log.debug('Time: %s; gracefully exiting ...' % str(mx.DateTime.utc().mjd))

# ...............................................
   def _clearThread(self):
      if self._scribe.isOpen:
         try:
            self._getLock()
            self._scribe.closeConnections()
         except Exception, e:
            self.log.warning('Error while failing: %s' % str(e))
         finally:
            self._freeLock()
            
      self.log.info('Time: %s; Clearing thread %s ...' % (str(mx.DateTime.utc().mjd), 
                                                  self.name))

# ...............................................
   def _notifyPeople(self, subject, message, recipients=None):
      if recipients is None:
         recipients = self.developers
      elif not (isinstance(recipients, ListType) 
                or isinstance(recipients, TupleType)):
         recipients = [recipients]
      notifier = EmailNotifier()
      try:
         notifier.sendMessage(recipients, subject, message)
      except Exception, e:
         self.log.error('Failed to notify %s about %s' 
                        % (str(recipients), subject))

# ...............................................
   def _getNetworkInfo(self):
      """ find host network info for public interface """
      ip = network = cidr = iface = None
      cmd = "/opt/rocks/bin/rocks list host attr localhost | grep Kickstart_Public"
      info, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE).communicate()
      lines = info.split("\n")
      for line in lines:
         if len(line):
            parts = line.split()
            if parts[1]  == "Kickstart_PublicAddress": ip = parts[2]
            if parts[1]  == "Kickstart_PublicNetwork": network = parts[2]
            if parts[1]  == "Kickstart_PublicNetmaskCIDR": cidr = parts[2]
            if parts[1]  == "Kickstart_PublicInterface": iface = parts[2]

      if ip is None:
         print "Missing IP address for public interface "
         hostname = socket.gethostname().lower()
         if hostname == 'badenov':
            ip = '129.237.183.10'
         elif hostname == 'hera':
            ip = '129.237.201.240'
         elif hostname == 'juno':
            ip = '129.237.201.230'
      return (ip, network, cidr, iface)
            
# .............................................................................
class _Pipeline(LMObject):
   """
   The pipeline asks for jobs and then runs them.
   @precondition: Must setAlgorithm and setModelScenario before running 
             start.  May addProjectionScenario one or more times to project
             model onto alternate scenarios (besides the original 
             model scenario).
   """
       
   def __init__(self, name):
      """
      @summary Constructor for the pipeline class
      @param processid: process id for this application
      @param name: Pipeline name (gbif, bison, etc), used to construct killfile and 
                     faulthandler log file.
      @note: Presence of killfile indicates the pipeline should be 
             stopped.
      """
      LMObject.__init__(self)
      self.name = name
      self.log = ScriptLogger(self.name)
      pid = os.getpid()
      
      self._faultLogname = os.path.join(APP_PATH, LOG_PATH, 
                                 'fault.%s.pipeline.%d.log' % (self.name, pid))
      self._faultLogfile = open(self._faultLogname, 'w')
      faulthandler.enable(file=self._faultLogfile)

      self.lock = threading.RLock()
      self._killFile = self.getKillfilename(self.name)
      success, msg = self._deleteFile(self._killFile)

      self.workers = []
      self.ready = False
      if not success:
         self.log.debug(msg)
      else:
         self.log.debug('Deleted %s' % self._killFile)

      self.log.info("Pipeline starting at process id %d, with kill file %s" 
                    % (pid, self._killFile))

# ...............................................
   @staticmethod
   def getKillfilename(name):
      killfile = os.path.join(APP_PATH, LOG_PATH, 'pipeline.%s.die' % name)
      return killfile

# ...............................................
   def run(self):
      """
      @summary Runs the pipeline
      @note: check status of cluster job with url:
             http://biocast.net/rest/jobs.py/getStatus?id=<jobId#>
      """
      if self.ready:
         for thrd in self.workers:
            thrd.start()
      else:
         return 'Pipeline not initialized. Find error and try again.'

      finished = False
      while not finished and not(os.path.exists(self._killFile)):
         self.log.debug('********* Main loop, sleeping 20s ********* ') 
         sleep(20)
         if os.path.exists(self._killFile):
            self._killWorkers()
         else:
            self._killWorkers(onError=True)
         finished = self.cleanup()
#       self._faultLogfile.close()
            
# ...............................................
   def cleanup(self):
      finished = False
      for i in range(len(self.workers)):
         thrd = self.workers[0]
         if not thrd._scribe.isOpen:
            thrd.join(TIMEOUT)
         if not thrd.isAlive():
            self.workers.remove(thrd)
            del thrd
      if len(self.workers) == 0:
         self._faultLogfile.close()
         finished = True
      return finished
                  
# ...............................................
   @staticmethod
   def killPipeline(killfile):
      try:
         f = open(killfile, 'w')
         f.write('DIE')
         f.close()
      except Exception, e: 
         print 'Failed to write %s: %s' % (killfile, str(e)) 

# ...............................................
   def _killWorkers(self, onError=False):
      for i in range(len(self.workers)):
         thrd = self.workers[0]
         if ( (onError and thrd.lastError is not None) 
              or not onError
              or os.path.exists(thrd.threadKillFile) ):
            self.log.debug('Killing thread %s  ...' % thrd.name)
            self._killWorker(thrd)

         if not thrd.isAlive():
            self.workers.remove(thrd)
            del thrd
         
# ...............................................
   def _killWorker(self, thrd):
      # Try one more time ... 
      if thrd.isAlive():
         thrd.join(TIMEOUT)
         
