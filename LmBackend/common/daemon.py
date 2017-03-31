"""
@summary: This module contains a base-Daemon class
@author: (edited by) CJ Grady
@version: 1.0
@status: beta
@note: Originally from www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import sys, os, time, atexit
import signal
import subprocess

from LmCommon.common.log import DaemonLogger

# ............................................................................. 
class DaemonCommands(object):
   """
   @summary: Class containing command constants
   """
   START = 'start'
   STOP = 'stop'
   RESTART = 'restart'
   
# ............................................................................. 
class Daemon(object):
   """
   @summary: A generic daemon class.
   
   Usage: subclass the Daemon class and override the run() method
   """
   # .............................
   def __init__(self, pidfile, log=None):
      self.pidfile = pidfile
      # Default variable to indicate that the process should continue
      self.keepRunning = True 
      # Register signals
      if log is not None:
         self.log = log
      else:
         #TODO: Change this, the PID file doesn't exist when we are here
         pid = open(pidfile).read().strip()
         self.log = DaemonLogger(pid)
      
      signal.signal(signal.SIGTERM, self._receiveSignal) # Stop signal
      signal.signal(signal.SIGUSR1, self._receiveSignal) # Update signal

   # ..........................................................................

   # ============================
   # = Daemon related functions =
   # ============================
   # .............................
   def daemonize(self):
      """
      @summary: do the UNIX double-fork magic, see Stevens' "Advanced
            Programming in the UNIX Environment" for details (ISBN 0201563177)
            http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
      """
      try:
         pid = os.fork()
         if pid > 0:
            # exit first parent
            sys.exit(0)
      except OSError, e:
         self.log.error("Fork #1 failed: %d (%s)" % e.errno, e.strerror)
         sys.exit(1)
       
      # decouple from parent environment
      os.chdir("/")
      os.setsid()
      os.umask(0)
       
      # do second fork
      try:
         pid = os.fork()
         if pid > 0:
            # exit from second parent
            sys.exit(0)
      except OSError, e:
         self.log.error("fork #2 failed: %d (%s)" % (e.errno, e.strerror))
         sys.exit(1)
      
      # redirect standard file descriptors
      
      # write pidfile
      atexit.register(self.delpid)
      pid = str(os.getpid())
      file(self.pidfile,'w+').write("%s\n" % pid)
       
   # .............................
   def delpid(self):
      """
      @summary: This function does a final cleanup operation of removing the 
                   pid file
      """
      os.remove(self.pidfile)
 
   # .............................
   def _receiveSignal(self, sigNum, stack):
      """
      @summary: Handler used to receive signals
      @param sigNum: The signal received
      @param stack: The stack at the time of signal
      """
      if sigNum == signal.SIGUSR1:
         self.onUpdate()
         signal.signal(signal.SIGUSR1, self._receiveSignal) # Update signal
      elif sigNum == signal.SIGTERM:
         self.onShutdown()
      else:
         message = "Unknown signal: %s" % sigNum
         self.log.error(message)
   
   # ..........................................................................

   # ============================
   # = Daemon control functions =
   # ============================
   # .............................
   def start(self):
      """
      @summary: Start the daemon
      """
      # Check for a pidfile to see if the daemon already runs
      try:
         pf = file(self.pidfile,'r')
         pid = int(pf.read().strip())
         pf.close()
      except IOError:
         pid = None
      
      if pid:
         message = "pidfile %s already exist. Daemon already running?"
         self.log.error(message % self.pidfile)
         print message % self.pidfile
         sys.exit(1)
      
      # Start the daemon
      self.daemonize()
      self.initialize()
      self.run()
   
   # .............................
   def stop(self):
      """
      @summary: Stop the daemon
      """
      # Get the pid from the pidfile
      try:
         pf = file(self.pidfile,'r')
         pid = int(pf.read().strip())
         pf.close()
      except IOError:
         pid = None
      
      if not pid:
         message = "pidfile %s does not exist. Daemon not running?"
         self.log.error(message % self.pidfile)
         return # not an error in a restart
      
      # Try killing the daemon process       
      try:
         #TODO: Put in a maximum wait time or maximum tries to kill
         while os.path.exists(self.pidfile):
            os.kill(pid, signal.SIGTERM)
            time.sleep(3)
      except OSError, err:
         err = str(err)
         if err.find("No such process") > 0:
            if os.path.exists(self.pidfile):
               os.remove(self.pidfile)
         else:
            self.log.error(str(err))
            sys.exit(1)
   
   # .............................
   def restart(self):
      """
      @summary: Restart the daemon
      """
      self.stop()
      self.start()
   
   # .............................
   def status(self):
      """
      @summary: Check the status of the daemon
      """
      # Check for a pidfile to see if the daemon is running
      try:
         pf = file(self.pidfile,'r')
         pid = int(pf.read().strip())
         pf.close()
      except IOError:
         pid = None
      
      if pid:
         cmd = 'ps -Alf | grep {} | grep -v grep | wc -l'.format(pid)
         info, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
                                      stderr=subprocess.PIPE).communicate()
         count = int(info.rstrip("\n"))
         
         if count == 1:
            msg = "Status: Process {} is running at PID {}".format(
                                       self.__class__.__name__, pid)
         else:
            msg = "Process {} is not running at PID {}, but lock file {} exists"\
                  .format(self.__class__.__name__, pid)
      else:
         msg = "Process {} is not running".format(self.__class__.__name__)
         
      self.log.info(msg)
      print msg
      sys.exit(1)

   # .............................
   def update(self):
      """
      @summary: Send a signal to the running process to have it update itself.
                   This update is defined by the subclass and could mean
                   re-reading a configuration file, checking the environment,
                   or something else.
      """
      # Get the pid from the pidfile
      try:
         pf = file(self.pidfile,'r')
         pid = int(pf.read().strip())
         pf.close()
      except IOError:
         pid = None
      
      if not pid:
         message = "pidfile %s does not exist. Daemon not running?"
         self.log.error(message % self.pidfile)
         return # not an error in a restart
      
      # Try killing the daemon process       
      try:
         os.kill(pid, signal.SIGUSR1)
      except OSError, err:
         err = str(err)
         if err.find("No such process") > 0:
            if os.path.exists(self.pidfile):
               os.remove(self.pidfile)
         else:
            print(str(err))
            sys.exit(1)
   
   # ..........................................................................

   # ======================
   # = Subclass functions =
   # ======================
   # .............................
   def initialize(self):
      """
      @summary: This function should be used to initialize the daemon process
      """
      pass
   
   # .............................
   def onUpdate(self):
      """
      @summary: This function should be implemented in a subclass and should
                   do whatever is necessary to update the running daemon
      """
      pass
   
   # .............................
   def onShutdown(self):
      """
      @summary: This function should be implemented in a subclass and should
                   perform a graceful shutdown of the daemon process.
      """
      self.keepRunning = False
   
   # .............................
   def run(self):
      """
      @summary: You should override this method when you subclass Daemon. It 
                   will be called after the process has been daemonized by 
                   start() or restart().
      """
      #while self.keepRunning:
         # do stuff
      pass
   
# .............................................................................
# Example usage
# .............................................................................
# class MyDaemon(Daemon):
#    # .............................
#    def initialize(self):
#       logging.basicConfig()
#    
#    # .............................
#    def run(self):
#       x = 0
#       while self.keepRunning:
#          self.log.debug("On x = %s" % x)
#          x += 1
#          sleep(1)
#       logging.debug("self.cont: %s" % self.cont)
#    
#    # .............................
#    def onUpdate(self):
#       logging.debug("Update signal caught!")
#       
#    # .............................
#    def onShutdown(self):
#       logging.debug("Shutdown signal caught!")
#       Daemon.onShutdown(self)
# .............................................................................
# if __name__ == "__main__":
#    
#    daemon = MyDaemon('/tmp/my-daemon.pid')
#    
#    if len(sys.argv) == 2:
#       if sys.argv[1].lower() == 'start':
#          daemon.start()
#       elif sys.argv[1].lower() == 'stop':
#          daemon.stop()
#       elif sys.argv[1].lower() == 'update':
#          daemon.update()
#       elif sys.argv[1].lower() == 'restart':
#          daemon.restart()
#       else:
#          print "Unknown command"
#          sys.exit(2)
#    else:
#       print "usage: %s start|stop|restart|update" % sys.argv[0]
#       sys.exit(2)
