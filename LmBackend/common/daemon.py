"""This module contains a base-Daemon class
"""
import atexit
import os
import signal
import subprocess
import sys
import time

from LmCommon.common.log import DaemonLogger

# ............................................................................. 
class DaemonCommands(object):
    """Class containing command constants
    """
    START = 'start'
    STOP = 'stop'
    RESTART = 'restart'
    
# ............................................................................. 
class Daemon(object):
    """A generic daemon class.

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
            if os.path.exists(pidfile):
                with open(pidfile) as in_pid:
                    pid = in_pid.read().strip()
            else:
                pid = 'unknown'
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
        except OSError as e:
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
        except OSError as e:
            self.log.error("fork #2 failed: %d (%s)" % (e.errno, e.strerror))
            sys.exit(1)
        
        # redirect standard file descriptors
        
        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)
         
    # .............................
    def delpid(self):
        """Final cleanup operation of removing the pid file
        """
        os.remove(self.pidfile)
 
    # .............................
    def _receiveSignal(self, sig_num, stack):
        """Handler used to receive signals

        Args:
            sig_num (int): The signal received
            stack: The stack at the time of the signal
        """
        if sig_num == signal.SIGUSR1:
            self.onUpdate()
            signal.signal(signal.SIGUSR1, self._receiveSignal) # Update signal
        elif sig_num == signal.SIGTERM:
            self.onShutdown()
        else:
            message = 'Unknown signal: {}'.format(sig_num)
            self.log.error(message)
    
    # ..........................................................................

    # ============================
    # = Daemon control functions =
    # ============================
    # .............................
    def start(self):
        """Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        
        if pid:
            msg = 'pidfile {} already exists. Deamon already running?'.format(
                self.pidfile)
            self.log.error(msg)
            print(msg)
            sys.exit(1)
        
        # Start the daemon
        self.daemonize()
        self.initialize()
        self.run()
    
    # .............................
    def stop(self):
        """Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pid = None
            with open(self.pidfile) as in_pid:
                pid = int(in_pid.read().strip())
        except IOError:
            pass
        
        if not pid:
            msg = 'pidfile {} does not exist. Daemon not running?'.format(
                self.pidfile)
            self.log.error(msg)
            return # not an error in a restart
        
        # Try killing the daemon process         
        try:
            max_time = 60  # Try to kill process for 60 seconds
            wait_time = 0
            while os.path.exists(self.pidfile) and wait_time < max_time:
                os.kill(pid, signal.SIGTERM)
                time.sleep(5)
                wait_time += 5
            if wait_time >= max_time and os.path.exists(self.pidfile):
                raise Exception('Could not kill process: {}'.format(pid))
        except OSError as os_err:
            err = str(os_err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                self.log.error(str(err))
                sys.exit(1)
    
    # .............................
    def restart(self):
        """Restart the daemon
        """
        self.stop()
        self.start()
    
    # .............................
    def status(self):
        """Check the status of the daemon
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
            info, err = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE).communicate()
            count = int(info.rstrip('\n'))
            
            if count == 1:
                msg = 'Status: Process {} is running at PID {}'.format(
                    self.__class__.__name__, pid)
            else:
                msg = ('Process {} is not running at PID {},'
                       'but lock file {} exists').format(
                           self.__class__.__name__, pid)
        else:
            msg = 'Process {} is not running'.format(self.__class__.__name__)
            
        self.log.info(msg)
        print(msg)
        sys.exit(1)

    # .............................
    def update(self):
        """Signal a running process to update itself.

        Send a signal to the running process to have it update itself. This
        update is defined by the subclass and could mean re-reading a
        configuration file, checking the environment, or something else.
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        
        if not pid:
            msg = 'pidfile {} does not exist.  Daemon not running?'.format(
                self.pidfile)
            self.log.error(msg)
            return # not an error in a restart
        
        # Try killing the daemon process         
        try:
            os.kill(pid, signal.SIGUSR1)
        except OSError as err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print(err)
                sys.exit(1)
    
    # ..........................................................................

    # ======================
    # = Subclass functions =
    # ======================
    # .............................
    def initialize(self):
        """This function should be used to initialize the daemon process
        """
        pass
    
    # .............................
    def onUpdate(self):
        """Do whatever is necessary to update the daemon
        """
        pass
    
    # .............................
    def onShutdown(self):
        """Perform a graceful shutdown operation of the daemon process.
        """
        self.keepRunning = False
    
    # .............................
    def run(self):
        """Main run method for a daemon process.
        """
        #while self.keepRunning:
            # do stuff
        pass
    
# .............................................................................
# Example usage
# .............................................................................
# class MyDaemon(Daemon):
#     # .............................
#     def initialize(self):
#         logging.basicConfig()
#     
#     # .............................
#     def run(self):
#         x = 0
#         while self.keepRunning:
#             self.log.debug("On x = %s" % x)
#             x += 1
#             sleep(1)
#         logging.debug("self.cont: %s" % self.cont)
#     
#     # .............................
#     def onUpdate(self):
#         logging.debug("Update signal caught!")
#         
#     # .............................
#     def onShutdown(self):
#         logging.debug("Shutdown signal caught!")
#         Daemon.onShutdown(self)
# .............................................................................
# if __name__ == "__main__":
#     
#     daemon = MyDaemon('/tmp/my-daemon.pid')
#     
#     if len(sys.argv) == 2:
#         if sys.argv[1].lower() == 'start':
#             daemon.start()
#         elif sys.argv[1].lower() == 'stop':
#             daemon.stop()
#         elif sys.argv[1].lower() == 'update':
#             daemon.update()
#         elif sys.argv[1].lower() == 'restart':
#             daemon.restart()
#         else:
#             print "Unknown command"
#             sys.exit(2)
#     else:
#         print "usage: %s start|stop|restart|update" % sys.argv[0]
#         sys.exit(2)
