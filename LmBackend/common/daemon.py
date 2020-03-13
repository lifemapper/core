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
class DaemonCommands:
    """Class containing command constants
    """
    START = 'start'
    STOP = 'stop'
    RESTART = 'restart'


# .............................................................................
class Daemon:
    """A generic daemon class.

    Usage: subclass the Daemon class and override the run() method
    """

    # .............................
    def __init__(self, pidfile, log=None):
        self.pidfile = pidfile
        # Default variable to indicate that the process should continue
        self.keep_running = True
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

        signal.signal(signal.SIGTERM, self._receive_signal)  # Stop signal
        signal.signal(signal.SIGUSR1, self._receive_signal)  # Update signal

    # ..........................................................................

    # ============================
    # = Daemon related functions =
    # ============================
    # .............................
    def daemonize(self):
        """Do the UNIX double-fork magic

        See:
            Stevens' "Advanced Programming in the UNIX Environment"
                (ISBN 0201563177)
                http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError as err:
            self.log.error(
                'Fork #1 failed: {} ({})'.format(err.errno, err.strerror))
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
        except OSError as err:
            self.log.error(
                'Fork #2 failed: {} ({})'.format(err.errno, err.strerror))
            sys.exit(1)

        # redirect standard file descriptors

        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        with open(self.pidfile, 'w') as pid_out_f:
            pid_out_f.write('{}\n'.format(pid))

    # .............................
    def delpid(self):
        """Final cleanup operation of removing the pid file
        """
        os.remove(self.pidfile)

    # .............................
    def _receive_signal(self, sig_num, stack):
        """Handler used to receive signals

        Args:
            sig_num (int): The signal received
            stack: The stack at the time of the signal
        """
        if sig_num == signal.SIGUSR1:
            self.on_update()
            # Update signal
            signal.signal(signal.SIGUSR1, self._receive_signal)
        elif sig_num == signal.SIGTERM:
            self.on_shutdown()
        else:
            message = 'Unknown signal: {}, stack: {}'.format(sig_num, stack)
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
            with open(self.pidfile, 'r') as pid_f:
                pid = int(pid_f.read().strip())
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
            return  # not an error in a restart

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
            with open(self.pidfile, 'r') as pid_f:
                pid = int(pid_f.read().strip())
        except IOError:
            pid = None

        if pid:
            cmd = 'ps -Alf | grep {} | grep -v grep | wc -l'.format(pid)
            info, _ = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE).communicate()
            count = int(info.rstrip('\n'))

            if count == 1:
                msg = 'Status: Process {} is running at PID {}'.format(
                    self.__class__.__name__, pid)
            else:
                msg = (
                    'Process {} is not running at PID {}, but {}'.format(
                        self.__class__.__name__, pid, 'lock file exists'))
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
            with open(self.pidfile, 'r') as pid_f:
                pid = int(pid_f.read().strip())
        except IOError:
            pid = None

        if not pid:
            msg = 'pidfile {} does not exist.  Daemon not running?'.format(
                self.pidfile)
            self.log.error(msg)
            return  # not an error in a restart

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

    # .............................
    def on_update(self):
        """Do whatever is necessary to update the daemon
        """

    # .............................
    def on_shutdown(self):
        """Perform a graceful shutdown operation of the daemon process.
        """
        self.keep_running = False

    # .............................
    def run(self):
        """Main run method for a daemon process.
        """
        # while self.keep_running:
        # do stuff
