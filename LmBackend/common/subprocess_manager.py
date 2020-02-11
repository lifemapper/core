"""Module containing classes and functions for subprocess management
"""
import errno
import multiprocessing
import os
import signal
from subprocess import Popen, PIPE
from time import sleep

from LmCommon.common.lm_xml import serialize, tostring

CONCURRENT_PROCESSES = max(1, multiprocessing.cpu_count() - 2)
WAIT_TIME = 10
MAX_RUN_TIME = 60 * 60 * 24  # 24 hours


# .............................................................................
class LongRunningProcessError(Exception):
    """Exception indicating that the process took too long to run
    """


# .............................................................................
def pid_exists(pid):
    """Checks if a process is running with the specified process id

    Args:
        pid (int) : The process id to check for
    """
    if pid < 0:
        return False
    if pid == 0:
        # According to "man 2 kill" PID 0 refers to every process
        # in the process group of the calling process.
        # On certain systems 0 is a valid PID but we have no way
        # to know that in a portable fashion.
        raise ValueError('invalid PID 0')
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return False
        if err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True

        # According to "man 2 kill" possible error values are
        # (EINVAL, EPERM, ESRCH)
        raise
    else:
        return True


# .............................................................................
class VariableContainer:
    """Creates a container for variables to be passed to a subprocess
    """

    # .............................
    def __init__(self, values):
        self.values = values

    # .............................
    def __str__(self):
        return tostring(serialize(self))

    # .............................
    def __unicode__(self):
        return tostring(serialize(self))


# .............................................................................
class SubprocessManager:
    """This class manages subprocesses

    Todo:
        Determine if this is still used.
    """

    # .............................
    def __init__(self, command_list=None, max_concurrent=CONCURRENT_PROCESSES):
        """
        Args:
            command_list: A list of commands to run as subprocesses
            max_concurrent: (optional) The maximum number of subprocesses to
                run concurrently.
        """
        self.procs = [] if command_list is None else command_list
        self.max_concurrent = max_concurrent
        self._running_procs = []

    # .............................
    def add_process_commands(self, command_list):
        """Adds a list of commands to the list to run.
        """
        self.procs.extend(command_list)

    # .............................
    def run_processes(self):
        """Runs the processes in self.procs
        """
        while len(self.procs) > 0:
            num_running = self.get_running_proc_count()
            num = min((self.max_concurrent - num_running), len(self.procs))

            while num > 0:
                proc = self.procs.pop(0)
                self.launch_process(proc)
                num = num - 1

            self.wait()

        while self.get_running_proc_count() > 0:
            self.wait()

    # .............................
    @staticmethod
    def wait(wait_seconds=WAIT_TIME):
        """Waits the specified amount of time
        """
        sleep(wait_seconds)

    # .............................
    def launch_process(self, cmd):
        """Launches a subprocess for the command provided
        """
        self._running_procs.append(Popen(cmd, shell=True))

    # .............................
    def get_running_proc_count(self):
        """Returns the number of running processes
        """
        num_running = 0
        for idx in range(len(self._running_procs)):
            if self._running_procs[idx].poll() is None:
                num_running = num_running + 1
            else:
                self._running_procs[idx] = None
        self._running_procs = [_f for _f in self._running_procs if _f]
        return num_running


# .............................................................................
class SubprocessRunner:
    """This class manages a subprocess
    """

    # .............................
    def __init__(self, cmd, wait_seconds=WAIT_TIME, kill_time=MAX_RUN_TIME):
        """Constructor for single command runner

        Args:
            cmd: The command to run
            wait_seconds: The number of seconds to wait between polls
            kill_time: The number of seconds to wait before killing the process
        """
        self.cmd = cmd
        self.wait_time = wait_seconds
        self.kill_time = kill_time
        self.my_proc = None

    # .............................
    def run(self):
        """Run the command

        Returns:
            exit status code, standard error
        """
        std_err = None
        self.my_proc = Popen(
            self.cmd, shell=True, stderr=PIPE, preexec_fn=os.setsid)
        pid = self.my_proc.pid
        self._wait()
        run_time = 0
        while self.my_proc.poll() is None and \
                run_time < self.kill_time and pid_exists(pid):
            self._wait()
            # Get std err.  This bumps process in case of immediate fail of
            #    Maxent
            _ = self.my_proc.stderr.read()
            run_time += self.wait_time

        if run_time >= self.kill_time:
            self.signal(signal.SIGTERM)
            raise LongRunningProcessError(
                'Process took too long to run (> {} seconds)'.format(
                    self.kill_time))

        # Get output
        exit_code = self.my_proc.poll()
        if self.my_proc.stderr is not None:
            std_err = self.my_proc.stderr.read()
        return exit_code, std_err

    # .............................
    def signal(self, signum):
        """Signal the running process
        """
        if self.my_proc is not None:
            os.killpg(os.getpgid(self.my_proc.pid), signum)

    # .............................
    def _wait(self):
        """Sleeps the specified amount of time
        """
        sleep(self.wait_time)
