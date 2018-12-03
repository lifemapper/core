"""Module containing classes and functions for subprocess management
"""
import errno
import multiprocessing
import os
import signal
from subprocess import Popen, PIPE
from time import sleep

from LmCommon.common.lmXml import serialize, tostring

CONCURRENT_PROCESSES = max(1, multiprocessing.cpu_count() - 2)
WAIT_TIME = 10
MAX_RUN_TIME = 60 * 60 * 24 # 24 hours

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
        elif err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True

# .............................................................................
class VariableContainer(object):
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
class SubprocessManager(object):
    """This class manages subprocesses
    """
    # .............................
    def __init__(self, commandList=[], maxConcurrent=CONCURRENT_PROCESSES):
        """
        Args:
            * commandList: A list of commands to run as subprocesses
            * maxConcurrent: (optional) The maximum number of subprocesses to 
                run concurrently
        """
        self.procs = commandList
        self.maxConcurrent = maxConcurrent
        self._runningProcs = []
    
    # .............................
    def addProcessCommands(self, commandList):
        """Adds a list of commands to the list to run 
        """
        self.procs.extend(commandList)
    
    # .............................
    def runProcesses(self):
        """Runs the processes in self.procs
        """
        while len(self.procs) > 0:
            numRunning = self.getNumberOfRunningProcesses()
            num = min((self.maxConcurrent - numRunning), len(self.procs))
            
            while num > 0:
                proc = self.procs.pop(0)
                self.launchProcess(proc)
                num = num - 1
            
            self.wait()
        
        while self.getNumberOfRunningProcesses() > 0:
            self.wait()
        
    # .............................
    def wait(self, waitSeconds=WAIT_TIME):
        """Waits the specified amount of time
        """
        sleep(waitSeconds)
    
    # .............................
    def launchProcess(self, cmd):
        """Launches a subprocess for the command provided
        """
        self._runningProcs.append(Popen(cmd, shell=True))
    
    # .............................
    def getNumberOfRunningProcesses(self):
        """Returns the number of running processes
        """
        numRunning = 0
        for idx in xrange(len(self._runningProcs)):
            if self._runningProcs[idx].poll() is None:
                numRunning = numRunning +1
            else:
                self._runningProcs[idx] = None
        self._runningProcs = filter(None, self._runningProcs)
        return numRunning

# .............................................................................
class SubprocessRunner(object):
    """This class manages a subprocess
    """
    # .............................
    def __init__(self, cmd, waitSeconds=WAIT_TIME, killTime=MAX_RUN_TIME):
        """Constructor for single command runner

        Args:
            * cmd: The command to run
            * waitSeconds: The number of seconds to wait between polls
            * killTime: The number of seconds to wait before killing the process
        """
        self.cmd = cmd
        self.waitTime = waitSeconds
        self.killTime = killTime
        self.myProc = None
    
    # .............................
    def run(self):
        """Run the command

        Returns:
            exit status code, standard error
        """
        stdErr = None
        self.myProc = Popen(
            self.cmd, shell=True, stderr=PIPE, preexec_fn=os.setsid)
        pid = self.myProc.pid
        self._wait()
        runTime = 0
        while self.myProc.poll() is None and \
                runTime < self.killTime and pid_exists(pid):
            self._wait()
            runTime += self.waitTime
            
        if runTime >= self.killTime:
            self.signal(signal.SIGTERM)
            raise Exception, 'Killed long running process'
            
        # Get output
        exitCode = self.myProc.poll()
        if self.myProc.stderr is not None:
            stdErr = self.myProc.stderr.read()
        return exitCode, stdErr
    
    # .............................
    def signal(self, signum):
        """Signal the running process
        """
        if self.myProc is not None:
            os.killpg(os.getpgid(self.myProc.pid), signum)
                
    # .............................
    def _wait(self):
        """Sleeps the specified amount of time
        """
        sleep(self.waitTime)
