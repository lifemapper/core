"""Tests for SDM BOOM jobs initiated by backend."""
import time

import lmtest.base.test_base as test_base

from LmCommon.common.lmconstants import LM_USER

from LmServer.base.utilities import is_lm_user
from LmServer.common.lmconstants import (SCRATCH_PATH, MATT_DAEMON_PID_FILE)
from LmServer.common.log import LmServerLogger
from LmServer.tools.matt_daemon import MattDaemon

# .....................................................................................
class MattDaemonRunningTest(test_base.LmTest):
    """Test whether Matt Daemon is running.  Start if it is not already."""

    # .............................
    def __init__(
        self,
        system_user,
        debug_log,
        debug_makeflows,
        debug_outputs,
        delay_time=0,
        delay_interval=3600
    ):
        """Construct the test."""
        test_base.LmTest.__init__(self, delay_time=delay_time)
        self.debug_log = debug_log
        self.debug_makeflows = debug_makeflows
        self.debug_outputs = debug_outputs
        self.user = system_user
        self.test_name = 'Matt Daemon test'

    # .............................
    def __repr__(self):
        """Return a string representation of this instance."""
        return self.test_name
    
    # .............................
    def _start_matt(self):
        mf_daemon = MattDaemon(
            MATT_DAEMON_PID_FILE, log=LmServerLogger("matt_daemon", add_console=True, add_file=True))
        # Set debugging
        mf_daemon.set_debug(
            self.debug_log, self.debug_makeflows, self.debug_outputs, SCRATCH_PATH)

        mf_daemon.start()

    # .............................
    def _get_matt_pid(self):
        pid = None
        try:
            with open(MATT_DAEMON_PID_FILE) as in_pid:
                pid = int(in_pid.read().strip())
        except IOError:
            pass
        except Exception as err:
            raise test_base.LmTestFailure(
                'Failed to read PID file: {}'.format(err)
            ) from err

        return pid
        
    # .............................
    def run_test(self):
        """Run the test."""
        if not is_lm_user():
            raise test_base.LmTestFailure(
                'Test matt_daemon_test must be run as user {}'.format(LM_USER)
            ) 

        matt_pid = self._get_matt_pid()
        if not matt_pid:
            self._start_matt()
            time.sleep(20)
            
            matt_pid = self._get_matt_pid()
            
        if not matt_pid:
            raise test_base.LmTestFailure('Failed to start matt_daemon: {}')

