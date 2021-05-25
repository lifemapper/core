"""Daemon for boom process
"""
import argparse
import logging
import sys
import time
import traceback

from LmBackend.common.daemon import Daemon
from LmCommon.common.lmconstants import LM_USER
from LmDbServer.boom.boomer import Boomer
from LmServer.base.utilities import is_lm_user
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import LMFileType, PUBLIC_ARCHIVE_NAME
from LmServer.common.localconstants import BOOM_PID_FILE, PUBLIC_USER
from LmServer.common.log import ScriptLogger


# .............................................................................
class DaBoom(Daemon):
    """Class to run the Boomer as a Daemon process."""
    # .............................
    def __init__(self, pidfile, config_fname, success_fname, priority=None):
        # Logfile
        secs = time.time()
        timestamp = "{}".format(
            time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(self.__class__.__name__.lower(), timestamp)
        log = ScriptLogger(logname, level=logging.INFO)

        Daemon.__init__(self, pidfile, log=log)
        self.boomer = Boomer(config_fname, success_fname, log=log)

    # .............................
    def initialize(self):
        """Initialize the Daemon."""
        self.boomer.initialize_me()

    # .............................
    def run(self):
        """Run the daemon."""
        print(
            'Running daBoom with config_fname = {}'.format(
                self.boomer.config_fname))
        try:
            while self.boomer.keep_walken:
                self.boomer.process_one_species()()
        except Exception as e:
            self.log.debug('Exception {} on potato'.format(str(e)))
            trace_back = traceback.format_exc()
            self.log.error("An error occurred")
            self.log.error(str(e))
            self.log.error(trace_back)
        finally:
            self.log.debug('Daboom finally stopping')
            self.on_shutdown()

    # .............................
    def on_shutdown(self):
        """Shutdown the daemon and related processes."""
        self.log.info('Shutdown!')
        # Stop walken the archive and saveNextStart
        self.boomer.close()
        Daemon.on_shutdown(self)

    # ...............................................
    @property
    def log_filename(self):
        """Return the log file name."""
        try:
            fname = self.log.baseFilename
        except AttributeError:
            fname = None
        return fname


# .............................................................................
def main():
    """Main method for script."""
    if not is_lm_user():
        print(("Run this script as `{}`".format(LM_USER)))
        sys.exit(2)
    earl = EarlJr()
    default_config_file = earl.create_filename(
        LMFileType.BOOM_CONFIG, obj_code=PUBLIC_ARCHIVE_NAME, usr=PUBLIC_USER)
    parser = argparse.ArgumentParser(
        description=(
            'Populate a Lifemapper archive with metadata for single- or '
            'multi-species computations specific to the configured input '
            'data or the data package named'))
    parser.add_argument(
        '-', '--config_file', default=default_config_file,
        help=('Configuration file for the archive, gridset, and grid to be '
              'created from these data'))
    parser.add_argument(
        'cmd', choices=['start', 'stop', 'restart'],
        help="The action that should be performed by the Boom daemon")

    args = parser.parse_args()
    config_fname = args.config_file
    cmd = args.cmd.lower()
    success_fname = './success'

    print('')
    print(
        'Running daboom with configFilename={} and command={}'.format(
            config_fname, cmd))
    print('')
    boomer = DaBoom(BOOM_PID_FILE, config_fname, success_fname)

    if cmd == 'start':
        boomer.start()
    elif cmd == 'stop':
        boomer.stop()
    elif cmd == 'restart':
        boomer.restart()
    elif cmd == 'status':
        boomer.status()
    else:
        print(("Unknown command: {}".format(cmd)))
        sys.exit(2)


# .............................................................................
if __name__ == '__main__':
    main()
