"""
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
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType, PUBLIC_ARCHIVE_NAME
from LmServer.common.localconstants import BOOM_PID_FILE
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger


# .............................................................................
class DaBoom(Daemon):
    """
    Class to run the Boomer as a Daemon process
    """

    # .............................
    def __init__(self, pidfile, configFname, priority=None):
        # Logfile
        secs = time.time()
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(self.__class__.__name__.lower(), timestamp)
        log = ScriptLogger(logname, level=logging.INFO)

        Daemon.__init__(self, pidfile, log=log)
        self.boomer = Boomer(configFname, log=log)

    # .............................
    def initialize(self):
        self.boomer.initializeMe()

    # .............................
    def run(self):
        print(('Running daBoom with configFname = {}'.format(self.boomer.configFname)))
        try:
            while self.boomer.keepWalken:
                self.boomer.processSpud()
        except Exception as e:
            self.log.debug('Exception {} on potato'.format(str(e)))
            tb = traceback.format_exc()
            self.log.error("An error occurred")
            self.log.error(str(e))
            self.log.error(tb)
        finally:
            self.log.debug('Daboom finally stopping')
            self.onShutdown()

    # .............................
    def onUpdate(self):
        self.log.info('Update signal caught!')

    # .............................
    def onShutdown(self):
        self.log.info('Shutdown!')
        # Stop walken the archive and saveNextStart
        self.boomer.close()
        Daemon.onShutdown(self)

    # ...............................................
    @property
    def logFilename(self):
        try:
            fname = self.log.baseFilename
        except:
            fname = None
        return fname


# .............................................................................
if __name__ == "__main__":
    if not is_lm_user():
        print(("Run this script as `{}`".format(LM_USER)))
        sys.exit(2)
    earl = EarlJr()
    defaultConfigFile = earl.createFilename(LMFileType.BOOM_CONFIG,
                                                         objCode=PUBLIC_ARCHIVE_NAME,
                                                         usr=PUBLIC_USER)
#     pth = earl.createDataPath(PUBLIC_USER, LMFileType.BOOM_CONFIG)
#     defaultConfigFile = os.path.join(pth, '{}{}'.format(PUBLIC_ARCHIVE_NAME,
#                                                                          LMFormat.CONFIG.ext))
    parser = argparse.ArgumentParser(
                description=('Populate a Lifemapper archive with metadata ' +
                                 'for single- or multi-species computations ' +
                                 'specific to the configured input data or the ' +
                                 'data package named.'))
    parser.add_argument('-', '--config_file', default=defaultConfigFile,
                help=('Configuration file for the archive, gridset, and grid ' +
                        'to be created from these data.'))
    parser.add_argument('cmd', choices=['start', 'stop', 'restart'],
                  help="The action that should be performed by the Boom daemon")

    args = parser.parse_args()
    configFname = args.config_file
    cmd = args.cmd.lower()

    print('')
    print(('Running daboom with configFilename={} and command={}'
            .format(configFname, cmd)))
    print('')
    boomer = DaBoom(BOOM_PID_FILE, configFname)

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

"""
"""
