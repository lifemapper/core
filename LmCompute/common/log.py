"""Module for Lifemapper logs in LmCompute
"""

import logging
import os

from LmCommon.common.log import LmLogger
from LmCompute.common.lmconstants import COMPUTE_LOG_PATH


# .............................................................................
class LmComputeLogger(LmLogger):
    """Compute loggers class

    Args:
        name: The name of this logger
        level: The lowest level to log at (anything below will be ignored)
        addConsole: (optional) Should a console logger be added
        addFile: (optional) Should a file logger be added
    """

    def __init__(self, name, level=logging.DEBUG, add_console=False,
                 add_file=False, log_filename=None):
        # In case level was set to None
        if level is None:
            level = logging.DEBUG
        LmLogger.__init__(self, name, level)
        if add_console:
            self._add_console_handler()
        if add_file:
            if log_filename is None:
                log_filename = os.path.join(
                    COMPUTE_LOG_PATH, '{}.log'.format(name))
            self._add_file_handler(log_filename)
