"""Module for Lifemapper server logging
"""

import logging
import os

from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.log import LmLogger
from LmServer.common.lmconstants import LOG_PATH, USER_LOG_PATH


# .............................................................................
class LmServerLogger(LmLogger):
    """Base class for server-side loggers.

    Args:
        name: The name of this logger
        level: The lowest level to log at (anything below will be ignored)
        add_console: (optional) Should a console logger be added
        add_file: (optional) Should a file logger be added
    """
    def __init__(self, name, level=logging.DEBUG, add_console=False,
                 add_file=False):
        LmLogger.__init__(self, name, level)
        if add_console:
            self._add_console_handler()
        if add_file:
            file_name = os.path.join(
                LOG_PATH, '{}{}'.format(name, LMFormat.LOG.ext))
            self._add_file_handler(file_name)


# .............................................................................
class WebLogger(LmServerLogger):
    """Log requests to the Lifemapper services."""
    def __init__(self, level=logging.DEBUG):
        LmServerLogger.__init__(self, 'web', level=level, add_file=True)


# .............................................................................
class ConsoleLogger(LmServerLogger):
    """Log output to the console."""
    def __init__(self, level=logging.DEBUG):
        LmServerLogger.__init__(self, 'console', level=level, add_console=True)


# .............................................................................
class ScriptLogger(LmServerLogger):
    """Log the events of a particular script."""
    def __init__(self, scriptName, level=logging.DEBUG):
        LmServerLogger.__init__(self, scriptName, level=level,
                                add_console=True, add_file=True)


# .............................................................................
class SolrLogger(LmServerLogger):
    """Log output from Lifemapper solr client tools."""
    def __init__(self, level=logging.DEBUG):
        LmServerLogger.__init__(self, 'lm_solr', level=level, add_file=True)


# .............................................................................
class UnittestLogger(LmServerLogger):
    """Log the results of a unit test."""
    def __init__(self, level=logging.DEBUG):
        name = 'unittest.%d' % os.getpid()
        LmServerLogger.__init__(self, name, level=level, add_console=True,
                                add_file=True)


# .............................................................................
class UserLogger(WebLogger):
    """Log information about a specific user's activities."""
    def __init__(self, user_id, level=logging.DEBUG):
        WebLogger.__init__(self, level=level)

        name = "user.%s" % user_id
        # Add user log file
        filename = os.path.join(
            USER_LOG_PATH, '{}{}'.format(name, LMFormat.LOG.ext))
        self._add_file_handler(filename)
