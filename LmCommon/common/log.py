"""Module for Lifemapper logging
"""
import logging
from logging.handlers import RotatingFileHandler
import os
import tempfile

from LmCommon.common.lmconstants import (
    LOG_DATE_FORMAT, LOG_FORMAT, LOGFILE_BACKUP_COUNT, LOGFILE_MAX_BYTES)


# .............................................................................
class LmLogger(logging.Logger):
    """Logging.logger wrapper
    """

    # ...............................
    def __init__(self, logger_name, level):
        """LmLogger constructor

        Args:
            logger_name (str): The name of the logger.  This will be used as
                the file name where the log will be written.
            level (int): The minimum log level to output.  Messages below this
                level will not be written.
        """
        logging.Logger.__init__(self, logger_name, level)
        self.formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)

    # ...............................
    def _add_console_handler(self):
        """Adds a log handler that outputs to the console
        """
        if not self._has_handler('stdout'):
            console_log_handler = logging.StreamHandler()
            console_log_handler.setLevel(self.level)
            console_log_handler.setFormatter(self.formatter)
            self.addHandler(console_log_handler)

    # ...............................
    def _add_file_handler(self, file_name):
        """Adds a log handler that outputs to a file.

        Args:
            file_name: The name fo the file to output to.
        """
        if not self._has_handler(file_name):
            file_log_handler = RotatingFileHandler(
                file_name, maxBytes=LOGFILE_MAX_BYTES,
                backupCount=LOGFILE_BACKUP_COUNT)
            file_log_handler.setLevel(self.level)
            file_log_handler.setFormatter(self.formatter)
            self.addHandler(file_log_handler)

    # ...............................
    def _has_handler(self, name):
        """Checks to see if a logger already has the specified handler

        Args:
            name (str): The name of the handler to look for.

        Returns:
            bool - Indicator if handler is already present.
        """
        for hdlr in self.handlers:
            if hdlr.stream.name == name:
                return True
        return False

    # ...............................
    @property
    def base_filename(self):
        """Return the first available handler base filename.
        """
        fname = None
        for hdlr in self.handlers:
            try:
                fname = hdlr.baseFilename
                return fname
            except Exception:
                pass


# .............................................................................
class DaemonLogger(LmLogger):
    """Fallback logger for deamon processes.
    """

    def __init__(self, pid, name=None, level=logging.DEBUG):
        if name:
            name = 'daemon.{}.{}'.format(name, pid)
        else:
            name = 'daemon.{}'.format(pid)
        LmLogger.__init__(self, name, level=level)
        file_name = os.path.join(tempfile.mkdtemp(), '{}.log'.format(name))
        self._add_file_handler(file_name)


# .............................................................................
class TestLogger(LmLogger):
    """Logger for tests
    """

    def __init__(self, script_name, level=logging.DEBUG):
        LmLogger.__init__(self, script_name, level=level)
        self._add_console_handler()
        file_name = os.path.join(
            tempfile.mkdtemp(), '{}.log'.format(script_name))
        self._add_file_handler(file_name)
