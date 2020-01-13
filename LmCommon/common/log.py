"""Module for Lifemapper logging
"""
import logging
from logging.handlers import RotatingFileHandler
import os
from types import ListType, TupleType
import sys
import traceback

from LmCommon.common.lmconstants import (LOG_DATE_FORMAT, LOG_FORMAT,
    LOGFILE_BACKUP_COUNT, LOGFILE_MAX_BYTES)

# TODO: send function name to logger for better info on source of problem
#         thisFunctionName = sys._getframe().f_code.co_name

class LmLogger(logging.Logger):
    """
    @summary: Logging.logger wrapper
    """
    # ..............................................
    def __init__(self, loggerName, level):
        """
        @summary: LmLogger constructor
        @param loggerName: The name of the logger.  This will be used as the 
                                     filename where the log will be written
        @param level: The minimum log level to output.  Messages below this level
                              will not be written
        """
        self.log = logging.getLogger(loggerName)
        self.log.setLevel(level)
        self.formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    
    # ..............................................
    def critical(self, message):
        """
        @summary: Wrapper around logging.Logger.critical
        @param message: The message to be output
        """
        try:
            self.log.critical(message)
        except Exception as e:
            pass
        
    # ..............................................
    def debug(self, message, location=None):
        """
        @summary: Wrapper around logging.Logger.debug
        @param message: The message to be output
        '{0}.{1} failed at line {2}; Exception: {3}'.
                        format(__name__, self.__class__.__name__, lineno(), str(e))
        """
        if location is not None:
#            message = '{0}: {1}'.format(location, message)
            message = '{}: {}'.format(location, message)
        try:
            self.log.debug(message)
        except Exception as e:
            pass
        
    # ..............................................
    def error(self, message):
        """
        @summary: Wrapper around logging.Logger.error
        @param message: The message to be output
        """
        try:
            self.log.error(message)
        except Exception as e:
            pass
    
    # ..............................................
    def exception(self, message):
        """
        @summary: Wrapper around logging.Logger.exception
        @param message: The message to be output
        """
        try:
            self.log.exception(message)
        except Exception as e:
            pass
    
    # ..............................................
    def info(self, message):
        """
        @summary: Wrapper around logging.Logger.info
        @param message: The message to be output
        """
        try:
            self.log.info(message)
        except Exception as e:
            pass
    
    # ..............................................
    def log(self, level, message):
        """
        @summary: Wrapper around logging.Logger.log
        @param level: The output level of the message
        @param message: The message to be output
        """
        try:
            self.log.log(level, message)
        except Exception as e:
            pass
        
    # ..............................................
    def warning(self, message):
        """
        @summary: Wrapper around logging.Logger.warning
        @param message: The message to be output
        """
        try:
            self.log.warning(message)
        except Exception as e:
            pass
        
    # ...............................................
    def reportError(self, msg):
        if type(msg) is ListType or type(msg) is TupleType:
            msg = ' '.join(msg)
        sysinfo = sys.exc_info()
        argStr = '\n'.join(str(arg) for arg in sysinfo[1].args)
        tb = sysinfo[2]
        if tb is not None:
            tbStr = '\n'.join(traceback.format_tb(tb))
        else:
            tbStr = str(sysinfo)
        errStr = '\n'.join([tbStr, argStr])
        self.error(msg)
        self.error(errStr)

    # ..............................................
    def _addConsoleHandler(self):
        """
        @summary: Adds a log handler that outputs to the console
        """
        try:
            if not self._hasHandler('<stderr>'):
                consoleLogHandler = logging.StreamHandler()
                consoleLogHandler.setLevel(self.log.level)
                consoleLogHandler.setFormatter(self.formatter)
                self.log.addHandler(consoleLogHandler)
        except:
            pass # Fails if the stream handler already exists

    # ..............................................
    def _addFileHandler(self, filename):
        """
        @summary: Adds a log handler that outputs to a file
        @param filename: The name of the file to output to
        """
        if not self._hasHandler(filename):
            fileLogHandler = RotatingFileHandler(filename, 
                                                             maxBytes=LOGFILE_MAX_BYTES, 
                                                             backupCount=LOGFILE_BACKUP_COUNT)
            fileLogHandler.setLevel(self.log.level)
            fileLogHandler.setFormatter(self.formatter)
            self.log.addHandler(fileLogHandler)
    
    # ..............................................
    def _hasHandler(self, name):
        """
        @summary: Checks to see if a logger already has the specified handler
        @param name: The name of the handler to look for
        @return: A boolean indicating if the handler was found
        """
        for x in self.log.handlers:
            try:
                if x.stream.name == name:
                    return True
            except:
                pass
        return False
    
    # ...............................................
    @property
    def baseFilename(self):
        fname = None
        for h in self.log.handlers:
            try:
                fname = h.baseFilename
                return fname
            except:
                pass
    
# .............................................................................
class DaemonLogger(LmLogger):
    """
    @summary: This is a fallback daemon logger class that can be used if no other
                     logger is provided to the daemon process.
    """
    def __init__(self, pid, name=None, level=logging.DEBUG):
        if name:
            name = 'daemon.{}.{}'.format(name, pid)
        else:
            name = 'daemon.{}'.format(pid)
        LmLogger.__init__(self, name, level=level)
        import tempfile
        fn = os.path.join(tempfile.mkdtemp(), '{}.log'.format(name))
        self._addFileHandler(fn)
