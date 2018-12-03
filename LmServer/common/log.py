"""
@summary: Module for Lifemapper server logging
@author: CJ Grady
@status: beta
@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

             Lifemapper Project, lifemapper [at] ku [dot] edu, 
             Biodiversity Institute,
             1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
    
             This program is free software; you can redistribute it and/or modify 
             it under the terms of the GNU General Public License as published by 
             the Free Software Foundation; either version 2 of the License, or (at 
             your option) any later version.
  
             This program is distributed in the hope that it will be useful, but 
             WITHOUT ANY WARRANTY; without even the implied warranty of 
             MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
             General Public License for more details.
  
             You should have received a copy of the GNU General Public License 
             along with this program; if not, write to the Free Software 
             Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
             02110-1301, USA.
"""

import logging
import os

from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.log import LmLogger

from LmServer.common.lmconstants import LOG_PATH, USER_LOG_PATH

# .............................................................................
class LmServerLogger(LmLogger):
    """
    @summary: This is the base class for Lifemapper server-side loggers.  We 
                     will have more control of these loggers and can do more with 
                     them such as retrieve reliable tracebacks and report errors, 
                     etc.
    @param name: The name of this logger
    @param level: The lowest level to log at (anything below will be ignored)
    @param addConsole: (optional) Should a console logger be added
    @param addFile: (optional) Should a file logger be added
    """
    def __init__(self, name, level=logging.DEBUG, addConsole=False, 
                             addFile=False):
        LmLogger.__init__(self, name, level)
        if addConsole:
            self._addConsoleHandler()
        if addFile:
            fn = os.path.join(LOG_PATH, '%s%s' % (name, LMFormat.LOG.ext))
            self._addFileHandler(fn)

# .............................................................................
class ConsoleLogger(LmServerLogger):
    """
    @summary: The console logger only logs output to the console
    """
    def __init__(self, level=logging.DEBUG):
        LmServerLogger.__init__(self, 'console', level=level, addConsole=True)

# .............................................................................
class DebugLogger(LmServerLogger):
    def __init__(self, level=logging.DEBUG):
        LmServerLogger.__init__(self, 'debug', level=level, addConsole=True, 
                                        addFile=True)

# .............................................................................
class JobMuleLogger(LmServerLogger):
    """
    @summary: The job mule logger is used by the job mule to log information 
                     about jobs requested and posted to the job server
    """
    def __init__(self, level=logging.DEBUG):
        LmServerLogger.__init__(self, 'jobMule', level=level, addFile=True)

# .............................................................................
class LmPublicLogger(LmServerLogger):
    """
    @summary: The web logger logs requests to the Lifemapper services
    @todo: Change the name of this to WebServiceLogger or WebLogger
    @note: The console logger has been removed.  This logger should be used by 
                 the web server and logging to the console won't be helpful
    """
    def __init__(self, level=logging.DEBUG):
        LmServerLogger.__init__(self, 'web', level=level, addFile=True)

# .............................................................................
class LuceneLogger(LmServerLogger):
    """
    @summary: The lucene logger is used by the Lifemapper Lucene process
    """
    def __init__(self, level=logging.DEBUG):
        LmServerLogger.__init__(self, 'lucene', level=level, addFile=True)

# .............................................................................
class MapLogger(LmServerLogger):
    """
    @summary: The map logger is used to log map requests
    @note: The console logger has been removed.  This logger is used by the web
                 server and logging to the console won't be helpful
    """
    def __init__(self, isDev=False):
        if isDev:
            level = logging.DEBUG
        else:
            level = logging.ERROR
        LmServerLogger.__init__(self, 'map', level=level, addFile=True)

# .............................................................................
class PipelineLogger(LmServerLogger):
    """
    @summary: The pipeline logger is used to log the events of a particular 
                     pipeline
    """
    def __init__(self, pipelineName, level=logging.DEBUG):
        name = 'pipeline.%s.%d' % (pipelineName, os.getpid())
        LmServerLogger.__init__(self, name, level=level, addConsole=True, 
                                        addFile=True)

# .............................................................................
class ScriptLogger(LmServerLogger):
    """
    @summary: The script logger is used to log the events of a particular script
    """
    def __init__(self, scriptName, level=logging.DEBUG):
        LmServerLogger.__init__(self, scriptName, level=level, addConsole=True, 
                                        addFile=True)

# .............................................................................
class ThreadLogger(LmServerLogger):
    """
    @summary: This thread logger logs the events of a specified script
    """
    def __init__(self, threadName, module='lm', level=logging.DEBUG):
        name = '%s.%s.%d' % (threadName, module, os.getpid())
        LmServerLogger.__init__(self, name, level=level, addConsole=True, 
                                        addFile=True)

# .............................................................................
class UnittestLogger(LmServerLogger):
    """
    @summary: The unit test logger logs the results of a unit test
    """
    def __init__(self, level=logging.DEBUG):
        name = 'unittest.%d' % os.getpid()
        LmServerLogger.__init__(self, name, level=level, addConsole=True, 
                                        addFile=True)

# .............................................................................
class UserLogger(LmPublicLogger):
    """
    @summary: The user logger logs information about a specific user's activities
    """
    def __init__(self, userId, level=logging.DEBUG):
        LmPublicLogger.__init__(self, level=level)
                
        name = "user.%s" % userId
        # Add user log file
        fn = os.path.join(USER_LOG_PATH, '%s%s' % (name, LMFormat.LOG.ext))
        self._addFileHandler(fn)
