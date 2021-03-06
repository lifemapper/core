"""
@summary: Module for Lifemapper logs in LmCompute
@author: CJ Grady
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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

from LmCommon.common.log import LmLogger

from LmCompute.common.lmconstants import COMPUTE_LOG_PATH

# .............................................................................
class LmComputeLogger(LmLogger):
   """
   @summary: This is the base class for Lifemapper LmCompute loggers.  We 
                will have more control of these loggers and can do more with 
                them such as retrieve reliable tracebacks and report errors, 
                etc.
   @param name: The name of this logger
   @param level: The lowest level to log at (anything below will be ignored)
   @param addConsole: (optional) Should a console logger be added
   @param addFile: (optional) Should a file logger be added
   """
   def __init__(self, name, level=logging.DEBUG, addConsole=False, 
                      addFile=False, logFilename=None):
      # In case level was set to None
      if level is None:
         level = logging.DEBUG
      LmLogger.__init__(self, name, level)
      if addConsole:
         self._addConsoleHandler()
      if addFile:
         if logFilename is not None:
            fn = logFilename
         else:
            fn = os.path.join(COMPUTE_LOG_PATH, '%s.log' % (name))
         self._addFileHandler(fn)

# .............................................................................
class TestLogger(LmComputeLogger):
   """
   @summary: This logger can be used with the job retriever to log job 
                retrieval actions
   """
   def __init__(self, scriptname, level=logging.DEBUG):
      LmComputeLogger.__init__(self, scriptname, level=level, addConsole=True, 
                               addFile=True)
