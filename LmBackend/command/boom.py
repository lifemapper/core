"""
@summary: This module contains command classes for BOOM processes
@author: CJ Grady
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import os

from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import BOOM_SCRIPTS_DIR, CMD_PYBIN

# .............................................................................
class BoomerCommand(_LmCommand):
   """
   @summary: This command will run the boomer
   """
   # ................................
   def __init__(self, configFile=None):
      """
      @summary: Construct the command object
      @param configFile: Configuration file for the boom run
      """
      _LmCommand.__init__(self)
      self.optArgs = ''
      if configFile is not None:
         self.inputs.append(configFile)
         self.optArgs += ' --config_file={}'.format(configFile)

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {}{}'.format(CMD_PYBIN, 
            os.path.join(BOOM_SCRIPTS_DIR, 'boomer.py'),
            self.optArgs)

# .............................................................................
class InitBoomCommand(_LmCommand):
   """
   @summary: This command will run the initboom script
   """
   # ................................
   def __init__(self, configFile=None, isFirstRun=False):
      """
      @summary: Construct the command object
      @param configFile: A configuration file to sue for the archive, gridset, 
                            and grid to be created
      @param isFirstRun: Compute multi-species matrix outputs for the matrices
      """
      _LmCommand.__init__(self)
      self.optArgs = ''
      if configFile is not None:
         self.inputs.append(configFile)
         self.optArgs += ' --config_file={}'.format(configFile)
      
      if isFirstRun:
         self.optArgs += ' --is_first_run'

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {}'.format(CMD_PYBIN, 
            os.path.join(BOOM_SCRIPTS_DIR, 'initboom.py'),
            self.optArgs)

