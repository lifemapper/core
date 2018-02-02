"""
@summary: This module contains the base class for Lifemapper commands.  These
             command objects can be used to run Lifemapper utilities or format
             requests for Makeflow
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
import subprocess

from LmBackend.common.cmd import MfRule
from LmServer.common.localconstants import APP_PATH

# .............................................................................
class _LmCommand(object):
   """
   @summary: A wrapper class for Lifemapper commands to run scripts
   @todo: Consider optional input / output parameters on base object so that
             all commands can add to the input / output attributes with 
             potentially unforeseen values
   """
   relDir = None
   scriptName = None
   
   # ................................
   def __init__(self):
      self.inputs = []
      self.outputs = []
      
   # ................................
   def call(self, **kwargs):
      """
      @summary: Wrapper around subprocess.call, named arguments sent to this 
                   function will be passed to subprocess.call
      """
      return subprocess.call(self.getCommand(), **kwargs)
   
   # ................................
   def getCommand(self):
      """
      """
      raise Exception, 'Get command is not implemented in the base class'
   
   # ................................
   def getMakeflowRule(self, local=False):
      """
      @summary: Get a MfRule object for this command
      """
      cmd = '{local}{cmd}'.format(local='LOCAL ' if local else '', 
                                  cmd=self.getCommand())
      rule = MfRule(cmd, self.outputs, dependencies=self.inputs)
      return rule

   # ................................
   def getScript(self):
      """
      @summary: Get the path to the script to run
      """
      return os.path.join(APP_PATH, self.relDir, self.scriptName)
   # ................................
   def Popen(self, **kwargs):
      """
      @summary: Wrapper around subprocess.Popen, named arguments sent to this
                   function will be passed through
      """
      return subprocess.Popen(self.getCommand(), **kwargs)
