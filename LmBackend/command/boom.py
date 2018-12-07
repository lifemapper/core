"""
@summary: This module contains command classes for BOOM processes
@author: CJ Grady
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
from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import BOOM_SCRIPTS_DIR, CMD_PYBIN

# .............................................................................
class BoomerCommand(_LmCommand):
    """
    @summary: This command will run the boomer
    """
    relDir = BOOM_SCRIPTS_DIR
    scriptName = 'boomer.py'

    # ................................
    def __init__(self, configFile, successFile):
        """
        @summary: Construct the command object
        @param configFile: Configuration file for the boom run
        """
        _LmCommand.__init__(self)
        self.optArgs = ''
        if configFile is not None and successFile is not None:
            self.inputs.append(configFile)
            self.outputs.append(successFile)
            self.optArgs += ' --config_file={}'.format(configFile)
            self.optArgs += ' --success_file={}'.format(successFile)

    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run 
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.optArgs)


    
    