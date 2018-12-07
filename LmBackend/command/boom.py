"""This module contains command classes for BOOM processes
"""
from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import BOOM_SCRIPTS_DIR, CMD_PYBIN

# .............................................................................
class BoomerCommand(_LmCommand):
    """This command will run the boomer
    """
    relDir = BOOM_SCRIPTS_DIR
    scriptName = 'boomer.py'

    # ................................
    def __init__(self, configFile, successFile):
        """Constructs the command object

        Args:
            configFile (str) : The file path to a BOOM config file.
            successFile (str) : The relative file path f
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


    
    