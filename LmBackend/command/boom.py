"""This module contains command classes for BOOM processes
"""
import os
import time

from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import BOOM_SCRIPTS_DIR
from LmCommon.common.lmconstants import LMFormat


# .............................................................................
class BoomerCommand(_LmCommand):
    """This command will run the boomer
    """
    relative_directory = BOOM_SCRIPTS_DIR
    script_name = 'boomer.py'

    # ................................
    def __init__(self, config_file_name, success_file_name):
        """Constructs the command object

        Args:
            config_file_name (str) : The file path to a BOOM config file.
            success_file_name (str) : The relative file path f
        @param config_file_name: Configuration file for the boom run
        """
        _LmCommand.__init__(self)
        if config_file_name is not None and success_file_name is not None:
            self.inputs.append(config_file_name)
            self.outputs.append(success_file_name)
            self.opt_args += ' --config_file={}'.format(config_file_name)
            self.opt_args += ' --success_file={}'.format(success_file_name)
