"""This module contains the base class for Lifemapper commands.

These command objects can be used to run Lifemapper utilities or format
requests for Makeflow
"""
import os
import subprocess

from LmBackend.common.cmd import MfRule
from LmBackend.common.lmconstants import CMD_PYBIN
from LmServer.common.localconstants import APP_PATH


# .............................................................................
class _LmCommand:
    """A wrapper class for Lifemapper commands to run scripts

    Todo:
        * Consider optional input / output parameters on base object so that
            all commands can add to the input / output attributes with
            potentially unforeseen values
    """
    relative_directory = None
    script_name = None

    # ................................
    def __init__(self):
        self.inputs = []
        self.outputs = []
        self.args = ''
        self.opt_args = ''

    # ................................
    def call(self, **kwargs):
        """Wrapper around subprocess.call

        Named arguments sent to this function will be passed to subprocess.call
        """
        return subprocess.call(self.get_command(), **kwargs)

    # ................................
    def get_command(self):
        """Gets the raw command to run
        """
        return '{} {} {} {}'.format(
            CMD_PYBIN, self.get_script(), self.opt_args, self.args)

    # ................................
    def get_makeflow_rule(self, local=False):
        """Get a MfRule object for this command
        """
        cmd = '{local}{cmd}'.format(
            local='LOCAL ' if local else '', cmd=self.get_command())
        rule = MfRule(cmd, self.outputs, dependencies=self.inputs)
        return rule

    # ................................
    def get_script(self):
        """Gets the path to the script to run
        """
        return os.path.join(
            APP_PATH, self.relative_directory, self.script_name)

    # ................................
    @property
    def script_basename(self):
        """Script basename property
        """
        script_base, _ = os.path.splitext(self.script_name)
        return script_base
