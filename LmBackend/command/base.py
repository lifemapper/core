"""This module contains the base class for Lifemapper commands.

These command objects can be used to run Lifemapper utilities or format
requests for Makeflow
"""
import os
import subprocess

from LmBackend.common.cmd import MfRule
from LmBackend.common.lmconstants import CMD_PYBIN, BACKEND_SCRIPTS_DIR
from LmServer.common.localconstants import APP_PATH

# .............................................................................
class _LmCommand(object):
    """A wrapper class for Lifemapper commands to run scripts

    Todo:
        * Consider optional input / output parameters on base object so that
            all commands can add to the input / output attributes with 
            potentially unforeseen values
    """
    relDir = None
    scriptName = None
    
    # ................................
    def __init__(self):
        self.inputs = []
        self.outputs = []
        # If these are missing or empty, command won't run
        self.required_inputs = []
        self.args = ''
        self.opt_args = ''
        
    # ................................
    def call(self, **kwargs):
        """Wrapper around subprocess.call

        Named arguments sent to this function will be passed to subprocess.call
        """
        return subprocess.call(self.getCommand(), **kwargs)
    
    # ................................
    def getCommand(self):
        """Gets the raw command to run
        """
        return '{} {} {} {}'.format(
            CMD_PYBIN, self.getScript(), self.opt_args, self.args)
    
    # ................................
    def getMakeflowRule(self, local=False):
        """Get a MfRule object for this command
        """
        cmd = '{local}{cmd}'.format(
            local='LOCAL ' if local else '', cmd=self.getCommand())
        rule = MfRule(cmd, self.outputs, dependencies=self.inputs)
        return rule

    # ................................
    #def getMakeflowRule(self, local=False):
    #    """
    #    @summary: Get a MfRule object for this command
    #    """
    #    wrapCmd = LmWrapperCommand(
    #        self.getCommand(), self.inputs, self.outputs, self.required_inputs)
    #    cmd = '{local}{cmd}'.format(local='LOCAL ' if local else '',
    #                                         cmd=wrapCmd.getCommand())
    #    rule = MfRule(cmd, self.outputs, dependencies=self.inputs)
    #    return rule

    # ................................
    def getScript(self):
        """Gets the path to the script to run
        """
        return os.path.join(APP_PATH, self.relDir, self.scriptName)
    
    # ................................
    @property
    def scriptBasename(self):
        scriptbase, _ = os.path.splitext(self.scriptName)
        return scriptbase
        
    # ................................
    def Popen(self, **kwargs):
        """Wrapper for subprocess.Popen

        Wrapper around subprocess.Popen, named arguments sent to this function
        will be passed through
        """
        return subprocess.Popen(self.getCommand(), **kwargs)

# ............................................................................
class LmWrapperCommand(_LmCommand):
    """This command wraps a command and ensures that it always creates the
    specified outputs

    Todo:
        * Make this a base class that we can inherit from
    """
    relDir = BACKEND_SCRIPTS_DIR
    scriptName = 'lm_wrapper.py'

    # ................................
    def __init__(self, wrap_command, inputs, outputs, required_inputs):
        """Construct the command object
        """
        _LmCommand.__init__(self)
        
        optArgs = ''
        for fn in required_inputs:
            optArgs += '-i {} '.format(fn)
        self.args = '{}"{}" {}'.format(optArgs, wrap_command, ' '.join(outputs))
        self.outputs = outputs
        
        self.inputs = inputs
            
    # ................................
    def getCommand(self):
        """Gets the concatenate matrices command
        """
        cmd = '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)
        return cmd
