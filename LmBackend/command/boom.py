"""This module contains command classes for BOOM processes
"""
import os
import time

from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import BOOM_SCRIPTS_DIR, CMD_PYBIN
from LmCommon.common.lmconstants import LMFormat

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


    
# .............................................................................
class CatalogBoomCommand(_LmCommand):
    """
    @summary: This command will create makeflows to:
                    * catalog boom archive inputs,
                    * catalog ScenarioPackage if necessary
                    * create GRIMs, 
                    * create an archive ini file, and 
                    * start the Boomer to walk through inputs
    """
    relDir = BOOM_SCRIPTS_DIR
    scriptName = 'initBoomJob.py'

    # ................................
    def __init__(self, config_filename, init_makeflow=False):
        """
        @summary: Construct the command object
        @param config_filename: The file location of the ini file 
                 with parameters for a boom/gridset
        """
        _LmCommand.__init__(self)
        
        if not os.path.exists(config_filename):
            raise Exception('Missing Boom configuration file {}'.format(config_filename))
        else:
            boomBasename, _ = os.path.splitext(os.path.basename(config_filename)) 
            # Logfile goes to LOG_DIR
            secs = time.time()
            timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}.{}'.format(self.scriptBasename, boomBasename, timestamp)
            logfilename = '{}{}'.format(logname, LMFormat.LOG.ext)
            
        # Script args
        self.args = config_filename
        self.args += ' --logname={}'.format(logname)
        if init_makeflow:
            self.args += ' --init_makeflow=True'
            
        self.outputs.append(logfilename)
            
    # ................................
    def getCommand(self):
        """
        @summary: Get the command
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)
