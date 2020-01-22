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


# .............................................................................
class CatalogBoomCommand(_LmCommand):
    """Catalog boom command

    This command will create makeflows to:
        * catalog boom archive inputs,
        * catalog ScenarioPackage if necessary
        * create GRIMs,
        * create an archive ini file, and
        * start the Boomer to walk through inputs
    """
    relative_directory = BOOM_SCRIPTS_DIR
    script_name = 'initWorkflow.py'

    # ................................
    def __init__(self, config_filename, init_makeflow=False):
        """Construct the command object

        Args:
            config_filename: The file location of the ini file with parameters
                for a boom/gridset
        """
        _LmCommand.__init__(self)

        if not os.path.exists(config_filename):
            raise Exception(
                'Missing Boom configuration file {}'.format(config_filename))

        boom_base_name, _ = os.path.splitext(os.path.basename(config_filename))
        # Logfile goes to LOG_DIR
        secs = time.time()
        timestamp = '{}'.format(
            time.strftime('%Y%m%d-%H%M', time.localtime(secs)))
        logname = '{}.{}.{}'.format(
            self.script_basename, boom_base_name, timestamp)
        logfilename = '{}{}'.format(logname, LMFormat.LOG.ext)

        # Script args
        self.args = config_filename
        self.args += ' --logname={}'.format(logname)
        if init_makeflow:
            self.args += ' --init_makeflow=True'

        self.outputs.append(logfilename)
