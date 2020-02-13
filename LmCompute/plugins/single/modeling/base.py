"""This module contains a base class for modeling software

Todo:
    * Use a logger.
    * Add code to check outputs.
    * Alphabetize functions.
    * Fill in header.
    * Fill in return for model and projection.
    * Collect snippets.
"""
import os
import shutil
import time
import zipfile

from LmBackend.common.layer_tools import process_layers_json
from LmBackend.common.lmobj import LMError
from LmBackend.common.metrics import LmMetricNames, LmMetrics
from LmBackend.common.subprocess_manager import (
    LongRunningProcessError, SubprocessRunner)
from LmCommon.common.lmconstants import JobStatus, LMFormat, ONE_HOUR_SECONDS
from LmCommon.common.ready_file import ready_filename
from LmCompute.common.log import LmComputeLogger


# .............................................................................
class ModelSoftwareWrapper:
    """Base class for modeling software wrappers
    """
    LOGGER_NAME = 'model_tool'  # Subclasses should change this
    RETRY_STATUSES = []

    # ...................................
    def __init__(self, work_dir, species_name, logger=None):
        """Constructor for wrapper

        Args:
            work_dir : A directory to perform the work.
            species_name : The name of the species the computations are for.
            logger : Optional. A logger to use when performing the computation.
        """
        self.work_dir = work_dir
        self.metrics = LmMetrics(None)
        self.snippets = []
        self.species_name = species_name

        # If work directory does not exist, create it
        if not os.path.exists(self.work_dir):
            os.makedirs(self.work_dir)

        if logger is None:
            self.logger = LmComputeLogger(self.LOGGER_NAME, add_console=True)
        else:
            self.logger = logger

    # ...................................
    def _build_command(self, tool, option_list):
        """Builds a command to run the tool

        Args:
            tool : Binary (or decorated binary) for the software tool to run.
            option_list : A list of options to send to the tool.
        """
        cmd = '{tool} {options}'.format(
            tool=tool, options=' '.join(option_list))
        self.logger.debug('Command: "{}"'.format(cmd))
        return cmd

    # ...................................
    def _check_projection(self):
        pass

    # ...................................
    def _check_model(self):
        pass

    # ...................................
    def _check_outputs(self):
        pass

    # ...................................
    @staticmethod
    def _find_error(std_err):
        raise LMError('_find_error must be implemented in subclasses')

    # ...................................
    @staticmethod
    def _process_layers(layer_json, layer_dir=None):
        """Read the layer JSON and process the layers accordingly

        Args:
            layer_json: JSON string with layer information.
            layer_dir: If present, create sym links in the directory to the
                layers.
        """
        lyrs = process_layers_json(layer_json, sym_dir=layer_dir)
        return lyrs

    # ...................................
    def _run_tool(self, cmd, num_tries=1):
        """Runs the tool

        Args:
            cmd : The command to run
            num_tries : The number of times to try this command
        """
        tries_left = max([num_tries, 1])
        try:
            cont = True
            while cont:
                cont = False
                tries_left -= 1
                spr = SubprocessRunner(cmd, kill_time=ONE_HOUR_SECONDS)
                start_time = time.time()
                proc_exit_status, proc_std_err = spr.run()
                end_time = time.time()
                self.metrics.add_metric(
                    LmMetricNames.RUNNING_TIME, end_time - start_time)
                status = JobStatus.COMPUTED

                if proc_exit_status > 0:
                    status = self._find_error(proc_std_err)

                    if status in self.RETRY_STATUSES:
                        self.logger.debug(
                            'Status is: {}, retries left: {}'.format(
                                status, tries_left))
                        if tries_left > 0:
                            cont = True
        except LongRunningProcessError:
            status = JobStatus.LM_LONG_RUNNING_JOB_ERROR
        except LMError:
            status = JobStatus.GENERAL_ERROR

        # Get size of output directory
        self.metrics.add_metric(
            LmMetricNames.OUTPUT_SIZE,
            self.metrics.get_dir_size(self.work_dir))
        self.metrics.add_metric(LmMetricNames.STATUS, status)

    # ...................................
    def clean_up(self):
        """Deletes work directory
        """
        shutil.rmtree(self.work_dir)

    # ...................................
    @staticmethod
    def create_model(points, layer_json, parameters_json, mask_filename=None,
                     crs_wkt=None):
        """Creates a model

        Args:
            points : A list of (local_id, x, y) point tuples.
            layer_json : Climate layer information in a JSON document.
            mask_filename : If provided, use this layer as a mask for the
                model.
            crs_wkt : Well-Known text describing the map projection of the
                points.

        Note:
            * Do not use this version, use a subclass.

        Raises:
            * Exception : This version should not be called, use a subclass.
        """
        raise Exception('Not implemented in base class')

    # ...................................
    @staticmethod
    def create_projection(ruleset_filename, layer_json, parameters_json=None,
                          mask_filename=None):
        """Creates a projection.

        Args:
            ruleset_filename : The file path to a previously created ruleset.
            layer_json : Climate layer information in a JSON document.
            parameters_json : Algorithm parameters in a JSON document.
            mask_filename : If provided, this is a file path to a mask layer
                to use for the projection.

        Note:
            * Do not use this version, use a subclass.

        Raises:
            * Exception : This version should not be called, use a subclass.
        """
        raise Exception('Not implemented in base class')

    # ...................................
    @staticmethod
    def get_ruleset_filename():
        """Return the ruleset filename generated by the model

        Note:
            Do not use this version, use a subclass.

        Raises:
            Exception : This version should not be called, use a subclass.
        """
        raise Exception('Not implemented in base class')

    # ...................................
    def get_output_package(self, destination_filename, overwrite=False):
        """Write the output package to the specified file location.

        Args:
            destination_filename : The location to write the (zipped) package
            overwrite : Should the file location be overwritten
        """
        ready_filename(destination_filename, overwrite=overwrite)

        with zipfile.ZipFile(
                destination_filename, 'w', compression=zipfile.ZIP_DEFLATED,
                allowZip64=True) as z_f:
            for base, _, files in os.walk(self.work_dir):
                if base.find('layers') == -1:  # Skip layers directory
                    for file in files:
                        # Don't add zip files
                        if (file.find(LMFormat.ZIP.ext) == -1
                                and os.path.exists(os.path.join(base, file))):
                            z_f.write(
                                os.path.join(base, file),
                                os.path.relpath(
                                    os.path.join(base, file), self.work_dir))

    # ...................................
    @staticmethod
    def get_log_filename():
        """Return the filename for the generated log

        Note:
            * Do not use this version, use a subclass.

        Raises:
            * Exception : This version should not be called, use a subclass.
        """
        raise Exception('Not implemented in base class')

    # ...................................
    def get_metrics(self):
        """Returns the metrics from the computation.
        """
        return self.metrics

    # ...................................
    @staticmethod
    def get_projection_filename():
        """Return the projection raster filename generated

        Note:
            * Do not use this version, use a subclass.

        Raises:
            * Exception : This version should not be called, use a subclass.
        """
        raise Exception('Not implemented in base class')

    # ...................................
    def get_status(self):
        """Return the status of the model
        """
        try:
            return self.metrics.get_metric(LmMetricNames.STATUS)
        except Exception:
            return None

    # ...................................
    @staticmethod
    def _copy_file(source_filename, destination_filename,
                   overwrite=False):
        """Copy source file to destination and prepare location if necessary

        Args:
            source_filename : The source file.
            destination_filename : The destination for the file.
            overwrite : Should the file location be overwritten.
        """
        if os.path.exists(source_filename):
            ready_filename(destination_filename, overwrite=overwrite)
            shutil.copy(source_filename, destination_filename)
        else:
            raise IOError('{} does not exist'.format(source_filename))

    # ...................................
    def copy_ruleset(self, destination_filename, overwrite=False):
        """Copy the ruleset to the specified destination

        Args:
            destination_filename : The destination for the file.
            overwrite : Should the file location be overwritten.
        """
        self._copy_file(
            self.get_ruleset_filename(), destination_filename,
            overwrite=overwrite)

    # ...................................
    def copy_projection(self, destination_filename, overwrite=False):
        """Copy the projection raster to the specified destination

        Args:
            destination_filename : The destination for the file.
            overwrite : Should the file location be overwritten.
        """
        self._copy_file(
            self.get_projection_filename(), destination_filename,
            overwrite=overwrite)

    # ...................................
    def copy_log_file(self, destination_filename, overwrite=False):
        """Copy the log to the specified destination

        Args:
            destination_filename : The destination for the file.
            overwrite : Should the file location be overwritten.
        """
        self._copy_file(
            self.get_log_filename(), destination_filename,
            overwrite=overwrite)
