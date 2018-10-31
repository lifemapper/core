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

from LmBackend.common.layerTools import processLayersJSON
from LmBackend.common.metrics import LmMetricNames, LmMetrics
from LmBackend.common.subprocessManager import SubprocessRunner

from LmCommon.common.lmconstants import JobStatus, LMFormat
from LmCommon.common.readyfile import readyFilename

from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger

# .............................................................................
class ModelSoftwareWrapper(object):
    """Base class for modeling software wrappers
    """
    LOGGER_NAME = 'model_tool' # Subclasses should change this
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
            self.logger = LmComputeLogger(self.LOGGER_NAME, addConsole=True)
    
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
    def _process_layers(self, layer_json, layer_dir=None):
        """Read the layer JSON and process the layers accordingly

        Args:
            layer_json : JSON string with layer information.
            layer_dir : If present, create sym links in the directory to the 
                layers.
        """
        lyrs = processLayersJSON(layer_json, symDir=layer_dir)
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
                spr = SubprocessRunner(cmd)
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
        except LmException, lme:
            status = lme.status
        
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
    def create_model(self, points, layer_json, parameters_json, 
                     mask_filename=None, crs_wkt=None):
        """Creates a model

        Args:
            points : A list of (local_id, x, y) point tuples.
            layer_json : Climate layer information in a JSON document.
            mask_filename : If provided, use this layer as a mask for the model.
            crs_wkt : Well-Known text describing the map projection of the 
                points.
        
        Note:
            * Do not use this version, use a subclass.
        
        Raises:
            * Exception : This version should not be called, use a subclass.
        """
        raise Exception('Not implemented in base class')
            
    # ...................................
    def create_projection(self, ruleset_filename, layer_json, 
                          parameters_json=None, mask_filename=None):
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
    def get_ruleset_filename(self):
        """Return the ruleset filename generated by the model

        Note:
            * Do not use this version, use a subclass.
        
        Raises:
            * Exception : This version should not be called, use a subclass.
        """
        raise Exception('Not implemented in base class')
    
    # ...................................
    def get_output_package(self, destination_filename, overwrite=False):
        """Write the output package to the specified file location.

        Args:
            destination_filename : The location to write the (zipped) package
            overwrite : Should the file location be overwritten
        """
        readyFilename(destination_filename, overwrite=overwrite)
        
        with zipfile.ZipFile(destination_filename, 'w', 
                             compression=zipfile.ZIP_DEFLATED, 
                             allowZip64=True) as zf:
            for base, _, files in os.walk(self.work_dir):
                if base.find('layers') == -1: # Skip layers directory
                    for f in files:
                        # Don't add zip files
                        if f.find(LMFormat.ZIP.ext) == -1:
                            zf.write(os.path.join(base, f), 
                                        os.path.relpath(os.path.join(base, f), 
                                                             self.work_dir))

    # ...................................
    def get_log_filename(self):
        """Return the filename for the generated log

        Note:
            * Do not use this version, use a subclass.
        
        Raises:
            * Exception : This version should not be called, use a subclass.
        """
        raise Exception('Not implemented in base class')
    
    # ...................................
    def get_projection_filename(self):
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
            return self.metrics[LmMetricNames.STATUS]
        except:
            return None

    # ...................................
    def _copy_file(self, source_filename, destination_filename,
                   overwrite=False):
        """Copy source file to destination and prepare location if necessary

        Args:
            source_filename : The source file.
            destination_filename : The destination for the file.
            overwrite : Should the file location be overwritten. 
        """
        if os.path.exists(source_filename):
            readyFilename(destination_filename, overwrite=overwrite)
            shutil.copy(source_filename, destination_filename)
        else:
            raise IOError, '{} does not exist'.format(source_filename)
        
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
    