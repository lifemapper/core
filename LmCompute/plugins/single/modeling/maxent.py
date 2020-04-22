"""Module containing the wrapper for the Maximum Entropy modeling tool

Note:
    * Commands are for maxent library version 3.4.1e
    * Commands may be backwards compatible

Todo:
    * Look for binaries
    * Algorithm code constant
    * Fix over-zealous error checking
"""
import os
import shutil
from time import sleep

from LmBackend.common.layer_tools import convert_ascii_to_mxe
from LmBackend.common.lmconstants import RegistryKey
from LmBackend.common.metrics import LmMetricNames
from LmCommon.common.lmconstants import (
    JobStatus, LMFormat, ProcessType, ENCODING)
from LmCompute.common.lmconstants import JAVA_CMD, ME_CMD
from LmCompute.plugins.single.modeling.base import ModelSoftwareWrapper
from LmCompute.plugins.single.modeling.maxent_constants import (
    DEFAULT_MAXENT_OPTIONS, DEFAULT_MAXENT_PARAMETERS, MAXENT_MODEL_TOOL,
    MAXENT_PROJECT_TOOL, MAXENT_VERSION)
from LmTest.validate.raster_validator import validate_raster_file
from LmTest.validate.text_validator import validate_text_file

# TODO: Should these be in constants somewhere?
ALGO_PARAMETERS_KEY = 'parameters'
PARAM_DEFAULT_KEY = 'default'
PARAM_NAME_KEY = 'name'
PARAM_OPTIONS_KEY = 'options'
PARAM_PROCESS_KEY = 'process'
PARAM_VALUE_KEY = 'value'


# .............................................................................
class MaxentWrapper(ModelSoftwareWrapper):
    """Class containing methods for using Maxent
    """
    LOGGER_NAME = 'maxent'
    RETRY_STATUSES = [JobStatus.ME_CORRUPTED_LAYER]

    # ...................................
    def _find_error(self, std_err):
        """Checks for information about the error

        Args:
            std_err : Standard error output from the application.
        """
        if not os.path.exists(self.get_projection_filename()):

            status = JobStatus.ME_GENERAL_ERROR

            # Log standard error
            self.logger.debug('Checking standard error')
            if std_err is not None:
                self.logger.debug(std_err)

                if std_err.find('Couldn\'t get file lock.') >= 0:
                    status = JobStatus.ME_FILE_LOCK_ERROR
                elif std_err.find(
                        'Could not reserve enough space for object heap') >= 0:
                    status = JobStatus.ME_HEAP_SPACE_ERROR
                elif std_err.find(
                        'Too small initial heap for new size specified') >= 0:
                    status = JobStatus.ME_HEAP_SPACE_ERROR
                elif std_err.find('because it has 0 training samples') >= 0:
                    status = JobStatus.ME_POINTS_ERROR
                elif std_err.find('Attempt to evaluate layer') >= 0:
                    # 1 at sample with no value
                    status = JobStatus.ME_CORRUPTED_LAYER

            self.logger.debug('Checking output')
            errfname = self.get_log_filename()

            # Look at Maxent error (might be more specific)
            if os.path.exists(errfname):
                with open(errfname, 'r', encoding=ENCODING) as in_f:
                    log_content = in_f.read()
                self.logger.debug('---------------------------------------')
                self.logger.debug(log_content)
                self.logger.debug('---------------------------------------')

                if log_content.find(
                        'have different geographic dimensions') >= 0:
                    status = JobStatus.ME_MISMATCHED_LAYER_DIMENSIONS
                elif log_content.find('NumberFormatException') >= 0:
                    status = JobStatus.ME_CORRUPTED_LAYER
                elif log_content.find(
                        'because it has 0 training samples') >= 0:
                    status = JobStatus.ME_POINTS_ERROR
                elif log_content.find('is missing from') >= 0:
                    # ex: Layer vap6190 is missing from layers/projectionScn
                    status = JobStatus.ME_LAYER_MISSING
                elif log_content.find(
                        'No background points with data in all layers') >= 0:
                    status = JobStatus.ME_POINTS_ERROR
                elif log_content.find(
                        'No features available: select more feature') >= 0:
                    status = JobStatus.ME_NO_FEATURES_CLASSES_AVAILABLE
        else:
            # Computed but process terminated with status 1, seems to be fine
            status = JobStatus.COMPUTED
        return status

    # ...................................
    @staticmethod
    def _process_mask(layer_dir, mask_filename):
        """Process an incoming mask file

        Args:
            layer_dir : The directory to store the mask sym link.
            mask_filename : The original mask that will be converted to MXE
                format and linked in the layer directory.
        """
        work_mask_filename = os.path.join(layer_dir,
                                          'mask{}'.format(LMFormat.MXE.ext))
        if not os.path.exists(work_mask_filename):
            convert_ascii_to_mxe(os.path.split(mask_filename)[0])
            shutil.move('{}{}'.format(
                os.path.splitext(os.path.abspath(mask_filename))[0],
                LMFormat.MXE.ext), work_mask_filename)
        return 'togglelayertype=mask'

    # ...................................
    @staticmethod
    def _process_parameters(parameter_json):
        """Process provided algorithm parameters JSON and return a list

        Args:
            parameter_json : A JSON dictionary of algorithm parameters.
        Todo:
            * Use a common set of constants for keys
        """
        algo_param_options = []
        for param in parameter_json[RegistryKey.PARAMETER]:
            param_name = param[PARAM_NAME_KEY]
            param_value = param[PARAM_VALUE_KEY]
            default_param = DEFAULT_MAXENT_PARAMETERS[param_name]
            if param_value is not None and param_value != 'None':
                val = default_param[PARAM_PROCESS_KEY](param_value)
                # Check for options
                if PARAM_OPTIONS_KEY in default_param:
                    val = default_param[PARAM_OPTIONS_KEY][val]
                if val != default_param[PARAM_DEFAULT_KEY]:
                    algo_param_options.append('{}={}'.format(param_name, val))
        return algo_param_options

    # ...................................
    def create_model(self, points, layer_json, parameters_json,
                     mask_filename=None, crs_wkt=None):
        """Create a Maxent Model

        Args:
            points : A list of (local_id, x, y) point tuples.
            layer_json : Climate layer information in a JSON document.
            mask_filename : If provided, use this layer as a mask for the
                model.
            crs_wkt : Well-Known text describing the map projection of the
                points.

        Note:
            * Overrides ModelSoftwareWrapper.create_model
        """
        self.metrics.add_metric(
            LmMetricNames.PROCESS_TYPE, ProcessType.ATT_MODEL)
        self.metrics.add_metric(LmMetricNames.ALGORITHM_CODE, 'ATT_MAXENT')
        self.metrics.add_metric(LmMetricNames.NUMBER_OF_FEATURES, len(points))
        self.metrics.add_metric(LmMetricNames.SOFTWARE_VERSION, MAXENT_VERSION)

        # Process points
        points_csv = os.path.join(self.work_dir, 'points.csv')
        with open(points_csv, 'w', encoding=ENCODING) as out_csv:
            out_csv.write("Species, X, Y\n")
            for _, x_coord, y_coord in list(points):
                out_csv.write(
                    '{}, {}, {}\n'.format(self.species_name, x_coord, y_coord))

        self.logger.debug('Point CRS WKT: {}'.format(crs_wkt))

        # Process layers
        layer_dir = os.path.join(self.work_dir, 'layers')
        try:
            os.makedirs(layer_dir)
        except Exception:
            pass
        _ = self._process_layers(layer_json, layer_dir)

        options = [
            ME_CMD,
            MAXENT_MODEL_TOOL,
            '-s {points}'.format(points=points_csv),
            '-o {work_dir}'.format(work_dir=self.work_dir),
            '-e {layer_dir}'.format(layer_dir=layer_dir),
        ]

        # Mask
        if mask_filename is not None:
            options.append(self._process_mask(layer_dir, mask_filename))

        options.extend(self._process_parameters(parameters_json))
        options.extend(DEFAULT_MAXENT_OPTIONS)

        self._run_tool(self._build_command(JAVA_CMD, options), num_tries=3)

        # If success, check model output
        if self.metrics.get_metric(
                LmMetricNames.STATUS) < JobStatus.GENERAL_ERROR:
            # Wait up to 10 seconds for the ruleset file to appear
            #    This may be necessary for files on shared disks
            waited = 0
            while not os.path.exists(self.get_ruleset_filename()) and \
                    waited < 10:
                sleep(1)
                waited += 1

            valid_model, model_msg = validate_text_file(
                self.get_ruleset_filename())
            if not valid_model:
                self.metrics.add_metric(
                    LmMetricNames.STATUS, JobStatus.ME_EXEC_MODEL_ERROR)
                self.logger.debug(
                    'Model failed for {}: {}'.format(
                        self.get_ruleset_filename(), model_msg))

        # If success, check projection output
        if self.metrics.get_metric(
                LmMetricNames.STATUS) < JobStatus.GENERAL_ERROR:
            # Wait up to 10 seconds for the projection file to appear
            #    This may be necessary for files on shared disks
            waited = 0
            while not os.path.exists(self.get_projection_filename()) and \
                    waited < 10:
                sleep(1)
                waited += 1

            valid_prj, prj_msg = validate_raster_file(
                self.get_projection_filename())
            if not valid_prj:
                self.metrics.add_metric(
                    LmMetricNames.STATUS, JobStatus.ME_EXEC_PROJECTION_ERROR)
                self.logger.debug('Projection failed: {}'.format(prj_msg))

    # ...................................
    def create_projection(self, ruleset_filename, layer_json,
                          parameters_json=None, mask_filename=None):
        """Create a Maxent Projection

        Args:
            ruleset_filename : The file path to a previously created ruleset.
            layer_json : Climate layer information in a JSON document.
            parameters_json : Algorithm parameters in a JSON document.
            mask_filename : If provided, this is a file path to a mask layer
                to use for the projection.

        Note:
            Overrides ModelSoftwareWrapper.create_projection
        """
        self.metrics.add_metric(
            LmMetricNames.PROCESS_TYPE, ProcessType.ATT_PROJECT)
        self.metrics.add_metric(LmMetricNames.ALGORITHM_CODE, 'ATT_MAXENT')
        self.metrics.add_metric(LmMetricNames.SOFTWARE_VERSION, MAXENT_VERSION)

        # Process layers
        layer_dir = os.path.join(self.work_dir, 'layers')
        try:
            os.makedirs(layer_dir)
        except Exception:
            pass
        _ = self._process_layers(layer_json, layer_dir)

        options = [
            ME_CMD,
            MAXENT_PROJECT_TOOL,
            ruleset_filename,
            layer_dir,
            self.get_projection_filename()
        ]

        # Mask
        if mask_filename is not None:
            options.append(self._process_mask(layer_dir, mask_filename))

        options.extend(self._process_parameters(parameters_json))
        options.extend(DEFAULT_MAXENT_OPTIONS)

        self._run_tool(self._build_command(JAVA_CMD, options), num_tries=3)

        # If success, check projection output
        if self.metrics.get_metric(
                LmMetricNames.STATUS) < JobStatus.GENERAL_ERROR:
            valid_prj, prj_msg = validate_raster_file(
                self.get_projection_filename())
            if not valid_prj:
                self.metrics.add_metric(
                    LmMetricNames.STATUS, JobStatus.ME_EXEC_PROJECTION_ERROR)
                self.logger.debug('Projection failed: {}'.format(prj_msg))

    # ...................................
    def get_log_filename(self):
        """Return the log file name
        """
        return os.path.join(self.work_dir, 'maxent.log')

    # ...................................
    def get_ruleset_filename(self):
        """Override method in base class
        """
        return os.path.join(
            self.work_dir, '{}{}'.format(self.species_name, '.lambdas'))

    # ...................................
    def get_projection_filename(self):
        """Override method in base class
        """
        return os.path.join(
            self.work_dir, '{}{}'.format(
                self.species_name, LMFormat.ASCII.ext))
