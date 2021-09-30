"""Module containing the wrapper for the openModeller modeling tool
"""
import os

from LmBackend.common.lmconstants import RegistryKey
from LmBackend.common.lmobj import LMError
from LmBackend.common.metrics import LmMetricNames
from LmCommon.common.lm_xml import (Element, fromstring, SubElement, tostring)
from LmCommon.common.lmconstants import (
    JobStatus, ProcessType, LMFormat, ENCODING)
from LmCompute.plugins.single.modeling.base import ModelSoftwareWrapper
from LmCompute.plugins.single.modeling.openModeller_constants import (
    DEFAULT_FILE_TYPE, OM_DEFAULT_LOG_LEVEL, OM_MODEL_TOOL, OM_PROJECT_TOOL,
    OM_VERSION)
from LmTest.validate.raster_validator import validate_raster_file
from LmTest.validate.xml_validator import validate_xml_file


# TODO: Should these be in constants somewhere?
# ALGORITHM_CODE_KEY = 'algorithm_code'
PARAM_NAME_KEY = 'name'
PARAM_VALUE_KEY = 'value'


# .............................................................................
class OpenModellerWrapper(ModelSoftwareWrapper):
    """Class containing methods for using openModeller
    """
    LOGGER_NAME = 'om'
    RETRY_STATUSES = []

    # ...................................
    def _find_error(self, std_err):
        """Checks for information about the error

        Args:
            std_err : Standard error output from the application.
        """
        status = JobStatus.OM_GENERAL_ERROR

        # Log standard error
        self.logger.debug('Checking standard error')
        if std_err is not None:
            self.logger.debug(std_err)
            # openModeller logs the error in a file.  Not sure what could be in
            #     standard error, but if something is found, check for it here

        self.logger.debug('Checking output')
        if os.path.exists(self.get_log_filename()):
            with open(self.get_log_filename(), 'w', encoding=ENCODING) as log_f:
                log_content = log_f.read()

            self.logger.debug('openModeller error log')
            self.logger.debug('-----------------------')
            self.logger.debug(log_content)
            self.logger.debug('-----------------------')

            if log_content.find('[Error] No presence points available') >= 0:
                status = JobStatus.OM_MOD_REQ_POINTS_MISSING_ERROR
            elif log_content.find(
                    '[Error] Cannot use zero presence points for') >= 0:
                status = JobStatus.OM_MOD_REQ_POINTS_MISSING_ERROR
            elif log_content.find(
                    '[Error] Algorithm could not be initialized') >= 0:
                status = JobStatus.OM_MOD_REQ_POINTS_OUT_OF_RANGE_ERROR
            elif log_content.find(
                    '[Error] Cannot create model without any presence') >= 0:
                status = JobStatus.OM_MOD_REQ_POINTS_OUT_OF_RANGE_ERROR
            elif log_content.find(
                    '[Error] XML Parser fatal error: not well-formed') >= 0:
                status = JobStatus.OM_MOD_REQ_ERROR
            elif log_content.find('[Error] Unable to open file') >= 0:
                status = JobStatus.OM_MOD_REQ_LAYER_ERROR
            elif log_content.find('[Error] Algorithm') >= 0:
                if log_content.find(
                        'not found',
                        log_content.find('[Error] Algorithm')) >= 0:
                    status = JobStatus.OM_MOD_REQ_ALGO_INVALID_ERROR
            elif log_content.find('[Error] Parameter') >= 0:
                if log_content.find(
                        'not set properly.\n',
                        log_content.find('[Error] Parameter')) >= 0:
                    status = JobStatus.OM_MOD_REQ_ALGOPARAM_MISSING_ERROR

        return status

    # ...................................
    def create_model(self, points, layer_json, parameters_json,
                     mask_filename=None, crs_wkt=None):
        """Create an openModeller model

        Args:
            points : A list of (local_id, x, y) point tuples.
            layer_json : Climate layer information in a JSON document.
            mask_filename : If provided, use this layer as a mask for the model
            crs_wkt : Well-Known text describing the map projection of the
                points.

        Note:
            * Overrides ModelSoftwareWrapper.create_model
        """
        self.metrics.add_metric(
            LmMetricNames.PROCESS_TYPE, ProcessType.OM_MODEL)
        self.metrics.add_metric(
            LmMetricNames.ALGORITHM_CODE,
            parameters_json[RegistryKey.ALGORITHM_CODE])
        self.metrics.add_metric(LmMetricNames.NUMBER_OF_FEATURES, len(points))
        self.metrics.add_metric(LmMetricNames.SOFTWARE_VERSION, OM_VERSION)

        # Process layers
        layer_filenames = self._process_layers(layer_json)

        # Create request
        omr = OmModelRequest(
            points, self.species_name, crs_wkt, layer_filenames,
            parameters_json, mask_filename=mask_filename)
        # Generate the model request XML file
        model_request_filename = os.path.join(
            self.work_dir, 'model_request.xml')
        with open(model_request_filename, 'wt', encoding=ENCODING) as req_f:
            cnt = omr.generate()
            # As of openModeller 1.3, need to remove <?xml ... first line
            if cnt.startswith("<?xml version".encode()):
                tmp = cnt.split('\n'.encode())
                cnt = '\n'.join(tmp[1:])

            req_f.write(cnt.decode())

        model_options = [
            '-r {}'.format(model_request_filename),
            '-m {}'.format(self.get_ruleset_filename()),
            '--log-level {}'.format(OM_DEFAULT_LOG_LEVEL),
            '--log-file {}'.format(self.get_log_filename())
        ]

        self._run_tool(
            self._build_command(OM_MODEL_TOOL, model_options), num_tries=3)

        # If success, check model output
        if self.metrics.get_metric(
                LmMetricNames.STATUS) < JobStatus.GENERAL_ERROR:
            valid_model, model_msg = validate_xml_file(
                self.get_ruleset_filename())
            if not valid_model:
                self.metrics.add_metric(
                    LmMetricNames.STATUS, JobStatus.OM_EXEC_MODEL_ERROR)
                self.logger.debug('Model failed: {}'.format(model_msg))

    # ...................................
    def create_projection(self, ruleset_filename, layer_json,
                          parameters_json=None, mask_filename=None):
        """Create an openModeller projection

        Args:
            ruleset_filename : The file path to a previously created ruleset.
            layer_json : Climate layer information in a JSON document.
            parameters_json : Algorithm parameters in a JSON document.
            mask_filename : If provided, this is a file path to a mask layer
                to use for the projection.

        Note:
            * Overrides ModelSoftwareWrapper.create_projection
        """
        self.metrics.add_metric(
            LmMetricNames.PROCESS_TYPE, ProcessType.ATT_PROJECT)
        self.metrics.add_metric(
            LmMetricNames.ALGORITHM_CODE,
            parameters_json[RegistryKey.ALGORITHM_CODE])
        self.metrics.add_metric(LmMetricNames.SOFTWARE_VERSION, OM_VERSION)

        # Process layers
        layer_filenames = self._process_layers(layer_json)

        # Build request
        # Generate the projection request XML file
        prj_request_filename = os.path.join(self.work_dir, 'proj_request.xml')
        omr = OmProjectionRequest(
            ruleset_filename, layer_filenames, mask_filename=mask_filename)

        with open(prj_request_filename, 'wt', encoding=ENCODING) as req_f:
            cnt = omr.generate()
            # As of openModeller 1.3, need to remove <?xml ... first line
            if cnt.startswith("<?xml version".encode()):
                tmp = cnt.split('\n'.encode())
                cnt = '\n'.join(tmp[1:])

            req_f.write(cnt.decode())

        status_filename = os.path.join(self.work_dir, 'status.out')

        prj_options = [
            '-r {}'.format(prj_request_filename),
            '-m {}'.format(self.get_projection_filename()),
            '--log-level {}'.format(OM_DEFAULT_LOG_LEVEL),
            '--log-file {}'.format(self.get_log_filename()),
            '--stat-file {}'.format(status_filename)
        ]

        self._run_tool(
            self._build_command(OM_PROJECT_TOOL, prj_options), num_tries=3)

        # If success, check projection output
        if self.metrics.get_metric(
                LmMetricNames.STATUS) < JobStatus.GENERAL_ERROR:
            valid_prj, prj_msg = validate_raster_file(
                self.get_projection_filename())
            if not valid_prj:
                self.metrics.add_metric(
                    LmMetricNames.STATUS, JobStatus.OM_EXEC_PROJECTION_ERROR)
                self.logger.debug('Projection failed: {}'.format(prj_msg))

    # ...................................
    def get_log_filename(self):
        """Returns the log file name
        """
        return os.path.join(self.work_dir, 'om.log')

    # ...................................
    def get_projection_filename(self):
        """Gets the projection filename
        """
        return os.path.join(
            self.work_dir, 'projection{}'.format(LMFormat.GTIFF.ext))

    # ...................................
    def get_ruleset_filename(self):
        """Gets the ruleset filename
        """
        return os.path.join(
            self.work_dir, 'ruleset{}'.format(LMFormat.XML.ext))


# .............................................................................
class OmRequest:
    """Base class for openModeller requests
    """

    # .................................
    def __init__(self):
        if self.__class__ == OmRequest:
            raise Exception('OmRequest base class should not be used directly')

    # .................................
    @staticmethod
    def generate():
        """Base method to generate a request
        """
        raise LMError('generate method must be overridden by a subclass')


# .............................................................................
class OmModelRequest(OmRequest):
    """Class for generating openModeller model requests
    """

    # .................................
    def __init__(self, points, points_label, crs_wkt, layer_filenames,
                 algorithm_json, mask_filename=None):
        """openModeller model request constructor

        Args:
            points : A list of point (id, x, y) tuples.
            points_label : A label for these points (taxon name).
            crs_wkt : WKT representing the coordinate system of the points.
            layer_filenames : A list of layer file names.
            algorithm_json : A JSON dictionary of algorithm parameters.
            mask_filename : A mask file name (could be None).

        Todo:
            * Take options and statistic options as inputs
            * Constants
        """
        super().__init__()
        self.options = [
            # Ignore duplicate points (same coordinates)
            # ('OccurrencesFilter', 'SpatiallyUnique'),
            # Ignore duplicate points (same environment values)
            # ('OccurrencesFilter', 'EnvironmentallyUnique')
            ]
        self.stat_options = {
            'ConfusionMatrix': {
                'Threshold': '0.5'
            },
            'RocCurve': {
                'Resolution': '15',
                'BackgroundPoints': '10000',
                'MaxOmission': '1.0'
            }
        }

        self.points = points
        self.points_label = points_label
        self.crs_wkt = crs_wkt
        self.layer_filenames = layer_filenames
        self.algorithm_code = algorithm_json[RegistryKey.ALGORITHM_CODE]
        self.algorithm_parameters = algorithm_json[RegistryKey.PARAMETER]
        self.mask_filename = mask_filename

    # .................................
    def generate(self):
        """Generates a model request string.

        Generates a model request string by building an XML tree and then
        serializing it
        """
        # Parent Element
        request_element = Element('ModelParameters')

        # Sampler Element
        sampler_element = SubElement(request_element, 'Sampler')
        environment_element = SubElement(
            sampler_element, 'Environment', attrib={
                'NumLayers': str(len(self.layer_filenames))})

        for lyr_filename in self.layer_filenames:
            SubElement(
                environment_element, 'Map',
                attrib={'Id': lyr_filename, 'IsCategorical': '0'})
        if self.mask_filename is not None:
            SubElement(
                environment_element, 'Mask', attrib={'Id': self.mask_filename})

        presence_element = SubElement(
            sampler_element, 'Presence', attrib={'Label': self.points_label})

        # SubElement(presence_element, 'CoordinateSystem', value=self.crs_wkt)

        for local_id, x_coord, y_coord in self.points:
            SubElement(
                presence_element, 'Point',
                attrib={'Id': str(local_id), 'X': str(x_coord), 'Y': str(y_coord)})

        # Algorithm Element
        algorithm_element = SubElement(
            request_element, 'Algorithm', attrib={'Id': self.algorithm_code})

        algorithm_parameters_element = SubElement(
            algorithm_element, 'Parameters')
        for param in self.algorithm_parameters:
            SubElement(
                algorithm_parameters_element, 'Parameter',
                attrib={'Id': param[PARAM_NAME_KEY],
                        'Value': str(param[PARAM_VALUE_KEY])})

        # Options Element
        options_element = SubElement(request_element, 'Options')
        for name, value in self.options:
            SubElement(options_element, name, value=str(value))

        # Statistics Element
        stats_element = SubElement(request_element, 'Statistics')
        SubElement(
            stats_element, 'ConfusionMatrix',
            attrib={
                'Threshold': str(self.stat_options['ConfusionMatrix']['Threshold'])
                })
        SubElement(
            stats_element, 'RocCurve',
            attrib={
                'Resolution': str(self.stat_options['RocCurve']['Resolution']),
                'BackgroundPoints': str(self.stat_options[
                    'RocCurve']['BackgroundPoints']),
                'MaxOmission': str(self.stat_options['RocCurve']['MaxOmission'])})

        return tostring(request_element, encoding=ENCODING)


# .............................................................................
class OmProjectionRequest(OmRequest):
    """Class for generating openModeller projection requests."""

    # .................................
    def __init__(self, ruleset_filename, layer_filenames, mask_filename=None):
        """Constructor for OmProjectionRequest class

        Args:
            ruleset_filename : A ruleset file generated by a model.
            layer_filenames : A list of layers to project the ruleset on to.
            mask_filename : An optional mask layer for the projection.
        """
        super().__init__()
        self.layer_filenames = layer_filenames
        self.mask_filename = mask_filename

        # Get the algorithm section out of the ruleset
        with open(ruleset_filename, 'r', encoding=ENCODING) as ruleset_in:
            ruleset = ruleset_in.read()
            mdl_element = fromstring(ruleset)
            # Find the algorithm element, and pull it out
            self.algorithm_element = mdl_element.find('Algorithm')

    # .................................
    def generate(self):
        """Generates a projection request string.

        Generates a projection request string by generating an XML tree and
        then serializing it
        """
        request_element = Element('ProjectionParameters')
        # Append algorithm section
        request_element.append(self.algorithm_element)

        # Environment section
        env_element = SubElement(
            request_element, 'Environment',
            attrib={'NumLayers': str(len(self.layer_filenames))})
        for lyr_fn in self.layer_filenames:
            SubElement(
                env_element, 'Map',
                attrib={'Id': lyr_fn, 'IsCategorical': '0'})

        if self.mask_filename is None:
            self.mask_filename = self.layer_filenames[0]

        SubElement(env_element, 'Mask', attrib={'Id': self.mask_filename})

        # OutputParameters Element
        output_parameters_element = SubElement(
            request_element, 'OutputParameters',
            attrib={'FileType': DEFAULT_FILE_TYPE})
        SubElement(
            output_parameters_element, 'TemplateLayer',
            attrib={'Id': self.mask_filename})

        return tostring(request_element, encoding=ENCODING)
