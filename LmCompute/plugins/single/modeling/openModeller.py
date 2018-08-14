"""
@summary: Module containing the wrapper for the openModeller modeling tool
@author: CJ Grady
@version: 5.0.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
import os

from LmBackend.common.metrics import LmMetricNames

from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.lmXml import (Element, fromstring, SubElement, tostring)

from LmCompute.plugins.single.modeling.base import ModelSoftwareWrapper
from LmCompute.plugins.single.modeling.openModeller_constants import (
                     DEFAULT_FILE_TYPE, OM_DEFAULT_LOG_LEVEL, OM_MODEL_TOOL, 
                     OM_PROJECT_TOOL, OM_VERSION)
from LmTest.validate.xml_validator import validate_xml_file
from LmTest.validate.raster_validator import validate_raster_file

# TODO: Should these be in constants somewhere?
ALGORITHM_CODE_KEY = 'algorithmCode'
PARAM_NAME_KEY = 'name'
PARAM_VALUE_KEY = 'value'

# .............................................................................
class OpenModellerWrapper(ModelSoftwareWrapper):
   """
   @summary: Class containing methods for using openModeller
   """
   LOGGER_NAME = 'om'
   RETRY_STATUSES = []
   
   # ...................................
   def _findError(self, std_err):
      """
      @summary: Checks for information about the error
      @param std_err: Standard error output from the application
      """
      status = JobStatus.OM_GENERAL_ERROR
      
      # Log standard error
      self.logger.debug('Checking standard error')
      if std_err is not None:
         self.logger.debug(std_err)
         # openModeller logs the error in a file.  Not sure what could be in 
         #    standard error, but if something is found, check for it here
         
      self.logger.debug('Checking output')
      if os.path.exists(self.get_log_filename()):
         with open(self.get_log_filename()) as logF:
            log_content = logF.read()
         
         self.logger.debug('openModeller error log')
         self.logger.debug('-----------------------')
         self.logger.debug(log_content)
         self.logger.debug('-----------------------')
      
         if log_content.find('[Error] No presence points available') >= 0:
            status = JobStatus.OM_MOD_REQ_POINTS_MISSING_ERROR
         elif log_content.find(
             '[Error] Cannot use zero presence points for sampling') >= 0:
            status = JobStatus.OM_MOD_REQ_POINTS_MISSING_ERROR
         elif log_content.find(
                           '[Error] Algorithm could not be initialized') >= 0:
            status = JobStatus.OM_MOD_REQ_POINTS_OUT_OF_RANGE_ERROR
         elif log_content.find(
                  '[Error] Cannot create model without any presence or absence'
                     ) >= 0:
            status = JobStatus.OM_MOD_REQ_POINTS_OUT_OF_RANGE_ERROR
         elif log_content.find(
                     '[Error] XML Parser fatal error: not well-formed') >= 0:
            status = JobStatus.OM_MOD_REQ_ERROR
         elif log_content.find('[Error] Unable to open file') >= 0:
            status = JobStatus.OM_MOD_REQ_LAYER_ERROR
         elif log_content.find('[Error] Algorithm') >= 0:
            if log_content.find('not found', 
                                log_content.find('[Error] Algorithm')) >= 0:
               status = JobStatus.OM_MOD_REQ_ALGO_INVALID_ERROR
         elif log_content.find('[Error] Parameter') >= 0:
            if log_content.find('not set properly.\n', 
                                 log_content.find('[Error] Parameter')) >= 0:
               status = JobStatus.OM_MOD_REQ_ALGOPARAM_MISSING_ERROR

      return status
   
   # ...................................
   def create_model(self, points, layer_json, parameters_json, 
                                          mask_filename=None, crs_wkt=None):
      """
      @summary: Create an openModeller model
      @param points: A list of (local_id, x, y) point tuples
      @param layer_json: Climate layer information in a JSON document
      @param mask_filename: If provided, use this layer as a mask for the model
      @param crs_wkt: Well-Known text describing the map projection of the 
                         points
      @note: Overrides ModelSoftwareWrapper.create_model
      """
      self.metrics.add_metric(LmMetricNames.PROCESS_TYPE, ProcessType.OM_MODEL)
      self.metrics.add_metric(LmMetricNames.ALGORITHM_CODE, 
                              parameters_json[ALGORITHM_CODE_KEY])
      self.metrics.add_metric(LmMetricNames.NUMBER_OF_FEATURES, len(points))
      self.metrics.add_metric(LmMetricNames.SOFTWARE_VERSION, OM_VERSION)
      
      # Process layers
      layer_filenames = self._process_layers(layer_json)
      
      # Create request
      omr = OmModelRequest(points, self.species_name, crs_wkt, layer_filenames, 
                           parameters_json, maskFn=mask_filename)
      # Generate the model request XML file
      model_request_filename = os.path.join(self.work_dir, 'model_request.xml')
      with open(model_request_filename, 'w') as reqF:
         cnt = omr.generate()
         # As of openModeller 1.3, need to remove <?xml ... first line
         if cnt.startswith("<?xml version"):
            tmp = cnt.split('\n')
            cnt = '\n'.join(tmp[1:])

         reqF.write(cnt)
      
      model_options = [
         '-r {}'.format(model_request_filename),
         '-m {}'.format(self.get_ruleset_filename()),
         '--log-level {}'.format(OM_DEFAULT_LOG_LEVEL),
         '--log-file {}'.format(self.get_log_filename())
      ]
      
      self._run_tool(self._build_command(OM_MODEL_TOOL, model_options), 
                                                                  num_tries=3)
      
      # If success, check model output
      if self.metrics[LmMetricNames.STATUS] < JobStatus.GENERAL_ERROR:
         valid_model, model_msg = validate_xml_file(
                                                   self.get_ruleset_filename())
         if not valid_model:
            self.metrics.add_metric(LmMetricNames.STATUS, 
                                                JobStatus.OM_EXEC_MODEL_ERROR)
            self.logger.debug('Model failed: {}'.format(model_msg))

   # ...................................
   def create_projection(self, ruleset_filename, layer_json, 
                                    parameters_json=None, mask_filename=None):
      """
      @summary: Create an openModeller model
      @param ruleset_filename: The file path to a previously created ruleset
      @param layer_json: Climate layer information in a JSON document
      @param parameters_json: Algorithm parameters in a JSON document
      @param mask_filename: If provided, this is a file path to a mask layer
                               to use for the projection
      @note: Overrides ModelSoftwareWrapper.create_projection
      """
      self.metrics.add_metric(LmMetricNames.PROCESS_TYPE, 
                                                      ProcessType.ATT_PROJECT)
      if parameters_json is not None and \
            parameters_json.has_key(ALGORITHM_CODE_KEY):
         self.metrics.add_metric(LmMetricNames.ALGORITHM_CODE, 
                                 parameters_json[ALGORITHM_CODE_KEY])
      self.metrics.add_metric(LmMetricNames.SOFTWARE_VERSION, OM_VERSION)
      
      # Process layers
      layer_filenames = self._process_layers(layer_json)
      
      
      # Build request
      # Generate the projection request XML file
      prj_request_filename = os.path.join(self.work_dir, 'proj_request.xml')
      omr = OmProjectionRequest(ruleset_filename, layer_filenames, 
                                                   mask_filename=mask_filename)
      
      with open(prj_request_filename, 'w') as reqF:
         cnt = omr.generate()
         # As of openModeller 1.3, need to remove <?xml ... first line
         if cnt.startswith("<?xml version"):
            tmp = cnt.split('\n')
            cnt = '\n'.join(tmp[1:])

         reqF.write(cnt)
      
      status_filename = os.path.join(self.work_dir, 'status.out')
      
      prj_options = [
         '-r {}'.format(prj_request_filename),
         '-m {}'.format(self.get_projection_filename()),
         '--log-level {}'.format(OM_DEFAULT_LOG_LEVEL),
         '--log-file {}'.format(self.get_log_filename()),
         '--stat-file {}'.format(status_filename)
      ]
      
      self._run_tool(self._build_command(OM_PROJECT_TOOL, prj_options), 
                                                                  num_tries=3)
      
      # If success, check projection output
      if self.metrics[LmMetricNames.STATUS] < JobStatus.GENERAL_ERROR:
         valid_prj, prj_msg = validate_raster_file(
                                                self.get_projection_filename())
         if not valid_prj:
            self.metrics.add_metric(LmMetricNames.STATUS, 
                                          JobStatus.OM_EXEC_PROJECTION_ERROR)
            self.logger.debug('Projection failed: {}'.format(prj_msg))

   # ...................................
   def get_log_filename(self):
      """
      @summary: Return the log file name
      """
      return os.path.join(self.work_dir, 'om.log')
   
# .............................................................................
class OmRequest(object):
   """
   @summary: Base class for openModeller requests
   """
   # .................................
   def __init__(self):
      if self.__class__ == OmRequest:
         raise Exception, 'OmRequest base class should not be used directly.'
      
   # .................................
   def generate(self):
      raise Exception, 'generate method must be overridden by a subclass'

# .............................................................................
class OmModelRequest(OmRequest):
   """
   @summary: Class for generating openModeller model requests
   """
   
   # .................................
   def __init__(self, points, points_label, crs_wkt, layer_filenames, 
                                          algorithm_json, mask_filename=None):
      """
      @summary: openModeller model request constructor
      @param points: A list of point (id, x, y) tuples
      @param points_abel: A label for these points (taxon name)
      @param crs_wkt: WKT representing the coordinate system of the points
      @param layer_filenames: A list of layer file names
      @param algorithm_json: A JSON dictionary of algorithm parameters
      @param mask_filename: A mask file name (could be None)
      @todo: Take options and statistic options as inputs
      @todo: Constants
      """
      self.options = [
                  #('OccurrencesFilter', 'SpatiallyUnique'), # Ignore duplicate points (same coordinates)
                  #('OccurrencesFilter', 'EnvironmentallyUnique') # Ignore duplicate points (same environment values)
                ]
      self.stat_options = {
                    'ConfusionMatrix' : {
                                           'Threshold' : '0.5'
                                        },
                    'RocCurve' :        {
                                           'Resolution' : '15',
                                           'BackgroundPoints' : '10000',
                                           'MaxOmission' : '1.0'
                                        }
                 }

      self.points = points
      self.points_abel = points_label
      self.crs_wkt = crs_wkt
      self.layer_filenames = layer_filenames
      self.algorithm_code = algorithm_json[ALGORITHM_CODE_KEY]
      self.algorithm_parameters = algorithm_json['parameters']
      self.mask_filename = mask_filename
      
   # .................................
   def generate(self):
      """
      @summary: Generates a model request string by building an XML tree and 
                   then serializing it
      """
      # Parent Element
      request_element = Element('ModelParameters')
      
      # Sampler Element
      sampler_element = SubElement(request_element, 'Sampler')
      environment_element = SubElement(sampler_element, 'Environment', 
                                                attrib={
                                                   'NumLayers': str(len(
                                                      self.layer_filenames))})
      for lyr_filename in self.layer_filenames:
         SubElement(environment_element, 'Map', 
                           attrib={'Id': lyr_filename, 'IsCategorical': '0'})
      if self.mask_filename is not None:
         SubElement(environment_element, 'Mask', 
                                             attrib={'Id': self.mask_filename})
      
      presence_element = SubElement(sampler_element, 'Presence', 
                                          attrib={'Label': self.points_label})
      SubElement(presence_element, 'CoordinateSystem', value=self.crs_wkt)
      
      for local_id, x, y in self.points:
         SubElement(presence_element, 'Point',
                                       attrib={'Id': local_id, 'X': x, 'Y': y})

      # Algorithm Element
      algorithm_element = SubElement(request_element, 'Algorithm', 
                                          attrib={'Id': self.algorithm_code})
      
      algorithm_parameters_element = SubElement(algorithm_element, 
                                                                  'Parameters')
      for param in self.algorithm_parameters:
         SubElement(algorithm_parameters_element, 'Parameter', 
                                    attrib={'Id': param[PARAM_NAME_KEY], 
                                            'Value': param[PARAM_VALUE_KEY]})
      
      # Options Element
      options_element = SubElement(request_element, 'Options')
      for name, value in self.options:
         SubElement(options_element, name, value=value)

      # Statistics Element      
      stats_element = SubElement(request_element, 'Statistics')
      SubElement(stats_element, 'ConfusionMatrix', attrib={
               'Threshold': self.stat_options['ConfusionMatrix']['Threshold']})
      SubElement(stats_element, 'RocCurve', attrib={
         'Resolution': self.stat_options['RocCurve']['Resolution'], 
         'BackgroundPoints': self.stat_options['RocCurve']['BackgroundPoints'],
         'MaxOmission': self.stat_options['RocCurve']['MaxOmission']})

      return tostring(request_element)

# .............................................................................
class OmProjectionRequest(OmRequest):
   """
   @summary: Class for generating openModeller projection requests
   """
   
   # .................................
   def __init__(self, ruleset_filename, layer_filenames, mask_filename=None):
      """
      @summary: Constructor for OmProjectionRequest class
      @param ruleset_filename: A ruleset file generated by a model
      @param layer_filenames: A list of layers to project the ruleset on to
      @param mask_filename: An optional mask layer for the projection
      """
      self.layer_filenames = layer_filenames
      self.mask_filename = mask_filename
      
      # Get the algorithm section out of the ruleset
      with open(ruleset_filename) as ruleset_in:
         ruleset = ruleset_in.read()
         mdl_element = fromstring(ruleset)
         # Find the algorithm element, and pull it out
         self.algorithm_element = mdl_element.find('Algorithm')
      
   # .................................
   def generate(self):
      """
      @summary: Generates a projection request string by generating an XML tree
                   and then serializing it
      """
      request_element = Element('ProjectionParameters')
      # Append algorithm section
      request_element.append(self.algorithm_element)
      
      # Environment section
      env_element = SubElement(request_element, 'Environment',  
                         attrib={'NumLayers': str(len(self.layer_filenames))})
      for lyrFn in self.layer_filenames:
         SubElement(env_element, 'Map', attrib={
                                          'Id': lyrFn, 'IsCategorical': '0'})
      
      if self.mask_filename is None:
         self.mask_filename = self.layer_filenames[0]

      SubElement(env_element, 'Mask', attrib={'Id': self.mask_filename})
      
      # OutputParameters Element
      output_parameters_element = SubElement(
                                       request_element, 'OutputParameters',  
                                       attrib={'FileType': DEFAULT_FILE_TYPE})
      SubElement(output_parameters_element, 'TemplateLayer', 
                                             attrib={'Id': self.mask_filename})
      
      return tostring(request_element)

