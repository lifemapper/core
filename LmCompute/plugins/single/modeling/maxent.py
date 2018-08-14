"""
@summary: Module containing the wrapper for the Maximum Entropy modeling tool
@author: CJ Grady
@version: 5.0.0
@status: alpha

@note: Commands are for maxent library version 3.4.1e
@note: Commands may be backwards compatible

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
@todo: Look for binaries
@todo: Algorithm code constant
"""
import os
import shutil

from LmBackend.common.layerTools import convertAsciisToMxes
from LmBackend.common.metrics import LmMetricNames

from LmCommon.common.lmconstants import JobStatus, LMFormat, ProcessType

from LmCompute.plugins.single.modeling.base import ModelSoftwareWrapper
from LmCompute.plugins.single.modeling.maxent_constants import (
                        MAXENT_MODEL_TOOL, MAXENT_PROJECT_TOOL, MAXENT_VERSION, 
                        DEFAULT_MAXENT_OPTIONS, DEFAULT_MAXENT_PARAMETERS)
from LmTest.validate.text_validator import validate_text_file
from LmTest.validate.raster_validator import validate_raster_file

# TODO: Should these be in constants somewhere?
ALGO_PARAMETERS_KEY = 'parameters'
PARAM_DEFAULT_KEY = 'default'
PARAM_NAME_KEY = 'name'
PARAM_OPTIONS_KEY = 'options'
PARAM_PROCESS_KEY = 'process'
PARAM_VALUE_KEY = 'value'

# .............................................................................
class MaxentWrapper(ModelSoftwareWrapper):
   """
   @summary: Class contianing methods for using Maxent
   """
   LOGGER_NAME = 'maxent'
   RETRY_STATUSES = [JobStatus.ME_CORRUPTED_LAYER]
   
   # ...................................
   def _findError(self, std_err):
      """
      @summary: Checks for information about the error
      @param std_err: Standard error output from the application
      """
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
            #1 at sample with no value
            status = JobStatus.ME_CORRUPTED_LAYER

      self.logger.debug('Checking output')
      errfname = self.get_log_filename()

      # Look at Maxent error (might be more specific)
      if os.path.exists(errfname):
         with open(errfname, 'r') as f:
            log_content = f.read()
         self.logger.debug('---------------------------------------')
         self.logger.debug(log_content)
         self.logger.debug('---------------------------------------')

         if log_content.find('have different geographic dimensions') >= 0:
            status = JobStatus.ME_MISMATCHED_LAYER_DIMENSIONS
         elif log_content.find('NumberFormatException') >= 0:
            status = JobStatus.ME_CORRUPTED_LAYER
         elif log_content.find('because it has 0 training samples') >= 0:
            status = JobStatus.ME_POINTS_ERROR
         elif log_content.find('is missing from') >= 0: 
            # ex: Layer vap6190_ann is missing from layers/projectionScn
            status = JobStatus.ME_LAYER_MISSING
         elif log_content.find(
                        'No background points with data in all layers') >= 0:
            status = JobStatus.ME_POINTS_ERROR
         elif log_content.find(
                     'No features available: select more feature types') >= 0:
            status = JobStatus.ME_NO_FEATURES_CLASSES_AVAILABLE

      return status
   
   # ...................................
   def _process_mask(self, layer_dir, mask_filename):
      """
      @summary: Process an incoming mask file
      @param layer_dir: The directory to store the mask sym link
      @param mask_filename: The original mask that will be converted to MXE 
                               format and linked in the layer directory
      """
      work_mask_filename = os.path.join(layer_dir, 
                                        'mask{}'.format(LMFormat.MXE.ext))
      if not os.path.exists(work_mask_filename):
         convertAsciisToMxes(os.path.split(mask_filename)[0])
         shutil.move('{}{}'.format(os.path.splitext(
                                    os.path.abspath(mask_filename))[0], 
                                   LMFormat.MXE.ext, work_mask_filename))
      return 'togglelayertype=mask'
      
   # ...................................
   def _process_parameters(self, parameter_json):
      """
      @summary: Process provided algorithm parameters JSON and return a list
      @param paramsFn: File location of algorithm parameters JSON
      """
      algo_param_options = []
      for param in parameter_json[ALGO_PARAMETERS_KEY]:
         param_name = param[PARAM_NAME_KEY]
         param_value = param[PARAM_VALUE_KEY]
         default_param = DEFAULT_MAXENT_PARAMETERS[param_name]
         if param_value is not None and param_value != 'None':
            v = default_param[PARAM_PROCESS_KEY](param_value)
            # Check for options
            if default_param.has_key(PARAM_OPTIONS_KEY):
               v = default_param[PARAM_OPTIONS_KEY][v]
            if v != default_param[PARAM_DEFAULT_KEY]:
               algo_param_options.append('{}={}'.format(param_name, v))
      return algo_param_options

   # ...................................
   def create_model(self, points, layer_json, parameters_json, 
                                          mask_filename=None, crs_wkt=None):
      """
      @summary: Create a MaxentModel
      @param points: A list of (local_id, x, y) point tuples
      @param layer_json: Climate layer information in a JSON document
      @param mask_filename: If provided, use this layer as a mask for the model
      @param crs_wkt: Well-Known text describing the map projection of the 
                         points
      @note: Overrides ModelSoftwareWrapper.create_model
      """
      self.metrics.add_metric(LmMetricNames.PROCESS_TYPE, 
                                                         ProcessType.ATT_MODEL)
      self.metrics.add_metric(LmMetricNames.ALGORITHM_CODE, 'ATT_MAXENT')
      self.metrics.add_metric(LmMetricNames.NUMBER_OF_FEATURES, len(points))
      self.metrics.add_metric(LmMetricNames.SOFTWARE_VERSION, MAXENT_VERSION)

      # Process points
      points_csv = os.path.join(self.work_dir, 'points.csv')
      with open(points_csv, 'w') as outCsv:
         outCsv.write("Species, X, Y\n")
         for _, x, y in list(points):
            outCsv.write("{0}, {1}, {2}\n".format(self.species_name, x, y))
      
      self.logger.debug('Point CRS WKT: {}'.format(crs_wkt))
   
      # Process layers
      layer_dir = os.path.join(self.work_dir, 'layers')
      try:
         os.makedirs(layer_dir)
      except:
         pass
      _ = self._process_layers(layer_json, layer_dir)
      
      options = [
         '-s {points}'.format(points=points_csv),
         '-o {work_dir}'.format(work_dir=self.work_dir),
         '-e {layer_dir}'.format(layer_dir=layer_dir),
      ]
      
      # Mask
      if mask_filename is not None:
         options.append(self._process_mask(layer_dir, mask_filename))
         
      options.extend(self._process_parameters(parameters_json))
      options.extend(DEFAULT_MAXENT_OPTIONS)
      
      self._run_tool(self._build_command(MAXENT_MODEL_TOOL, options), 
                                                                  num_tries=3)

      # If success, check model output
      if self.metrics[LmMetricNames.STATUS] < JobStatus.GENERAL_ERROR:
         valid_model, model_msg = validate_text_file(
                                                   self.get_ruleset_filename())
         if not valid_model:
            self.metrics.add_metric(LmMetricNames.STATUS, 
                                                JobStatus.ME_EXEC_MODEL_ERROR)
            self.logger.debug('Model failed: {}'.format(model_msg))

      # If success, check projection output
      if self.metrics[LmMetricNames.STATUS] < JobStatus.GENERAL_ERROR:
         valid_prj, prj_msg = validate_raster_file(
                                                self.get_projection_filename())
         if not valid_prj:
            self.metrics.add_metric(LmMetricNames.STATUS, 
                                          JobStatus.ME_EXEC_PROJECTION_ERROR)
            self.logger.debug('Projection failed: {}'.format(prj_msg))


   # ...................................
   def create_projection(self, ruleset_filename, layer_json, 
                                    parameters_json=None, mask_filename=None):
      """
      @summary: Create a MaxentModel
      @param ruleset_filename: The file path to a previously created ruleset
      @param layer_json: Climate layer information in a JSON document
      @param parameters_json: Algorithm parameters in a JSON document
      @param mask_filename: If provided, this is a file path to a mask layer
                               to use for the projection
      @note: Overrides ModelSoftwareWrapper.create_projection
      """
      self.metrics.add_metric(LmMetricNames.PROCESS_TYPE, 
                                                      ProcessType.ATT_PROJECT)
      self.metrics.add_metric(LmMetricNames.ALGORITHM_CODE, 'ATT_MAXENT')
      self.metrics.add_metric(LmMetricNames.SOFTWARE_VERSION, MAXENT_VERSION)
      
      # Process layers
      layer_dir = os.path.join(self.work_dir, 'layers')
      try:
         os.makedirs(layer_dir)
      except:
         pass
      _ = self._process_layers(layer_json, layer_dir)
      
      
      options = [
         ruleset_filename, 
         layer_dir, 
         self.get_ascii_output_filename(), 
      ]
      
      # Mask
      if mask_filename is not None:
         options.append(self._process_mask(layer_dir, mask_filename))
         
      options.extend(self._process_parameters(parameters_json))
      options.extend(DEFAULT_MAXENT_OPTIONS)
      
      self._run_tool(self._build_command(MAXENT_PROJECT_TOOL, options), 
                     num_tries=3)

      # If success, check projection output
      if self.metrics[LmMetricNames.STATUS] < JobStatus.GENERAL_ERROR:
         valid_prj, prj_msg = validate_raster_file(
                                                self.get_projection_filename())
         if not valid_prj:
            self.metrics.add_metric(LmMetricNames.STATUS, 
                                          JobStatus.ME_EXEC_PROJECTION_ERROR)
            self.logger.debug('Projection failed: {}'.format(prj_msg))

   # ...................................
   def get_log_filename(self):
      """
      @summary: Return the log file name
      """
      return os.path.join(self.work_dir, 'maxent.log')
   
