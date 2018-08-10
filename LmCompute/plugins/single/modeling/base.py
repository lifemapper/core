'''
Created on Aug 10, 2018

@author: cjgrady
@todo: Use logger
@todo: Organize imports
'''
import os
import time
from LmBackend.common.layerTools import processLayersJSON
import shutil
from LmBackend.common.subprocessManager import SubprocessRunner
from LmBackend.common.metrics import LmMetricNames, LmMetrics
from LmCommon.common.lmconstants import JobStatus
from LmCompute.common.lmObj import LmException

# .............................................................................
class ModelSoftwareWrapper(object):
   """
   """
   LOGGER_NAME = 'model_tool' # Subclasses should change this
   RETRY_STATUSES = []
   
   # ...................................
   def __init__(self, work_dir, species_name, logger=None):
      self.work_dir = work_dir
      self.metrics = LmMetrics(None)
      self.species_name = species_name
      
      # If work directory does not exist, create it
      if not os.path.exists(self.work_dir):
         os.makedirs(self.work_dir)
      
      if logger is None:
         self.logger = LmComputeLogger(self.LOGGER_NAME, addConsole=True)
   
   # ...................................
   def _build_command(self, tool, option_list):
      """
      @summary: Builds a command to run the tool
      @param tool: Binary (or decorated binary) for the software tool to run
      @param option_list: A list of options to send to the tool
      """
      cmd = '{tool} {options}'.format(tool=tool, options=' '.join(option_list))
      self.logger.debug('Command: "{}"'.format(cmd))
      return cmd
   
   # ...................................
   def _check_outputs(self):
      pass
   
   # ...................................
   def _process_layers(self, layer_json, layer_dir=None):
      """
      @summary: Read the layer JSON and process the layers accordingly
      @param layer_json: JSON string with layer information
      @param layer_dir: If present, create sym links in the directory to the 
                           layers
      """
      lyrs = processLayersJSON(layer_json, symDir=layer_dir)
      return lyrs
   
   # ...................................
   def _run_tool(self, cmd, num_tries=1):
      """
      @summary: Runs the tool
      @param cmd: The command to run
      @param num_tries: The number of times to try this command
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
            self.metrics.add_metric(LmMetricNames.RUNNING_TIME, 
                                                         end_time - start_time)
            status = JobStatus.COMPUTED
            
            if proc_exit_status > 0:
               status = self._find_error(proc_std_err)
               
               if status in self.RETRY_STATUSES:
                  self.logger.debug('Status is: {}, retries left: {}'.format(
                                                         status, tries_left))
                  if tries_left > 0:
                     cont = True
      except LmException, lme:
         status = lme.status
      
      # Get size of output directory
      self.metrics.add_metric(LmMetricNames.OUTPUT_SIZE, 
                                    self.metrics.get_dir_size(self.work_dir))
      self.metrics.add_metric(LmMetricNames.STATUS, status)
   
   # ...................................
   def clean_up(self):
      """
      @summary: Deletes work directory
      """
      shutil.rmtree(self.work_dir)
   
   # ...................................
   def create_model(self, points, layer_json, parameters_json, 
                    mask_filename=None, crs_wkt=None):
      pass
   
   # ...................................
   def create_projection(self, ruleset_filename, layer_json, 
                         parameters_json=None, mask_filename=None):
      pass
   
   # ...................................
   def get_ruleset_filename(self):
      pass
   
   # ...................................
   def get_output_package(self, dest_filename):
      pass
   
   # ...................................
   def get_projection_filename(self):
      pass

   # ...................................
   def copy_ruleset(self, dest_filename):
      pass
   
   # ...................................
   def copy_projection(self, dest_filename):
      pass
   
   # ...................................
   def copy_log_file(self, dest_filename):
      pass
   
   